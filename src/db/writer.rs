//! Bounded audit write queue with a single batching writer task.
//!
//! Data and control use separate channels so `Seal` can stop data consumption
//! while rotation renames the file, without losing items already queued.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

use crate::db::engine::{DbError, Value};
use crate::db::payload::Compressed;
use crate::db::records::{payload_params, record_params, Record};
use crate::db::store::AuditStore;

const RECORD_COLS: &str = "id, Time, TimeMs, IP, Method, Endpoint, Type, Model, Backend, \
    SessionId, ParentSessionId, RequestId, SessionAffinity, UserAgent, ClientName, ClientVersion, ApiKey, \
    Status, Error, RetryCount, FinishReason, \
    LatencyMs, TtftMs, UpstreamMs, StreamMs, \
    CompletionTokens, PromptTokens, TotalTokens, Tool, Multimodal, \
    RequestBytes, ResponseBytes, PromptBytes, RequestTailBytes, AnswerBytes, \
    MessageCount, SystemCount, ToolCount, AssistantCount, ToolResultCount, ImageCount, \
    Prompt, RequestTail, Answer, ToolNames, SearchText";

const RECORD_ROW: &str = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, \
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

const PAYLOAD_COLS: &str = "record_id, codec, dict_id, request, response, headers, \
    request_raw_len, response_raw_len, headers_raw_len";

const PAYLOAD_ROW: &str = "(?, ?, ?, ?, ?, ?, ?, ?, ?)";

pub struct AuditItem {
    pub record: Record,
    pub request: Compressed,
    pub response: Compressed,
    pub headers: Compressed,
}

#[derive(Debug, Clone)]
pub struct WriterConfig {
    pub batch_size: usize,
    pub flush_interval: Duration,
    pub queue_capacity: usize,
    pub enqueue_timeout: Duration,
    pub max_attempts: u32,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            batch_size: env_usize("AUDIT_BATCH_SIZE", 200),
            flush_interval: Duration::from_millis(env_usize("AUDIT_BATCH_TIMEOUT_MS", 20) as u64),
            queue_capacity: env_usize("AUDIT_QUEUE_CAPACITY", 10_000),
            enqueue_timeout: Duration::from_millis(
                env_usize("AUDIT_ENQUEUE_TIMEOUT_MS", 5_000) as u64
            ),
            max_attempts: env_usize("AUDIT_MAX_ATTEMPTS", 3) as u32,
        }
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value: &usize| *value > 0)
        .unwrap_or(default)
}

#[derive(Default)]
pub struct WriterStats {
    pub enqueued: AtomicU64,
    pub flushed_rows: AtomicU64,
    pub batches: AtomicU64,
    pub dropped: AtomicU64,
    pub failed_batches: AtomicU64,
    pub last_batch_ms: AtomicU64,
    pub max_batch_ms: AtomicU64,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct WriterSnapshot {
    pub enqueued: u64,
    pub flushed_rows: u64,
    pub batches: u64,
    pub dropped: u64,
    pub failed_batches: u64,
    pub last_batch_ms: u64,
    pub max_batch_ms: u64,
    pub queue_depth: usize,
    pub queue_capacity: usize,
}

impl WriterStats {
    fn snapshot(&self, queue_depth: usize, queue_capacity: usize) -> WriterSnapshot {
        WriterSnapshot {
            enqueued: self.enqueued.load(Ordering::Relaxed),
            flushed_rows: self.flushed_rows.load(Ordering::Relaxed),
            batches: self.batches.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            failed_batches: self.failed_batches.load(Ordering::Relaxed),
            last_batch_ms: self.last_batch_ms.load(Ordering::Relaxed),
            max_batch_ms: self.max_batch_ms.load(Ordering::Relaxed),
            queue_depth,
            queue_capacity,
        }
    }
}

enum WriterCmd {
    Flush(oneshot::Sender<Result<(), DbError>>),
    Seal(oneshot::Sender<Result<(), DbError>>),
    Resume(Arc<AuditStore>, oneshot::Sender<Result<(), DbError>>),
    Reset(oneshot::Sender<Result<u64, DbError>>),
    Shutdown(oneshot::Sender<Result<(), DbError>>),
}

pub struct AuditWriter {
    data_tx: mpsc::Sender<AuditItem>,
    ctrl_tx: mpsc::Sender<WriterCmd>,
    stats: Arc<WriterStats>,
    handle: Mutex<Option<JoinHandle<()>>>,
    cfg: WriterConfig,
}

impl AuditWriter {
    pub async fn start(store: Arc<AuditStore>, cfg: WriterConfig) -> Result<Arc<Self>, DbError> {
        let next_id = next_id(&store).await?;
        let (data_tx, data_rx) = mpsc::channel(cfg.queue_capacity);
        let (ctrl_tx, ctrl_rx) = mpsc::channel(32);
        let stats = Arc::new(WriterStats::default());
        let task_stats = stats.clone();
        let task_cfg = cfg.clone();
        let handle = tokio::spawn(run(
            Some(store),
            next_id,
            data_rx,
            ctrl_rx,
            task_stats,
            task_cfg,
        ));
        Ok(Arc::new(Self {
            data_tx,
            ctrl_tx,
            stats,
            handle: Mutex::new(Some(handle)),
            cfg,
        }))
    }

    pub fn config(&self) -> &WriterConfig {
        &self.cfg
    }

    pub fn snapshot(&self) -> WriterSnapshot {
        let capacity = self.data_tx.max_capacity();
        let depth = capacity.saturating_sub(self.data_tx.capacity());
        self.stats.snapshot(depth, capacity)
    }

    pub async fn enqueue(&self, item: AuditItem) -> bool {
        self.stats.enqueued.fetch_add(1, Ordering::Relaxed);
        match self.data_tx.try_send(item) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(item)) => {
                match tokio::time::timeout(self.cfg.enqueue_timeout, self.data_tx.send(item)).await
                {
                    Ok(Ok(())) => true,
                    _ => {
                        self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                        false
                    }
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.stats.dropped.fetch_add(1, Ordering::Relaxed);
                false
            }
        }
    }

    pub async fn flush(&self) -> Result<(), DbError> {
        let (tx, rx) = oneshot::channel();
        self.ctrl_tx
            .send(WriterCmd::Flush(tx))
            .await
            .map_err(|_| DbError::Closed)?;
        rx.await.map_err(|_| DbError::Closed)?
    }

    pub async fn seal(&self) -> Result<(), DbError> {
        let (tx, rx) = oneshot::channel();
        self.ctrl_tx
            .send(WriterCmd::Seal(tx))
            .await
            .map_err(|_| DbError::Closed)?;
        rx.await.map_err(|_| DbError::Closed)?
    }

    pub async fn resume(&self, store: Arc<AuditStore>) -> Result<(), DbError> {
        let (tx, rx) = oneshot::channel();
        self.ctrl_tx
            .send(WriterCmd::Resume(store, tx))
            .await
            .map_err(|_| DbError::Closed)?;
        rx.await.map_err(|_| DbError::Closed)?
    }

    pub async fn reset(&self) -> Result<u64, DbError> {
        let (tx, rx) = oneshot::channel();
        self.ctrl_tx
            .send(WriterCmd::Reset(tx))
            .await
            .map_err(|_| DbError::Closed)?;
        rx.await.map_err(|_| DbError::Closed)?
    }

    pub async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        if self.ctrl_tx.send(WriterCmd::Shutdown(tx)).await.is_ok() {
            let _ = rx.await;
        }
        if let Some(handle) = self.handle.lock().await.take() {
            let _ = tokio::time::timeout(Duration::from_secs(10), handle).await;
        }
    }
}

async fn next_id(store: &AuditStore) -> Result<i64, DbError> {
    let row = store
        .query_one(
            "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM records",
            Vec::new(),
        )
        .await?;
    Ok(row.map(|row| row.get_i64(0).unwrap_or(1)).unwrap_or(1))
}

async fn run(
    mut store: Option<Arc<AuditStore>>,
    mut next_id_value: i64,
    mut data_rx: mpsc::Receiver<AuditItem>,
    mut ctrl_rx: mpsc::Receiver<WriterCmd>,
    stats: Arc<WriterStats>,
    cfg: WriterConfig,
) {
    let mut batch: Vec<AuditItem> = Vec::with_capacity(cfg.batch_size);
    let mut batch_start: Option<Instant> = None;

    loop {
        if store.is_none() {
            match ctrl_rx.recv().await {
                Some(WriterCmd::Resume(new_store, ack)) => match next_id(&new_store).await {
                    Ok(id) => {
                        store = Some(new_store);
                        next_id_value = id;
                        let _ = ack.send(Ok(()));
                    }
                    Err(error) => {
                        let _ = ack.send(Err(error));
                    }
                },
                Some(WriterCmd::Shutdown(ack)) => {
                    let _ = ack.send(Ok(()));
                    break;
                }
                Some(WriterCmd::Seal(ack)) => {
                    let _ = ack.send(Ok(()));
                }
                Some(WriterCmd::Flush(ack)) => {
                    let _ = ack.send(Err(DbError::Shape("writer is sealed".to_string())));
                }
                Some(WriterCmd::Reset(ack)) => {
                    let _ = ack.send(Err(DbError::Shape("writer is sealed".to_string())));
                }
                None => break,
            }
            continue;
        }

        let deadline = batch_start
            .map(|start| start + cfg.flush_interval)
            .unwrap_or_else(|| Instant::now() + Duration::from_secs(3600));
        let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));

        tokio::select! {
            item = data_rx.recv() => {
                match item {
                    Some(item) => {
                        if batch.is_empty() { batch_start = Some(Instant::now()); }
                        batch.push(item);
                        if batch.len() >= cfg.batch_size {
                            if let Err(error) =
                                flush_and_reset(store.as_ref(), &mut batch, &mut next_id_value, &stats).await
                            {
                                tracing::error!(%error, "audit batch flush failed");
                            }
                            batch_start = None;
                        }
                    }
                    None => break,
                }
            }
            _ = sleep => {
                if let Err(error) =
                    flush_and_reset(store.as_ref(), &mut batch, &mut next_id_value, &stats).await
                {
                    tracing::error!(%error, "audit batch flush failed");
                }
                batch_start = None;
            }
            cmd = ctrl_rx.recv() => {
                let Some(cmd) = cmd else { break; };
                match cmd {
                    WriterCmd::Flush(ack) => {
                        let result = drain_and_flush(
                            store.as_ref(),
                            &mut data_rx,
                            &mut batch,
                            &mut next_id_value,
                            &stats,
                            &cfg,
                        )
                        .await;
                        batch_start = None;
                        let _ = ack.send(result);
                    }
                    WriterCmd::Seal(ack) => {
                        let mut result = drain_and_flush(
                            store.as_ref(),
                            &mut data_rx,
                            &mut batch,
                            &mut next_id_value,
                            &stats,
                            &cfg,
                        )
                        .await;
                        if result.is_ok() {
                            if let Some(current) = store.as_ref() {
                                result = current.checkpoint_truncate().await;
                            }
                        }
                        batch_start = None;
                        if result.is_ok() {
                            store = None;
                        }
                        let _ = ack.send(result);
                    }
                    WriterCmd::Resume(new_store, ack) => {
                        match next_id(&new_store).await {
                            Ok(id) => {
                                store = Some(new_store);
                                next_id_value = id;
                                let _ = ack.send(Ok(()));
                            }
                            Err(error) => { let _ = ack.send(Err(error)); }
                        }
                    }
                    WriterCmd::Reset(ack) => {
                        while data_rx.try_recv().is_ok() {}
                        batch.clear();
                        batch_start = None;
                        let result = reset_store(store.as_ref()).await;
                        if let Ok(_) = &result { next_id_value = 1; }
                        let _ = ack.send(result);
                    }
                    WriterCmd::Shutdown(ack) => {
                        let result = drain_and_flush(
                            store.as_ref(),
                            &mut data_rx,
                            &mut batch,
                            &mut next_id_value,
                            &stats,
                            &cfg,
                        )
                        .await;
                        let _ = ack.send(result);
                        break;
                    }
                }
            }
        }
    }
}

async fn drain_and_flush(
    store: Option<&Arc<AuditStore>>,
    data_rx: &mut mpsc::Receiver<AuditItem>,
    batch: &mut Vec<AuditItem>,
    next_id_value: &mut i64,
    stats: &WriterStats,
    cfg: &WriterConfig,
) -> Result<(), DbError> {
    loop {
        while batch.len() < cfg.batch_size {
            match data_rx.try_recv() {
                Ok(item) => batch.push(item),
                Err(_) => break,
            }
        }
        if batch.is_empty() {
            return Ok(());
        }
        let full = batch.len() >= cfg.batch_size;
        flush_and_reset(store, batch, next_id_value, stats).await?;
        if !full {
            return Ok(());
        }
    }
}

async fn flush_and_reset(
    store: Option<&Arc<AuditStore>>,
    batch: &mut Vec<AuditItem>,
    next_id_value: &mut i64,
    stats: &WriterStats,
) -> Result<(), DbError> {
    if batch.is_empty() {
        return Ok(());
    }
    let Some(store) = store else {
        return Err(DbError::Shape("no active store".to_string()));
    };
    let started = Instant::now();
    let start_id = *next_id_value;
    let result = flush_with_retry(store, batch, start_id, stats).await;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    stats.last_batch_ms.store(elapsed_ms, Ordering::Relaxed);
    stats.max_batch_ms.fetch_max(elapsed_ms, Ordering::Relaxed);
    match result {
        Ok(()) => {
            *next_id_value = start_id + batch.len() as i64;
            stats
                .flushed_rows
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            stats.batches.fetch_add(1, Ordering::Relaxed);
            batch.clear();
            Ok(())
        }
        Err(error) => {
            stats.failed_batches.fetch_add(1, Ordering::Relaxed);
            stats
                .dropped
                .fetch_add(batch.len() as u64, Ordering::Relaxed);
            batch.clear();
            Err(error)
        }
    }
}

async fn flush_with_retry(
    store: &AuditStore,
    batch: &[AuditItem],
    start_id: i64,
    stats: &WriterStats,
) -> Result<(), DbError> {
    let mut attempt: u32 = 0;
    loop {
        let (record_sql, record_params_all, payload_sql, payload_params_all) =
            build_batch(batch, start_id);
        match execute_batch(
            store,
            &record_sql,
            record_params_all,
            &payload_sql,
            payload_params_all,
        )
        .await
        {
            Ok(()) => return Ok(()),
            Err(error) if error.is_retryable() && attempt < 3 => {
                attempt += 1;
                stats.failed_batches.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(10 * u64::from(attempt))).await;
            }
            Err(error) => return Err(error),
        }
    }
}

fn build_batch(batch: &[AuditItem], start_id: i64) -> (String, Vec<Value>, String, Vec<Value>) {
    let rows = vec![RECORD_ROW; batch.len()].join(",");
    let record_sql = format!("INSERT INTO records ({RECORD_COLS}) VALUES {rows}");
    let payload_rows = vec![PAYLOAD_ROW; batch.len()].join(",");
    let payload_sql = format!("INSERT INTO payloads ({PAYLOAD_COLS}) VALUES {payload_rows}");

    let mut record_values = Vec::with_capacity(batch.len() * 45);
    let mut payload_values = Vec::with_capacity(batch.len() * 9);
    for (index, item) in batch.iter().enumerate() {
        let id = start_id + index as i64;
        record_values.extend(record_params(&item.record, id));
        payload_values.extend(payload_params(
            id,
            &item.request,
            &item.response,
            &item.headers,
        ));
    }
    (record_sql, record_values, payload_sql, payload_values)
}

async fn execute_batch(
    store: &AuditStore,
    record_sql: &str,
    record_values: Vec<Value>,
    payload_sql: &str,
    payload_values: Vec<Value>,
) -> Result<(), DbError> {
    let conn = store.write().await;
    conn.begin_immediate().await?;
    let result = async {
        conn.execute(record_sql, record_values).await?;
        conn.execute(payload_sql, payload_values).await?;
        Ok::<(), DbError>(())
    }
    .await;
    match result {
        Ok(()) => conn.commit().await,
        Err(error) => {
            let _ = conn.rollback().await;
            Err(error)
        }
    }
}

async fn reset_store(store: Option<&Arc<AuditStore>>) -> Result<u64, DbError> {
    let Some(store) = store else {
        return Err(DbError::Shape("no active store".to_string()));
    };
    let before = store
        .query_one("SELECT COUNT(*) FROM records", Vec::new())
        .await?
        .map(|row| row.get_i64(0).unwrap_or(0))
        .unwrap_or(0);
    let conn = store.write().await;
    conn.begin_immediate().await?;
    let result = async {
        conn.execute("DELETE FROM payloads", Vec::new()).await?;
        conn.execute("DELETE FROM records", Vec::new()).await?;
        Ok::<(), DbError>(())
    }
    .await;
    match result {
        Ok(()) => {
            conn.commit().await?;
            Ok(before as u64)
        }
        Err(error) => {
            let _ = conn.rollback().await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::payload::compress;
    use crate::db::records::Record;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("qq-writer-{tag}-{nanos}.db"))
    }

    fn item(id: i64) -> AuditItem {
        let prompt = format!("prompt-{id}");
        let answer = format!("answer-{id}");
        AuditItem {
            record: Record {
                time: "2026-10-23 00:00:00.000000".to_string(),
                time_ms: 1_792_700_000_000 + id,
                ip: "127.0.0.1".to_string(),
                method: Some("POST".to_string()),
                endpoint: Some("/v1/chat/completions".to_string()),
                model: "test-model".to_string(),
                r#type: "chat.completions".to_string(),
                backend: Some("mock".to_string()),
                session_id: None,
                parent_session_id: None,
                request_id: Some(format!("req-{id}")),
                session_affinity: None,
                user_agent: None,
                client_name: None,
                client_version: None,
                api_key: None,
                status: Some(200),
                error: None,
                retry_count: None,
                finish_reason: Some("stop".to_string()),
                latency_ms: Some(1.0),
                ttft_ms: None,
                upstream_ms: None,
                stream_ms: None,
                completion_tokens: 2,
                prompt_tokens: 1,
                total_tokens: 3,
                tool: false,
                multimodal: false,
                request_bytes: 10,
                response_bytes: 10,
                prompt_bytes: 10,
                request_tail_bytes: 10,
                answer_bytes: 10,
                message_count: 1,
                system_count: 0,
                tool_count: 0,
                assistant_count: 0,
                tool_result_count: 0,
                image_count: 0,
                prompt: prompt.clone(),
                request_tail: prompt,
                answer: answer.clone(),
                tool_names: String::new(),
                headers: String::new(),
                request: format!("{{\"id\":{id}}}"),
                response: answer,
            },
            request: compress(b"{\"request\":true}"),
            response: compress(b"{\"response\":true}"),
            headers: compress(b"{}"),
        }
    }

    #[tokio::test]
    async fn batches_all_rows_and_payloads() {
        let path = temp_path("batch");
        let store = AuditStore::open(&path, false, Duration::from_secs(5))
            .await
            .unwrap();
        let writer = AuditWriter::start(
            store.clone(),
            WriterConfig {
                batch_size: 10,
                flush_interval: Duration::from_millis(5),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        for id in 0..25 {
            assert!(writer.enqueue(item(id)).await);
        }
        writer.flush().await.unwrap();
        let count = store
            .query_one("SELECT COUNT(*) FROM records", Vec::new())
            .await
            .unwrap()
            .unwrap()
            .get_i64(0)
            .unwrap();
        assert_eq!(count, 25);
        let payloads = store
            .query_one("SELECT COUNT(*) FROM payloads", Vec::new())
            .await
            .unwrap()
            .unwrap()
            .get_i64(0)
            .unwrap();
        assert_eq!(payloads, 25);
        let ids = store
            .query("SELECT id FROM records ORDER BY id", Vec::new())
            .await
            .unwrap();
        let ids: Vec<i64> = ids.iter().map(|row| row.get_i64(0).unwrap()).collect();
        assert_eq!(ids, (1..=25).collect::<Vec<_>>());
        writer.shutdown().await;
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn seal_blocks_data_and_resume_continues_ids() {
        let path = temp_path("seal");
        let store = AuditStore::open(&path, false, Duration::from_secs(5))
            .await
            .unwrap();
        let writer = AuditWriter::start(store.clone(), WriterConfig::default())
            .await
            .unwrap();
        writer.enqueue(item(1)).await;
        writer.seal().await.unwrap();
        writer.enqueue(item(2)).await;
        writer.resume(store.clone()).await.unwrap();
        writer.flush().await.unwrap();
        let count = store
            .query_one("SELECT COUNT(*) FROM records", Vec::new())
            .await
            .unwrap()
            .unwrap()
            .get_i64(0)
            .unwrap();
        assert_eq!(count, 2);
        writer.shutdown().await;
        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn reset_clears_records_and_restarts_ids() {
        let path = temp_path("reset");
        let store = AuditStore::open(&path, false, Duration::from_secs(5))
            .await
            .unwrap();
        let writer = AuditWriter::start(store.clone(), WriterConfig::default())
            .await
            .unwrap();
        for id in 0..3 {
            writer.enqueue(item(id)).await;
        }
        writer.flush().await.unwrap();
        assert_eq!(writer.reset().await.unwrap(), 3);
        writer.enqueue(item(10)).await;
        writer.flush().await.unwrap();
        let row = store
            .query_one("SELECT id FROM records", Vec::new())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.get_i64(0).unwrap(), 1);
        writer.shutdown().await;
        drop(store);
        let _ = std::fs::remove_file(&path);
    }
}
