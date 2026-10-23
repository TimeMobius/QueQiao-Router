//! Audit write throughput benchmark for the batching writer.
//!
//!   cargo run --release --bin bench_writer -- --rows 20000 --batch 150

use std::time::{Duration, Instant};

use queqiao_router::db::payload::compress;
use queqiao_router::db::records::Record;
use queqiao_router::db::store::AuditStore;
use queqiao_router::db::writer::{AuditItem, AuditWriter, WriterConfig};

fn parse_args() -> (usize, usize, usize) {
    let mut rows = 20_000;
    let mut batch = 150;
    let mut payload = 10_240;
    let mut args = std::env::args().skip(1);
    while let Some(flag) = args.next() {
        let value: usize = args.next().and_then(|v| v.parse().ok()).unwrap_or(0);
        match flag.as_str() {
            "--rows" => rows = value,
            "--batch" => batch = value,
            "--payload" => payload = value,
            _ => {}
        }
    }
    (rows, batch, payload)
}

fn item(id: i64, payload_bytes: usize) -> AuditItem {
    let request = format!(
        "{{\"model\":\"bench-model\",\"messages\":[{{\"role\":\"user\",\"content\":\"{}\"}}]}}",
        "x".repeat(payload_bytes)
    );
    let answer = "y".repeat(2_048);
    let record = Record {
        time: "2026-10-23 00:00:00.000000".to_string(),
        time_ms: 1_792_700_000_000 + id,
        ip: "127.0.0.1".to_string(),
        method: Some("POST".to_string()),
        endpoint: Some("/v1/chat/completions".to_string()),
        model: "bench-model".to_string(),
        r#type: "chat.completions".to_string(),
        backend: Some("bench".to_string()),
        session_id: None,
        parent_session_id: None,
        request_id: Some(format!("bench-{id}")),
        session_affinity: None,
        user_agent: None,
        client_name: Some("bench".to_string()),
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
        request_bytes: request.len() as i64,
        response_bytes: answer.len() as i64,
        prompt_bytes: payload_bytes as i64,
        request_tail_bytes: payload_bytes as i64,
        answer_bytes: answer.len() as i64,
        message_count: 1,
        system_count: 0,
        tool_count: 0,
        assistant_count: 0,
        tool_result_count: 0,
        image_count: 0,
        prompt: format!("{}{id}", "p".repeat(1024)),
        request_tail: format!("{}{id}", "t".repeat(1024)),
        answer: answer.clone(),
        tool_names: String::new(),
        headers: "{}".to_string(),
        request: request.clone(),
        response: answer,
    };
    AuditItem {
        record,
        request: compress(request.as_bytes()),
        response: compress(b"{\"choices\":[]}"),
        headers: compress(b"{}"),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (rows, batch, payload) = parse_args();
    let path = std::env::temp_dir().join(format!(
        "qq-writer-bench-{}-{}-{}.db",
        std::process::id(),
        batch,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let store = AuditStore::open(&path, false, Duration::from_secs(30)).await?;
    let writer = AuditWriter::start(
        store.clone(),
        WriterConfig {
            batch_size: batch,
            flush_interval: Duration::from_millis(5),
            queue_capacity: rows.max(1),
            enqueue_timeout: Duration::from_secs(120),
            max_attempts: 3,
        },
    )
    .await?;

    let started = Instant::now();
    for id in 0..rows {
        if !writer.enqueue(item(id as i64, payload)).await {
            return Err("enqueue dropped an item".into());
        }
    }
    let enqueued = started.elapsed();
    writer.flush().await?;
    let total = started.elapsed();

    let count = match store
        .query_one("SELECT COUNT(*) FROM records", Vec::new())
        .await?
    {
        Some(row) => row.get_i64(0)?,
        None => 0,
    };
    let stats = writer.snapshot();
    println!(
        "rows={rows} batch={batch} payload={payload}B enqueue={:.1}ms total={:.1}ms rows/s={:.0} count={count}",
        enqueued.as_secs_f64() * 1000.0,
        total.as_secs_f64() * 1000.0,
        rows as f64 / total.as_secs_f64()
    );
    println!(
        "batches={} flushed_rows={} dropped={} failed_batches={} max_batch_ms={}",
        stats.batches, stats.flushed_rows, stats.dropped, stats.failed_batches, stats.max_batch_ms
    );
    writer.shutdown().await;
    drop(store);
    let _ = std::fs::remove_file(&path);
    Ok(())
}
