//! 派生汇总库：把不可变归档预聚合成按本地自然日的统计，供分析接口快速读取。
//!
//! 该库是**纯派生数据**：可随时删除，缺失或指纹不符时调用方回退到扫描路径。
//! 文件与审计库同目录（Docker 只持久化 `logs/`），默认名 `audit_aggregates.db`。

use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{Datelike, Local, TimeZone};
use once_cell::sync::OnceCell;
use std::sync::Arc;
use tracing::{info, warn};

use crate::db::engine::{Db, DbError, OpenOptions as DbOpenOptions, Value};
use crate::db::store::AuditStore;

/// 汇总库 schema 版本；结构变化时递增，旧库会被重建。
pub(crate) const SCHEMA_VERSION: i64 = 1;
/// 直方图桶宽与上限，必须与 `analysis_api::params` 保持一致。
pub(crate) const HIST_BUCKET_MS: i64 = 50;
pub(crate) const HIST_CAP_MS: i64 = 60_000;
pub(crate) const HIST_BUCKETS: usize = (HIST_CAP_MS / HIST_BUCKET_MS) as usize + 1;

/// 预聚合的分组维度（与 `dimension_expr` 的白名单一致，`hour` 除外）。
pub(crate) const DIMS: [&str; 7] = [
    "model", "apikey", "ip", "type", "backend", "client", "status",
];

const ROLLUP_FILE: &str = "audit_aggregates.db";
const BUILD_BATCH: i64 = 5_000;
const LOCK_TIMEOUT: Duration = Duration::from_secs(30);

const DDL: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS shard_meta (
        shard_id TEXT PRIMARY KEY,
        path TEXT NOT NULL,
        size INTEGER NOT NULL,
        mtime_ms INTEGER NOT NULL,
        row_count INTEGER NOT NULL,
        min_ms INTEGER,
        max_ms INTEGER,
        schema_version INTEGER NOT NULL,
        built_at_ms INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS day_agg (
        shard_id TEXT NOT NULL,
        day_ms INTEGER NOT NULL,
        rows_all INTEGER NOT NULL,
        counted INTEGER NOT NULL,
        success INTEGER NOT NULL,
        errors INTEGER NOT NULL,
        prompt_tokens INTEGER NOT NULL,
        completion_tokens INTEGER NOT NULL,
        total_tokens INTEGER NOT NULL,
        latency_sum REAL NOT NULL,
        latency_count INTEGER NOT NULL,
        latency_max REAL NOT NULL,
        ttft_sum REAL NOT NULL,
        ttft_count INTEGER NOT NULL,
        PRIMARY KEY (shard_id, day_ms)
    )",
    "CREATE TABLE IF NOT EXISTS day_dim (
        shard_id TEXT NOT NULL,
        day_ms INTEGER NOT NULL,
        dim TEXT NOT NULL,
        value TEXT NOT NULL,
        rows_all INTEGER NOT NULL,
        counted INTEGER NOT NULL,
        success INTEGER NOT NULL,
        errors INTEGER NOT NULL,
        prompt_tokens INTEGER NOT NULL,
        completion_tokens INTEGER NOT NULL,
        total_tokens INTEGER NOT NULL,
        latency_sum REAL NOT NULL,
        latency_count INTEGER NOT NULL,
        latency_max REAL NOT NULL,
        ttft_sum REAL NOT NULL,
        ttft_count INTEGER NOT NULL,
        PRIMARY KEY (shard_id, day_ms, dim, value)
    )",
    "CREATE TABLE IF NOT EXISTS day_hist (
        shard_id TEXT NOT NULL,
        day_ms INTEGER NOT NULL,
        kind TEXT NOT NULL,
        buckets BLOB NOT NULL,
        PRIMARY KEY (shard_id, day_ms, kind)
    )",
    "CREATE TABLE IF NOT EXISTS day_distinct (
        shard_id TEXT NOT NULL,
        day_ms INTEGER NOT NULL,
        dim TEXT NOT NULL,
        value TEXT NOT NULL,
        PRIMARY KEY (shard_id, day_ms, dim, value)
    )",
    "CREATE INDEX IF NOT EXISTS idx_day_dim_lookup ON day_dim(shard_id, dim, day_ms)",
    "CREATE INDEX IF NOT EXISTS idx_day_distinct_lookup ON day_distinct(shard_id, dim, day_ms)",
    "CREATE INDEX IF NOT EXISTS idx_day_hist_lookup ON day_hist(shard_id, day_ms)",
    "CREATE TABLE IF NOT EXISTS active_state (
        shard_id TEXT PRIMARY KEY,
        last_id INTEGER NOT NULL,
        max_time_ms INTEGER NOT NULL DEFAULT 0,
        dev INTEGER NOT NULL DEFAULT 0,
        ino INTEGER NOT NULL DEFAULT 0,
        updated_at_ms INTEGER NOT NULL
    )",
];

type RollupStore = (PathBuf, Arc<Db>);
static STORE: OnceCell<RollupStore> = OnceCell::new();

/// 可加合的计数结构，语义与 `analysis_api::sql` 中的聚合完全一致。
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct Acc {
    pub(crate) rows_all: i64,
    pub(crate) counted: i64,
    pub(crate) success: i64,
    pub(crate) errors: i64,
    pub(crate) prompt_tokens: i64,
    pub(crate) completion_tokens: i64,
    pub(crate) total_tokens: i64,
    pub(crate) latency_sum: f64,
    pub(crate) latency_count: i64,
    pub(crate) latency_max: f64,
    pub(crate) ttft_sum: f64,
    pub(crate) ttft_count: i64,
}

impl Acc {
    pub(crate) fn add(&mut self, other: &Acc) {
        self.rows_all += other.rows_all;
        self.counted += other.counted;
        self.success += other.success;
        self.errors += other.errors;
        self.prompt_tokens += other.prompt_tokens;
        self.completion_tokens += other.completion_tokens;
        self.total_tokens += other.total_tokens;
        self.latency_sum += other.latency_sum;
        self.latency_count += other.latency_count;
        self.latency_max = self.latency_max.max(other.latency_max);
        self.ttft_sum += other.ttft_sum;
        self.ttft_count += other.ttft_count;
    }

    fn observe(
        &mut self,
        status: Option<i64>,
        tokens: (i64, i64, i64),
        latency: Option<f64>,
        ttft: Option<f64>,
    ) {
        self.rows_all += 1;
        if let Some(status) = status.filter(|value| *value >= 200) {
            self.counted += 1;
            if status < 400 {
                self.success += 1;
            }
        }
        if status.is_some_and(|value| value >= 400) {
            self.errors += 1;
        }
        self.prompt_tokens += tokens.0;
        self.completion_tokens += tokens.1;
        self.total_tokens += tokens.2;
        if let Some(value) = latency {
            self.latency_sum += value;
            self.latency_count += 1;
            self.latency_max = self.latency_max.max(value);
        }
        if let Some(value) = ttft {
            self.ttft_sum += value;
            self.ttft_count += 1;
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct DayAgg {
    pub(crate) day_ms: i64,
    pub(crate) acc: Acc,
}

/// 一次范围读取的结果。
#[derive(Debug, Default)]
pub(crate) struct Slice {
    pub(crate) days: Vec<DayAgg>,
    /// 全范围按维度值累加后的结果（已合并同日多行）。
    pub(crate) dims: HashMap<&'static str, HashMap<String, Acc>>,
    pub(crate) distinct: HashMap<&'static str, Vec<String>>,
    pub(crate) latency_hist: HashMap<i64, i64>,
    pub(crate) ttft_hist: HashMap<i64, i64>,
}

/// 汇总库路径：与审计库同目录。
pub(crate) fn rollup_path(audit_path: &Path) -> PathBuf {
    let directory = audit_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    directory.join(ROLLUP_FILE)
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(crate) fn local_day_ms(timestamp_ms: i64) -> Option<i64> {
    let datetime = Local.timestamp_millis_opt(timestamp_ms).single()?;
    Local
        .with_ymd_and_hms(datetime.year(), datetime.month(), datetime.day(), 0, 0, 0)
        .single()
        .map(|value| value.timestamp_millis())
}

/// 本地日边界（含 DST 导致的 23/25 小时日）。
pub(crate) fn next_local_day(day_ms: i64) -> Option<i64> {
    let datetime = Local.timestamp_millis_opt(day_ms).single()?;
    datetime
        .checked_add_signed(chrono::Duration::days(1))
        .and_then(|next| {
            Local
                .with_ymd_and_hms(next.year(), next.month(), next.day(), 0, 0, 0)
                .single()
        })
        .map(|value| value.timestamp_millis())
}

pub(crate) fn local_day_label(day_ms: i64, month: bool) -> Option<String> {
    let datetime = Local.timestamp_millis_opt(day_ms).single()?;
    Some(if month {
        format!("{:04}-{:02}", datetime.year(), datetime.month())
    } else {
        format!(
            "{:04}-{:02}-{:02}",
            datetime.year(),
            datetime.month(),
            datetime.day()
        )
    })
}

async fn open_store(audit_path: &Path) -> Result<Arc<Db>, DbError> {
    let path = rollup_path(audit_path);
    if let Some((cached_path, db)) = STORE.get() {
        if cached_path == &path {
            return Ok(db.clone());
        }
    }
    let db = Db::open(&path, DbOpenOptions::default()).await?;
    let conn = db.connect()?;
    for ddl in DDL {
        conn.execute_batch(ddl).await?;
    }
    let _ = conn
        .execute_batch("ALTER TABLE active_state ADD COLUMN max_time_ms INTEGER NOT NULL DEFAULT 0")
        .await;
    conn.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))
        .await?;
    // 首次打开时合并上次进程遗留的 WAL，避免主文件长期停留在 4KB。
    if let Err(error) = conn
        .query("PRAGMA wal_checkpoint(TRUNCATE)", Vec::new())
        .await
    {
        tracing::debug!(%error, "rollup open checkpoint skipped");
    }
    let db = Arc::new(db);
    let _ = STORE.set((path, db.clone()));
    Ok(db)
}

fn file_stamp(path: &Path) -> Option<(i64, i64)> {
    let metadata = fs::metadata(path).ok()?;
    let size = i64::try_from(metadata.len()).ok()?;
    let mtime = metadata.modified().ok()?;
    let mtime_ms = i64::try_from(mtime.duration_since(UNIX_EPOCH).ok()?.as_millis()).ok()?;
    Some((size, mtime_ms))
}

/// 校验汇总是否与当前归档文件匹配。
async fn valid(
    db: &Db,
    shard_id: &str,
    path: &Path,
    min_ms: Option<i64>,
    max_ms: Option<i64>,
) -> bool {
    let Some((size, mtime_ms)) = file_stamp(path) else {
        return false;
    };
    let Ok(conn) = db.connect() else {
        return false;
    };
    let row = conn
        .query_one(
            "SELECT size, mtime_ms, min_ms, max_ms, schema_version FROM shard_meta WHERE shard_id = ?",
            vec![Value::Text(shard_id.to_string())],
        )
        .await
        .ok()
        .flatten();
    let Some(row) = row else {
        return false;
    };
    row.get_i64(4).ok() == Some(SCHEMA_VERSION)
        && row.get_i64(0).ok() == Some(size)
        && row.get_i64(1).ok() == Some(mtime_ms)
        && row.get_opt_i64(2).ok().flatten() == min_ms
        && row.get_opt_i64(3).ok().flatten() == max_ms
}

/// 读取指定归档在整日区间内的汇总；不可用时返回 `None`。
pub(crate) async fn read_slice(
    audit_path: &Path,
    shard_id: &str,
    archive_path: &Path,
    min_ms: Option<i64>,
    max_ms: Option<i64>,
    first_day: i64,
    end_day_exclusive: i64,
) -> Option<Slice> {
    if end_day_exclusive <= first_day {
        return Some(Slice::default());
    }
    let db = open_store(audit_path).await.ok()?;
    if !valid(&db, shard_id, archive_path, min_ms, max_ms).await {
        return None;
    }
    read_range(&db, shard_id, first_day, end_day_exclusive).await
}

/// 读取 active 分片在整日区间内的汇总；该分片的汇总尚未构建时返回 `None`。
pub(crate) async fn read_active_slice(
    audit_path: &Path,
    shard_id: &str,
    first_day: i64,
    end_day_exclusive: i64,
) -> Option<Slice> {
    if end_day_exclusive <= first_day {
        return Some(Slice::default());
    }
    let db = open_store(audit_path).await.ok()?;
    active_state(&db, shard_id).await.ok()??;
    read_range(&db, shard_id, first_day, end_day_exclusive).await
}

async fn read_range(
    db: &Db,
    shard_id: &str,
    first_day: i64,
    end_day_exclusive: i64,
) -> Option<Slice> {
    let conn = db.connect().ok()?;
    let mut slice = Slice::default();

    let rows = conn
        .query(
            "SELECT day_ms, rows_all, counted, success, errors, prompt_tokens, completion_tokens, \
             total_tokens, latency_sum, latency_count, latency_max, ttft_sum, ttft_count \
             FROM day_agg WHERE shard_id = ? AND day_ms >= ? AND day_ms < ?",
            vec![
                Value::Text(shard_id.to_string()),
                Value::Integer(first_day),
                Value::Integer(end_day_exclusive),
            ],
        )
        .await
        .ok()?;
    for row in &rows {
        slice.days.push(DayAgg {
            day_ms: row.get_i64(0).ok()?,
            acc: acc_from_row(row, 1)?,
        });
    }

    for dim in DIMS {
        let rows = conn
            .query(
                "SELECT value, SUM(rows_all), SUM(counted), SUM(success), SUM(errors), \
                 SUM(prompt_tokens), SUM(completion_tokens), SUM(total_tokens), \
                 SUM(latency_sum), SUM(latency_count), MAX(latency_max), \
                 SUM(ttft_sum), SUM(ttft_count) \
                 FROM day_dim WHERE shard_id = ? AND dim = ? AND day_ms >= ? AND day_ms < ? \
                 GROUP BY value",
                vec![
                    Value::Text(shard_id.to_string()),
                    Value::Text(dim.to_string()),
                    Value::Integer(first_day),
                    Value::Integer(end_day_exclusive),
                ],
            )
            .await
            .ok()?;
        let mut values = HashMap::new();
        for row in &rows {
            let value = row.get_opt_string(0).ok()??;
            values.insert(value, acc_from_row(row, 1)?);
        }
        slice.dims.insert(dim, values);
    }

    for dim in ["model", "ip"] {
        let rows = conn
            .query(
                "SELECT DISTINCT value FROM day_distinct \
                 WHERE shard_id = ? AND dim = ? AND day_ms >= ? AND day_ms < ?",
                vec![
                    Value::Text(shard_id.to_string()),
                    Value::Text(dim.to_string()),
                    Value::Integer(first_day),
                    Value::Integer(end_day_exclusive),
                ],
            )
            .await
            .ok()?;
        let mut values = Vec::with_capacity(rows.len());
        for row in &rows {
            values.push(row.get_opt_string(0).ok()??);
        }
        slice.distinct.insert(dim, values);
    }

    let rows = conn
        .query(
            "SELECT kind, buckets FROM day_hist WHERE shard_id = ? AND day_ms >= ? AND day_ms < ?",
            vec![
                Value::Text(shard_id.to_string()),
                Value::Integer(first_day),
                Value::Integer(end_day_exclusive),
            ],
        )
        .await
        .ok()?;
    for row in &rows {
        let Some(kind) = row.get_opt_str(0).ok().flatten() else {
            continue;
        };
        let blob = row.get_opt_blob(1).ok().flatten()?.to_vec();
        let target = if kind == "latency" {
            &mut slice.latency_hist
        } else {
            &mut slice.ttft_hist
        };
        merge_blob(target, &blob)?;
    }
    Some(slice)
}

struct ActiveState {
    last_id: i64,
    max_time_ms: i64,
    identity: (i64, i64),
}

async fn active_state(db: &Db, shard_id: &str) -> Result<Option<ActiveState>, DbError> {
    let conn = db.connect()?;
    let row = conn
        .query_one(
            "SELECT last_id, max_time_ms, dev, ino FROM active_state WHERE shard_id = ?",
            vec![Value::Text(shard_id.to_string())],
        )
        .await?;
    match row {
        Some(row) => Ok(Some(ActiveState {
            last_id: row.get_i64(0)?,
            max_time_ms: row.get_i64(1)?,
            identity: (row.get_i64(2)?, row.get_i64(3)?),
        })),
        None => Ok(None),
    }
}

/// 完整自然日区间 `[first_full, full_end)`；无完整自然日时退化为 `(from, from)`。
/// 是否启用派生汇总（`ANALYSIS_ROLLUP=off` 可关闭）。
pub(crate) fn enabled() -> bool {
    !std::env::var("ANALYSIS_ROLLUP")
        .map(|value| value.eq_ignore_ascii_case("off"))
        .unwrap_or(false)
}

pub(crate) fn full_day_bounds(from: i64, to: i64) -> Option<(i64, i64)> {
    let from_day = local_day_ms(from)?;
    let first_full = if from == from_day {
        from_day
    } else {
        next_local_day(from_day)?
    };
    let to_day = local_day_ms(to)?;
    let to_day_end = next_local_day(to_day)?;
    let full_end = if to + 1 >= to_day_end {
        to_day_end
    } else {
        to_day
    };
    Some((first_full, full_end))
}

/// 完整日之外的边界窗口，必须逐段回扫原始库。
pub(crate) fn correction_windows(
    from: i64,
    to: i64,
    first_full: i64,
    full_end: i64,
) -> Vec<(i64, i64)> {
    let mut windows = Vec::new();
    if from < first_full {
        windows.push((from, first_full - 1));
    }
    if full_end <= to {
        windows.push((full_end, to));
    }
    windows
}

fn file_identity(path: &Path) -> (i64, i64) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Ok(metadata) = fs::metadata(path) {
            return (metadata.dev() as i64, metadata.ino() as i64);
        }
    }
    (0, 0)
}

async fn shard_rollup_rows(db: &Db, shard_id: &str) -> Result<i64, DbError> {
    let conn = db.connect()?;
    let row = conn
        .query_one(
            "SELECT COUNT(*) FROM day_agg WHERE shard_id = ?",
            vec![Value::Text(shard_id.to_string())],
        )
        .await?;
    Ok(row
        .and_then(|row| row.get_opt_i64(0).ok().flatten())
        .unwrap_or(0))
}

async fn max_record_id(store: &AuditStore) -> Result<i64, DbError> {
    let rows = store
        .query("SELECT MAX(id) AS max_id FROM records", Vec::new())
        .await?;
    Ok(rows
        .first()
        .and_then(|row| row.get_opt_i64(0).ok().flatten())
        .unwrap_or(0))
}

/// active 分片是否已聚合到 `needed_ms`（通常为最后一个完整自然日的末尾）。
pub(crate) async fn active_caught_up(
    audit_path: &Path,
    shard_id: &str,
    store: &AuditStore,
    needed_ms: i64,
) -> bool {
    let Ok(db) = open_store(audit_path).await else {
        return false;
    };
    let Ok(Some(state)) = active_state(&db, shard_id).await else {
        return false;
    };
    if state.max_time_ms >= needed_ms {
        return true;
    }
    let pending = store
        .query(
            "SELECT 1 FROM records WHERE id > ? AND TimeMs IS NOT NULL AND TimeMs <= ? LIMIT 1",
            vec![Value::Integer(state.last_id), Value::Integer(needed_ms)],
        )
        .await;
    matches!(pending, Ok(rows) if rows.is_empty())
}

/// 后台重建去抖：`min_interval_ms` 内只放行一次。
pub(crate) fn claim_build_spawn(min_interval_ms: i64) -> bool {
    static LAST_SPAWN_MS: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);
    let now = now_ms();
    let last = LAST_SPAWN_MS.load(std::sync::atomic::Ordering::Relaxed);
    if now.saturating_sub(last) < min_interval_ms {
        return false;
    }
    LAST_SPAWN_MS.store(now, std::sync::atomic::Ordering::Relaxed);
    true
}

async fn clear_shard(db: &Db, shard_id: &str) -> Result<(), DbError> {
    let conn = db.connect()?;
    for table in [
        "day_agg",
        "day_dim",
        "day_hist",
        "day_distinct",
        "shard_meta",
    ] {
        conn.execute(
            &format!("DELETE FROM {table} WHERE shard_id = ?"),
            vec![Value::Text(shard_id.to_string())],
        )
        .await?;
    }
    Ok(())
}

async fn acquire_build_lock(audit_path: &Path) -> Result<std::fs::File, DbError> {
    let lock_path = PathBuf::from(format!("{}.lock", rollup_path(audit_path).display()));
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|error| DbError::Shape(format!("cannot open rollup lock: {error}")))?;
    let deadline = tokio::time::Instant::now() + LOCK_TIMEOUT;
    loop {
        match lock.try_lock() {
            Ok(()) => return Ok(lock),
            Err(std::fs::TryLockError::WouldBlock) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(DbError::Driver(turso::Error::Busy(
                        "rollup lock timed out".to_string(),
                    )));
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(std::fs::TryLockError::Error(error)) => {
                return Err(DbError::Shape(format!("cannot lock rollup: {error}")));
            }
        }
    }
}

#[derive(Default)]
struct Delta {
    max_time_ms: i64,
    days: HashMap<i64, Acc>,
    dims: HashMap<(&'static str, i64, String), Acc>,
    distinct: HashSet<(&'static str, i64, String)>,
    hist: HashMap<(i64, &'static str), Vec<u32>>,
    max_id: i64,
    scanned: i64,
}

async fn scan_after(store: &AuditStore, last_id: i64) -> Result<Delta, DbError> {
    let mut delta = Delta {
        max_id: last_id,
        max_time_ms: 0,
        ..Delta::default()
    };
    let mut cursor = last_id;
    loop {
        let rows = store
            .query(
                "SELECT id, TimeMs, Model, ApiKey, IP, Type, Backend, ClientName, Status, \
                 PromptTokens, CompletionTokens, TotalTokens, LatencyMs, TtftMs \
                 FROM records WHERE id > ? ORDER BY id LIMIT ?",
                vec![Value::Integer(cursor), Value::Integer(BUILD_BATCH)],
            )
            .await?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            let id = row.get_i64(0)?;
            cursor = id;
            delta.max_id = delta.max_id.max(id);
            delta.scanned += 1;
            let Some(time_ms) = row.get_opt_i64(1)? else {
                continue;
            };
            delta.max_time_ms = delta.max_time_ms.max(time_ms);
            let Some(day_ms) = local_day_ms(time_ms) else {
                continue;
            };
            let status = row.get_opt_i64(8)?;
            let tokens = (
                row.get_opt_i64(9)?.unwrap_or(0),
                row.get_opt_i64(10)?.unwrap_or(0),
                row.get_opt_i64(11)?.unwrap_or(0),
            );
            let latency = row.get_opt_f64(12)?;
            let ttft = row.get_opt_f64(13)?;
            delta
                .days
                .entry(day_ms)
                .or_default()
                .observe(status, tokens, latency, ttft);

            for (name, column) in [
                ("model", 2),
                ("apikey", 3),
                ("ip", 4),
                ("type", 5),
                ("backend", 6),
                ("client", 7),
            ] {
                let raw = row.get_opt_string(column)?;
                let value = normalize_dim(raw.as_deref());
                delta
                    .dims
                    .entry((name, day_ms, value))
                    .or_default()
                    .observe(status, tokens, latency, ttft);
                if matches!(name, "model" | "ip") {
                    if let Some(raw) = raw.as_deref() {
                        delta.distinct.insert((name, day_ms, raw.to_string()));
                    }
                }
            }
            let status_value = status
                .map(|value| value.to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            delta
                .dims
                .entry(("status", day_ms, status_value))
                .or_default()
                .observe(status, tokens, latency, ttft);

            if let Some(bucket) = histogram_bucket(latency) {
                let entry = delta
                    .hist
                    .entry((day_ms, "latency"))
                    .or_insert_with(|| vec![0; HIST_BUCKETS]);
                entry[bucket as usize] += 1;
            }
            if let Some(bucket) = histogram_bucket(ttft) {
                let entry = delta
                    .hist
                    .entry((day_ms, "ttft"))
                    .or_insert_with(|| vec![0; HIST_BUCKETS]);
                entry[bucket as usize] += 1;
            }
        }
        if (rows.len() as i64) < BUILD_BATCH {
            break;
        }
    }
    Ok(delta)
}

/// 把 `id` 水位之后的新增记录累加进 active 分片的按日汇总。
pub(crate) async fn ensure_active(
    audit_path: &Path,
    shard_id: &str,
    store: &AuditStore,
) -> Result<(), DbError> {
    let db = open_store(audit_path).await?;
    let _lock = acquire_build_lock(audit_path).await?;
    let max_id = max_record_id(store).await?;
    let identity = file_identity(audit_path);
    let state = active_state(&db, shard_id).await?;
    let mut last_id = state.as_ref().map(|state| state.last_id).unwrap_or(0);
    let mut max_time_ms = state.as_ref().map(|state| state.max_time_ms).unwrap_or(0);
    let rotated = state
        .as_ref()
        .is_some_and(|state| state.identity != identity);
    let stale_without_state = state.is_none() && shard_rollup_rows(&db, shard_id).await? > 0;
    if rotated || stale_without_state || max_id < last_id {
        clear_shard(&db, shard_id).await?;
        last_id = 0;
        max_time_ms = 0;
    }
    if max_id == last_id {
        return Ok(());
    }
    let delta = scan_after(store, last_id).await?;
    max_time_ms = max_time_ms.max(delta.max_time_ms);
    apply_deltas(&db, shard_id, &delta, max_id, max_time_ms, identity).await
}

async fn apply_deltas(
    db: &Db,
    shard_id: &str,
    delta: &Delta,
    max_id: i64,
    max_time_ms: i64,
    identity: (i64, i64),
) -> Result<(), DbError> {
    let mut days: Vec<i64> = delta.days.keys().copied().collect();
    days.extend(delta.dims.keys().map(|(_, day, _)| *day));
    days.extend(delta.hist.keys().map(|(day, _)| *day));
    days.extend(delta.distinct.iter().map(|(_, day, _)| *day));
    days.sort_unstable();
    days.dedup();

    let conn = db.connect()?;
    conn.execute_batch("BEGIN IMMEDIATE").await?;
    let write = async {
        for day in days {
            let existing = conn
                .query_one(
                    "SELECT rows_all, counted, success, errors, prompt_tokens, completion_tokens, \
                     total_tokens, latency_sum, latency_count, latency_max, ttft_sum, ttft_count \
                     FROM day_agg WHERE shard_id = ? AND day_ms = ?",
                    vec![Value::Text(shard_id.to_string()), Value::Integer(day)],
                )
                .await?;
            let mut acc = existing
                .as_ref()
                .and_then(|row| acc_from_row(row, 0))
                .unwrap_or_default();
            if let Some(add) = delta.days.get(&day) {
                acc.add(add);
            }
            conn.execute(
                "DELETE FROM day_agg WHERE shard_id = ? AND day_ms = ?",
                vec![Value::Text(shard_id.to_string()), Value::Integer(day)],
            )
            .await?;
            if acc.rows_all > 0 {
                let mut params = acc_params(shard_id, day, &acc);
                conn.execute(
                    "INSERT INTO day_agg (shard_id, day_ms, rows_all, counted, success, errors, \
                     prompt_tokens, completion_tokens, total_tokens, latency_sum, latency_count, \
                     latency_max, ttft_sum, ttft_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    std::mem::take(&mut params),
                )
                .await?;
            }

            let mut dims: HashMap<(String, String), Acc> = HashMap::new();
            for row in conn
                .query(
                    "SELECT dim, value, rows_all, counted, success, errors, prompt_tokens, \
                     completion_tokens, total_tokens, latency_sum, latency_count, latency_max, \
                     ttft_sum, ttft_count FROM day_dim WHERE shard_id = ? AND day_ms = ?",
                    vec![Value::Text(shard_id.to_string()), Value::Integer(day)],
                )
                .await?
            {
                let Some(dim) = row.get_opt_string(0)? else {
                    continue;
                };
                let Some(value) = row.get_opt_string(1)? else {
                    continue;
                };
                if let Some(acc) = acc_from_row(&row, 2) {
                    dims.insert((dim, value), acc);
                }
            }
            for ((name, delta_day, value), add) in &delta.dims {
                if *delta_day != day {
                    continue;
                }
                dims.entry(((*name).to_string(), value.clone()))
                    .or_default()
                    .add(add);
            }
            conn.execute(
                "DELETE FROM day_dim WHERE shard_id = ? AND day_ms = ?",
                vec![Value::Text(shard_id.to_string()), Value::Integer(day)],
            )
            .await?;
            for ((dim, value), acc) in &dims {
                if acc.rows_all == 0 {
                    continue;
                }
                let mut params = acc_params(shard_id, day, acc);
                params.push(Value::Text(dim.clone()));
                params.push(Value::Text(value.clone()));
                conn.execute(
                    "INSERT INTO day_dim (shard_id, day_ms, rows_all, counted, success, errors, \
                     prompt_tokens, completion_tokens, total_tokens, latency_sum, latency_count, \
                     latency_max, ttft_sum, ttft_count, dim, value) \
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    params,
                )
                .await?;
            }

            let mut hist: HashMap<String, Vec<u32>> = HashMap::new();
            for row in conn
                .query(
                    "SELECT kind, buckets FROM day_hist WHERE shard_id = ? AND day_ms = ?",
                    vec![Value::Text(shard_id.to_string()), Value::Integer(day)],
                )
                .await?
            {
                let Some(kind) = row.get_opt_string(0)? else {
                    continue;
                };
                let Some(blob) = row.get_opt_blob(1)? else {
                    continue;
                };
                let mut buckets = vec![0_u32; HIST_BUCKETS];
                if blob.len() == HIST_BUCKETS * 4 {
                    for (index, chunk) in blob.chunks_exact(4).enumerate() {
                        buckets[index] =
                            u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
                    }
                }
                hist.insert(kind, buckets);
            }
            for ((delta_day, kind), add) in &delta.hist {
                if *delta_day != day {
                    continue;
                }
                let entry = hist
                    .entry((*kind).to_string())
                    .or_insert_with(|| vec![0; HIST_BUCKETS]);
                for (index, count) in add.iter().enumerate() {
                    entry[index] = entry[index].saturating_add(*count);
                }
            }
            conn.execute(
                "DELETE FROM day_hist WHERE shard_id = ? AND day_ms = ?",
                vec![Value::Text(shard_id.to_string()), Value::Integer(day)],
            )
            .await?;
            for (kind, buckets) in &hist {
                if buckets.iter().all(|count| *count == 0) {
                    continue;
                }
                conn.execute(
                    "INSERT INTO day_hist (shard_id, day_ms, kind, buckets) VALUES (?,?,?,?)",
                    vec![
                        Value::Text(shard_id.to_string()),
                        Value::Integer(day),
                        Value::Text(kind.clone()),
                        Value::Blob(encode_histogram(buckets)),
                    ],
                )
                .await?;
            }

            let mut distinct: HashSet<(String, String)> = HashSet::new();
            for row in conn
                .query(
                    "SELECT dim, value FROM day_distinct WHERE shard_id = ? AND day_ms = ?",
                    vec![Value::Text(shard_id.to_string()), Value::Integer(day)],
                )
                .await?
            {
                let Some(dim) = row.get_opt_string(0)? else {
                    continue;
                };
                let Some(value) = row.get_opt_string(1)? else {
                    continue;
                };
                distinct.insert((dim, value));
            }
            for (name, delta_day, value) in &delta.distinct {
                if *delta_day == day {
                    distinct.insert(((*name).to_string(), value.clone()));
                }
            }
            conn.execute(
                "DELETE FROM day_distinct WHERE shard_id = ? AND day_ms = ?",
                vec![Value::Text(shard_id.to_string()), Value::Integer(day)],
            )
            .await?;
            for (dim, value) in &distinct {
                conn.execute(
                    "INSERT INTO day_distinct (shard_id, day_ms, dim, value) VALUES (?,?,?,?)",
                    vec![
                        Value::Text(shard_id.to_string()),
                        Value::Integer(day),
                        Value::Text(dim.clone()),
                        Value::Text(value.clone()),
                    ],
                )
                .await?;
            }
        }

        conn.execute(
            "DELETE FROM active_state WHERE shard_id = ?",
            vec![Value::Text(shard_id.to_string())],
        )
        .await?;
        conn.execute(
            "INSERT INTO active_state (shard_id, last_id, max_time_ms, dev, ino, updated_at_ms) \
             VALUES (?,?,?,?,?,?)",
            vec![
                Value::Text(shard_id.to_string()),
                Value::Integer(max_id),
                Value::Integer(max_time_ms),
                Value::Integer(identity.0),
                Value::Integer(identity.1),
                Value::Integer(now_ms()),
            ],
        )
        .await?;
        Ok::<(), DbError>(())
    }
    .await;
    match write {
        Ok(()) => {
            conn.execute_batch("COMMIT").await?;
            if let Err(error) = conn
                .query("PRAGMA wal_checkpoint(TRUNCATE)", Vec::new())
                .await
            {
                tracing::debug!(%error, "active rollup checkpoint skipped");
            }
            info!(
                shard = shard_id,
                rows = delta.scanned,
                last_id = max_id,
                "active rollup updated"
            );
            Ok(())
        }
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK").await;
            Err(error)
        }
    }
}

fn acc_from_row(row: &crate::db::engine::Row, offset: usize) -> Option<Acc> {
    Some(Acc {
        rows_all: row.get_opt_i64(offset).ok()?.unwrap_or(0),
        counted: row.get_opt_i64(offset + 1).ok()?.unwrap_or(0),
        success: row.get_opt_i64(offset + 2).ok()?.unwrap_or(0),
        errors: row.get_opt_i64(offset + 3).ok()?.unwrap_or(0),
        prompt_tokens: row.get_opt_i64(offset + 4).ok()?.unwrap_or(0),
        completion_tokens: row.get_opt_i64(offset + 5).ok()?.unwrap_or(0),
        total_tokens: row.get_opt_i64(offset + 6).ok()?.unwrap_or(0),
        latency_sum: row.get_opt_f64(offset + 7).ok()?.unwrap_or(0.0),
        latency_count: row.get_opt_i64(offset + 8).ok()?.unwrap_or(0),
        latency_max: row.get_opt_f64(offset + 9).ok()?.unwrap_or(0.0),
        ttft_sum: row.get_opt_f64(offset + 10).ok()?.unwrap_or(0.0),
        ttft_count: row.get_opt_i64(offset + 11).ok()?.unwrap_or(0),
    })
}

fn merge_blob(target: &mut HashMap<i64, i64>, blob: &[u8]) -> Option<()> {
    if blob.len() != HIST_BUCKETS * 4 {
        return None;
    }
    for (index, chunk) in blob.chunks_exact(4).enumerate() {
        let count = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]) as i64;
        if count > 0 {
            *target.entry(index as i64).or_insert(0) += count;
        }
    }
    Some(())
}

fn histogram_bucket(value: Option<f64>) -> Option<i64> {
    let value = value?;
    Some((value.clamp(0.0, HIST_CAP_MS as f64) / HIST_BUCKET_MS as f64) as i64)
}

fn encode_histogram(buckets: &[u32]) -> Vec<u8> {
    let mut blob = Vec::with_capacity(HIST_BUCKETS * 4);
    for value in buckets {
        blob.extend_from_slice(&value.to_le_bytes());
    }
    blob
}

struct BuildInput<'a> {
    audit_path: &'a Path,
    shard_id: &'a str,
    archive_path: &'a Path,
    store: &'a AuditStore,
    min_ms: Option<i64>,
    max_ms: Option<i64>,
}

/// 后台构建入口：失败只记录日志，不影响请求路径。
pub(crate) async fn ensure_for_shard(
    audit_path: PathBuf,
    shard_id: String,
    archive_path: PathBuf,
    store: Arc<AuditStore>,
    min_ms: Option<i64>,
    max_ms: Option<i64>,
) -> Result<(), DbError> {
    ensure(&BuildInput {
        audit_path: &audit_path,
        shard_id: &shard_id,
        archive_path: &archive_path,
        store: &store,
        min_ms,
        max_ms,
    })
    .await
}

async fn ensure(input: &BuildInput<'_>) -> Result<(), DbError> {
    let db = open_store(input.audit_path).await?;
    if valid(
        &db,
        input.shard_id,
        input.archive_path,
        input.min_ms,
        input.max_ms,
    )
    .await
    {
        return Ok(());
    }
    let _lock = acquire_build_lock(input.audit_path).await?;
    if valid(
        &db,
        input.shard_id,
        input.archive_path,
        input.min_ms,
        input.max_ms,
    )
    .await
    {
        return Ok(());
    }
    let started = std::time::Instant::now();
    match build(&db, input).await {
        Ok((rows, days)) => {
            info!(
                shard = input.shard_id,
                rows,
                days,
                elapsed_ms = started.elapsed().as_millis() as u64,
                "archive rollup built"
            );
            Ok(())
        }
        Err(error) => {
            warn!(shard = input.shard_id, %error, "archive rollup build failed");
            Err(error)
        }
    }
}

async fn build(db: &Db, input: &BuildInput<'_>) -> Result<(i64, usize), DbError> {
    let mut days: HashMap<i64, Acc> = HashMap::new();
    let mut distinct: HashMap<(&'static str, i64, String), ()> = HashMap::new();
    let mut dims_days: HashMap<(&'static str, i64, String), Acc> = HashMap::new();
    let mut hist: HashMap<(i64, &'static str), Vec<u32>> = HashMap::new();
    let mut last_id = i64::MIN;
    let mut scanned = 0_i64;

    loop {
        let rows = input
            .store
            .query(
                "SELECT id, TimeMs, Model, ApiKey, IP, Type, Backend, ClientName, Status, \
                 PromptTokens, CompletionTokens, TotalTokens, LatencyMs, TtftMs \
                 FROM records WHERE id > ? ORDER BY id LIMIT ?",
                vec![Value::Integer(last_id), Value::Integer(BUILD_BATCH)],
            )
            .await?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            last_id = row.get_i64(0)?;
            scanned += 1;
            let Some(time_ms) = row.get_opt_i64(1)? else {
                continue;
            };
            let Some(day_ms) = local_day_ms(time_ms) else {
                continue;
            };
            let status = row.get_opt_i64(8)?;
            let tokens = (
                row.get_opt_i64(9)?.unwrap_or(0),
                row.get_opt_i64(10)?.unwrap_or(0),
                row.get_opt_i64(11)?.unwrap_or(0),
            );
            let latency = row.get_opt_f64(12)?;
            let ttft = row.get_opt_f64(13)?;

            let entry = days.entry(day_ms).or_default();
            entry.observe(status, tokens, latency, ttft);

            for (name, column) in [
                ("model", 2),
                ("apikey", 3),
                ("ip", 4),
                ("type", 5),
                ("backend", 6),
                ("client", 7),
            ] {
                let raw = row.get_opt_string(column)?;
                let value = normalize_dim(raw.as_deref());
                dims_days
                    .entry((name, day_ms, value))
                    .or_default()
                    .observe(status, tokens, latency, ttft);
                if matches!(name, "model" | "ip") {
                    if let Some(raw) = raw.as_deref() {
                        distinct.insert((name, day_ms, raw.to_string()), ());
                    }
                }
            }
            let status_value = status
                .map(|value| value.to_string())
                .unwrap_or_else(|| "Unknown".to_string());
            dims_days
                .entry(("status", day_ms, status_value))
                .or_default()
                .observe(status, tokens, latency, ttft);

            if let Some(bucket) = histogram_bucket(latency) {
                let entry = hist
                    .entry((day_ms, "latency"))
                    .or_insert_with(|| vec![0; HIST_BUCKETS]);
                entry[bucket as usize] += 1;
            }
            if let Some(bucket) = histogram_bucket(ttft) {
                let entry = hist
                    .entry((day_ms, "ttft"))
                    .or_insert_with(|| vec![0; HIST_BUCKETS]);
                entry[bucket as usize] += 1;
            }
        }
        if (rows.len() as i64) < BUILD_BATCH {
            break;
        }
    }

    let conn = db.connect()?;
    conn.execute_batch("BEGIN IMMEDIATE").await?;
    let write = async {
        for suffix in ["day_agg", "day_dim", "day_hist", "day_distinct", "shard_meta"] {
            conn.execute(
                &format!("DELETE FROM {suffix} WHERE shard_id = ?"),
                vec![Value::Text(input.shard_id.to_string())],
            )
            .await?;
        }
        for (day_ms, acc) in &days {
            conn.execute(
                "INSERT INTO day_agg (shard_id, day_ms, rows_all, counted, success, errors, \
                 prompt_tokens, completion_tokens, total_tokens, latency_sum, latency_count, \
                 latency_max, ttft_sum, ttft_count) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                acc_params(input.shard_id, *day_ms, acc),
            )
            .await?;
        }
        for ((dim, day_ms, value), acc) in &dims_days {
            let mut params = acc_params(input.shard_id, *day_ms, acc);
            params.push(Value::Text(dim.to_string()));
            params.push(Value::Text(value.clone()));
            conn.execute(
                "INSERT INTO day_dim (shard_id, day_ms, rows_all, counted, success, errors, \
                 prompt_tokens, completion_tokens, total_tokens, latency_sum, latency_count, \
                 latency_max, ttft_sum, ttft_count, dim, value) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                params,
            )
            .await?;
        }
        for ((dim, day_ms, value), ()) in &distinct {
            conn.execute(
                "INSERT INTO day_distinct (shard_id, day_ms, dim, value) VALUES (?,?,?,?)",
                vec![
                    Value::Text(input.shard_id.to_string()),
                    Value::Integer(*day_ms),
                    Value::Text((*dim).to_string()),
                    Value::Text(value.clone()),
                ],
            )
            .await?;
        }
        for ((day_ms, kind), buckets) in &hist {
            conn.execute(
                "INSERT INTO day_hist (shard_id, day_ms, kind, buckets) VALUES (?,?,?,?)",
                vec![
                    Value::Text(input.shard_id.to_string()),
                    Value::Integer(*day_ms),
                    Value::Text((*kind).to_string()),
                    Value::Blob(encode_histogram(buckets)),
                ],
            )
            .await?;
        }
        let (size, mtime_ms) = file_stamp(input.archive_path)
            .ok_or_else(|| DbError::Shape("archive disappeared during rollup".to_string()))?;
        conn.execute(
            "INSERT INTO shard_meta (shard_id, path, size, mtime_ms, row_count, min_ms, max_ms, \
             schema_version, built_at_ms) VALUES (?,?,?,?,?,?,?,?,?)",
            vec![
                Value::Text(input.shard_id.to_string()),
                Value::Text(input.archive_path.display().to_string()),
                Value::Integer(size),
                Value::Integer(mtime_ms),
                Value::Integer(scanned),
                input.min_ms.map(Value::Integer).unwrap_or(Value::Null),
                input.max_ms.map(Value::Integer).unwrap_or(Value::Null),
                Value::Integer(SCHEMA_VERSION),
                Value::Integer(now_ms()),
            ],
        )
        .await?;
        Ok::<(), DbError>(())
    }
    .await;
    match write {
        Ok(()) => {
            conn.execute_batch("COMMIT").await?;
            // 构建完成后把 WAL 合并回主文件：派生库仍可安全删除，但备份/巡检时
            // 不应只剩一个 4KB 的空壳。失败只记录日志，不影响已提交的数据。
            if let Err(error) = conn
                .query("PRAGMA wal_checkpoint(TRUNCATE)", Vec::new())
                .await
            {
                tracing::debug!(%error, "rollup checkpoint skipped");
            }
            Ok((scanned, days.len()))
        }
        Err(error) => {
            let _ = conn.execute_batch("ROLLBACK").await;
            Err(error)
        }
    }
}

fn acc_params(shard_id: &str, day_ms: i64, acc: &Acc) -> Vec<Value> {
    vec![
        Value::Text(shard_id.to_string()),
        Value::Integer(day_ms),
        Value::Integer(acc.rows_all),
        Value::Integer(acc.counted),
        Value::Integer(acc.success),
        Value::Integer(acc.errors),
        Value::Integer(acc.prompt_tokens),
        Value::Integer(acc.completion_tokens),
        Value::Integer(acc.total_tokens),
        Value::Real(acc.latency_sum),
        Value::Integer(acc.latency_count),
        Value::Real(acc.latency_max),
        Value::Real(acc.ttft_sum),
        Value::Integer(acc.ttft_count),
    ]
}

fn normalize_dim(value: Option<&str>) -> String {
    match value {
        Some(value) if !value.is_empty() => value.to_string(),
        _ => "Unknown".to_string(),
    }
}

/// 删除某个分片的汇总（归档被移除时调用）。
#[allow(dead_code)]
pub(crate) async fn forget(audit_path: &Path, shard_id: &str) -> Result<(), DbError> {
    let db = open_store(audit_path).await?;
    let conn = db.connect()?;
    for suffix in [
        "day_agg",
        "day_dim",
        "day_hist",
        "day_distinct",
        "shard_meta",
    ] {
        conn.execute(
            &format!("DELETE FROM {suffix} WHERE shard_id = ?"),
            vec![Value::Text(shard_id.to_string())],
        )
        .await?;
    }
    let _ = conn
        .query("PRAGMA wal_checkpoint(TRUNCATE)", Vec::new())
        .await;
    Ok(())
}

#[cfg(test)]
#[path = "aggregate_tests.rs"]
mod tests;
