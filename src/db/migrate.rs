//! One-shot v7 (SQLite/FTS5) -> v8 (Turso) audit database migration.
//!
//! Usage:
//!   cargo run --bin migrate_v7 -- --from /path/record.db --to /path/record_v8.db
//!        [--batch 200] [--resume] [--verify-only]
//!
//! The copy runner reads its source without modifying it. Automatic startup
//! first normalizes the legacy journal and holds a SQLite write reservation
//! through snapshot creation and cutover. Ids and payload associations survive.

use std::fs::{self, File, OpenOptions};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use crate::db::engine::{DbError, Value};
use crate::db::store::AuditStore;
use sqlx::pool::PoolConnection;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Row;
use sqlx::Sqlite;
use tokio::time::sleep;

const RECORD_COLUMNS: &str = "id, Time, TimeMs, IP, Method, Endpoint, Type, Model, Backend, \
    SessionId, ParentSessionId, RequestId, SessionAffinity, UserAgent, ClientName, ClientVersion, ApiKey, \
    Status, Error, RetryCount, FinishReason, \
    LatencyMs, TtftMs, UpstreamMs, StreamMs, \
    CompletionTokens, PromptTokens, TotalTokens, Tool, Multimodal, \
    RequestBytes, ResponseBytes, PromptBytes, RequestTailBytes, AnswerBytes, \
    MessageCount, SystemCount, ToolCount, AssistantCount, ToolResultCount, ImageCount, \
    Prompt, RequestTail, Answer, ToolNames";

const INSERT_RECORD: &str = "INSERT INTO records (\
    id, Time, TimeMs, IP, Method, Endpoint, Type, Model, Backend, \
    SessionId, ParentSessionId, RequestId, SessionAffinity, UserAgent, ClientName, ClientVersion, ApiKey, \
    Status, Error, RetryCount, FinishReason, \
    LatencyMs, TtftMs, UpstreamMs, StreamMs, \
    CompletionTokens, PromptTokens, TotalTokens, Tool, Multimodal, \
    RequestBytes, ResponseBytes, PromptBytes, RequestTailBytes, AnswerBytes, \
    MessageCount, SystemCount, ToolCount, AssistantCount, ToolResultCount, ImageCount, \
    Prompt, RequestTail, Answer, ToolNames, SearchText) VALUES (\
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

pub struct MigrateOptions {
    pub from: String,
    pub to: String,
    pub batch: usize,
    pub limit: i64,
    pub resume: bool,
    pub defer_fts: bool,
    pub verify_only: bool,
}

pub use crate::db::store::{probe_schema as detect_schema, SchemaKind};

const MIGRATION_TIMEOUT: Duration = Duration::from_secs(30);
const LOCK_RETRY: Duration = Duration::from_millis(50);
const STAGE_SUFFIX: &str = ".v8-stage";
const TARGET_SUFFIX: &str = ".v8-target";

struct SnapshotLock {
    pool: sqlx::SqlitePool,
    connection: PoolConnection<Sqlite>,
}

impl SnapshotLock {
    async fn close(self) {
        drop(self.connection);
        self.pool.close().await;
    }
}

pub fn parse_args() -> MigrateOptions {
    let mut from = None;
    let mut to = None;
    let mut batch = 200usize;
    let mut limit = i64::MAX;
    let mut resume = false;
    let mut defer_fts = false;
    let mut verify_only = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--from" => from = args.next(),
            "--to" => to = args.next(),
            "--batch" => batch = args.next().and_then(|v| v.parse().ok()).unwrap_or(200),
            "--limit" => limit = args.next().and_then(|v| v.parse().ok()).unwrap_or(i64::MAX),
            "--resume" => resume = true,
            "--migrate-db" => {}
            "--verify-db" => verify_only = true,
            "--defer-fts" => defer_fts = true,
            "--verify-only" => verify_only = true,
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }
    let default_from = crate::db::resolve_db_path();
    let from = from.unwrap_or_else(|| default_from.display().to_string());
    let to = to.unwrap_or_else(|| format!("{from}.v8"));
    MigrateOptions {
        from,
        to,
        batch: batch.max(1),
        limit,
        resume,
        defer_fts,
        verify_only,
    }
}

fn opt_text(row: &sqlx::sqlite::SqliteRow, name: &str) -> Result<Value, DbError> {
    row.try_get::<Option<String>, _>(name)
        .map_err(crate::db::map_sqlx_error)
        .map(|value| value.map(Value::Text).unwrap_or(Value::Null))
}

fn opt_int(row: &sqlx::sqlite::SqliteRow, name: &str) -> Result<Value, DbError> {
    row.try_get::<Option<i64>, _>(name)
        .map_err(crate::db::map_sqlx_error)
        .map(|value| value.map(Value::Integer).unwrap_or(Value::Null))
}

fn opt_real(row: &sqlx::sqlite::SqliteRow, name: &str) -> Result<Value, DbError> {
    row.try_get::<Option<f64>, _>(name)
        .map_err(crate::db::map_sqlx_error)
        .map(|value| value.map(Value::Real).unwrap_or(Value::Null))
}

fn flag(row: &sqlx::sqlite::SqliteRow, name: &str) -> Result<Value, DbError> {
    row.try_get::<Option<i64>, _>(name)
        .map_err(crate::db::map_sqlx_error)
        .map(|value| Value::Integer(value.unwrap_or(0)))
}

fn record_params(row: &sqlx::sqlite::SqliteRow) -> Result<Vec<Value>, DbError> {
    Ok(vec![
        Value::Integer(row.try_get("id").map_err(crate::db::map_sqlx_error)?),
        opt_text(row, "Time")?,
        opt_int(row, "TimeMs")?,
        opt_text(row, "IP")?,
        opt_text(row, "Method")?,
        opt_text(row, "Endpoint")?,
        opt_text(row, "Type")?,
        opt_text(row, "Model")?,
        opt_text(row, "Backend")?,
        opt_text(row, "SessionId")?,
        opt_text(row, "ParentSessionId")?,
        opt_text(row, "RequestId")?,
        opt_text(row, "SessionAffinity")?,
        opt_text(row, "UserAgent")?,
        opt_text(row, "ClientName")?,
        opt_text(row, "ClientVersion")?,
        opt_text(row, "ApiKey")?,
        opt_int(row, "Status")?,
        opt_text(row, "Error")?,
        opt_int(row, "RetryCount")?,
        opt_text(row, "FinishReason")?,
        opt_real(row, "LatencyMs")?,
        opt_real(row, "TtftMs")?,
        opt_real(row, "UpstreamMs")?,
        opt_real(row, "StreamMs")?,
        opt_int(row, "CompletionTokens")?,
        opt_int(row, "PromptTokens")?,
        opt_int(row, "TotalTokens")?,
        flag(row, "Tool")?,
        flag(row, "Multimodal")?,
        opt_int(row, "RequestBytes")?,
        opt_int(row, "ResponseBytes")?,
        opt_int(row, "PromptBytes")?,
        opt_int(row, "RequestTailBytes")?,
        opt_int(row, "AnswerBytes")?,
        opt_int(row, "MessageCount")?,
        opt_int(row, "SystemCount")?,
        opt_int(row, "ToolCount")?,
        opt_int(row, "AssistantCount")?,
        opt_int(row, "ToolResultCount")?,
        opt_int(row, "ImageCount")?,
        opt_text(row, "Prompt")?,
        opt_text(row, "RequestTail")?,
        opt_text(row, "Answer")?,
        opt_text(row, "ToolNames")?,
        Value::Text(search_text(row)?),
    ])
}

fn search_text(row: &sqlx::sqlite::SqliteRow) -> Result<String, DbError> {
    let get = |name: &str| {
        row.try_get::<Option<String>, _>(name)
            .map_err(crate::db::map_sqlx_error)
            .map(|value| value.unwrap_or_default())
    };
    Ok(format!(
        "{}\n{}\n{}",
        get("Prompt")?,
        get("RequestTail")?,
        get("Answer")?
    ))
}

fn payload_params(row: &sqlx::sqlite::SqliteRow) -> Result<Vec<Value>, DbError> {
    let blob = |name: &str| -> Result<Value, DbError> {
        row.try_get::<Option<Vec<u8>>, _>(name)
            .map_err(crate::db::map_sqlx_error)
            .map(|value| value.map(Value::Blob).unwrap_or(Value::Null))
    };
    Ok(vec![
        Value::Integer(
            row.try_get("record_id")
                .map_err(crate::db::map_sqlx_error)?,
        ),
        opt_text(row, "codec")?,
        opt_text(row, "dict_id")?,
        blob("request")?,
        blob("response")?,
        blob("headers")?,
        opt_int(row, "request_raw_len")?,
        opt_int(row, "response_raw_len")?,
        opt_int(row, "headers_raw_len")?,
    ])
}

async fn target_max_id(store: &AuditStore) -> Result<Option<i64>, DbError> {
    let row = store
        .query_one("SELECT MAX(id) AS m FROM records", Vec::new())
        .await?;
    match row {
        Some(row) => row.get_opt_i64(0),
        None => Ok(None),
    }
}

async fn open_source(path: &str) -> Result<sqlx::SqlitePool, DbError> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .busy_timeout(Duration::from_secs(30));
    SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(30))
        .connect_with(options)
        .await
        .map_err(crate::db::map_sqlx_error)
}

pub async fn run(options: &MigrateOptions) -> Result<(), DbError> {
    let source = open_source(&options.from).await?;
    let target = if options.verify_only {
        AuditStore::open(&options.to, true, Duration::from_secs(30)).await?
    } else if options.defer_fts {
        AuditStore::open_deferred_fts(&options.to, Duration::from_secs(30)).await?
    } else {
        AuditStore::open(&options.to, false, Duration::from_secs(30)).await?
    };
    let result = run_copy(options, &source, &target).await;
    drop(target);
    source.close().await;
    result
}

async fn run_copy(
    options: &MigrateOptions,
    source: &sqlx::SqlitePool,
    target: &AuditStore,
) -> Result<(), DbError> {
    if options.verify_only {
        return crate::db::migrate_verify::verify(source, target).await;
    }

    let src_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM records")
        .fetch_one(source)
        .await
        .map_err(crate::db::map_sqlx_error)?;
    let mut last_id = if options.resume {
        target_max_id(target).await?
    } else {
        None
    };
    if let Some(last_id) = last_id {
        println!("resuming after id={last_id}");
    }
    println!("source rows: {src_total}, batch={}", options.batch);

    let started = Instant::now();
    let mut migrated = 0_i64;
    loop {
        let record_sql = if last_id.is_some() {
            format!("SELECT {RECORD_COLUMNS} FROM records WHERE id > ? ORDER BY id LIMIT ?")
        } else {
            format!("SELECT {RECORD_COLUMNS} FROM records ORDER BY id LIMIT ?")
        };
        let mut record_query = sqlx::query(&record_sql);
        if let Some(last_id) = last_id {
            record_query = record_query.bind(last_id);
        }
        let rows = record_query
            .bind(options.batch as i64)
            .fetch_all(source)
            .await
            .map_err(crate::db::map_sqlx_error)?;
        if rows.is_empty() {
            break;
        }
        let max_id = rows
            .last()
            .ok_or_else(|| DbError::Shape("migration batch unexpectedly empty".to_string()))?
            .try_get::<i64, _>("id")
            .map_err(crate::db::map_sqlx_error)?;
        let payload_sql = if last_id.is_some() {
            "SELECT record_id, codec, dict_id, request, response, headers, \
             request_raw_len, response_raw_len, headers_raw_len \
             FROM payloads WHERE record_id > ? AND record_id <= ? ORDER BY record_id"
        } else {
            "SELECT record_id, codec, dict_id, request, response, headers, \
             request_raw_len, response_raw_len, headers_raw_len \
             FROM payloads WHERE record_id <= ? ORDER BY record_id"
        };
        let mut payload_query = sqlx::query(payload_sql);
        if let Some(last_id) = last_id {
            payload_query = payload_query.bind(last_id);
        }
        let payloads = payload_query
            .bind(max_id)
            .fetch_all(source)
            .await
            .map_err(crate::db::map_sqlx_error)?;

        let conn = target.write().await;
        conn.begin_immediate().await?;
        let batch_result = async {
            for row in &rows {
                conn.execute(INSERT_RECORD, record_params(row)?).await?;
            }
            for payload in &payloads {
                conn.execute(
                    "INSERT INTO payloads (record_id, codec, dict_id, request, response, headers, \
                     request_raw_len, response_raw_len, headers_raw_len) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    payload_params(payload)?,
                )
                .await?;
            }
            Ok::<(), DbError>(())
        }
        .await;
        match batch_result {
            Ok(()) => conn.commit().await?,
            Err(error) => {
                if let Err(rollback_error) = conn.rollback().await {
                    tracing::warn!(%rollback_error, "migration batch rollback failed");
                }
                return Err(error);
            }
        }
        drop(conn);

        migrated += rows.len() as i64;
        last_id = Some(max_id);
        let elapsed = started.elapsed().as_secs_f64();
        if migrated >= options.limit {
            println!("stopping at --limit {migrated}");
            break;
        }
        println!(
            "id={max_id} rows={migrated}/{src_total} ({:.0} rows/s, {:.1}s)",
            migrated as f64 / elapsed.max(0.001),
            elapsed
        );
    }

    println!(
        "migration finished: {migrated} rows in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    if options.defer_fts {
        let started = Instant::now();
        println!("building FTS index...");
        target.create_fts_index().await?;
        println!("FTS index built in {:.1}s", started.elapsed().as_secs_f64());
    }
    target.checkpoint_truncate().await?;

    if migrated == src_total {
        crate::db::migrate_verify::verify(source, target).await
    } else {
        println!("partial migration ({migrated}/{src_total}); skipping full verification");
        Ok(())
    }
}

pub async fn run_cli() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    run(&args).await.map_err(Into::into)
}

pub(crate) async fn migrate_path_to_v8(path: &Path) -> Result<(), DbError> {
    let _lock = acquire_lock(path).await?;
    let stage = artifact_path(path, STAGE_SUFFIX);
    let target = artifact_path(path, TARGET_SUFFIX);
    match detect_schema(path).await? {
        SchemaKind::V8 => {
            remove_artifact(&stage)?;
            remove_artifact(&target)?;
            return Ok(());
        }
        SchemaKind::Newer(version) => {
            return Err(DbError::Shape(format!(
                "audit database schema v{version} is newer than this binary (v8)"
            )))
        }
        SchemaKind::Empty => {
            return Err(DbError::Shape(
                "cannot migrate an empty audit database".to_string(),
            ))
        }
        SchemaKind::V7 | SchemaKind::Legacy => {}
        SchemaKind::Unknown => {
            return Err(DbError::Shape(
                "unrecognized audit schema; refusing automatic migration".to_string(),
            ))
        }
    }

    remove_artifact(&stage)?;
    remove_artifact(&target)?;
    let permissions = fs::metadata(path)
        .map_err(|error| DbError::Shape(format!("cannot stat '{}': {error}", path.display())))?
        .permissions();

    let source_lock = snapshot_source(path, &stage, permissions.clone()).await?;
    let result = async {
        if matches!(detect_schema(&stage).await?, SchemaKind::Legacy) {
            migrate_staged_legacy(&stage).await?;
        }
        if !matches!(detect_schema(&stage).await?, SchemaKind::V7) {
            return Err(DbError::Shape(
                "staged SQLite source did not become v7".to_string(),
            ));
        }

        let options = MigrateOptions {
            from: stage.display().to_string(),
            to: target.display().to_string(),
            batch: 200,
            limit: i64::MAX,
            resume: false,
            defer_fts: true,
            verify_only: false,
        };
        create_empty_artifact(&target, &permissions)?;
        run(&options).await?;
        replace_active(path, &target, permissions)?;
        remove_artifact(&stage)
    }
    .await;
    source_lock.close().await;
    if result.is_ok() {
        remove_sidecars(path)?;
    }
    result
}

async fn acquire_lock(path: &Path) -> Result<File, DbError> {
    let lock_path = artifact_path(path, ".v8-migrate.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|error| DbError::Shape(format!("cannot open migration lock: {error}")))?;
    let deadline = tokio::time::Instant::now() + MIGRATION_TIMEOUT;
    loop {
        match file.try_lock() {
            Ok(()) => return Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => {
                if tokio::time::Instant::now() >= deadline {
                    return Err(DbError::Driver(turso::Error::Busy(format!(
                        "migration lock held for '{}', timed out",
                        path.display()
                    ))));
                }
                sleep(LOCK_RETRY).await;
            }
            Err(std::fs::TryLockError::Error(error)) => {
                return Err(DbError::Shape(format!(
                    "cannot acquire migration lock: {error}"
                )))
            }
        }
    }
}

async fn snapshot_source(
    source: &Path,
    stage: &Path,
    permissions: std::fs::Permissions,
) -> Result<SnapshotLock, DbError> {
    let options = SqliteConnectOptions::new()
        .filename(source)
        .read_only(false)
        .create_if_missing(false)
        .busy_timeout(MIGRATION_TIMEOUT);
    let pool = SqlitePoolOptions::new()
        .max_connections(2)
        .acquire_timeout(MIGRATION_TIMEOUT)
        .connect_with(options)
        .await
        .map_err(crate::db::map_sqlx_error)?;
    let mut connection = pool.acquire().await.map_err(crate::db::map_sqlx_error)?;
    sqlx::query("PRAGMA journal_mode = DELETE")
        .execute(&mut *connection)
        .await
        .map_err(crate::db::map_sqlx_error)?;
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *connection)
        .await
        .map_err(crate::db::map_sqlx_error)?;
    let mut snapshot_connection = pool.acquire().await.map_err(crate::db::map_sqlx_error)?;
    create_empty_artifact(stage, &permissions)?;
    let escaped = stage.to_string_lossy().replace('\'', "''");
    sqlx::query(&format!("VACUUM INTO '{escaped}'"))
        .execute(&mut *snapshot_connection)
        .await
        .map_err(crate::db::map_sqlx_error)?;
    drop(snapshot_connection);
    Ok(SnapshotLock { pool, connection })
}

async fn migrate_staged_legacy(stage: &Path) -> Result<(), DbError> {
    let options = SqliteConnectOptions::new()
        .filename(stage)
        .read_only(false)
        .create_if_missing(false)
        .busy_timeout(MIGRATION_TIMEOUT);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(MIGRATION_TIMEOUT)
        .connect_with(options)
        .await
        .map_err(crate::db::map_sqlx_error)?;
    let result = crate::db::legacy::migrate_archive(&pool)
        .await
        .map_err(crate::db::map_sqlx_error);
    pool.close().await;
    result
}

fn artifact_path(path: &Path, suffix: &str) -> PathBuf {
    PathBuf::from(format!("{}{}", path.display(), suffix))
}

fn create_empty_artifact(path: &Path, permissions: &std::fs::Permissions) -> Result<(), DbError> {
    File::create(path)
        .and_then(|_| fs::set_permissions(path, permissions.clone()))
        .map_err(|error| {
            DbError::Shape(format!(
                "cannot prepare migration artifact '{}': {error}",
                path.display()
            ))
        })
}

fn remove_artifact(path: &Path) -> Result<(), DbError> {
    for suffix in ["", "-wal", "-shm", "-journal"] {
        let candidate = if suffix.is_empty() {
            path.to_path_buf()
        } else {
            artifact_path(path, suffix)
        };
        match fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(DbError::Shape(format!(
                    "cannot remove migration artifact '{}': {error}",
                    candidate.display()
                )))
            }
        }
    }
    Ok(())
}

pub(crate) fn remove_sidecars(path: &Path) -> Result<(), DbError> {
    for suffix in ["-wal", "-shm", "-journal"] {
        let candidate = artifact_path(path, suffix);
        match fs::remove_file(&candidate) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(DbError::Shape(format!(
                    "cannot remove database sidecar '{}': {error}",
                    candidate.display()
                )))
            }
        }
    }
    Ok(())
}

fn replace_active(
    active: &Path,
    target: &Path,
    permissions: std::fs::Permissions,
) -> Result<(), DbError> {
    prepare_target_for_cutover(target)?;
    fs::set_permissions(target, permissions).map_err(|error| {
        DbError::Shape(format!("cannot preserve database permissions: {error}"))
    })?;
    File::open(target)
        .and_then(|file| file.sync_all())
        .map_err(|error| DbError::Shape(format!("cannot sync staged database: {error}")))?;

    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = artifact_path(active, suffix);
        if sidecar.exists()
            && fs::metadata(&sidecar)
                .map_err(|error| {
                    DbError::Shape(format!("cannot inspect database sidecar: {error}"))
                })?
                .len()
                > 0
        {
            return Err(DbError::Shape(format!(
                "database sidecar '{}' was not drained before cutover",
                sidecar.display()
            )));
        }
    }

    let backup = crate::db::unique_backup_path(active);
    fs::hard_link(active, &backup)
        .or_else(|_| fs::copy(active, &backup).map(|_| ()))
        .map_err(|error| DbError::Shape(format!("cannot create database backup: {error}")))?;

    let mut moved_sidecars = Vec::new();
    for suffix in ["-wal", "-shm", "-journal"] {
        let sidecar = artifact_path(active, suffix);
        if sidecar.exists() {
            let backup_sidecar = artifact_path(&backup, suffix);
            if let Err(error) = fs::rename(&sidecar, &backup_sidecar) {
                for (original, preserved) in moved_sidecars.iter().rev() {
                    if let Err(restore_error) = fs::rename(preserved, original) {
                        tracing::error!(%restore_error, "database sidecar rollback failed");
                    }
                }
                return Err(DbError::Shape(format!(
                    "cannot preserve database sidecar: {error}"
                )));
            }
            moved_sidecars.push((sidecar, backup_sidecar));
        }
    }

    if let Err(error) = fs::rename(target, active) {
        for (sidecar, backup_sidecar) in moved_sidecars.iter().rev() {
            if let Err(restore_error) = fs::rename(backup_sidecar, sidecar) {
                tracing::error!(%restore_error, "database sidecar rollback failed");
            }
        }
        return Err(DbError::Shape(format!(
            "cannot atomically replace active database: {error}"
        )));
    }
    sync_parent(active)
}

fn prepare_target_for_cutover(target: &Path) -> Result<(), DbError> {
    for suffix in ["-wal", "-journal"] {
        let sidecar = artifact_path(target, suffix);
        match fs::metadata(&sidecar) {
            Ok(metadata) if metadata.len() > 0 => {
                return Err(DbError::Shape(format!(
                    "migration target sidecar '{}' was not drained before cutover",
                    sidecar.display()
                )));
            }
            Ok(_) => {}
            Err(error) if error.kind() == ErrorKind::NotFound => {}
            Err(error) => {
                return Err(DbError::Shape(format!(
                    "cannot inspect migration target sidecar '{}': {error}",
                    sidecar.display()
                )));
            }
        }
    }
    remove_sidecars(target)
}

fn sync_parent(path: &Path) -> Result<(), DbError> {
    let directory = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|error| DbError::Shape(format!("cannot sync database directory: {error}")))
}

#[cfg(test)]
#[path = "migrate_cutover_tests.rs"]
mod cutover_tests;
