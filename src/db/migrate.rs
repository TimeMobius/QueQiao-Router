//! One-shot v7 (SQLite/FTS5) -> v8 (Turso) audit database migration.
//!
//! Usage:
//!   cargo run --bin migrate_v7 -- --from /path/record.db --to /path/record_v8.db
//!        [--batch 200] [--resume] [--verify-only]
//!
//! The source is opened read-only and never modified. Ids are preserved so
//! `payloads.record_id` and the dashboard cursor keep working.

use std::time::{Duration, Instant};

use crate::db::engine::Value;
use crate::db::store::AuditStore;
use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::Row;

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

fn opt_text(row: &sqlx::sqlite::SqliteRow, name: &str) -> Value {
    row.try_get::<Option<String>, _>(name)
        .ok()
        .flatten()
        .map(Value::Text)
        .unwrap_or(Value::Null)
}

fn opt_int(row: &sqlx::sqlite::SqliteRow, name: &str) -> Value {
    row.try_get::<Option<i64>, _>(name)
        .ok()
        .flatten()
        .map(Value::Integer)
        .unwrap_or(Value::Null)
}

fn opt_real(row: &sqlx::sqlite::SqliteRow, name: &str) -> Value {
    row.try_get::<Option<f64>, _>(name)
        .ok()
        .flatten()
        .map(Value::Real)
        .unwrap_or(Value::Null)
}

fn flag(row: &sqlx::sqlite::SqliteRow, name: &str) -> Value {
    Value::Integer(
        row.try_get::<Option<i64>, _>(name)
            .ok()
            .flatten()
            .unwrap_or(0),
    )
}

fn record_params(row: &sqlx::sqlite::SqliteRow) -> Vec<Value> {
    vec![
        Value::Integer(row.get::<i64, _>("id")),
        opt_text(row, "Time"),
        opt_int(row, "TimeMs"),
        opt_text(row, "IP"),
        opt_text(row, "Method"),
        opt_text(row, "Endpoint"),
        opt_text(row, "Type"),
        opt_text(row, "Model"),
        opt_text(row, "Backend"),
        opt_text(row, "SessionId"),
        opt_text(row, "ParentSessionId"),
        opt_text(row, "RequestId"),
        opt_text(row, "SessionAffinity"),
        opt_text(row, "UserAgent"),
        opt_text(row, "ClientName"),
        opt_text(row, "ClientVersion"),
        opt_text(row, "ApiKey"),
        opt_int(row, "Status"),
        opt_text(row, "Error"),
        opt_int(row, "RetryCount"),
        opt_text(row, "FinishReason"),
        opt_real(row, "LatencyMs"),
        opt_real(row, "TtftMs"),
        opt_real(row, "UpstreamMs"),
        opt_real(row, "StreamMs"),
        opt_int(row, "CompletionTokens"),
        opt_int(row, "PromptTokens"),
        opt_int(row, "TotalTokens"),
        flag(row, "Tool"),
        flag(row, "Multimodal"),
        opt_int(row, "RequestBytes"),
        opt_int(row, "ResponseBytes"),
        opt_int(row, "PromptBytes"),
        opt_int(row, "RequestTailBytes"),
        opt_int(row, "AnswerBytes"),
        opt_int(row, "MessageCount"),
        opt_int(row, "SystemCount"),
        opt_int(row, "ToolCount"),
        opt_int(row, "AssistantCount"),
        opt_int(row, "ToolResultCount"),
        opt_int(row, "ImageCount"),
        opt_text(row, "Prompt"),
        opt_text(row, "RequestTail"),
        opt_text(row, "Answer"),
        opt_text(row, "ToolNames"),
        Value::Text(search_text(row)),
    ]
}

fn search_text(row: &sqlx::sqlite::SqliteRow) -> String {
    let get = |name: &str| {
        row.try_get::<Option<String>, _>(name)
            .ok()
            .flatten()
            .unwrap_or_default()
    };
    format!(
        "{}\n{}\n{}",
        get("Prompt"),
        get("RequestTail"),
        get("Answer")
    )
}

fn payload_params(row: &sqlx::sqlite::SqliteRow) -> Vec<Value> {
    let blob = |name: &str| -> Value {
        row.try_get::<Option<Vec<u8>>, _>(name)
            .ok()
            .flatten()
            .map(Value::Blob)
            .unwrap_or(Value::Null)
    };
    vec![
        Value::Integer(row.get::<i64, _>("record_id")),
        opt_text(row, "codec"),
        opt_text(row, "dict_id"),
        blob("request"),
        blob("response"),
        blob("headers"),
        opt_int(row, "request_raw_len"),
        opt_int(row, "response_raw_len"),
        opt_int(row, "headers_raw_len"),
    ]
}

async fn target_max_id(store: &AuditStore) -> i64 {
    store
        .query_one("SELECT COALESCE(MAX(id), 0) AS m FROM records", Vec::new())
        .await
        .ok()
        .flatten()
        .and_then(|row| row.get_i64(0).ok())
        .unwrap_or(0)
}

async fn open_source(path: &str) -> Result<sqlx::SqlitePool, Box<dyn std::error::Error>> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .busy_timeout(Duration::from_secs(30));
    Ok(SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(30))
        .connect_with(options)
        .await?)
}

async fn verify(
    source: &sqlx::SqlitePool,
    target: &AuditStore,
) -> Result<(), Box<dyn std::error::Error>> {
    let src_records: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM records")
        .fetch_one(source)
        .await?;
    let src_payloads: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM payloads")
        .fetch_one(source)
        .await?;
    let (src_min, src_max): (i64, i64) =
        sqlx::query_as("SELECT MIN(TimeMs), MAX(TimeMs) FROM records")
            .fetch_one(source)
            .await?;
    let (src_req, src_resp, src_hdr): (i64, i64, i64) = sqlx::query_as(
        "SELECT COALESCE(SUM(length(request)),0), COALESCE(SUM(length(response)),0), COALESCE(SUM(length(headers)),0) FROM payloads",
    )
    .fetch_one(source)
    .await?;

    let dst_records = match target
        .query_one("SELECT COUNT(*) FROM records", Vec::new())
        .await?
    {
        Some(row) => row.get_i64(0)?,
        None => 0,
    };
    let dst_payloads = match target
        .query_one("SELECT COUNT(*) FROM payloads", Vec::new())
        .await?
    {
        Some(row) => row.get_i64(0)?,
        None => 0,
    };
    let (dst_min, dst_max) = match target
        .query_one("SELECT MIN(TimeMs), MAX(TimeMs) FROM records", Vec::new())
        .await?
    {
        Some(row) => (row.get_i64(0)?, row.get_i64(1)?),
        None => (0, 0),
    };
    let (dst_req, dst_resp, dst_hdr) = match target
        .query_one(
            "SELECT COALESCE(SUM(length(request)),0), COALESCE(SUM(length(response)),0), COALESCE(SUM(length(headers)),0) FROM payloads",
            Vec::new(),
        )
        .await?
    {
        Some(row) => (row.get_i64(0)?, row.get_i64(1)?, row.get_i64(2)?),
        None => (0, 0, 0),
    };

    println!("\n== verify ==");
    println!("records:  src={src_records} dst={dst_records}");
    println!("payloads: src={src_payloads} dst={dst_payloads}");
    println!("timeMs:   src={src_min}..{src_max} dst={dst_min}..{dst_max}");
    println!(
        "bytes:    req {src_req}/{dst_req}  resp {src_resp}/{dst_resp}  hdr {src_hdr}/{dst_hdr}"
    );

    if src_records != dst_records
        || src_payloads != dst_payloads
        || (src_min, src_max) != (dst_min, dst_max)
        || (src_req, src_resp, src_hdr) != (dst_req, dst_resp, dst_hdr)
    {
        return Err("verification failed: totals differ".into());
    }

    // Compare raw compressed payload hashes on a spread of ids.
    let ids = [1_i64, 2, 1000, 50_000, src_records / 2, src_records];
    for id in ids {
        let src_row =
            sqlx::query("SELECT request, response, headers FROM payloads WHERE record_id = ?")
                .bind(id)
                .fetch_optional(source)
                .await?;
        let Some(src_row) = src_row else { continue };
        let dst_row = target
            .query_one(
                "SELECT request, response, headers FROM payloads WHERE record_id = ?",
                vec![Value::Integer(id)],
            )
            .await?;
        let Some(dst_row) = dst_row else {
            return Err(format!("payload {id} missing in target").into());
        };
        for (idx, name) in ["request", "response", "headers"].iter().enumerate() {
            let src_blob: Option<Vec<u8>> = src_row.try_get(*name).ok();
            let dst_blob = dst_row.get_opt_blob(idx)?.map(|b| b.to_vec());
            let hash = |v: &Option<Vec<u8>>| {
                v.as_ref().map(|bytes| {
                    let mut h = Sha256::new();
                    h.update(bytes);
                    format!("{:x}", h.finalize())
                })
            };
            if hash(&src_blob) != hash(&dst_blob) {
                return Err(format!("payload {id} {name} hash mismatch").into());
            }
        }
    }

    // FTS parity on the three text fields (source FTS5 trigram vs target ngram).
    for term in ["hello", "error", "model", "request", "function"] {
        let src_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM records WHERE id IN (SELECT rowid FROM records_fts WHERE records_fts MATCH ?)",
        )
        .bind(term)
        .fetch_one(source)
        .await?;
        let dst_count = match target
            .query_one(
                "SELECT COUNT(*) FROM records WHERE fts_match(SearchText, ?) AND instr(lower(SearchText), ?) > 0",
                vec![
                    Value::Text(term.to_string()),
                    Value::Text(term.to_lowercase()),
                ],
            )
            .await?
        {
            Some(row) => row.get_i64(0)?,
            None => 0,
        };
        let mark = if src_count == dst_count { "ok" } else { "DIFF" };
        println!("fts '{term}': src={src_count} dst={dst_count} {mark}");
    }

    println!("verification passed");
    Ok(())
}

pub async fn run_cli() -> Result<(), Box<dyn std::error::Error>> {
    let args = parse_args();
    let source = open_source(&args.from).await?;
    let target = if args.defer_fts {
        AuditStore::open_deferred_fts(&args.to, Duration::from_secs(30)).await?
    } else {
        AuditStore::open(&args.to, false, Duration::from_secs(30)).await?
    };

    if args.verify_only {
        return verify(&source, &target).await;
    }

    let src_total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM records")
        .fetch_one(&source)
        .await?;
    let mut last_id = if args.resume {
        target_max_id(&target).await
    } else {
        0
    };
    if last_id > 0 {
        println!("resuming after id={last_id}");
    }
    println!("source rows: {src_total}, batch={}", args.batch);

    let started = Instant::now();
    let mut migrated = 0_i64;
    loop {
        let rows = sqlx::query(&format!(
            "SELECT {RECORD_COLUMNS} FROM records WHERE id > ? ORDER BY id LIMIT ?"
        ))
        .bind(last_id)
        .bind(args.batch as i64)
        .fetch_all(&source)
        .await?;
        if rows.is_empty() {
            break;
        }
        let max_id = rows
            .last()
            .map(|r| r.get::<i64, _>("id"))
            .unwrap_or(last_id);
        let payloads = sqlx::query(
            "SELECT record_id, codec, dict_id, request, response, headers, \
             request_raw_len, response_raw_len, headers_raw_len \
             FROM payloads WHERE record_id > ? AND record_id <= ? ORDER BY record_id",
        )
        .bind(last_id)
        .bind(max_id)
        .fetch_all(&source)
        .await?;

        let conn = target.write().await;
        conn.begin_immediate().await?;
        for row in &rows {
            conn.execute(INSERT_RECORD, record_params(row)).await?;
        }
        if !payloads.is_empty() {
            for payload in &payloads {
                conn.execute(
                    "INSERT INTO payloads (record_id, codec, dict_id, request, response, headers, \
                     request_raw_len, response_raw_len, headers_raw_len) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    payload_params(payload),
                )
                .await?;
            }
        }
        conn.commit().await?;
        drop(conn);

        migrated += rows.len() as i64;
        last_id = max_id;
        let elapsed = started.elapsed().as_secs_f64();
        if migrated >= args.limit {
            println!("stopping at --limit {migrated}");
            break;
        }
        println!(
            "id={last_id} rows={migrated}/{src_total} ({:.0} rows/s, {:.1}s)",
            migrated as f64 / elapsed.max(0.001),
            elapsed
        );
    }

    println!(
        "migration finished: {migrated} rows in {:.1}s",
        started.elapsed().as_secs_f64()
    );
    if args.defer_fts {
        let started = Instant::now();
        println!("building FTS index...");
        target.create_fts_index().await?;
        println!("FTS index built in {:.1}s", started.elapsed().as_secs_f64());
        if let Err(e) = target.checkpoint_truncate().await {
            eprintln!("final checkpoint failed: {e}");
        }
    }

    if migrated == src_total {
        verify(&source, &target).await
    } else {
        println!("partial migration ({migrated}/{src_total}); skipping full verification");
        Ok(())
    }
}
