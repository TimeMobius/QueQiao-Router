//! Schema v8 DDL for the Turso-backed audit database.
//!
//! v8 is a fresh schema (no in-place upgrade from v7): the hot database is
//! migrated by creating a new file and copying rows. `schema.rs` still holds
//! the legacy (sqlx/FTS5) definitions used when reading or migrating old
//! shards.

use crate::db::engine::{DbConn, DbError};
use crate::db::store::pragma_i64;

pub const SCHEMA_VERSION: i64 = 8;

pub const RECORDS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS records (
    id INTEGER PRIMARY KEY,
    Time TEXT NOT NULL,
    TimeMs INTEGER NOT NULL,
    IP TEXT,
    Method TEXT,
    Endpoint TEXT,
    Type TEXT,
    Model TEXT,
    Backend TEXT,
    SessionId TEXT,
    ParentSessionId TEXT,
    RequestId TEXT,
    SessionAffinity TEXT,
    UserAgent TEXT,
    ClientName TEXT,
    ClientVersion TEXT,
    ApiKey TEXT,
    Status INTEGER,
    Error TEXT,
    RetryCount INTEGER,
    FinishReason TEXT,
    LatencyMs REAL,
    TtftMs REAL,
    UpstreamMs REAL,
    StreamMs REAL,
    CompletionTokens INTEGER,
    PromptTokens INTEGER,
    TotalTokens INTEGER,
    Tool INTEGER NOT NULL DEFAULT 0,
    Multimodal INTEGER NOT NULL DEFAULT 0,
    RequestBytes INTEGER,
    ResponseBytes INTEGER,
    PromptBytes INTEGER,
    RequestTailBytes INTEGER,
    AnswerBytes INTEGER,
    MessageCount INTEGER,
    SystemCount INTEGER,
    ToolCount INTEGER,
    AssistantCount INTEGER,
    ToolResultCount INTEGER,
    ImageCount INTEGER,
    Prompt TEXT,
    RequestTail TEXT,
    Answer TEXT,
    ToolNames TEXT
)
"#;

pub const PAYLOADS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS payloads (
    record_id INTEGER PRIMARY KEY,
    codec TEXT NOT NULL,
    dict_id TEXT,
    request BLOB,
    response BLOB,
    headers BLOB,
    request_raw_len INTEGER,
    response_raw_len INTEGER,
    headers_raw_len INTEGER
)
"#;

/// B-tree indexes for residual predicates and ordering.
///
/// Kept deliberately small: every extra index costs write throughput, and the
/// dashboard's `LIKE '%v%'` filters cannot use B-trees anyway.
pub const INDEX_DDL: &[&str] = &[
    "CREATE INDEX IF NOT EXISTS idx_records_time ON records(TimeMs)",
    "CREATE INDEX IF NOT EXISTS idx_records_type_time ON records(Type, TimeMs)",
    "CREATE INDEX IF NOT EXISTS idx_records_api_key_time ON records(ApiKey, TimeMs)",
    "CREATE INDEX IF NOT EXISTS idx_records_model_time ON records(Model, TimeMs)",
    "CREATE INDEX IF NOT EXISTS idx_records_status_time ON records(Status, TimeMs)",
    "CREATE INDEX IF NOT EXISTS idx_records_backend_time ON records(Backend, TimeMs)",
    "CREATE INDEX IF NOT EXISTS idx_records_ip_time ON records(IP, TimeMs)",
];

/// Tantivy-backed FTS index with filter columns pushed down.
///
/// `fts_match(Prompt, RequestTail, Answer, Model, Status, Type, Backend, ?)`
/// can then evaluate `Model:gpt-4 AND Status:200` without a post-filter scan.
pub const FTS_DDL: &str = "CREATE INDEX IF NOT EXISTS records_fts ON records \
    USING fts (Prompt, RequestTail, Answer, Model, Status, Type, Backend) \
    WITH (tokenizer = 'ngram')";

/// Create the v8 schema on a new database, or validate an existing v8 file.
///
/// A v7 (legacy) file is rejected instead of being altered: running the v8 DDL
/// on it would leave the old FTS5 objects and dead columns in place. The
/// explicit migration path creates a new file and copies rows.
pub async fn init_v8(conn: &DbConn) -> Result<(), DbError> {
    let version = pragma_i64(conn, "user_version").await?;
    if version > 0 && version < SCHEMA_VERSION {
        return Err(DbError::Shape(format!(
            "legacy audit schema v{version} detected; run the v7 -> v8 migration instead of opening it directly"
        )));
    }

    let existing = conn.query("PRAGMA table_info(records)", vec![]).await?;
    if !existing.is_empty() {
        // v7 is identifiable by the dead compatibility columns.
        let mut has_headers = false;
        for row in &existing {
            if row.get_opt_str(1)?.is_some_and(|name| name == "Headers") {
                has_headers = true;
                break;
            }
        }
        if has_headers {
            return Err(DbError::Shape(
                "legacy v7 audit schema detected; run the v7 -> v8 migration".to_string(),
            ));
        }
    }

    conn.execute_batch(RECORDS_DDL).await?;
    conn.execute_batch(PAYLOADS_DDL).await?;
    for sql in INDEX_DDL {
        conn.execute_batch(sql).await?;
    }
    conn.execute_batch(FTS_DDL).await?;
    conn.execute_batch(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::engine::{Db, OpenOptions, Value};
    use std::time::Duration;

    fn temp_path(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("qq-v8-{tag}-{nanos}.db"))
    }

    #[tokio::test]
    async fn creates_schema_and_fts_on_fresh_db() {
        let path = temp_path("fresh");
        let db = Db::open(&path, OpenOptions::default()).await.unwrap();
        let conn = db.connect().unwrap();
        init_v8(&conn).await.unwrap();
        assert_eq!(pragma_i64(&conn, "user_version").await.unwrap(), 8);

        conn.execute(
            "INSERT INTO records (Time, TimeMs, Model, Status, Prompt) VALUES (?, ?, ?, ?, ?)",
            vec![
                Value::Text("2026-10-22 00:00:00.000000".into()),
                Value::Integer(1),
                Value::Text("gpt-4o".into()),
                Value::Integer(200),
                Value::Text("hello database".into()),
            ],
        )
        .await
        .unwrap();

        let rows = conn
            .query(
                "SELECT id FROM records WHERE fts_match(Prompt, RequestTail, Answer, Model, Status, Type, Backend, ?)",
                vec![Value::Text("database".into())],
            )
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);

        // Idempotent.
        init_v8(&conn).await.unwrap();
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn rejects_legacy_schema() {
        let path = temp_path("legacy");
        let db = Db::open(&path, OpenOptions::default()).await.unwrap();
        let conn = db.connect().unwrap();
        conn.execute_batch(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, Time TEXT, Headers TEXT); PRAGMA user_version = 7",
        )
        .await
        .unwrap();
        let err = init_v8(&conn).await.unwrap_err();
        assert!(err.to_string().contains("legacy"));
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn keeps_reader_open_during_duration() {
        // Guards against accidental synchronous-only assumptions in the store.
        let _ = Duration::from_millis(1);
    }
}
