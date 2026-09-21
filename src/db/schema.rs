//! 审计库的 schema 定义与迁移逻辑。
//!
//! 集中维护表结构版本、增量列/索引、payloads 与 FTS5 全文检索 DDL，
//! 以及 `migrate` 迁移入口。`legacy.rs` 复用这里的常量完成遗留归档迁移。

use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use tracing::{info, warn};

pub(crate) const SCHEMA_VERSION: i64 = 7;

/// v1 新增列；经 `PRAGMA table_info` 守卫后逐列 `ADD COLUMN`，以兼容旧库与测试套件预建的表结构。
pub(crate) const NEW_COLUMNS: &[(&str, &str)] = &[
    ("TimeMs", "INTEGER"),
    ("Method", "TEXT"),
    ("Endpoint", "TEXT"),
    ("Backend", "TEXT"),
    ("SessionId", "TEXT"),
    ("ParentSessionId", "TEXT"),
    ("RequestId", "TEXT"),
    ("SessionAffinity", "TEXT"),
    ("UserAgent", "TEXT"),
    ("ClientName", "TEXT"),
    ("ClientVersion", "TEXT"),
    ("ApiKey", "TEXT"),
    ("Status", "INTEGER"),
    ("Error", "TEXT"),
    ("RetryCount", "INTEGER"),
    ("FinishReason", "TEXT"),
    ("LatencyMs", "REAL"),
    ("TtftMs", "REAL"),
    ("UpstreamMs", "REAL"),
    ("StreamMs", "REAL"),
    ("RequestBytes", "INTEGER"),
    ("ResponseBytes", "INTEGER"),
    ("PromptBytes", "INTEGER"),
    ("RequestTailBytes", "INTEGER"),
    ("AnswerBytes", "INTEGER"),
    ("MessageCount", "INTEGER"),
    ("SystemCount", "INTEGER"),
    ("ToolCount", "INTEGER"),
    ("AssistantCount", "INTEGER"),
    ("ToolResultCount", "INTEGER"),
    ("ImageCount", "INTEGER"),
    ("Prompt", "TEXT"),
    ("RequestTail", "TEXT"),
    ("Answer", "TEXT"),
    ("ToolNames", "TEXT"),
    ("payload_id", "INTEGER"),
];

/// v1 索引：(SQL, 依赖列)。仅当依赖列齐备时创建，避免测试预建表缺列导致失败。
pub(crate) const NEW_INDEXES: &[(&str, &[&str])] = &[
    (
        "CREATE INDEX IF NOT EXISTS idx_records_time ON records(TimeMs)",
        &["TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_type_time ON records(Type, TimeMs)",
        &["Type", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_api_key_time ON records(ApiKey, TimeMs)",
        &["ApiKey", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_model_time ON records(Model COLLATE NOCASE, TimeMs)",
        &["Model", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_ip_time ON records(IP COLLATE NOCASE, TimeMs)",
        &["IP", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_backend_time ON records(Backend, TimeMs)",
        &["Backend", "TimeMs"],
    ),
];

/// v6 移除的索引：筛选谓词为 `LIKE '%v%'` 时无法命中 B-tree，或列本身无区分度。
/// 它们只增加写入成本，不带来读取收益；若将来改为范围前缀检索，可按需重建。
const DROPPED_INDEXES: &[&str] = &[
    "idx_records_status_time",
    "idx_records_model_time",
    "idx_records_session",
    "idx_records_parent_session",
    "idx_records_request_id",
    "idx_records_ip_time",
    "idx_records_list_covering",
    "idx_records_api_key",
];

pub(crate) const PAYLOADS_DDL: &str = r#"
CREATE TABLE IF NOT EXISTS payloads (
    record_id INTEGER PRIMARY KEY REFERENCES records(id),
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

/// v3 全文检索：external-content + trigram（2 字中文用 LIKE，≥3 字用 MATCH）。
pub(crate) const FTS_DDL: &[&str] = &[
    "CREATE VIRTUAL TABLE IF NOT EXISTS records_fts USING fts5(\
         Prompt, RequestTail, Answer, \
         content='records', content_rowid='id', tokenize='trigram')",
    "CREATE TRIGGER IF NOT EXISTS records_fts_ai AFTER INSERT ON records BEGIN \
         INSERT INTO records_fts(rowid, Prompt, RequestTail, Answer) \
         VALUES (new.id, new.Prompt, new.RequestTail, new.Answer); END",
    "CREATE TRIGGER IF NOT EXISTS records_fts_ad AFTER DELETE ON records BEGIN \
         INSERT INTO records_fts(records_fts, rowid, Prompt, RequestTail, Answer) \
         VALUES ('delete', old.id, old.Prompt, old.RequestTail, old.Answer); END",
    "CREATE TRIGGER IF NOT EXISTS records_fts_au AFTER UPDATE ON records BEGIN \
         INSERT INTO records_fts(records_fts, rowid, Prompt, RequestTail, Answer) \
         VALUES ('delete', old.id, old.Prompt, old.RequestTail, old.Answer); \
         INSERT INTO records_fts(rowid, Prompt, RequestTail, Answer) \
         VALUES (new.id, new.Prompt, new.RequestTail, new.Answer); END",
];

/// 读取指定表的现有列名。
pub(crate) async fn table_columns(
    pool: &SqlitePool,
    table: &str,
) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query(&format!("PRAGMA table_info({})", table))
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect())
}

pub(crate) async fn migrate(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(pool)
        .await?;
    if version >= SCHEMA_VERSION {
        return Ok(());
    }

    let existing = table_columns(pool, "records").await?;

    for (name, decl) in NEW_COLUMNS {
        if !existing.iter().any(|c| c == name) {
            sqlx::query(&format!("ALTER TABLE records ADD COLUMN {} {}", name, decl))
                .execute(pool)
                .await?;
        }
    }

    let has_col =
        |c: &str| existing.iter().any(|e| e == c) || NEW_COLUMNS.iter().any(|(n, _)| *n == c);

    if version < 6 {
        for name in DROPPED_INDEXES {
            sqlx::query(&format!("DROP INDEX IF EXISTS {name}"))
                .execute(pool)
                .await?;
        }
    }

    for (sql, required) in NEW_INDEXES {
        if required.iter().all(|c| has_col(c)) {
            sqlx::query(sql).execute(pool).await?;
        }
    }

    sqlx::query(PAYLOADS_DDL).execute(pool).await?;

    if version < 3 && has_col("Prompt") && has_col("RequestTail") && has_col("Answer") {
        let mut fts_ready = true;
        for sql in FTS_DDL {
            if let Err(e) = sqlx::query(sql).execute(pool).await {
                warn!("Failed to initialize FTS5 search index: {}", e);
                fts_ready = false;
                break;
            }
        }
        if fts_ready {
            let _ = sqlx::query("INSERT INTO records_fts(records_fts) VALUES('rebuild')")
                .execute(pool)
                .await;
        }
    }

    if version < 6 {
        let _ = sqlx::query("PRAGMA optimize").execute(pool).await;
    }

    // PRAGMA 不支持绑定参数；此处为编译期常量，无注入风险。
    sqlx::query(&format!("PRAGMA user_version = {}", SCHEMA_VERSION))
        .execute(pool)
        .await?;

    info!("Database schema migrated to version {}", SCHEMA_VERSION);
    Ok(())
}
