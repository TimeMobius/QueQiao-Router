use chrono::{DateTime, Datelike, Local};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use sqlx::Row;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use tracing::{error, info, warn};

use crate::config::types::Config;
use crate::state::app_state::AppState;
use std::sync::Arc;

pub mod extract;
pub mod payload;
pub mod records;

const SCHEMA_VERSION: i64 = 5;

/// v1 新增列；经 `PRAGMA table_info` 守卫后逐列 `ADD COLUMN`，以兼容旧库与测试套件预建的表结构。
const NEW_COLUMNS: &[(&str, &str)] = &[
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
const NEW_INDEXES: &[(&str, &[&str])] = &[
    (
        "CREATE INDEX IF NOT EXISTS idx_records_time ON records(TimeMs)",
        &["TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_type_time ON records(Type, TimeMs)",
        &["Type", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_model_time ON records(Model, TimeMs)",
        &["Model", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_status_time ON records(Status, TimeMs)",
        &["Status", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_session ON records(SessionId)",
        &["SessionId"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_parent_session ON records(ParentSessionId)",
        &["ParentSessionId"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_request_id ON records(RequestId)",
        &["RequestId"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_api_key ON records(ApiKey)",
        &["ApiKey"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_ip_time ON records(IP, TimeMs)",
        &["IP", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_backend_time ON records(Backend, TimeMs)",
        &["Backend", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_list_covering ON records(\
             Type, TimeMs DESC, id, Model, Status, Backend, LatencyMs, TtftMs, \
             PromptTokens, CompletionTokens, TotalTokens, MessageCount, ToolCount, FinishReason)",
        &[
            "Type",
            "TimeMs",
            "Model",
            "Status",
            "Backend",
            "LatencyMs",
            "TtftMs",
            "PromptTokens",
            "CompletionTokens",
            "TotalTokens",
            "MessageCount",
            "ToolCount",
            "FinishReason",
        ],
    ),
];

const PAYLOADS_DDL: &str = r#"
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
const FTS_DDL: &[&str] = &[
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
async fn table_columns(pool: &SqlitePool, table: &str) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query(&format!("PRAGMA table_info({})", table))
        .fetch_all(pool)
        .await?;
    Ok(rows
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect())
}

async fn migrate(pool: &SqlitePool) -> Result<(), sqlx::Error> {
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

    // PRAGMA 不支持绑定参数；此处为编译期常量，无注入风险。
    sqlx::query(&format!("PRAGMA user_version = {}", SCHEMA_VERSION))
        .execute(pool)
        .await?;

    info!("Database schema migrated to version {}", SCHEMA_VERSION);
    Ok(())
}

/// 初始化数据库连接池
pub async fn init_db_pool(_config: &Config) -> Result<SqlitePool, sqlx::Error> {
    let database_url =
        std::env::var("RECD_PATH").unwrap_or_else(|_| "sqlite:./record.db".to_string());

    let db_path = database_url
        .strip_prefix("sqlite:")
        .unwrap_or(&database_url);

    let options = SqliteConnectOptions::from_str(db_path)?.create_if_missing(true);

    let pool = SqlitePool::connect_with(options).await?;

    // 保留原表名与 12 列以兼容测试套件；新增列由 migrate 补齐
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Time TEXT,
            IP TEXT,
            Model TEXT,
            Type TEXT,
            CompletionTokens INTEGER,
            PromptTokens INTEGER,
            TotalTokens INTEGER,
            Tool BOOLEAN,
            Multimodal BOOLEAN,
            Headers TEXT,
            Request TEXT,
            Response TEXT
        )
        "#,
    )
    .execute(&pool)
    .await?;

    migrate(&pool).await?;

    info!("Database at '{}' initialized successfully", db_path);
    Ok(pool)
}

/// 检查并轮换数据库
pub async fn check_and_rotate(app_state: &Arc<AppState>) {
    let _lock = app_state.db_rotation_lock.lock().await;

    let db_path_with_prefix =
        std::env::var("RECD_PATH").unwrap_or_else(|_| "sqlite:./record.db".to_string());

    // Strip the "sqlite:" prefix to get the actual file path
    let db_path_str = db_path_with_prefix
        .strip_prefix("sqlite:")
        .unwrap_or(&db_path_with_prefix);

    let db_path = Path::new(db_path_str);

    if !db_path.exists() {
        return;
    }

    let metadata = match fs::metadata(db_path) {
        Ok(meta) => meta,
        Err(e) => {
            error!("Failed to get metadata for database file: {}", e);
            return;
        }
    };

    // 同时获取修改时间和创建时间，取最早的值
    // 这样既能保证测试通过（测试设置mtime，ctime会被系统更新）
    // 也能在实际场景中正确处理（如果created时间更早的话）
    let mtime = metadata.modified().ok();
    let ctime = metadata.created().ok();

    let earliest_time = match (mtime, ctime) {
        (Some(mt), Some(ct)) => {
            if mt < ct {
                mt
            } else {
                ct
            }
        }
        (Some(mt), None) => mt,
        (None, Some(ct)) => ct,
        (None, None) => {
            error!("无法获取文件的时间信息");
            return;
        }
    };

    let mod_datetime: DateTime<Local> = earliest_time.into();
    let now = Local::now();

    if mod_datetime.year() != now.year() || mod_datetime.month() != now.month() {
        let archive_dir = db_path.parent().unwrap_or_else(|| Path::new("."));
        let archive_filename = format!("record_{}.db", mod_datetime.format("%Y%m"));
        let mut archive_path = archive_dir.join(&archive_filename);

        info!(
            "Database rotation needed. Archiving {} to {}",
            db_path.display(),
            archive_path.display()
        );

        if archive_path.exists() {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let new_archive_filename =
                format!("record_{}_{}.db", mod_datetime.format("%Y%m"), timestamp);
            archive_path = archive_dir.join(new_archive_filename);
            warn!(
                "Archive file already exists. Renaming to {}",
                archive_path.display()
            );
        }

        // Acquire write lock to block new requests and safely rotate
        let mut pool_guard = app_state.db_pool.write().await;

        // Close the current connection pool to release file locks
        pool_guard.close().await;

        match fs::rename(db_path, &archive_path) {
            Ok(_) => info!("Database archived successfully."),
            Err(e) => {
                error!("Failed to archive database: {}", e);
            }
        }

        info!("Re-initializing database pool after rotation.");
        let config = app_state.config_manager.get_config().await;
        match init_db_pool(&config).await {
            Ok(new_pool) => {
                *pool_guard = new_pool;
                info!("Database pool re-initialized successfully.");
            }
            Err(e) => {
                error!(
                    "Failed to re-initialize database pool after rotation: {}",
                    e
                );
            }
        }
    }
}
