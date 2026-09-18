use chrono::Local;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool};
use sqlx::Row;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use tracing::{error, info, warn};

use crate::config::types::Config;
use crate::state::app_state::AppState;
use std::sync::Arc;

pub mod extract;
pub mod payload;
pub mod records;
pub mod rotation;

const SCHEMA_VERSION: i64 = 7;

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

/// 解析 `RECD_PATH` 得到数据库文件路径（去掉 `sqlite:` 前缀）。
fn resolve_db_path() -> std::path::PathBuf {
    let url = std::env::var("RECD_PATH").unwrap_or_else(|_| "sqlite:./record.db".to_string());
    std::path::PathBuf::from(url.strip_prefix("sqlite:").unwrap_or(&url))
}

/// 当前 record.db 中 MAX(TimeMs) 所属的本地月份；空表/无值返回 None。
async fn current_data_yyyymm(app_state: &Arc<AppState>) -> Option<i32> {
    let pool = app_state.db_pool.read().await;
    let max: Option<i64> = sqlx::query_scalar("SELECT MAX(TimeMs) FROM records")
        .fetch_one(&*pool)
        .await
        .ok()
        .flatten();
    max.and_then(rotation::yyyymm_from_ms)
}

/// 启动时确定内存中的 active 月份与边界（数据驱动，不 stat 文件）。
pub async fn init_rotation_state(app_state: &Arc<AppState>) {
    let now_month = rotation::yyyymm(Local::now());
    let active = current_data_yyyymm(app_state).await.unwrap_or(now_month);
    app_state.active_yyyymm.store(active, Ordering::Release);
    app_state
        .next_month_boundary_ms
        .store(rotation::next_month_boundary_ms(active), Ordering::Release);
}

/// 轮转动作；调用方必须已持有 db_rotation_lock。
async fn rotate_locked(app_state: &Arc<AppState>, seal_yyyymm: i32) {
    let db_path = resolve_db_path();
    if !db_path.exists() {
        return;
    }

    let archive_dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let mut archive_path = archive_dir.join(format!("record_{:06}.db", seal_yyyymm));

    info!(
        "Database rotation needed. Archiving {} to {}",
        db_path.display(),
        archive_path.display()
    );

    // 同名归档已存在时追加 unix 秒后缀，避免覆盖历史归档。
    if archive_path.exists() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        archive_path = archive_dir.join(format!("record_{:06}_{}.db", seal_yyyymm, timestamp));
        warn!(
            "Archive file already exists. Renaming to {}",
            archive_path.display()
        );
    }

    // 获取写锁以阻塞新的请求写入，安全完成轮转。
    let mut pool_guard = app_state.db_pool.write().await;

    // 关闭当前连接池以释放文件锁。
    pool_guard.close().await;

    match fs::rename(&db_path, &archive_path) {
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
            let now_month = rotation::yyyymm(Local::now());
            app_state.active_yyyymm.store(now_month, Ordering::Release);
            app_state.next_month_boundary_ms.store(
                rotation::next_month_boundary_ms(now_month),
                Ordering::Release,
            );
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

/// 后台数据驱动检查。
pub async fn check_and_rotate(app_state: &Arc<AppState>) {
    let _lock = app_state.db_rotation_lock.lock().await;
    let now_month = rotation::yyyymm(Local::now());
    if let Some(data_month) = current_data_yyyymm(app_state).await {
        if data_month < now_month {
            rotate_locked(app_state, data_month).await;
            return;
        }
        if data_month != app_state.active_yyyymm.load(Ordering::Acquire) {
            app_state.active_yyyymm.store(data_month, Ordering::Release);
            app_state.next_month_boundary_ms.store(
                rotation::next_month_boundary_ms(data_month),
                Ordering::Release,
            );
        }
    }
}

/// 写路径零成本边界检查；仅在跨月那一刻才做一次 DB 查询与轮转。
pub async fn rotate_if_needed(app_state: &Arc<AppState>, time_ms: i64) {
    if time_ms < app_state.next_month_boundary_ms.load(Ordering::Acquire) {
        return; // 快路径：一次原子加载 + 比较
    }
    let _lock = app_state.db_rotation_lock.lock().await;
    if time_ms < app_state.next_month_boundary_ms.load(Ordering::Acquire) {
        return; // 获取锁后二次检查，避免重复轮转
    }
    let now_month = rotation::yyyymm(Local::now());
    let seal = match current_data_yyyymm(app_state).await {
        Some(d) if d < now_month => d,
        _ => app_state.active_yyyymm.load(Ordering::Acquire),
    };
    rotate_locked(app_state, seal).await;
}
