use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::str::FromStr;
use std::time::Duration;
use tracing::info;

use crate::config::types::Config;

pub mod archive;
pub mod extract;
pub mod legacy;
pub mod payload;
pub mod records;
pub mod records_query;
pub mod rotation;
pub mod schema;

pub use rotation::{check_and_rotate, init_rotation_state, rotate_if_needed};

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

    schema::migrate(&pool).await?;

    info!("Database at '{}' initialized successfully", db_path);
    Ok(pool)
}

/// 解析 `RECD_PATH` 得到数据库文件路径（去掉 `sqlite:` 前缀）。
pub(crate) fn resolve_db_path() -> std::path::PathBuf {
    let url = std::env::var("RECD_PATH").unwrap_or_else(|_| "sqlite:./record.db".to_string());
    std::path::PathBuf::from(url.strip_prefix("sqlite:").unwrap_or(&url))
}

/// 为 dashboard 只读分析查询开第二个连接池，与写池分离，避免大表聚合占满
/// 写池连接后饿死请求日志写入。
///
/// 不做 `read_only(true)`：只读连接无法在 NFS 上恢复热日志（hot journal）。
/// 也不设置 `journal_mode`：默认 `delete` 下读写提交互斥，写池与查询池各自独立。
pub async fn init_query_pool() -> Result<SqlitePool, sqlx::Error> {
    let path = resolve_db_path();
    let path_str = path.to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path_str)?.create_if_missing(false);
    SqlitePoolOptions::new()
        .max_connections(16)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options)
        .await
}
