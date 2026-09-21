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

    // 连接前判断文件是否已存在：全新库（本次刚创建）必须走普通初始化，
    // 只有"已存在且仍是 V0 结构"的老库才需要阻塞式 legacy 数据迁移。
    let db_exists = std::path::Path::new(db_path).exists();
    let pool = SqlitePool::connect_with(options).await?;

    // 兼容测试套件的 12 列基表；已有库不会重复创建。
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

    let existing = schema::table_columns(&pool, "records").await?;

    // V0 遗留活跃库（缺 TimeMs 列）：旧数据仍是明文正文且没有 TimeMs/payloads/FTS，
    // 必须在这里阻塞启动并完整迁移（schema + 数据回填一起），否则服务会带着
    // 无法按时间检索、详情回放为空的数据上线。迁移失败直接阻止启动。
    if db_exists && is_legacy_schema(&existing) {
        info!(
            "Active database '{}' is V0 (no TimeMs); running blocking legacy migration...",
            db_path
        );
        legacy::migrate_archive(&pool).await?;
        info!("Active database legacy migration completed.");
    }

    schema::migrate(&pool).await?;

    info!("Database at '{}' initialized successfully", db_path);
    Ok(pool)
}

/// V0 遗留 schema 判定：`records` 表缺少 `TimeMs` 列。
/// 仅凭列判断不足以区分全新库与老 V0 库，调用方还需叠加文件存在性判断。
fn is_legacy_schema(columns: &[String]) -> bool {
    !columns.iter().any(|c| c == "TimeMs")
}

#[cfg(test)]
mod tests {
    use super::is_legacy_schema;

    #[test]
    fn legacy_schema_has_no_time_ms() {
        let v0 = ["id", "Time", "IP", "Model", "Type", "Request", "Response"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        assert!(is_legacy_schema(&v0));
    }

    #[test]
    fn modern_schema_has_time_ms() {
        let modern = ["id", "Time", "TimeMs", "Prompt", "ApiKey"]
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        assert!(!is_legacy_schema(&modern));
    }
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
