use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;
use tracing::{info, warn};

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
    init_db_pool_with_url(&database_url).await
}

async fn init_db_pool_with_url(database_url: &str) -> Result<SqlitePool, sqlx::Error> {
    let db_path = database_url
        .strip_prefix("sqlite:")
        .unwrap_or(&database_url);

    let options = SqliteConnectOptions::from_str(db_path)?.create_if_missing(true);

    // 连接前判断文件是否已存在：全新库（本次刚创建）必须走普通初始化，
    // 只有"已存在且仍是 V0 结构"的老库才需要阻塞式 legacy 数据迁移。
    let db_exists = std::path::Path::new(db_path).exists();
    let mut pool = SqlitePool::connect_with(options).await?;

    // 兼容测试套件的 12 列基表；已有库不会重复创建。
    ensure_base_records_table(&pool).await?;

    let existing = schema::table_columns(&pool, "records").await?;

    // V0 遗留活跃库（缺 TimeMs）：迁移成功才继续使用原文件；失败则隔离原文件，
    // 让服务用全新 active 库启动，避免容器因同一个坏库无限重启阻塞。
    if db_exists && is_legacy_schema(&existing) {
        info!(
            "Active database '{}' is V0 (no TimeMs); running blocking legacy migration...",
            db_path
        );
        pool.close().await;
        if let Err(error) = migrate_database_or_quarantine(Path::new(db_path)).await {
            if is_sqlite_busy_error(&error) {
                return Err(error);
            }
            warn!(
                "Active database legacy migration failed for '{}': {}; quarantining and creating a fresh active database",
                db_path, error
            );
            let fresh_options = SqliteConnectOptions::from_str(db_path)?.create_if_missing(true);
            pool = SqlitePool::connect_with(fresh_options).await?;
            ensure_base_records_table(&pool).await?;
        } else {
            info!("Active database legacy migration completed.");
            let reopened_options =
                SqliteConnectOptions::from_str(db_path)?.create_if_missing(false);
            pool = SqlitePool::connect_with(reopened_options).await?;
        }
    }

    schema::migrate(&pool).await?;

    info!("Database at '{}' initialized successfully", db_path);
    Ok(pool)
}

pub(crate) async fn migrate_database_in_place(path: &Path) -> Result<(), sqlx::Error> {
    let path_str = path.to_string_lossy();
    let options = SqliteConnectOptions::from_str(&path_str)?
        .read_only(false)
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options)
        .await?;
    let result = legacy::migrate_archive(&pool).await;
    pool.close().await;
    result
}

pub(crate) async fn migrate_database_or_quarantine(path: &Path) -> Result<(), sqlx::Error> {
    if let Err(error) = migrate_database_in_place(path).await {
        if is_sqlite_busy_error(&error) {
            return Err(error);
        }
        let quarantined = quarantine_database(path).map_err(|quarantine_error| {
            sqlx::Error::Protocol(format!(
                "database migration failed: {error}; quarantine failed: {quarantine_error}"
            ))
        })?;
        warn!(
            "Failed database quarantined: '{}' -> '{}': {}",
            path.display(),
            quarantined.display(),
            error
        );
        return Err(sqlx::Error::Protocol(format!(
            "database migration failed; quarantined as '{}'",
            quarantined.display()
        )));
    }
    Ok(())
}

pub(crate) fn is_sqlite_busy_error(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(database_error) => database_error.code().is_some_and(|code| {
            code.eq_ignore_ascii_case("SQLITE_BUSY")
                || code.eq_ignore_ascii_case("SQLITE_LOCKED")
                || code
                    .parse::<i32>()
                    .is_ok_and(|code| matches!(code & 0xff, 5 | 6))
        }),
        _ => false,
    }
}

async fn ensure_base_records_table(pool: &SqlitePool) -> Result<(), sqlx::Error> {
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
    .execute(pool)
    .await?;
    Ok(())
}

/// V0 遗留 schema 判定：`records` 表缺少 `TimeMs` 列。
/// 仅凭列判断不足以区分全新库与老 V0 库，调用方还需叠加文件存在性判断。
fn is_legacy_schema(columns: &[String]) -> bool {
    !columns.iter().any(|c| c == "TimeMs")
}

#[cfg(test)]
mod tests {
    use super::{init_db_pool_with_url, is_legacy_schema};
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::Row;
    use std::str::FromStr;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!("qq_db_{}_{}_{}", std::process::id(), tag, nanos))
    }

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

    #[tokio::test]
    async fn quarantines_failed_active_migration_and_starts_fresh() {
        let dir = temp_dir("active_quarantine");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("record.db");
        let url = format!("sqlite:{}", path.display());
        let options = SqliteConnectOptions::from_str(&url)
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE records (Time TEXT PRIMARY KEY, IP TEXT, Model TEXT, Request TEXT, Response TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool.close().await;

        let fresh = init_db_pool_with_url(&url).await.unwrap();
        let columns = sqlx::query("PRAGMA table_info(records)")
            .fetch_all(&fresh)
            .await
            .unwrap();
        let names: Vec<String> = columns.iter().map(|row| row.get("name")).collect();
        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&fresh)
            .await
            .unwrap();

        assert!(path.exists());
        assert!(dir.join("record.db.bak").exists());
        assert!(names.iter().any(|name| name == "TimeMs"));
        assert_eq!(version, crate::db::schema::SCHEMA_VERSION);

        fresh.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn keeps_locked_archive_in_place() {
        let dir = temp_dir("locked_archive");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("record_202608.db");
        let url = format!("sqlite:{}", path.display());
        let options = SqliteConnectOptions::from_str(&url)
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(options)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE records (id INTEGER PRIMARY KEY, Time TEXT, Request TEXT, Response TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let mut transaction = pool.begin().await.unwrap();
        sqlx::query("INSERT INTO records (Time, Request, Response) VALUES ('now', '{}', '{}')")
            .execute(&mut *transaction)
            .await
            .unwrap();

        let result = super::migrate_database_or_quarantine(&path).await;

        let error = result.expect_err("the active transaction must hold a write lock");
        assert!(super::is_sqlite_busy_error(&error));
        assert!(path.exists());
        assert!(!dir.join("record_202608.db.bak").exists());

        transaction.rollback().await.unwrap();
        pool.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// 解析 `RECD_PATH` 得到数据库文件路径（去掉 `sqlite:` 前缀）。
pub(crate) fn resolve_db_path() -> std::path::PathBuf {
    let url = std::env::var("RECD_PATH").unwrap_or_else(|_| "sqlite:./record.db".to_string());
    std::path::PathBuf::from(url.strip_prefix("sqlite:").unwrap_or(&url))
}

pub(crate) fn quarantine_database(path: &Path) -> std::io::Result<PathBuf> {
    let backup = unique_backup_path(path);
    std::fs::rename(path, &backup)?;
    for suffix in ["-journal", "-wal", "-shm"] {
        let sidecar = PathBuf::from(format!("{}{}", path.display(), suffix));
        if sidecar.exists() {
            let sidecar_backup = unique_backup_path(&sidecar);
            std::fs::rename(sidecar, sidecar_backup)?;
        }
    }
    Ok(backup)
}

fn unique_backup_path(path: &Path) -> PathBuf {
    let base = PathBuf::from(format!("{}.bak", path.display()));
    let mut candidate = base.clone();
    let mut suffix = 1u32;
    while candidate.exists() {
        candidate = PathBuf::from(format!("{}.{}", base.display(), suffix));
        suffix = suffix.saturating_add(1);
    }
    candidate
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
