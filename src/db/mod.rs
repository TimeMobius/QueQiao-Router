use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::info;

pub(crate) mod aggregate;
pub mod archive;
pub mod engine;
pub mod extract;
pub mod legacy;
pub mod migrate;
pub(crate) mod migrate_verify;
pub mod payload;
pub mod records;
pub mod records_query;
pub mod rotation;
pub mod schema;
pub mod store;
pub mod turso_schema;
pub mod writer;

pub use rotation::{check_and_rotate, init_rotation_state, rotate_if_needed};

/// Map a legacy sqlx error into the engine error type, preserving busy/conflict
/// classification so the archive scanner can distinguish "retry later" from
/// "permanently broken".
pub(crate) fn map_sqlx_error(error: sqlx::Error) -> engine::DbError {
    if is_sqlite_retryable_error(&error) {
        engine::DbError::Driver(turso::Error::Busy(error.to_string()))
    } else {
        engine::DbError::Shape(error.to_string())
    }
}

pub(crate) fn is_sqlite_retryable_error(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(database_error) => database_error.code().is_some_and(|code| {
            code.eq_ignore_ascii_case("SQLITE_BUSY")
                || code.eq_ignore_ascii_case("SQLITE_LOCKED")
                || code
                    .parse::<i32>()
                    .is_ok_and(|code| matches!(code & 0xff, 5 | 6 | 8))
        }),
        _ => false,
    }
}

/// Open the active audit database with the v8 schema.
pub async fn init_db() -> Result<std::sync::Arc<store::AuditStore>, engine::DbError> {
    let path = resolve_db_path();
    match migrate::detect_schema(&path).await? {
        migrate::SchemaKind::V7 | migrate::SchemaKind::Legacy => {
            migrate::migrate_path_to_v8(&path).await?;
        }
        migrate::SchemaKind::Newer(version) => {
            return Err(engine::DbError::Shape(format!(
                "audit database schema v{version} is newer than this binary (v8)"
            )))
        }
        migrate::SchemaKind::V8 => {
            migrate::migrate_path_to_v8(&path).await?;
        }
        migrate::SchemaKind::Empty | migrate::SchemaKind::Unknown => {}
    }
    let db = store::AuditStore::open(&path, false, Duration::from_secs(30)).await?;
    info!("Audit database at '{}' initialized (v8)", path.display());
    Ok(db)
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

pub(crate) fn unique_backup_path(path: &Path) -> PathBuf {
    let base = PathBuf::from(format!("{}.bak", path.display()));
    let mut candidate = base.clone();
    let mut suffix = 1u32;
    while candidate.exists()
        || ["-journal", "-wal", "-shm"]
            .iter()
            .any(|sidecar| PathBuf::from(format!("{}{}", candidate.display(), sidecar)).exists())
    {
        candidate = PathBuf::from(format!("{}.{}", base.display(), suffix));
        suffix = suffix.saturating_add(1);
    }
    candidate
}

#[cfg(test)]
mod tests {
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use std::str::FromStr;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!("qq_db_{}_{}_{}", std::process::id(), tag, nanos))
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

        let result = crate::db::migrate::migrate_path_to_v8(&path).await;

        let error = result.expect_err("the active transaction must hold a write lock");
        assert!(error.is_busy());
        assert!(path.exists());
        assert!(!dir.join("record_202608.db.bak").exists());

        transaction.rollback().await.unwrap();
        pool.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod migration_tests;
