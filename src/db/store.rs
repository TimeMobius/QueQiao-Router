//! The audit store: one open database file plus its write connection and
//! reader pool.
//!
//! Rotation swaps the whole `Arc<AuditStore>`; in-flight readers keep their
//! clone (and therefore the old inode) while new requests use the new store.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard};

use crate::db::engine::{
    self, Db, DbConn, DbError, OpenOptions, ReaderGuard, ReaderPool, Row, Value,
};

/// Default number of concurrent readers for one shard.
pub const DEFAULT_MAX_READERS: usize = 8;

/// One open audit database (active or archived) with its IO handles.
pub struct AuditStore {
    path: PathBuf,
    db: Db,
    write: Mutex<DbConn>,
    readers: ReaderPool,
    read_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaKind {
    Empty,
    Legacy,
    V7,
    V8,
    Newer(i64),
    Unknown,
}

/// Read-only schema probe. Never opens the file writable.
pub async fn probe_schema(path: &Path) -> Result<SchemaKind, DbError> {
    if !path.exists() {
        return Ok(SchemaKind::Empty);
    }
    if std::fs::metadata(path).map(|m| m.len()).unwrap_or(0) == 0 {
        return Ok(SchemaKind::Empty);
    }
    let db = Db::open(
        path,
        OpenOptions {
            read_only: true,
            index_method: true,
            vacuum: false,
            busy_timeout: Duration::from_secs(5),
        },
    )
    .await?;
    let conn = db.connect()?;
    let version = pragma_i64(&conn, "user_version").await?;
    if version > 8 {
        return Ok(SchemaKind::Newer(version));
    }
    let columns = conn.query("PRAGMA table_info(records)", Vec::new()).await?;
    if columns.is_empty() {
        let user_object = conn
            .query_one(
                "SELECT 1 FROM sqlite_master WHERE substr(name, 1, 7) <> 'sqlite_' LIMIT 1",
                Vec::new(),
            )
            .await?;
        return Ok(if version == 0 && user_object.is_none() {
            SchemaKind::Empty
        } else {
            SchemaKind::Unknown
        });
    }
    let mut has_search = false;
    for row in &columns {
        if let Some(name) = row.get_opt_str(1)? {
            has_search |= name == "SearchText";
        }
    }
    if version == 7 {
        return Ok(SchemaKind::V7);
    }
    if has_search {
        return Ok(SchemaKind::V8);
    }
    if version < 7 {
        return Ok(SchemaKind::Legacy);
    }
    Ok(SchemaKind::Unknown)
}

impl AuditStore {
    /// Open a database. When writable, the v8 schema is created if the file is
    /// new; a legacy schema is rejected with a clear error instead of being
    /// silently altered (migration is an explicit, separate operation).
    pub async fn open(
        path: impl AsRef<Path>,
        read_only: bool,
        busy_timeout: Duration,
    ) -> Result<Arc<AuditStore>, DbError> {
        Self::open_with_fts(path, read_only, busy_timeout, true).await
    }

    /// Open a writable database without the FTS index; call
    /// [`AuditStore::create_fts_index`] after a bulk load.
    pub async fn open_deferred_fts(
        path: impl AsRef<Path>,
        busy_timeout: Duration,
    ) -> Result<Arc<AuditStore>, DbError> {
        Self::open_with_fts(path, false, busy_timeout, false).await
    }

    async fn open_with_fts(
        path: impl AsRef<Path>,
        read_only: bool,
        busy_timeout: Duration,
        with_fts: bool,
    ) -> Result<Arc<AuditStore>, DbError> {
        let path = path.as_ref().to_path_buf();
        let opts = OpenOptions {
            read_only,
            index_method: true,
            vacuum: false,
            busy_timeout,
        };
        if !read_only && path.exists() {
            match probe_schema(&path).await? {
                SchemaKind::Empty | SchemaKind::V8 => {}
                SchemaKind::Legacy => {
                    return Err(DbError::Shape(
                        "legacy audit database; automatic startup migration is required"
                            .to_string(),
                    ))
                }
                SchemaKind::V7 => {
                    return Err(DbError::Shape(
                        "legacy v7 audit database; run --migrate-db before opening it".to_string(),
                    ))
                }
                SchemaKind::Newer(version) => {
                    return Err(DbError::Shape(format!(
                        "audit schema v{version} is newer than this binary"
                    )))
                }
                SchemaKind::Unknown => {
                    return Err(DbError::Shape(
                        "unrecognized audit schema; refusing to open writable".to_string(),
                    ))
                }
            }
        }
        let db = Db::open(&path, opts).await?;
        let write_conn = db.connect()?;

        if !read_only {
            crate::db::turso_schema::init_v8_schema(&write_conn, with_fts).await?;
        }

        let readers = ReaderPool::new(db.clone(), DEFAULT_MAX_READERS, Duration::from_secs(5));
        Ok(Arc::new(AuditStore {
            path,
            db,
            write: Mutex::new(write_conn),
            readers,
            read_only,
        }))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub fn db(&self) -> &Db {
        &self.db
    }

    /// Acquire the single write connection. Writes are serialized in-process.
    pub async fn write(&self) -> MutexGuard<'_, DbConn> {
        self.write.lock().await
    }

    /// Acquire a pooled reader connection.
    pub async fn read(&self) -> Result<ReaderGuard, DbError> {
        self.readers.acquire().await
    }

    /// Convenience: run a read query on a pooled connection.
    pub async fn query(
        &self,
        sql: impl AsRef<str>,
        params: Vec<Value>,
    ) -> Result<Vec<Row>, DbError> {
        self.read().await?.query(sql, params).await
    }

    /// Convenience: run a read query returning at most one row.
    pub async fn query_one(
        &self,
        sql: impl AsRef<str>,
        params: Vec<Value>,
    ) -> Result<Option<Row>, DbError> {
        self.read().await?.query_one(sql, params).await
    }

    /// Convenience: run one write statement on the serialized write connection.
    pub async fn execute(&self, sql: impl AsRef<str>, params: Vec<Value>) -> Result<u64, DbError> {
        self.write().await.execute(sql, params).await
    }

    /// Merge FTS segments after runtime writes; keeps query latency bounded.
    pub async fn optimize_fts(&self) -> Result<(), DbError> {
        let conn = self.write().await;
        conn.execute_batch("OPTIMIZE INDEX").await
    }

    /// Build the FTS index after a bulk load (deferred-FTS mode).
    pub async fn create_fts_index(&self) -> Result<(), DbError> {
        let conn = self.write().await;
        crate::db::turso_schema::create_fts(&conn).await
    }

    /// Flush and truncate the WAL. Called before sealing a shard and by the
    /// periodic checkpoint task.
    pub async fn checkpoint_truncate(&self) -> Result<(), DbError> {
        let conn = self.write().await;
        let rows = conn
            .query("PRAGMA wal_checkpoint(TRUNCATE)", Vec::new())
            .await?;
        let status = rows
            .first()
            .ok_or_else(|| DbError::Shape("checkpoint returned no status".to_string()))?;
        if status.get_i64(0)? != 0 || status.get_i64(1)? != status.get_i64(2)? {
            return Err(DbError::Driver(turso::Error::Busy(
                "checkpoint did not drain the WAL".to_string(),
            )));
        }
        Ok(())
    }
}

/// Build a `Value::Text` from an optional string.
pub fn opt_text(value: Option<&str>) -> Value {
    match value {
        Some(v) => Value::Text(v.to_string()),
        None => Value::Null,
    }
}

/// Build a `Value::Integer` from an optional integer.
pub fn opt_int(value: Option<i64>) -> Value {
    match value {
        Some(v) => Value::Integer(v),
        None => Value::Null,
    }
}

/// Build a `Value::Real` from an optional float.
pub fn opt_real(value: Option<f64>) -> Value {
    match value {
        Some(v) => Value::Real(v),
        None => Value::Null,
    }
}

/// Build a `Value::Blob` from an optional byte slice.
pub fn opt_blob(value: Option<&[u8]>) -> Value {
    match value {
        Some(v) => Value::Blob(v.to_vec()),
        None => Value::Null,
    }
}

/// Small helper for `PRAGMA` reads that expect a single integer.
pub async fn pragma_i64(conn: &DbConn, name: &str) -> Result<i64, DbError> {
    let rows = conn.pragma(name).await?;
    rows.first().map(|row| row.get_i64(0)).unwrap_or(Ok(0))
}

// Re-export commonly used engine items so call sites import one module.
pub use engine::{DbError as StoreError, Row as StoreRow};

#[cfg(test)]
#[path = "store_probe_tests.rs"]
mod probe_tests;

#[cfg(test)]
#[path = "store_checkpoint_tests.rs"]
mod checkpoint_busy_tests;

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    use crate::db::engine::Value;

    #[tokio::test]
    async fn checkpoint_truncate_succeeds() {
        let path = std::env::temp_dir().join(format!(
            "qq-checkpoint-{}.db",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = AuditStore::open(&path, false, Duration::from_secs(5))
            .await
            .unwrap();
        store
            .execute(
                "INSERT INTO records (Time, TimeMs, Model) VALUES (?, ?, ?)",
                vec![
                    Value::Text("x".into()),
                    Value::Integer(1),
                    Value::Text("m".into()),
                ],
            )
            .await
            .unwrap();
        store.checkpoint_truncate().await.unwrap();
        store.optimize_fts().await.unwrap();
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn probe_rejects_v7_without_touching_bytes() {
        let path = std::env::temp_dir().join(format!(
            "qq-probe-v7-{}.db",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let db = Db::open(&path, OpenOptions::default()).await.unwrap();
        db.connect()
            .unwrap()
            .execute_batch(
                "CREATE TABLE records (id INTEGER PRIMARY KEY, Time TEXT, Headers TEXT);\
                 PRAGMA user_version=7",
            )
            .await
            .unwrap();
        drop(db);

        let before = std::fs::read(&path).unwrap();
        assert!(matches!(probe_schema(&path).await.unwrap(), SchemaKind::V7));
        let error = match AuditStore::open(&path, false, Duration::from_secs(1)).await {
            Ok(_) => panic!("writable open of a v7 database must be refused"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("v7"), "{error}");
        assert_eq!(before, std::fs::read(&path).unwrap());
        let _ = std::fs::remove_file(&path);
    }
}
