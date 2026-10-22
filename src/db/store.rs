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

impl AuditStore {
    /// Open a database. When writable, the v8 schema is created if the file is
    /// new; a legacy schema is rejected with a clear error instead of being
    /// silently altered (migration is an explicit, separate operation).
    pub async fn open(
        path: impl AsRef<Path>,
        read_only: bool,
        busy_timeout: Duration,
    ) -> Result<Arc<AuditStore>, DbError> {
        let path = path.as_ref().to_path_buf();
        let opts = OpenOptions {
            read_only,
            index_method: true,
            vacuum: false,
            busy_timeout,
        };
        let db = Db::open(&path, opts).await?;
        let write_conn = db.connect()?;

        if !read_only {
            crate::db::turso_schema::init_v8(&write_conn).await?;
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

    /// Flush and truncate the WAL. Called before sealing a shard and by the
    /// periodic checkpoint task.
    pub async fn checkpoint_truncate(&self) -> Result<(), DbError> {
        let conn = self.write().await;
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
            .await?;
        conn.cacheflush()?;
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
