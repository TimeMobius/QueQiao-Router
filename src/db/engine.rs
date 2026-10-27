//! Storage engine abstraction.
//!
//! Every driver-specific detail (`turso` types, error kinds, row decoding) is
//! confined to this module. The rest of the audit DB layer and the handlers
//! only deal with [`Db`], [`DbConn`], [`Row`], [`Value`] and [`DbError`].
//!
//! Design notes:
//! - One [`Db`] handle per database file; it is cheap to clone.
//! - [`DbConn`] is a single connection. Writes go through one serialized
//!   connection; reads go through a semaphore-bounded reader pool.
//! - [`Value`] is the driver's value type re-exported, so call sites build
//!   parameters directly without a second conversion layer.

use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use turso::{Builder, Connection, Database};

pub use turso::Value;

/// Errors surfaced by the storage engine.
#[derive(Debug)]
pub enum DbError {
    Driver(turso::Error),
    /// A row did not have the expected column layout/type.
    Shape(String),
    /// The reader pool was exhausted for longer than the configured timeout.
    PoolTimeout(Duration),
    /// The pool has been closed.
    Closed,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Driver(e) => write!(f, "{e}"),
            DbError::Shape(msg) => write!(f, "row shape error: {msg}"),
            DbError::PoolTimeout(d) => write!(f, "db reader pool timed out after {d:?}"),
            DbError::Closed => write!(f, "db pool is closed"),
        }
    }
}

impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DbError::Driver(e) => Some(e),
            _ => None,
        }
    }
}

impl From<turso::Error> for DbError {
    fn from(value: turso::Error) -> Self {
        DbError::Driver(value)
    }
}

impl DbError {
    /// Transient lock/snapshot contention; the caller may retry.
    pub fn is_busy(&self) -> bool {
        matches!(
            self,
            DbError::Driver(turso::Error::Busy(_) | turso::Error::BusySnapshot(_))
        )
    }

    /// Write-write conflict under MVCC; the caller may retry.
    pub fn is_conflict(&self) -> bool {
        matches!(self, DbError::Driver(turso::Error::BusySnapshot(_)))
    }

    pub fn is_readonly(&self) -> bool {
        matches!(self, DbError::Driver(turso::Error::Readonly(_)))
    }

    pub fn is_constraint(&self) -> bool {
        matches!(self, DbError::Driver(turso::Error::Constraint(_)))
    }

    pub fn is_no_rows(&self) -> bool {
        matches!(self, DbError::Driver(turso::Error::QueryReturnedNoRows))
    }

    /// Whether retrying the statement can plausibly succeed.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            DbError::Driver(
                turso::Error::Busy(_)
                    | turso::Error::BusySnapshot(_)
                    | turso::Error::Interrupt(_)
                    | turso::Error::IoError(_, _)
            ) | DbError::PoolTimeout(_)
        )
    }
}

/// Options used when opening a database file.
#[derive(Debug, Clone)]
pub struct OpenOptions {
    pub read_only: bool,
    /// Enables `CREATE INDEX ... USING fts` (and opening databases that use it).
    pub index_method: bool,
    /// Enables `VACUUM` (the archived migration path uses `VACUUM INTO`).
    pub vacuum: bool,
    /// Busy timeout applied to every connection.
    pub busy_timeout: Duration,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            read_only: false,
            index_method: true,
            vacuum: false,
            busy_timeout: Duration::from_secs(30),
        }
    }
}

/// A handle to one database file.
#[derive(Clone)]
pub struct Db {
    inner: Database,
    opts: OpenOptions,
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Db").field("opts", &self.opts).finish()
    }
}

impl Db {
    /// Open (or create) a local database.
    pub async fn open(path: impl AsRef<Path>, opts: OpenOptions) -> Result<Db, DbError> {
        let path = path.as_ref();
        let path_str = path
            .to_str()
            .ok_or_else(|| DbError::Shape(format!("database path is not valid UTF-8: {path:?}")))?;
        let mut builder = Builder::new_local(path_str);
        if opts.read_only {
            builder = builder.read_only(true);
        }
        if opts.index_method {
            builder = builder.experimental_index_method(true);
        }
        if opts.vacuum {
            builder = builder.experimental_vacuum(true);
        }
        let inner = builder.build().await?;
        Ok(Db { inner, opts })
    }

    /// Open a new connection with the configured busy timeout.
    pub fn connect(&self) -> Result<DbConn, DbError> {
        let conn = self.inner.connect()?;
        conn.busy_timeout(self.opts.busy_timeout)?;
        Ok(DbConn { inner: conn })
    }

    pub fn options(&self) -> &OpenOptions {
        &self.opts
    }
}

/// A single connection to the database.
pub struct DbConn {
    inner: Connection,
}

impl fmt::Debug for DbConn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbConn").finish()
    }
}

impl DbConn {
    /// Run a query and collect every row.
    pub async fn query(
        &self,
        sql: impl AsRef<str>,
        params: Vec<Value>,
    ) -> Result<Vec<Row>, DbError> {
        let mut rows = self.inner.query(sql, params).await?;
        let names = rows.column_names();
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(Row::from_turso(&row, &names)?);
        }
        Ok(out)
    }

    /// Run a query and return at most one row.
    pub async fn query_one(
        &self,
        sql: impl AsRef<str>,
        params: Vec<Value>,
    ) -> Result<Option<Row>, DbError> {
        let mut rows = self.inner.query(sql, params).await?;
        let names = rows.column_names();
        match rows.next().await? {
            Some(row) => Ok(Some(Row::from_turso(&row, &names)?)),
            None => Ok(None),
        }
    }

    /// Run a statement that returns no rows; returns the affected row count.
    pub async fn execute(&self, sql: impl AsRef<str>, params: Vec<Value>) -> Result<u64, DbError> {
        Ok(self.inner.execute(sql, params).await?)
    }

    /// Execute one or more statements without parameters (DDL, PRAGMAs).
    pub async fn execute_batch(&self, sql: impl AsRef<str>) -> Result<(), DbError> {
        Ok(self.inner.execute_batch(sql).await?)
    }

    /// Rowid of the most recent successful INSERT on this connection.
    pub fn last_insert_rowid(&self) -> i64 {
        self.inner.last_insert_rowid()
    }

    /// Run a PRAGMA returning rows (e.g. `table_info(records)`).
    pub async fn pragma(&self, name: &str) -> Result<Vec<Row>, DbError> {
        let mut rows = self.inner.query(format!("PRAGMA {name}"), ()).await?;
        let names = rows.column_names();
        let mut out = Vec::new();
        while let Some(row) = rows.next().await? {
            out.push(Row::from_turso(&row, &names)?);
        }
        Ok(out)
    }

    /// Set a PRAGMA value.
    pub async fn set_pragma(&self, name: &str, value: impl fmt::Display) -> Result<(), DbError> {
        self.execute_batch(format!("PRAGMA {name} = {value}")).await
    }

    /// Begin an immediate (write) transaction.
    pub async fn begin_immediate(&self) -> Result<(), DbError> {
        self.execute_batch("BEGIN IMMEDIATE").await
    }

    pub async fn commit(&self) -> Result<(), DbError> {
        self.execute_batch("COMMIT").await
    }

    pub async fn rollback(&self) -> Result<(), DbError> {
        self.execute_batch("ROLLBACK").await
    }

    pub fn is_autocommit(&self) -> Result<bool, DbError> {
        Ok(self.inner.is_autocommit()?)
    }

    pub fn cacheflush(&self) -> Result<(), DbError> {
        Ok(self.inner.cacheflush()?)
    }
}

/// An owned row with driver-independent accessors.
#[derive(Clone, Debug)]
pub struct Row {
    values: Vec<Value>,
    names: Vec<String>,
}

impl Row {
    fn from_turso(row: &turso::Row, names: &[String]) -> Result<Row, DbError> {
        let mut values = Vec::with_capacity(row.column_count());
        for idx in 0..row.column_count() {
            values.push(row.get_value(idx)?);
        }
        Ok(Row {
            values,
            names: names.to_vec(),
        })
    }

    /// Look up a column by name (case-insensitive, as SQLite treats names).
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.names
            .iter()
            .position(|candidate| candidate.eq_ignore_ascii_case(name))
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn value(&self, idx: usize) -> Result<&Value, DbError> {
        self.values
            .get(idx)
            .ok_or_else(|| DbError::Shape(format!("column index {idx} out of bounds")))
    }

    pub fn get_i64(&self, idx: usize) -> Result<i64, DbError> {
        match self.value(idx)? {
            Value::Integer(v) => Ok(*v),
            Value::Real(v) => Ok(*v as i64),
            other => Err(DbError::Shape(format!(
                "column {idx}: expected integer, got {other:?}"
            ))),
        }
    }

    pub fn get_opt_i64(&self, idx: usize) -> Result<Option<i64>, DbError> {
        match self.value(idx)? {
            Value::Null => Ok(None),
            Value::Integer(v) => Ok(Some(*v)),
            Value::Real(v) => Ok(Some(*v as i64)),
            other => Err(DbError::Shape(format!(
                "column {idx}: expected integer/null, got {other:?}"
            ))),
        }
    }

    pub fn get_f64(&self, idx: usize) -> Result<f64, DbError> {
        match self.value(idx)? {
            Value::Real(v) => Ok(*v),
            Value::Integer(v) => Ok(*v as f64),
            other => Err(DbError::Shape(format!(
                "column {idx}: expected real, got {other:?}"
            ))),
        }
    }

    pub fn get_opt_f64(&self, idx: usize) -> Result<Option<f64>, DbError> {
        match self.value(idx)? {
            Value::Null => Ok(None),
            Value::Real(v) => Ok(Some(*v)),
            Value::Integer(v) => Ok(Some(*v as f64)),
            other => Err(DbError::Shape(format!(
                "column {idx}: expected real/null, got {other:?}"
            ))),
        }
    }

    pub fn get_str(&self, idx: usize) -> Result<&str, DbError> {
        match self.value(idx)? {
            Value::Text(v) => Ok(v.as_str()),
            other => Err(DbError::Shape(format!(
                "column {idx}: expected text, got {other:?}"
            ))),
        }
    }

    pub fn get_opt_str(&self, idx: usize) -> Result<Option<&str>, DbError> {
        match self.value(idx)? {
            Value::Null => Ok(None),
            Value::Text(v) => Ok(Some(v.as_str())),
            other => Err(DbError::Shape(format!(
                "column {idx}: expected text/null, got {other:?}"
            ))),
        }
    }

    /// Text-like accessor used for columns stored as TEXT but bound loosely.
    pub fn get_string(&self, idx: usize) -> Result<String, DbError> {
        match self.value(idx)? {
            Value::Text(v) => Ok(v.clone()),
            Value::Integer(v) => Ok(v.to_string()),
            Value::Real(v) => Ok(v.to_string()),
            Value::Blob(v) => Ok(String::from_utf8_lossy(v).into_owned()),
            Value::Null => Ok(String::new()),
        }
    }

    pub fn get_opt_string(&self, idx: usize) -> Result<Option<String>, DbError> {
        match self.value(idx)? {
            Value::Null => Ok(None),
            Value::Text(v) => Ok(Some(v.clone())),
            Value::Integer(v) => Ok(Some(v.to_string())),
            Value::Real(v) => Ok(Some(v.to_string())),
            Value::Blob(v) => Ok(Some(String::from_utf8_lossy(v).into_owned())),
        }
    }

    pub fn get_blob(&self, idx: usize) -> Result<&[u8], DbError> {
        match self.value(idx)? {
            Value::Blob(v) => Ok(v.as_slice()),
            other => Err(DbError::Shape(format!(
                "column {idx}: expected blob, got {other:?}"
            ))),
        }
    }

    pub fn get_opt_blob(&self, idx: usize) -> Result<Option<&[u8]>, DbError> {
        match self.value(idx)? {
            Value::Null => Ok(None),
            Value::Blob(v) => Ok(Some(v.as_slice())),
            other => Err(DbError::Shape(format!(
                "column {idx}: expected blob/null, got {other:?}"
            ))),
        }
    }

    fn idx_or_err(&self, name: &str) -> Result<usize, DbError> {
        self.index_of(name)
            .ok_or_else(|| DbError::Shape(format!("column '{name}' not found in row")))
    }

    pub fn get_i64_by_name(&self, name: &str) -> Result<i64, DbError> {
        self.get_i64(self.idx_or_err(name)?)
    }

    pub fn get_opt_i64_by_name(&self, name: &str) -> Result<Option<i64>, DbError> {
        self.get_opt_i64(self.idx_or_err(name)?)
    }

    pub fn get_f64_by_name(&self, name: &str) -> Result<f64, DbError> {
        self.get_f64(self.idx_or_err(name)?)
    }

    pub fn get_opt_f64_by_name(&self, name: &str) -> Result<Option<f64>, DbError> {
        match self.value(self.idx_or_err(name)?)? {
            Value::Null => Ok(None),
            Value::Real(v) => Ok(Some(*v)),
            Value::Integer(v) => Ok(Some(*v as f64)),
            other => Err(DbError::Shape(format!(
                "column '{name}': expected real/null, got {other:?}"
            ))),
        }
    }

    pub fn get_str_by_name(&self, name: &str) -> Result<&str, DbError> {
        self.get_str(self.idx_or_err(name)?)
    }

    pub fn get_opt_str_by_name(&self, name: &str) -> Result<Option<&str>, DbError> {
        self.get_opt_str(self.idx_or_err(name)?)
    }

    pub fn get_opt_string_by_name(&self, name: &str) -> Result<Option<String>, DbError> {
        self.get_opt_string(self.idx_or_err(name)?)
    }
}

/// A semaphore-bounded reader pool.
///
/// Connections are cheap, so each acquisition opens a fresh one (with a reuse
/// fast-path left for a later optimization). The semaphore bounds concurrent
/// readers, which also bounds memory used by result sets and TLS-less readers.
pub struct ReaderPool {
    db: Db,
    permits: Arc<Semaphore>,
    timeout: Duration,
}

impl ReaderPool {
    pub fn new(db: Db, max_readers: usize, timeout: Duration) -> Self {
        Self {
            db,
            permits: Arc::new(Semaphore::new(max_readers.max(1))),
            timeout,
        }
    }

    pub async fn acquire(&self) -> Result<ReaderGuard, DbError> {
        let permit = tokio::time::timeout(self.timeout, self.permits.clone().acquire_owned())
            .await
            .map_err(|_| DbError::PoolTimeout(self.timeout))?
            .map_err(|_| DbError::Closed)?;
        let conn = self.db.connect()?;
        Ok(ReaderGuard {
            conn,
            _permit: permit,
        })
    }
}

/// A pooled reader connection; releasing it returns the permit.
pub struct ReaderGuard {
    conn: DbConn,
    _permit: OwnedSemaphorePermit,
}

impl fmt::Debug for ReaderGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReaderGuard").finish()
    }
}

impl std::ops::Deref for ReaderGuard {
    type Target = DbConn;
    fn deref(&self) -> &DbConn {
        &self.conn
    }
}

// Compile-time guarantees required by `AppState`/axum handlers.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<Db>();
    assert_send_sync::<DbConn>();
    assert_send_sync::<ReaderPool>();
    assert_send_sync::<ReaderGuard>();
};

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_db_path(tag: &str) -> std::path::PathBuf {
        let mut p = std::env::temp_dir();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        p.push(format!("queqiao-engine-{tag}-{nanos}.db"));
        p
    }

    #[tokio::test]
    async fn roundtrip_crud_and_types() {
        let path = temp_db_path("crud");
        let db = Db::open(&path, OpenOptions::default()).await.unwrap();
        let conn = db.connect().unwrap();
        conn.execute_batch(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT, score REAL, data BLOB, note TEXT)",
        )
        .await
        .unwrap();
        conn.execute(
            "INSERT INTO t (id, name, score, data, note) VALUES (?, ?, ?, ?, ?)",
            vec![
                Value::Integer(1),
                Value::Text("alice".into()),
                Value::Real(1.5),
                Value::Blob(vec![1, 2, 3]),
                Value::Null,
            ],
        )
        .await
        .unwrap();

        let row = conn
            .query_one(
                "SELECT id, name, score, data, note FROM t WHERE id = ?",
                vec![Value::Integer(1)],
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.get_i64(0).unwrap(), 1);
        assert_eq!(row.get_str(1).unwrap(), "alice");
        assert_eq!(row.get_f64(2).unwrap(), 1.5);
        assert_eq!(row.get_blob(3).unwrap(), &[1, 2, 3]);
        assert_eq!(row.get_opt_str(4).unwrap(), None);

        drop(conn);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn reader_pool_bounds_and_debug() {
        let path = temp_db_path("pool");
        let db = Db::open(&path, OpenOptions::default()).await.unwrap();
        let pool = ReaderPool::new(db, 2, Duration::from_secs(1));
        let a = pool.acquire().await.unwrap();
        let b = pool.acquire().await.unwrap();
        assert!(pool
            .acquire()
            .await
            .unwrap_err()
            .to_string()
            .contains("timed out"));
        drop(a);
        drop(b);
        assert!(pool.acquire().await.is_ok());
        let _ = std::fs::remove_file(&path);
    }
}
