use super::{AuditStore, Db, DbError, OpenOptions};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn refuses_unrelated_sqlite_database_without_modifying_it() {
    let directory = std::env::temp_dir().join(format!(
        "qq-unrelated-db-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("record.db");
    let database = Db::open(&path, OpenOptions::default()).await.unwrap();
    let connection = database.connect().unwrap();
    connection
        .execute_batch("CREATE TABLE unrelated (value TEXT); INSERT INTO unrelated VALUES ('keep')")
        .await
        .unwrap();
    connection
        .query("PRAGMA wal_checkpoint(TRUNCATE)", Vec::new())
        .await
        .unwrap();
    drop(connection);
    drop(database);
    let original = std::fs::read(&path).unwrap();

    let opened = AuditStore::open(&path, false, Duration::from_secs(1)).await;

    assert!(matches!(opened, Err(DbError::Shape(_))));
    assert_eq!(original, std::fs::read(&path).unwrap());
    std::fs::remove_dir_all(directory).unwrap();
}
