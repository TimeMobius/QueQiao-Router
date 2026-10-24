use super::AuditStore;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[tokio::test]
async fn refuses_truncation_until_existing_read_snapshot_is_released() {
    let directory = std::env::temp_dir().join(format!(
        "qq-checkpoint-busy-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let store = AuditStore::open(
        directory.join("record.db"),
        false,
        Duration::from_millis(10),
    )
    .await
    .unwrap();
    store
        .execute("INSERT INTO records (id) VALUES (1)", Vec::new())
        .await
        .unwrap();
    let reader = store.read().await.unwrap();
    reader.execute_batch("BEGIN").await.unwrap();
    reader
        .query("SELECT COUNT(*) FROM records", Vec::new())
        .await
        .unwrap();
    store
        .execute("INSERT INTO records (id) VALUES (2)", Vec::new())
        .await
        .unwrap();

    let checkpoint = store.checkpoint_truncate().await;

    assert!(checkpoint.unwrap_err().is_busy());
    reader.execute_batch("ROLLBACK").await.unwrap();
    drop(reader);
    store.checkpoint_truncate().await.unwrap();
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}
