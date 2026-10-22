use super::*;
use crate::db::engine::Value;
use crate::db::store::AuditStore;
use std::time::Duration;

#[tokio::test]
async fn complete_archive_scans_once_and_partial_archive_keeps_time_filter() {
    let path = std::env::temp_dir().join(format!(
        "qq_shards_full_{}_{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let store = AuditStore::open(&path, false, Duration::from_secs(5))
        .await
        .unwrap();
    for time_ms in [0_i64, 30 * DAY_MS] {
        store
            .execute(
                "INSERT INTO records (Time, TimeMs, Model) VALUES (?, ?, ?)",
                vec![
                    Value::Text("t".into()),
                    Value::Integer(time_ms),
                    Value::Text("m".into()),
                ],
            )
            .await
            .unwrap();
    }
    let shard = ShardInput {
        id: "archive".to_string(),
        store: store.clone(),
        path: None,
        min_ms: Some(0),
        max_ms: Some(30 * DAY_MS),
        is_active: false,
        active_month: None,
    };
    let params = AnalysisParams::default();
    let query = |where_sql: &str| format!("SELECT COUNT(*) AS c FROM records{where_sql}");

    let full = fetch_sliced(&[shard.clone()], &params, 0, 30 * DAY_MS, query)
        .await
        .unwrap();
    assert_eq!(full.len(), 1);
    assert_eq!(full[0][0].get_i64_by_name("c").unwrap(), 2);

    let partial = fetch_sliced(&[shard], &params, 1, 30 * DAY_MS, query)
        .await
        .unwrap();
    assert_eq!(partial.len(), 4);
    assert_eq!(
        partial
            .iter()
            .map(|rows| rows[0].get_i64_by_name("c").unwrap())
            .sum::<i64>(),
        1
    );
    let _ = std::fs::remove_file(&path);
}
