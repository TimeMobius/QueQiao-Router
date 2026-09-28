use super::*;
use sqlx::Row;

#[tokio::test]
async fn complete_archive_scans_once_and_partial_archive_keeps_time_filter() {
    let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::query("CREATE TABLE records (TimeMs INTEGER)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO records (TimeMs) VALUES (0), (NULL), (?)")
        .bind(30 * DAY_MS)
        .execute(&pool)
        .await
        .unwrap();
    let shard = ShardInput {
        id: "archive".to_string(),
        pool,
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
    assert_eq!(full[0][0].get::<i64, _>("c"), 2);

    let partial = fetch_sliced(&[shard], &params, 1, 30 * DAY_MS, query)
        .await
        .unwrap();
    assert_eq!(partial.len(), 4);
    assert_eq!(
        partial
            .iter()
            .map(|rows| rows[0].get::<i64, _>("c"))
            .sum::<i64>(),
        1
    );
}
