use super::super::metrics_cache::MetricValue;
use super::*;
use std::path::PathBuf;

#[test]
fn merges_counts_exactly_and_maximum_across_shards() {
    let mut result = metrics_cache::GroupRows::new();
    let mut first = metrics_cache::GroupRows::new();
    first.insert(
        "m".to_string(),
        HashMap::from([
            (
                MetricName::Requests,
                MetricValue::Integer(9_007_199_254_740_993),
            ),
            (MetricName::MaxLatency, MetricValue::Real(23.0)),
        ]),
    );
    let mut second = metrics_cache::GroupRows::new();
    second.insert(
        "m".to_string(),
        HashMap::from([
            (MetricName::Requests, MetricValue::Integer(7)),
            (MetricName::MaxLatency, MetricValue::Real(12.0)),
        ]),
    );
    let metrics = [MetricName::Requests, MetricName::MaxLatency];
    merge_groups(&mut result, &first, &metrics);
    merge_groups(&mut result, &second, &metrics);
    assert_eq!(
        result["m"][&MetricName::Requests],
        MetricValue::Integer(9_007_199_254_741_000)
    );
    assert_eq!(
        result["m"][&MetricName::MaxLatency],
        MetricValue::Real(23.0)
    );
}

#[test]
fn average_uses_hidden_sum_and_count_after_merging() {
    let query = super::super::MetricQuery {
        id: "average".to_string(),
        period: None,
        from: None,
        to: None,
        group_by: GroupBy::Single("model".to_string()),
        metrics: vec![MetricName::AvgLatency],
        filters: Default::default(),
        order_by: MetricName::AvgLatency,
        limit: 100,
    };
    let required = required_metrics(&query);
    assert!(required.contains(&MetricName::LatencySum));
    assert!(required.contains(&MetricName::LatencyCount));
    let mut groups = metrics_cache::GroupRows::new();
    groups.insert(
        "m".to_string(),
        HashMap::from([
            (MetricName::LatencySum, MetricValue::Real(90.0)),
            (MetricName::LatencyCount, MetricValue::Integer(3)),
        ]),
    );
    let plan = PlannedQuery {
        from: 0,
        to: 1,
        shards: vec![],
        jobs: vec![],
    };
    let value = render_result(&query, plan, groups);
    assert_eq!(value["items"][0]["avgLatency"], 30.0);
    assert!(value["items"][0].get("latencySum").is_none());
}

#[test]
fn error_rate_is_derived_from_merged_request_counts() {
    let query = super::super::MetricQuery {
        id: "rate".to_string(),
        period: None,
        from: None,
        to: None,
        group_by: GroupBy::Single("model".to_string()),
        metrics: vec![MetricName::ErrorRate],
        filters: Default::default(),
        order_by: MetricName::ErrorRate,
        limit: 100,
    };
    let required = required_metrics(&query);
    assert!(required.contains(&MetricName::Requests));
    let mut groups = metrics_cache::GroupRows::new();
    groups.insert(
        "m".to_string(),
        HashMap::from([
            (MetricName::Requests, MetricValue::Integer(10)),
            (MetricName::Errors, MetricValue::Integer(2)),
        ]),
    );
    let plan = PlannedQuery {
        from: 0,
        to: 1,
        shards: vec![],
        jobs: vec![],
    };
    let value = render_result(&query, plan, groups);
    assert_eq!(value["items"][0]["errorRate"], 0.2);
    assert!(value["items"][0].get("requests").is_none());
}

#[tokio::test]
async fn integer_latency_sum_decodes_as_real_for_average() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    let row = sqlx::query("SELECT CAST(5 AS INTEGER) AS latencySum")
        .fetch_one(&pool)
        .await
        .unwrap();
    let value = row_metric(&row, MetricName::LatencySum).unwrap();
    assert_eq!(value, MetricValue::Real(5.0));
}

#[tokio::test]
async fn completed_archive_uses_full_scan_only_when_bounds_fit() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    let shard = ShardInput {
        id: "archive".to_string(),
        pool,
        path: None,
        min_ms: Some(10),
        max_ms: Some(20),
        is_active: false,
        active_month: None,
    };
    assert!(shard_is_complete(&shard, 10, 20, 100));
    assert!(!shard_is_complete(&shard, 11, 20, 100));
    assert!(!shard_is_complete(&shard, 10, 19, 100));
}

#[test]
fn full_shard_still_excludes_rows_without_timestamp() {
    let query = super::super::MetricQuery {
        id: "month".to_string(),
        period: None,
        from: None,
        to: None,
        group_by: GroupBy::Single("model".to_string()),
        metrics: vec![MetricName::Requests],
        filters: Default::default(),
        order_by: MetricName::Requests,
        limit: 100,
    };
    let (where_sql, binds) = build_filters(&query, 10, 20, true);
    assert!(where_sql.contains("TimeMs IS NOT NULL"));
    assert!(binds.is_empty());
}

#[tokio::test]
async fn period_queries_share_full_shard_even_with_different_metrics() {
    let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
    let shard = ShardInput {
        id: "active".to_string(),
        pool,
        path: None,
        min_ms: None,
        max_ms: None,
        is_active: true,
        active_month: None,
    };
    let first = super::super::MetricQuery {
        id: "month".to_string(),
        period: None,
        from: None,
        to: None,
        group_by: GroupBy::Single("model".to_string()),
        metrics: vec![MetricName::Requests],
        filters: Default::default(),
        order_by: MetricName::Requests,
        limit: 100,
    };
    let mut second = first.clone();
    second.id = "year".to_string();
    second.metrics.push(MetricName::Errors);
    let (where_sql, binds) = build_filters(&first, 0, 10, true);
    let (other_where, other_binds) = build_filters(&second, 0, 10, true);
    assert_eq!(
        job_key(&shard, &first.group_by, &where_sql, &binds),
        job_key(&shard, &second.group_by, &other_where, &other_binds)
    );
}

#[tokio::test]
async fn archive_cache_key_changes_when_file_changes() {
    let path = std::env::temp_dir().join(format!(
        "qq_metrics_cache_{}_{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, b"initial").unwrap();
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    let job = Job {
        shard: ShardInput {
            id: "archive".to_string(),
            pool,
            path: Some(PathBuf::from(&path)),
            min_ms: Some(1),
            max_ms: Some(2),
            is_active: false,
            active_month: None,
        },
        full: true,
        where_sql: " WHERE 1=1".to_string(),
        binds: vec![],
        group: GroupBy::Single("model".to_string()),
        metrics: vec![MetricName::Requests],
        cache_key: None,
    };
    let before = archive_cache_key(&job, "SELECT COUNT(*) FROM records").unwrap();
    std::fs::write(&path, b"changed and longer").unwrap();
    let after = archive_cache_key(&job, "SELECT COUNT(*) FROM records").unwrap();
    assert_ne!(before, after);
    std::fs::remove_file(path).unwrap();
}
