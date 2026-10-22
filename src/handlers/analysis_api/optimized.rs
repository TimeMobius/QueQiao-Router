use crate::db::engine::{DbError, Row};
use crate::db::records_query;

use super::params::{to_list_params, AnalysisParams};
use super::shards::{fetch_one_shard, ShardInput};

pub(super) async fn fetch_optimized<F>(
    shards: &[ShardInput],
    params: &AnalysisParams,
    from: i64,
    to: i64,
    make_sql: F,
) -> Result<Vec<Vec<Row>>, DbError>
where
    F: Fn(&str) -> String,
{
    let mut tasks = Vec::with_capacity(shards.len());
    for shard in shards {
        let store = shard.store.clone();
        let mut filters = to_list_params(params, from, to);
        if !shard.is_active
            && shard.min_ms.is_some_and(|min| min >= from)
            && shard.max_ms.is_some_and(|max| max <= to)
        {
            filters.from = None;
            filters.to = None;
        }
        let (mut where_sql, binds) = records_query::build_filters(&filters);
        if filters.from.is_none() {
            where_sql.push_str(" AND TimeMs IS NOT NULL");
        }
        let sql = make_sql(&where_sql);
        tasks.push(tokio::spawn(async move {
            fetch_one_shard(&store, &sql, &binds).await
        }));
    }
    let mut result = Vec::with_capacity(tasks.len());
    for task in futures::future::join_all(tasks).await {
        result.push(task.map_err(|error| DbError::Shape(error.to_string()))??);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::engine::Value;
    use crate::db::store::AuditStore;
    use std::time::Duration;

    #[tokio::test]
    async fn full_archive_and_partial_archive_return_the_same_bounded_rows() {
        let path = std::env::temp_dir().join(format!(
            "qq_optimized_{}_{}.db",
            std::process::id(),
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
                    Value::Text("a".into()),
                    Value::Integer(10),
                    Value::Text("m".into()),
                ],
            )
            .await
            .unwrap();
        store
            .execute(
                "INSERT INTO records (Time, TimeMs, Model) VALUES (?, ?, ?)",
                vec![
                    Value::Text("b".into()),
                    Value::Integer(20),
                    Value::Text("m".into()),
                ],
            )
            .await
            .unwrap();
        let shards = [ShardInput {
            id: "archive".to_string(),
            store: store.clone(),
            path: None,
            min_ms: Some(10),
            max_ms: Some(20),
            is_active: false,
            active_month: None,
        }];
        let query = |where_sql: &str| format!("SELECT COUNT(*) AS c FROM records{where_sql}");
        let full = fetch_optimized(&shards, &AnalysisParams::default(), 10, 20, query)
            .await
            .unwrap();
        assert_eq!(full[0][0].get_i64_by_name("c").unwrap(), 2);
        let partial = fetch_optimized(&shards, &AnalysisParams::default(), 11, 20, query)
            .await
            .unwrap();
        assert_eq!(partial[0][0].get_i64_by_name("c").unwrap(), 1);
        let _ = std::fs::remove_file(&path);
    }
}
