use chrono::Local;
use serde_json::Value;
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use crate::db::records_query::Bind;
use crate::handlers::analysis_api::shards::{collect_shards, fetch_one_shard, ShardInput};
use crate::state::app_state::AppState;

use super::metrics_result::{merge_groups, render_result};
use super::metrics_sql::{
    build_filters, build_sql, group_expression, resolve_query_range, row_metric,
};
use super::{metrics_cache, GroupBy, MetricName, MetricQuery};

pub(super) struct PlannedQuery {
    pub(super) from: i64,
    pub(super) to: i64,
    pub(super) shards: Vec<String>,
    pub(super) jobs: Vec<String>,
}

struct Job {
    shard: ShardInput,
    full: bool,
    where_sql: String,
    binds: Vec<Bind>,
    group: GroupBy,
    metrics: Vec<MetricName>,
    cache_key: Option<String>,
}

pub(super) async fn execute_queries(
    app_state: &Arc<AppState>,
    queries: &[MetricQuery],
) -> Result<serde_json::Map<String, Value>, sqlx::Error> {
    let now = Local::now();
    let ranges: Vec<(i64, i64)> = queries
        .iter()
        .map(|query| resolve_query_range(query, now))
        .collect();
    let range_from = ranges
        .iter()
        .map(|(from, _)| *from)
        .min()
        .unwrap_or(now.timestamp_millis());
    let range_to = ranges
        .iter()
        .map(|(_, to)| *to)
        .max()
        .unwrap_or(now.timestamp_millis());
    let candidates = collect_shards(app_state, range_from, range_to).await;
    let mut plans = Vec::with_capacity(queries.len());
    let mut jobs: HashMap<String, Job> = HashMap::new();
    for (query, (from, to)) in queries.iter().zip(ranges) {
        let shards: Vec<&ShardInput> = candidates
            .iter()
            .filter(|shard| {
                shard.is_active
                    || shard.min_ms.is_some_and(|min| min <= to)
                        && shard.max_ms.is_some_and(|max| max >= from)
            })
            .collect();
        let mut keys = Vec::with_capacity(shards.len());
        for shard in &shards {
            let full = shard_is_complete(shard, from, to, now.timestamp_millis());
            let (where_sql, binds) = build_filters(query, from, to, full);
            let key = job_key(shard, &query.group_by, &where_sql, &binds);
            let job = jobs.entry(key.clone()).or_insert_with(|| Job {
                shard: (*shard).clone(),
                full,
                where_sql,
                binds,
                group: query.group_by.clone(),
                metrics: Vec::new(),
                cache_key: None,
            });
            for metric in required_metrics(query) {
                if !job.metrics.contains(&metric) {
                    job.metrics.push(metric);
                }
            }
            keys.push(key);
        }
        plans.push(PlannedQuery {
            from,
            to,
            shards: shards.iter().map(|shard| shard.id.clone()).collect(),
            jobs: keys,
        });
    }

    let mut results = HashMap::new();
    let mut tasks = Vec::new();
    for (key, mut job) in jobs {
        job.metrics.sort_by_key(|metric| metric.alias());
        let sql = build_sql(&group_expression(&job.group), &job.metrics, &job.where_sql);
        job.cache_key = archive_cache_key(&job, &sql);
        if let Some(rows) = job.cache_key.as_deref().and_then(metrics_cache::get) {
            results.insert(key, rows);
            continue;
        }
        tasks.push(tokio::spawn(async move {
            let rows = fetch_one_shard(&job.shard.pool, &sql, &job.binds).await?;
            let mut groups = metrics_cache::GroupRows::new();
            for row in rows {
                let name = row.try_get::<String, _>("metric_group")?;
                let entry = groups.entry(name).or_default();
                for metric in &job.metrics {
                    if !matches!(
                        metric,
                        MetricName::AvgLatency
                            | MetricName::AvgTtft
                            | MetricName::SuccessRate
                            | MetricName::ErrorRate
                    ) {
                        entry.insert(*metric, row_metric(&row, *metric)?);
                    }
                }
            }
            Ok::<_, sqlx::Error>((key, groups, job.cache_key, job.shard.is_active))
        }));
    }
    for task in futures::future::join_all(tasks).await {
        let (key, groups, cache_key, active) =
            task.map_err(|error| sqlx::Error::Protocol(error.to_string()))??;
        if let Some(cache_key) = cache_key {
            metrics_cache::insert(cache_key, active, &groups);
        }
        results.insert(key, groups);
    }

    let mut output = serde_json::Map::new();
    for (query, plan) in queries.iter().zip(plans) {
        let mut merged = metrics_cache::GroupRows::new();
        for key in &plan.jobs {
            if let Some(groups) = results.get(key) {
                merge_groups(&mut merged, groups, &required_metrics(query));
            }
        }
        output.insert(query.id.clone(), render_result(query, plan, merged));
    }
    Ok(output)
}

fn job_key(shard: &ShardInput, group: &GroupBy, where_sql: &str, binds: &[Bind]) -> String {
    format!("{}:{group:?}:{where_sql}:{binds:?}", shard.id)
}

fn required_metrics(query: &MetricQuery) -> Vec<MetricName> {
    let mut required = query.metrics.clone();
    required.push(query.order_by);
    for metric in [MetricName::AvgLatency, MetricName::AvgTtft] {
        if required.contains(&metric) {
            let dependencies = match metric {
                MetricName::AvgLatency => [MetricName::LatencySum, MetricName::LatencyCount],
                MetricName::AvgTtft => [MetricName::TtftSum, MetricName::TtftCount],
                _ => continue,
            };
            required.extend(dependencies);
        }
    }
    if required.contains(&MetricName::SuccessRate) || required.contains(&MetricName::ErrorRate) {
        required.extend([
            MetricName::Requests,
            MetricName::Success,
            MetricName::Errors,
        ]);
    }
    required.sort_by_key(|metric| metric.alias());
    required.dedup();
    required
}

fn shard_is_complete(shard: &ShardInput, from: i64, to: i64, now: i64) -> bool {
    shard.is_active && to >= now && shard.active_month_covered(from, to)
        || !shard.is_active
            && shard.min_ms.is_some_and(|min| min >= from)
            && shard.max_ms.is_some_and(|max| max <= to)
}

fn archive_cache_key(job: &Job, sql: &str) -> Option<String> {
    if !job.full {
        return None;
    }
    if job.shard.is_active {
        return Some(format!(
            "active:{}:{sql}:{:?}",
            job.shard.active_month?, job.binds
        ));
    }
    let path = job.shard.path.as_ref()?;
    let metadata = std::fs::metadata(path).ok()?;
    let modified = metadata
        .modified()
        .ok()?
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()?;
    Some(format!(
        "{}:{}:{}:{sql}:{:?}",
        path.display(),
        metadata.len(),
        modified.as_nanos(),
        job.binds
    ))
}

#[cfg(test)]
#[path = "metrics_engine_tests.rs"]
mod tests;
