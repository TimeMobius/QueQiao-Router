use chrono::Local;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::SystemTime;

use crate::db::aggregate;
use crate::db::engine::{DbError, Row, Value as DbValue};
use crate::handlers::analysis_api::shards::{collect_shards, fetch_one_shard, ShardInput};
use crate::state::app_state::AppState;

use super::metrics_result::{merge_groups, render_result};
use super::metrics_sql::{
    build_filters, build_filters_for, build_sql, group_expression, resolve_query_range, row_metric,
};
use super::{metrics_cache, GroupBy, MetricFilters, MetricName, MetricQuery};

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
    binds: Vec<DbValue>,
    group: GroupBy,
    metrics: Vec<MetricName>,
    cache_key: Option<String>,
    filters: MetricFilters,
    from: i64,
    to: i64,
    rollup_dim: Option<&'static str>,
}

pub(super) async fn execute_queries(
    app_state: &Arc<AppState>,
    queries: &[MetricQuery],
) -> Result<serde_json::Map<String, Value>, DbError> {
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
                filters: query.filters.clone(),
                from,
                to,
                rollup_dim: rollup_dim_for(query),
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
        job.cache_key = archive_cache_key(&job, &sql).await;
        if let Some(rows) = job.cache_key.as_deref().and_then(metrics_cache::get) {
            results.insert(key, rows);
            continue;
        }
        let audit_path = crate::db::resolve_db_path();
        tasks.push(tokio::spawn(async move {
            let groups = match job.rollup_dim {
                Some(dim) => {
                    match rollup_groups(
                        &audit_path,
                        &job.shard,
                        dim,
                        &job.filters,
                        job.from,
                        job.to,
                        &job.metrics,
                    )
                    .await
                    {
                        Ok(Some(groups)) => groups,
                        Ok(None) => scan_groups(&job, &sql).await?,
                        Err(error) => {
                            tracing::warn!(%error, "metrics rollup read failed; scanning instead");
                            scan_groups(&job, &sql).await?
                        }
                    }
                }
                None => scan_groups(&job, &sql).await?,
            };
            Ok::<_, DbError>((key, groups, job.cache_key, job.shard.is_active))
        }));
    }
    for task in futures::future::join_all(tasks).await {
        let (key, groups, cache_key, active) =
            task.map_err(|error| DbError::Shape(error.to_string()))??;
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

fn rollup_dim_for(query: &MetricQuery) -> Option<&'static str> {
    if !aggregate::enabled() {
        return None;
    }
    let GroupBy::Single(name) = &query.group_by else {
        return None;
    };
    if !filters_are_empty(&query.filters) {
        return None;
    }
    aggregate::DIMS
        .iter()
        .copied()
        .find(|dim| *dim == name.as_str())
}

fn filters_are_empty(filters: &MetricFilters) -> bool {
    filters.type_.as_deref().is_none_or(str::is_empty)
        && filters.model.as_deref().is_none_or(str::is_empty)
        && filters.status.is_none()
        && filters.backend.as_deref().is_none_or(str::is_empty)
        && filters.ip.as_deref().is_none_or(str::is_empty)
        && filters.client.as_deref().is_none_or(str::is_empty)
        && filters.apikey.as_deref().is_none_or(str::is_empty)
}

fn acc_metric(acc: &aggregate::Acc, metric: MetricName) -> Option<metrics_cache::MetricValue> {
    use metrics_cache::MetricValue;
    Some(match metric {
        MetricName::Requests => MetricValue::Integer(acc.counted),
        MetricName::Success => MetricValue::Integer(acc.success),
        MetricName::Errors => MetricValue::Integer(acc.errors),
        MetricName::PromptTokens => MetricValue::Integer(acc.prompt_tokens),
        MetricName::CompletionTokens => MetricValue::Integer(acc.completion_tokens),
        MetricName::TotalTokens => MetricValue::Integer(acc.total_tokens),
        MetricName::LatencySum => MetricValue::Real(acc.latency_sum),
        MetricName::LatencyCount => MetricValue::Integer(acc.latency_count),
        MetricName::MaxLatency => MetricValue::Real(acc.latency_max),
        MetricName::TtftSum => MetricValue::Real(acc.ttft_sum),
        MetricName::TtftCount => MetricValue::Integer(acc.ttft_count),
        MetricName::AvgLatency
        | MetricName::AvgTtft
        | MetricName::SuccessRate
        | MetricName::ErrorRate => return None,
    })
}

fn groups_from_rows(
    rows: Vec<Row>,
    metrics: &[MetricName],
) -> Result<metrics_cache::GroupRows, DbError> {
    let mut groups = metrics_cache::GroupRows::new();
    for row in rows {
        let name = row.get_str_by_name("metric_group")?.to_string();
        let entry = groups.entry(name).or_default();
        for metric in metrics {
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
    Ok(groups)
}

async fn scan_groups(job: &Job, sql: &str) -> Result<metrics_cache::GroupRows, DbError> {
    let rows = fetch_one_shard(&job.shard.store, sql, &job.binds).await?;
    groups_from_rows(rows, &job.metrics)
}

async fn rollup_groups(
    audit_path: &Path,
    shard: &ShardInput,
    dim: &'static str,
    filters: &MetricFilters,
    from: i64,
    to: i64,
    metrics: &[MetricName],
) -> Result<Option<metrics_cache::GroupRows>, DbError> {
    if shard.is_active {
        spawn_active_build_if_stale(audit_path.to_path_buf(), shard);
    }
    let Some((first_full, full_end)) = aggregate::full_day_bounds(from, to) else {
        return Ok(None);
    };
    if full_end <= first_full {
        return Ok(None);
    }
    let mut tail_from_id = None;
    let slice = if shard.is_active {
        if !aggregate::active_caught_up(audit_path, &shard.id, &shard.store, full_end - 1).await {
            spawn_active_build(audit_path.to_path_buf(), shard);
            return Ok(None);
        }
        // 末尾部分自然日（通常是“今天”）用增量水位截断：天桶读到水位，
        // 水位之后按 id 回扫原始库，避免每次查询都重扫当天全量。
        let detail_end = aggregate::local_day_ms(to)
            .and_then(aggregate::next_local_day)
            .unwrap_or(full_end);
        tail_from_id = aggregate::active_watermark(audit_path, &shard.id)
            .await
            .filter(|watermark| to >= watermark.max_time_ms)
            .map(|watermark| watermark.last_id);
        let slice_end = if tail_from_id.is_some() {
            detail_end
        } else {
            full_end
        };
        aggregate::read_active_slice(audit_path, &shard.id, first_full, slice_end).await
    } else {
        let Some(path) = shard.path.clone() else {
            return Ok(None);
        };
        let slice = aggregate::read_slice(
            audit_path,
            &shard.id,
            &path,
            shard.min_ms,
            shard.max_ms,
            first_full,
            full_end,
        )
        .await;
        if slice.is_none() {
            spawn_archive_build(audit_path.to_path_buf(), shard);
        }
        slice
    };
    let Some(slice) = slice else {
        return Ok(None);
    };
    let Some(values) = slice.dims.get(dim) else {
        return Ok(None);
    };
    let mut groups = metrics_cache::GroupRows::new();
    for (value, acc) in values {
        let entry = groups.entry(value.clone()).or_default();
        for metric in metrics {
            if let Some(value) = acc_metric(acc, *metric) {
                entry.insert(*metric, value);
            }
        }
    }
    let group = GroupBy::Single(dim.to_string());
    // 首个不完整自然日仍按时间窗口回扫。
    if from < first_full {
        let edge =
            scan_edge_groups(shard, filters, &group, metrics, from, first_full - 1, None).await?;
        merge_groups(&mut groups, &edge, metrics);
    }
    if full_end <= to {
        let edge =
            scan_edge_groups(shard, filters, &group, metrics, full_end, to, tail_from_id).await?;
        merge_groups(&mut groups, &edge, metrics);
    }
    Ok(Some(groups))
}

async fn scan_edge_groups(
    shard: &ShardInput,
    filters: &MetricFilters,
    group: &GroupBy,
    metrics: &[MetricName],
    from: i64,
    to: i64,
    after_id: Option<i64>,
) -> Result<metrics_cache::GroupRows, DbError> {
    let (mut where_sql, mut binds) = build_filters_for(filters, from, to, false);
    if let Some(after_id) = after_id {
        where_sql.push_str(" AND id > ?");
        binds.push(DbValue::Integer(after_id));
    }
    let sql = build_sql(&group_expression(group), metrics, &where_sql);
    let rows = fetch_one_shard(&shard.store, &sql, &binds).await?;
    groups_from_rows(rows, metrics)
}

fn spawn_active_build(audit_path: std::path::PathBuf, shard: &ShardInput) {
    if !aggregate::claim_build_spawn(30_000) {
        return;
    }
    let shard_id = shard.id.clone();
    let store = shard.store.clone();
    tokio::spawn(async move {
        if let Err(error) = aggregate::ensure_active(&audit_path, &shard_id, &store).await {
            tracing::warn!(%error, "active rollup unavailable; metrics uses scan path");
        }
    });
}

/// 水位超过 1 小时才触发一次后台补齐；查询本身不等待构建完成。
fn spawn_active_build_if_stale(audit_path: std::path::PathBuf, shard: &ShardInput) {
    if !aggregate::claim_build_spawn(30_000) {
        return;
    }
    let shard_id = shard.id.clone();
    let store = shard.store.clone();
    tokio::spawn(async move {
        if !aggregate::active_rollup_stale(
            &audit_path,
            &shard_id,
            aggregate::ACTIVE_ROLLUP_MAX_AGE_MS,
        )
        .await
        {
            return;
        }
        if let Err(error) = aggregate::ensure_active(&audit_path, &shard_id, &store).await {
            tracing::warn!(%error, "active rollup stale refresh failed; metrics keeps scan path");
        }
    });
}

fn spawn_archive_build(audit_path: std::path::PathBuf, shard: &ShardInput) {
    let Some(path) = shard.path.clone() else {
        return;
    };
    let shard_id = shard.id.clone();
    let store = shard.store.clone();
    let (min_ms, max_ms) = (shard.min_ms, shard.max_ms);
    tokio::spawn(async move {
        if let Err(error) =
            aggregate::ensure_for_shard(audit_path, shard_id, path, store, min_ms, max_ms).await
        {
            tracing::warn!(%error, "archive rollup unavailable; metrics uses scan path");
        }
    });
}

fn job_key(shard: &ShardInput, group: &GroupBy, where_sql: &str, binds: &[DbValue]) -> String {
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

/// 归档缓存键：用文件指纹（大小 + mtime）区分内容。
///
/// `metadata` 在 NFS 上是同步 RPC，这里放到阻塞线程池，避免卡住 tokio worker。
async fn archive_cache_key(job: &Job, sql: &str) -> Option<String> {
    if !job.full {
        return None;
    }
    if job.shard.is_active {
        return Some(format!(
            "active:{}:{sql}:{:?}",
            job.shard.active_month?, job.binds
        ));
    }
    let path = job.shard.path.clone()?;
    let (len, modified) = tokio::task::spawn_blocking({
        let path = path.clone();
        move || {
            let metadata = std::fs::metadata(&path).ok()?;
            let modified = metadata.modified().ok()?;
            Some((metadata.len(), modified))
        }
    })
    .await
    .ok()??;
    let modified = modified.duration_since(SystemTime::UNIX_EPOCH).ok()?;
    Some(format!(
        "{}:{}:{}:{sql}:{:?}",
        path.display(),
        len,
        modified.as_nanos(),
        job.binds
    ))
}

#[cfg(test)]
#[path = "metrics_engine_tests.rs"]
mod tests;
