//! 端点：`GET /dashboard/api/analysis`。

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;

use crate::db::records_query;
use crate::state::app_state::AppState;

use super::aggregation::{
    col_text, merge_group_map, merge_histograms, merge_summaries, metrics_from_row, percentile_ms,
    summary_from_row, Metrics, SummaryAgg,
};
use super::cache::{cache_get, cache_key, cache_put, internal};
use super::log_scan::scan_error_logs;
use super::params::{
    clamp_page, clamp_page_size, clamp_top, dimension_expr, effective_interval, metric_value,
    order_metric, resolve_range, to_list_params, AnalysisParams, COUNT_CAP, DEFAULT_DIM_EXPR,
    DISTINCT_CAP, GROUP_CAP, LATENCY_COL, MAX_BUCKETS, TTFT_COL, UNKNOWN,
};
use super::shards::{
    collect_shards, distinct_count_across_shards, fetch_all_shards, fetch_sliced,
    skipped_archive_warning,
};
use super::sql::{dims_sql, latency_hist_sql, summary_sql, trend_sql};

pub async fn analysis(
    State(app_state): State<Arc<AppState>>,
    Query(params): Query<AnalysisParams>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let key = cache_key(&params, "analysis");
    if let Some(cached) = cache_get(&key) {
        return Ok(Json(cached));
    }
    let value = build_analysis(&app_state, &params).await?;
    cache_put(key, value.clone());
    Ok(Json(value))
}

async fn build_analysis(
    app_state: &Arc<AppState>,
    p: &AnalysisParams,
) -> Result<Value, (StatusCode, String)> {
    let (from, to) = resolve_range(p);
    let (interval, interval_warning) = effective_interval(p.interval.as_deref(), from, to);

    let requested_dim = p.dimension.as_deref().unwrap_or("model");
    let dim = if dimension_expr(requested_dim).is_some() {
        requested_dim
    } else {
        "model"
    };
    let dim_expr = dimension_expr(dim).unwrap_or(DEFAULT_DIM_EXPR);

    let mut warnings: Vec<String> = Vec::new();
    if let Some(w) = interval_warning {
        warnings.push(w);
    }

    let shards = collect_shards(app_state, from, to).await;
    let registered: HashSet<String> = app_state
        .archive_registry
        .all_shards()
        .await
        .iter()
        .map(|s| s.id.clone())
        .collect();
    if let Some(w) = skipped_archive_warning(&registered) {
        warnings.push(w);
    }
    let shard_ids: Vec<String> = shards.iter().map(|s| s.id.clone()).collect();

    // 汇总
    let (where_sql, binds) = records_query::build_filters(&to_list_params(p, from, to));
    let summary_rows = fetch_all_shards(&shards, &summary_sql(&where_sql), &binds)
        .await
        .map_err(internal)?;
    let summaries: Vec<SummaryAgg> = summary_rows
        .iter()
        .flat_map(|rows| rows.iter().map(summary_from_row))
        .collect();
    let mut summary = merge_summaries(&summaries);

    // 单分片时 COUNT(DISTINCT) 已精确；跨分片改为并集去重，避免重复计数。
    if shards.len() > 1 {
        let (models, models_capped) =
            distinct_count_across_shards(&shards, &where_sql, &binds, "Model")
                .await
                .map_err(internal)?;
        let (ips, ips_capped) = distinct_count_across_shards(&shards, &where_sql, &binds, "IP")
            .await
            .map_err(internal)?;
        summary.models = models as i64;
        summary.ips = ips as i64;
        if models_capped || ips_capped {
            warnings.push(format!("distinct model/ip count capped at {DISTINCT_CAP}"));
        }
    }

    // 趋势
    let trend_rows = fetch_sliced(&shards, p, from, to, |w| trend_sql(&interval, w))
        .await
        .map_err(internal)?;
    let per_shard_trend: Vec<Vec<(String, Metrics)>> = trend_rows
        .iter()
        .map(|rows| {
            rows.iter()
                .map(|r| {
                    (
                        col_text(r, "label").unwrap_or_else(|| UNKNOWN.to_string()),
                        metrics_from_row(r),
                    )
                })
                .collect()
        })
        .collect();
    let mut trend: Vec<(String, Metrics)> = merge_group_map(per_shard_trend).into_iter().collect();
    trend.sort_by(|a, b| a.0.cmp(&b.0));
    if trend.len() > MAX_BUCKETS as usize {
        let drop = trend.len() - MAX_BUCKETS as usize;
        trend.drain(..drop);
        warnings.push(format!("trend capped to last {MAX_BUCKETS} buckets"));
    }

    // 错误日志捕获的是 DB 之外的另一类错误（硬失败 500/503/404/400/422 等，多为非流式
    // 请求且未写入 DB），与 DB 的流式错误（499/502）互不重叠，可直接相加得到完整错误集，
    // 并按趋势粒度并入对应时间桶。
    let log_dir = std::path::PathBuf::from(crate::logging::DEFAULT_LOG_DIR);
    let interval_for_scan = interval.clone();
    let scan_filters = to_list_params(p, from, to);
    let log_scan = tokio::task::spawn_blocking(move || {
        scan_error_logs(
            &log_dir,
            Some(from),
            Some(to),
            1,
            &interval_for_scan,
            &scan_filters,
        )
    })
    .await
    .map_err(internal)?;
    for (label, count) in &log_scan.per_bucket {
        if let Some(entry) = trend.iter_mut().find(|(l, _)| l == label) {
            entry.1.errors += *count;
        } else {
            trend.push((
                label.clone(),
                Metrics {
                    errors: *count,
                    ..Default::default()
                },
            ));
        }
    }
    trend.sort_by(|a, b| a.0.cmp(&b.0));
    if trend.len() > MAX_BUCKETS as usize {
        let drop = trend.len() - MAX_BUCKETS as usize;
        trend.drain(..drop);
    }

    // 维度
    let dim_rows = fetch_sliced(&shards, p, from, to, |w| {
        dims_sql(w, dim_expr, GROUP_CAP + 1)
    })
    .await
    .map_err(internal)?;
    let mut dim_capped = false;
    let per_shard_dims: Vec<Vec<(String, Metrics)>> = dim_rows
        .iter()
        .map(|rows| {
            let mut v: Vec<(String, Metrics)> = rows
                .iter()
                .map(|r| {
                    (
                        col_text(r, "name").unwrap_or_else(|| UNKNOWN.to_string()),
                        metrics_from_row(r),
                    )
                })
                .collect();
            if v.len() > GROUP_CAP {
                dim_capped = true;
                v.truncate(GROUP_CAP);
            }
            v
        })
        .collect();
    let mut groups: Vec<(String, Metrics)> = merge_group_map(per_shard_dims).into_iter().collect();
    let order = order_metric(p.order_by.as_deref().unwrap_or("requests"));
    groups.sort_by(|a, b| {
        metric_value(&b.1, order)
            .cmp(&metric_value(&a.1, order))
            .then_with(|| a.0.cmp(&b.0))
    });

    // Top 取排序前的完整合并集合，始终按 requests 降序。
    let top_limit = clamp_top(p.top_limit);
    let mut top_sorted = groups.clone();
    top_sorted.sort_by(|a, b| b.1.requests.cmp(&a.1.requests).then_with(|| a.0.cmp(&b.0)));
    let top: Vec<Value> = top_sorted
        .into_iter()
        .take(top_limit)
        .map(|(name, m)| json!({"name": name, "value": m.requests}))
        .collect();

    let total_all = groups.len() as i64;
    let mut total_exact = true;
    if dim_capped {
        warnings.push(format!("dimension groups capped at {GROUP_CAP} per shard"));
        total_exact = false;
    }
    if total_all > COUNT_CAP {
        groups.truncate(COUNT_CAP as usize);
        total_exact = false;
    }

    let page = clamp_page(p.page);
    let page_size = clamp_page_size(p.page_size);
    let start = page.saturating_sub(1).saturating_mul(page_size);
    let start = usize::try_from(start).unwrap_or(usize::MAX);
    let items: Vec<Value> = groups
        .iter()
        .skip(start)
        .take(page_size as usize)
        .map(|(name, m)| {
            json!({
                "name": name,
                "requests": m.requests,
                "success": m.success,
                "errors": m.errors,
                "promptTokens": m.prompt_tokens,
                "completionTokens": m.completion_tokens,
                "totalTokens": m.total_tokens,
                "avgLatencyMs": m.avg_latency(),
                "avgTtftMs": m.avg_ttft(),
            })
        })
        .collect();

    let trend_json: Vec<Value> = trend
        .iter()
        .map(|(label, m)| {
            json!({
                "label": label,
                "success": m.success,
                "errors": m.errors,
                "promptTokens": m.prompt_tokens,
                "completionTokens": m.completion_tokens,
                "totalTokens": m.total_tokens,
                "avgLatencyMs": m.avg_latency(),
            })
        })
        .collect();

    // 时延分位采用固定宽度直方图合并后近似；均值/最大来自精确聚合，无额外扫描放大。
    let latency_hist =
        fetch_all_shards(&shards, &latency_hist_sql(&where_sql, LATENCY_COL), &binds)
            .await
            .map_err(internal)?;
    let latency_buckets = merge_histograms(&latency_hist);
    let ttft_hist = fetch_all_shards(&shards, &latency_hist_sql(&where_sql, TTFT_COL), &binds)
        .await
        .map_err(internal)?;
    let ttft_buckets = merge_histograms(&ttft_hist);

    let db_errors = summary.metrics.errors;
    let log_errors = log_scan.total_error_entries;

    Ok(json!({
        "from": from,
        "to": to,
        "interval": interval,
        "dimension": dim,
        "shards": shard_ids,
        "warnings": warnings,
        "summary": {
            "requests": summary.metrics.requests + log_errors,
            "success": summary.metrics.success,
            "errors": db_errors + log_errors,
            "dbErrors": db_errors,
            "logErrors": log_errors,
            "promptTokens": summary.metrics.prompt_tokens,
            "completionTokens": summary.metrics.completion_tokens,
            "totalTokens": summary.metrics.total_tokens,
            "models": summary.models,
            "ips": summary.ips,
            "latency": {
                "avgMs": summary.metrics.avg_latency(),
                "maxMs": (summary.metrics.latency_count > 0).then_some(summary.metrics.latency_max),
                "p50Ms": percentile_ms(&latency_buckets, summary.metrics.latency_count, 0.50),
                "p95Ms": percentile_ms(&latency_buckets, summary.metrics.latency_count, 0.95),
                "p99Ms": percentile_ms(&latency_buckets, summary.metrics.latency_count, 0.99),
                "avgTtftMs": summary.metrics.avg_ttft(),
                "p95TtftMs": percentile_ms(&ttft_buckets, summary.metrics.ttft_count, 0.95),
            },
        },
        "trend": trend_json,
        "dimensions": {
            "name": dim,
            "page": page,
            "pageSize": page_size,
            "total": total_all.min(COUNT_CAP),
            "totalExact": total_exact,
            "items": items,
        },
        "top": top,
    }))
}
