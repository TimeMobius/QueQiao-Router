//! 端点：`GET /dashboard/api/analysis/errors`。

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;

use crate::state::app_state::AppState;

use super::aggregation::{col_i64, col_text, merge_error_maps, DbErrorKey};
use super::cache::{cache_get, cache_key, cache_put, internal};
use super::log_scan::scan_error_logs;
use super::params::{
    effective_interval, resolve_range, to_list_params, AnalysisParams, COUNT_CAP, ERRORS_LIMIT_MAX,
    UNKNOWN,
};
use super::shards::{collect_shards, fetch_sliced, skipped_archive_warning};
use super::sql::errors_db_sql;

pub async fn analysis_errors(
    State(app_state): State<Arc<AppState>>,
    Query(params): Query<AnalysisParams>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let key = cache_key(&params, "errors");
    if let Some(cached) = cache_get(&key) {
        return Ok(Json(cached));
    }
    let is_log = params
        .source
        .as_deref()
        .map(|s| s.eq_ignore_ascii_case("log"))
        .unwrap_or(false);
    let value = if is_log {
        build_errors_log(&params).await?
    } else {
        build_errors_db(&app_state, &params).await?
    };
    cache_put(key, value.clone());
    Ok(Json(value))
}

async fn build_errors_db(
    app_state: &Arc<AppState>,
    p: &AnalysisParams,
) -> Result<Value, (StatusCode, String)> {
    let (from, to) = resolve_range(p);
    let mut warnings: Vec<String> = Vec::new();
    let shards = collect_shards(app_state, from, to).await;
    if shards.len() > 1 {
        warnings.push("counts are summed across shards".to_string());
    }
    let skipped = app_state.archive_registry.skipped_count().await;
    if let Some(w) = skipped_archive_warning(skipped) {
        warnings.push(w);
    }
    let shard_ids: Vec<String> = shards.iter().map(|s| s.id.clone()).collect();

    let rows_per_shard = fetch_sliced(&shards, p, from, to, errors_db_sql)
        .await
        .map_err(internal)?;

    let maps: Vec<HashMap<DbErrorKey, i64>> = rows_per_shard
        .iter()
        .map(|rows| {
            let mut map: HashMap<DbErrorKey, i64> = HashMap::new();
            for r in rows {
                let key = (
                    col_i64(r, "status"),
                    col_text(r, "model").unwrap_or_else(|| UNKNOWN.to_string()),
                    col_text(r, "backend").unwrap_or_default(),
                    col_text(r, "error").unwrap_or_else(|| "-".to_string()),
                );
                *map.entry(key).or_insert(0) += col_i64(r, "c");
            }
            map
        })
        .collect();
    let mut items: Vec<(DbErrorKey, i64)> = merge_error_maps(maps).into_iter().collect();
    items.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| a.0 .0.cmp(&b.0 .0))
            .then_with(|| a.0 .1.cmp(&b.0 .1))
    });

    let total_all = items.len() as i64;
    let total_exact = total_all <= COUNT_CAP;
    if !total_exact {
        warnings.push(format!("error groups capped at {COUNT_CAP}"));
    }
    items.truncate(COUNT_CAP as usize);

    let limit = p.limit.unwrap_or(20).clamp(1, ERRORS_LIMIT_MAX) as usize;
    let items: Vec<Value> = items
        .into_iter()
        .take(limit)
        .map(|((status, model, backend, error), count)| {
            json!({
                "status": status,
                "model": model,
                "backend": backend,
                "error": error,
                "count": count,
            })
        })
        .collect();

    Ok(json!({
        "source": "db",
        "from": from,
        "to": to,
        "shards": shard_ids,
        "warnings": warnings,
        "items": items,
        "total": total_all.min(COUNT_CAP),
    }))
}

async fn build_errors_log(p: &AnalysisParams) -> Result<Value, (StatusCode, String)> {
    let limit = p.limit.unwrap_or(20).clamp(1, ERRORS_LIMIT_MAX) as usize;
    let from = p.from;
    let to = p.to;
    let dir = std::path::PathBuf::from(crate::logging::DEFAULT_LOG_DIR);
    let (rf, rt) = resolve_range(p);
    let (interval, _) = effective_interval(p.interval.as_deref(), rf, rt);

    let scan_filters = to_list_params(p, rf, rt);
    let scan = tokio::task::spawn_blocking(move || {
        scan_error_logs(&dir, from, to, limit, &interval, &scan_filters)
    })
    .await
    .map_err(internal)?;

    let items: Vec<Value> = scan
        .groups
        .iter()
        .map(|g| {
            json!({
                "kind": g.kind,
                "status": g.status,
                "model": g.model,
                "backend": g.backend,
                "error": g.error,
                "count": g.count,
            })
        })
        .collect();

    Ok(json!({
        "source": "log",
        "files": scan.files,
        "warnings": scan.warnings,
        "items": items,
        "unparsed": scan.unparsed,
        "total": scan.total,
    }))
}
