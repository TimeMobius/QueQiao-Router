//! 只读分析 API：聚合历史审计数据（active 库 + 月度归档）与 `logs/error.*.log`。
//!
//! - `GET /dashboard/api/analysis`：汇总 / 趋势 / 维度分页 / Top。
//! - `GET /dashboard/api/analysis/errors`：错误分组（数据库或日志文件）。
//!
//! 设计约束：零 schema 变更；只读查询；分片查询并发受 `Semaphore` 限制；结果带
//! 15s 进程内缓存；所有标识符来自白名单，所有值均参数绑定。绝不返回请求体、
//! 响应体、原始 API Key 或原始日志行。

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteRow;
use sqlx::{Row, SqlitePool};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;

use crate::db::archive::ACTIVE_SHARD;
use crate::handlers::{error_log_api, records_api};
use crate::state::app_state::AppState;

const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;
const MONTH_MS: i64 = 2_592_000_000;
const DEFAULT_WINDOW_MS: i64 = 7 * DAY_MS;
const MAX_BUCKETS: i64 = 400;
const GROUP_CAP: usize = 5_000;
const COUNT_CAP: i64 = 10_000;
const DISTINCT_CAP: i64 = 10_000;
const PAGE_SIZE_MAX: i64 = 200;
const TOP_LIMIT_MAX: i64 = 50;
const ERRORS_LIMIT_MAX: i64 = 100;
const CACHE_TTL: Duration = Duration::from_secs(15);
const CACHE_MAX_ENTRIES: usize = 256;
const MAX_LOG_LINES: i64 = 200_000;
const MAX_LOG_FILE_BYTES: u64 = 32 * 1024 * 1024;
const SHARD_CONCURRENCY: usize = 8;
const DEFAULT_DIM_EXPR: &str = "COALESCE(NULLIF(Model, ''), 'Unknown')";
const UNKNOWN: &str = "Unknown";

/// 限制同时打到 SQLite 的分片查询数，避免跨月/多归档时打开过多连接。
static SHARD_SEM: Lazy<Semaphore> = Lazy::new(|| Semaphore::new(SHARD_CONCURRENCY));
/// 15s TTL 的进程内结果缓存；键为规范化查询串，插入时清理过期项并设上限。
static CACHE: Lazy<Mutex<HashMap<String, (Instant, Value)>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

/// 分析端点的查询参数。`source`/`limit` 仅错误端点使用，其余端点忽略。
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct AnalysisParams {
    pub from: Option<i64>,
    pub to: Option<i64>,
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub model: Option<String>,
    pub status: Option<i64>,
    pub backend: Option<String>,
    pub ip: Option<String>,
    pub client: Option<String>,
    pub apikey: Option<String>,
    pub errors: Option<String>,
    pub interval: Option<String>,
    pub dimension: Option<String>,
    #[serde(rename = "orderBy")]
    pub order_by: Option<String>,
    pub page: Option<i64>,
    #[serde(rename = "pageSize")]
    pub page_size: Option<i64>,
    #[serde(rename = "topLimit")]
    pub top_limit: Option<i64>,
    pub source: Option<String>,
    pub limit: Option<i64>,
}

/// 一次多分片查询的输入分片（只读归档或 active）。
#[derive(Clone)]
struct ShardInput {
    id: String,
    pool: SqlitePool,
}

#[derive(Debug, Default, Clone, Copy)]
struct Metrics {
    requests: i64,
    success: i64,
    errors: i64,
    prompt_tokens: i64,
    completion_tokens: i64,
    total_tokens: i64,
}

impl Metrics {
    fn add(&mut self, other: Metrics) {
        self.requests += other.requests;
        self.success += other.success;
        self.errors += other.errors;
        self.prompt_tokens += other.prompt_tokens;
        self.completion_tokens += other.completion_tokens;
        self.total_tokens += other.total_tokens;
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct SummaryAgg {
    metrics: Metrics,
    /// 各分片 DISTINCT 计数之和，仅在单分片时精确。
    models: i64,
    ips: i64,
}

type DbErrorKey = (i64, String, String, String);
type LogErrorKey = (
    String,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
);

// ---------------------------------------------------------------------------
// 参数解析与白名单
// ---------------------------------------------------------------------------

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 始终有界：缺省 `to = now`、`from = now - 7d`。
fn resolve_range(p: &AnalysisParams) -> (i64, i64) {
    let now = now_ms();
    let to = p.to.unwrap_or(now);
    let from = p.from.unwrap_or(to.saturating_sub(DEFAULT_WINDOW_MS));
    (from, to)
}

/// 把分析参数映射回 records 的过滤参数，复用完全一致的筛选语义。
///
/// `from`/`to` 传入已解析的区间，保证查询始终有时间上界（缺省 7 天窗口）。
fn to_list_params(p: &AnalysisParams, from: i64, to: i64) -> records_api::ListParams {
    records_api::ListParams {
        from: Some(from),
        to: Some(to),
        type_: p.type_.clone(),
        model: p.model.clone(),
        status: p.status,
        backend: p.backend.clone(),
        ip: p.ip.clone(),
        client: p.client.clone(),
        apikey: p.apikey.clone(),
        errors: p.errors.clone(),
        ..Default::default()
    }
}

fn auto_interval(span: i64) -> String {
    if span <= 48 * HOUR_MS {
        "hour".to_string()
    } else if span <= 92 * DAY_MS {
        "day".to_string()
    } else {
        "month".to_string()
    }
}

fn bucket_count(interval: &str, span: i64) -> i64 {
    let unit = match interval {
        "hour" => HOUR_MS,
        "day" => DAY_MS,
        _ => MONTH_MS,
    };
    span.max(0) / unit + 1
}

/// 解析 `interval`（非法值回退自动），并在桶数超限时自动粗化到 day→month。
fn effective_interval(requested: Option<&str>, from: i64, to: i64) -> (String, Option<String>) {
    let span = to.saturating_sub(from).max(0);
    let mut interval = match requested {
        Some("hour") => "hour",
        Some("day") => "day",
        Some("month") => "month",
        _ => return (auto_interval(span), None),
    }
    .to_string();

    let initial = interval.clone();
    let mut warning = None;
    while interval != "month" && bucket_count(&interval, span) > MAX_BUCKETS {
        interval = if interval == "hour" {
            "day".to_string()
        } else {
            "month".to_string()
        };
        warning = Some(format!(
            "interval auto-coarsened from {initial} to {interval} (> {MAX_BUCKETS} buckets)"
        ));
    }
    (interval, warning)
}

/// 维度 → 分组标签 SQL 表达式（白名单，绝不拼接用户文本）。
fn dimension_expr(dim: &str) -> Option<&'static str> {
    Some(match dim {
        "model" => DEFAULT_DIM_EXPR,
        "apikey" => "COALESCE(NULLIF(ApiKey, ''), 'Unknown')",
        "ip" => "COALESCE(NULLIF(IP, ''), 'Unknown')",
        "type" => "COALESCE(NULLIF(Type, ''), 'Unknown')",
        "backend" => "COALESCE(NULLIF(Backend, ''), 'Unknown')",
        "client" => "COALESCE(NULLIF(ClientName, ''), 'Unknown')",
        "status" => "COALESCE(CAST(Status AS TEXT), 'Unknown')",
        "hour" => "COALESCE(strftime('%H:00', TimeMs/1000, 'unixepoch', 'localtime'), 'Unknown')",
        _ => return None,
    })
}

fn bucket_expr(interval: &str) -> &'static str {
    match interval {
        "hour" => "strftime('%Y-%m-%d %H', TimeMs/1000, 'unixepoch', 'localtime')",
        "month" => "strftime('%Y-%m', TimeMs/1000, 'unixepoch', 'localtime')",
        _ => "strftime('%Y-%m-%d', TimeMs/1000, 'unixepoch', 'localtime')",
    }
}

fn order_metric(order_by: &str) -> &'static str {
    match order_by {
        "errors" => "errors",
        "tokens" => "totalTokens",
        _ => "requests",
    }
}

fn metric_value(m: &Metrics, order: &str) -> i64 {
    match order {
        "errors" => m.errors,
        "totalTokens" => m.total_tokens,
        _ => m.requests,
    }
}

fn clamp_page(v: Option<i64>) -> i64 {
    v.unwrap_or(1).max(1)
}

fn clamp_page_size(v: Option<i64>) -> i64 {
    v.unwrap_or(20).clamp(1, PAGE_SIZE_MAX)
}

fn clamp_top(v: Option<i64>) -> usize {
    v.unwrap_or(8).clamp(1, TOP_LIMIT_MAX) as usize
}

// ---------------------------------------------------------------------------
// SQL（标识符全部来自白名单/常量）
// ---------------------------------------------------------------------------

const SQL_SUCCESS: &str =
    "COALESCE(SUM(CASE WHEN Status >= 200 AND Status < 400 THEN 1 ELSE 0 END),0)";
const SQL_ERRORS: &str = "COALESCE(SUM(CASE WHEN Status >= 400 THEN 1 ELSE 0 END),0)";
const SQL_PROMPT: &str = "COALESCE(SUM(COALESCE(PromptTokens,0)),0)";
const SQL_COMPLETION: &str = "COALESCE(SUM(COALESCE(CompletionTokens,0)),0)";
const SQL_TOTAL: &str = "COALESCE(SUM(COALESCE(TotalTokens,0)),0)";

fn summary_sql(where_sql: &str) -> String {
    format!(
        "SELECT COUNT(*) AS requests, {SQL_SUCCESS} AS success, {SQL_ERRORS} AS errors, \
         {SQL_PROMPT} AS promptTokens, {SQL_COMPLETION} AS completionTokens, \
         {SQL_TOTAL} AS totalTokens, COUNT(DISTINCT Model) AS models, \
         COUNT(DISTINCT IP) AS ips FROM records{where_sql}"
    )
}

fn trend_sql(interval: &str, where_sql: &str) -> String {
    format!(
        "SELECT {} AS label, {SQL_SUCCESS} AS success, {SQL_ERRORS} AS errors, \
         {SQL_PROMPT} AS promptTokens, {SQL_COMPLETION} AS completionTokens, \
         {SQL_TOTAL} AS totalTokens FROM records{where_sql} GROUP BY 1 ORDER BY 1 ASC",
        bucket_expr(interval)
    )
}

fn dims_sql(where_sql: &str, expr: &str, cap: usize) -> String {
    format!(
        "SELECT {expr} AS name, COUNT(*) AS requests, {SQL_SUCCESS} AS success, \
         {SQL_ERRORS} AS errors, {SQL_PROMPT} AS promptTokens, \
         {SQL_COMPLETION} AS completionTokens, {SQL_TOTAL} AS totalTokens \
         FROM records{where_sql} GROUP BY 1 LIMIT {cap}"
    )
}

fn errors_db_sql(where_sql: &str) -> String {
    format!(
        "SELECT Status AS status, COALESCE(NULLIF(Model,''),'Unknown') AS model, \
         COALESCE(Backend,'') AS backend, COALESCE(NULLIF(Error,''),'-') AS error, \
         COUNT(*) AS c FROM records{where_sql} AND Status >= 400 GROUP BY 1,2,3,4 \
         ORDER BY c DESC LIMIT {}",
        COUNT_CAP + 1
    )
}

// ---------------------------------------------------------------------------
// 分片查询（并发受限）
// ---------------------------------------------------------------------------

async fn fetch(
    pool: &SqlitePool,
    sql: &str,
    binds: &[records_api::Bind],
) -> Result<Vec<SqliteRow>, sqlx::Error> {
    records_api::bind_all(sqlx::query(sql), binds)
        .fetch_all(pool)
        .await
}

async fn fetch_all_shards(
    shards: &[ShardInput],
    sql: &str,
    binds: &[records_api::Bind],
) -> Result<Vec<Vec<SqliteRow>>, sqlx::Error> {
    let futs = shards.iter().map(|s| {
        let pool = s.pool.clone();
        let sql = sql.to_string();
        let binds = binds.to_vec();
        async move {
            let _permit = SHARD_SEM.acquire().await;
            fetch(&pool, &sql, &binds).await
        }
    });
    let results = futures::future::join_all(futs).await;
    let mut out = Vec::with_capacity(results.len());
    for r in results {
        out.push(r?);
    }
    Ok(out)
}

/// 跨分片精确基数：各分片取 DISTINCT 值后在 Rust 侧并集去重，避免
/// 「各分片 COUNT(DISTINCT) 相加」在跨月时重复计数。`column` 仅接受白名单字面量。
async fn distinct_count_across_shards(
    shards: &[ShardInput],
    where_sql: &str,
    binds: &[records_api::Bind],
    column: &str,
) -> Result<(usize, bool), sqlx::Error> {
    let sql = format!(
        "SELECT DISTINCT {column} AS v FROM records{where_sql} \
         AND {column} IS NOT NULL AND {column} <> '' LIMIT {DISTINCT_CAP}"
    );
    let rows = fetch_all_shards(shards, &sql, binds).await?;
    let mut set: HashSet<String> = HashSet::new();
    for shard_rows in &rows {
        for row in shard_rows {
            if let Some(value) = col_text(row, "v") {
                set.insert(value);
            }
        }
    }
    Ok((set.len(), set.len() >= DISTINCT_CAP as usize))
}

/// 组装分片列表：active 恒在首位，命中候选归档追加在后。
async fn collect_shards(app_state: &Arc<AppState>, from: i64, to: i64) -> Vec<ShardInput> {
    let candidates = app_state
        .archive_registry
        .candidates(Some(from), Some(to))
        .await;
    let active = app_state.db_pool.read().await.clone();
    let mut shards = Vec::with_capacity(candidates.len() + 1);
    shards.push(ShardInput {
        id: ACTIVE_SHARD.to_string(),
        pool: active,
    });
    for c in &candidates {
        shards.push(ShardInput {
            id: c.id.clone(),
            pool: c.pool.clone(),
        });
    }
    shards
}

/// 检测归档目录中未被注册的 `record_YYYYMM*` 文件（遗留/不支持分片）。
fn skipped_archive_warning(registered: &HashSet<String>) -> Option<String> {
    let path = crate::db::resolve_db_path();
    let dir = path.parent()?;
    let entries = std::fs::read_dir(dir).ok()?;
    let mut skipped = 0usize;
    for entry in entries.flatten() {
        let p = entry.path();
        if !p.is_file() {
            continue;
        }
        let Some(stem) = p.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Some(rest) = stem.strip_prefix("record_") else {
            continue;
        };
        let bytes = rest.as_bytes();
        if bytes.len() < 6 || !bytes[..6].iter().all(|b| b.is_ascii_digit()) {
            continue;
        }
        if !registered.contains(stem) {
            skipped += 1;
        }
    }
    (skipped > 0).then(|| format!("{skipped} legacy/unsupported archive shard(s) skipped"))
}

// ---------------------------------------------------------------------------
// 行解析与合并
// ---------------------------------------------------------------------------

fn col_i64(row: &SqliteRow, name: &str) -> i64 {
    row.try_get::<Option<i64>, _>(name)
        .ok()
        .flatten()
        .unwrap_or(0)
}

fn col_text(row: &SqliteRow, name: &str) -> Option<String> {
    row.try_get::<Option<String>, _>(name).ok().flatten()
}

fn metrics_from_row(row: &SqliteRow) -> Metrics {
    Metrics {
        requests: col_i64(row, "requests"),
        success: col_i64(row, "success"),
        errors: col_i64(row, "errors"),
        prompt_tokens: col_i64(row, "promptTokens"),
        completion_tokens: col_i64(row, "completionTokens"),
        total_tokens: col_i64(row, "totalTokens"),
    }
}

fn summary_from_row(row: &SqliteRow) -> SummaryAgg {
    SummaryAgg {
        metrics: metrics_from_row(row),
        models: col_i64(row, "models"),
        ips: col_i64(row, "ips"),
    }
}

fn merge_summaries(aggs: &[SummaryAgg]) -> SummaryAgg {
    let mut out = SummaryAgg::default();
    for a in aggs {
        out.metrics.add(a.metrics);
        out.models += a.models;
        out.ips += a.ips;
    }
    out
}

fn merge_group_map(per_shard: Vec<Vec<(String, Metrics)>>) -> HashMap<String, Metrics> {
    let mut out: HashMap<String, Metrics> = HashMap::new();
    for shard in per_shard {
        for (name, m) in shard {
            out.entry(name).or_default().add(m);
        }
    }
    out
}

fn merge_error_maps(maps: Vec<HashMap<DbErrorKey, i64>>) -> HashMap<DbErrorKey, i64> {
    let mut out: HashMap<DbErrorKey, i64> = HashMap::new();
    for m in maps {
        for (k, v) in m {
            *out.entry(k).or_insert(0) += v;
        }
    }
    out
}

fn normalize_opt(value: Option<String>) -> Option<String> {
    value.and_then(|s| {
        let t = s.trim();
        if t.is_empty() || t == "-" {
            None
        } else {
            Some(t.to_string())
        }
    })
}

// ---------------------------------------------------------------------------
// 缓存
// ---------------------------------------------------------------------------

fn cache_key(p: &AnalysisParams, kind: &str) -> String {
    format!("{kind}:{}", serde_json::to_string(p).unwrap_or_default())
}

fn cache_get(key: &str) -> Option<Value> {
    let mut cache = CACHE.lock().ok()?;
    let expired = cache
        .get(key)
        .map(|(t, _)| t.elapsed() >= CACHE_TTL)
        .unwrap_or(false);
    if expired {
        cache.remove(key);
        return None;
    }
    cache.get(key).map(|(_, v)| v.clone())
}

fn cache_put(key: String, value: Value) {
    if let Ok(mut cache) = CACHE.lock() {
        cache.retain(|_, (t, _)| t.elapsed() < CACHE_TTL);
        if cache.len() >= CACHE_MAX_ENTRIES {
            cache.clear();
        }
        cache.insert(key, (Instant::now(), value));
    }
}

fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    tracing::error!("analysis api error: {e}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "query failed".to_string(),
    )
}

// ---------------------------------------------------------------------------
// 端点：/dashboard/api/analysis
// ---------------------------------------------------------------------------

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

    let (where_sql, binds) = records_api::build_filters(&to_list_params(p, from, to));

    // 汇总
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
    let trend_rows = fetch_all_shards(&shards, &trend_sql(&interval, &where_sql), &binds)
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

    // 维度
    let dim_rows = fetch_all_shards(
        &shards,
        &dims_sql(&where_sql, dim_expr, GROUP_CAP + 1),
        &binds,
    )
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
            })
        })
        .collect();

    Ok(json!({
        "from": from,
        "to": to,
        "interval": interval,
        "dimension": dim,
        "shards": shard_ids,
        "warnings": warnings,
        "summary": {
            "requests": summary.metrics.requests,
            "success": summary.metrics.success,
            "errors": summary.metrics.errors,
            "promptTokens": summary.metrics.prompt_tokens,
            "completionTokens": summary.metrics.completion_tokens,
            "totalTokens": summary.metrics.total_tokens,
            "models": summary.models,
            "ips": summary.ips,
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

// ---------------------------------------------------------------------------
// 端点：/dashboard/api/analysis/errors
// ---------------------------------------------------------------------------

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

    let (where_sql, binds) = records_api::build_filters(&to_list_params(p, from, to));
    let rows_per_shard = fetch_all_shards(&shards, &errors_db_sql(&where_sql), &binds)
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

    let scan = tokio::task::spawn_blocking(move || scan_error_logs(&dir, from, to, limit))
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

// ---------------------------------------------------------------------------
// 错误日志文件扫描（blocking 任务内执行）
// ---------------------------------------------------------------------------

struct LogGroup {
    kind: String,
    status: Option<i64>,
    model: Option<String>,
    backend: Option<String>,
    error: Option<String>,
    count: i64,
}

struct LogScan {
    groups: Vec<LogGroup>,
    unparsed: i64,
    files: Vec<String>,
    warnings: Vec<String>,
    total: i64,
}

/// 解析 `17/Sep/2026:07:28:00 +0000` 为 epoch 毫秒；失败返回 None。
fn parse_log_time(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_str(s, "%d/%b/%Y:%H:%M:%S %z")
        .ok()
        .map(|dt| dt.timestamp_millis())
}

fn scan_error_logs(dir: &Path, from: Option<i64>, to: Option<i64>, limit: usize) -> LogScan {
    let mut warnings: Vec<String> = Vec::new();
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("error.") && name.ends_with(".log") && entry.path().is_file() {
                files.push(entry.path());
            }
        }
    }
    files.sort();

    let mut counts: HashMap<LogErrorKey, i64> = HashMap::new();
    let mut unparsed: i64 = 0;
    let mut total_lines: i64 = 0;
    let mut read_files: Vec<String> = Vec::new();
    let range_active = from.is_some() || to.is_some();
    let lo = from.unwrap_or(i64::MIN);
    let hi = to.unwrap_or(i64::MAX);

    'outer: for path in &files {
        let Ok(file) = std::fs::File::open(path) else {
            continue;
        };
        read_files.push(
            path.file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
        );
        let mut file_bytes: u64 = 0;
        for line in BufReader::new(file).lines() {
            let Ok(line) = line else {
                break;
            };
            file_bytes += line.len() as u64 + 1;
            total_lines += 1;
            if total_lines > MAX_LOG_LINES {
                warnings.push(format!("log scan truncated at {MAX_LOG_LINES} lines"));
                break 'outer;
            }
            if file_bytes > MAX_LOG_FILE_BYTES {
                warnings.push("log scan truncated by per-file byte limit".to_string());
                break;
            }
            let entry = error_log_api::parse_line(&line);
            if entry.kind == "unparsed" {
                unparsed += 1;
            }
            if range_active {
                match entry.time.as_deref().and_then(parse_log_time) {
                    Some(t) => {
                        if t < lo || t > hi {
                            continue;
                        }
                    }
                    // 时间无法解析的条目被保留并计数。
                    None => unparsed += 1,
                }
            }
            let key: LogErrorKey = (
                entry.kind,
                entry.status,
                normalize_opt(entry.model),
                normalize_opt(entry.backend),
                normalize_opt(entry.error),
            );
            *counts.entry(key).or_insert(0) += 1;
        }
    }

    let mut groups: Vec<LogGroup> = counts
        .into_iter()
        .map(|((kind, status, model, backend, error), count)| LogGroup {
            kind,
            status,
            model,
            backend,
            error,
            count,
        })
        .collect();
    groups.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.kind.cmp(&b.kind)));
    let total = groups.len() as i64;
    groups.truncate(limit);
    LogScan {
        groups,
        unparsed,
        files: read_files,
        warnings,
        total,
    }
}

// ---------------------------------------------------------------------------
// 测试
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_auto_by_span_and_explicit_override() {
        assert_eq!(effective_interval(None, 0, HOUR_MS).0, "hour");
        assert_eq!(effective_interval(None, 0, 10 * DAY_MS).0, "day");
        assert_eq!(effective_interval(None, 0, 200 * DAY_MS).0, "month");
        assert_eq!(effective_interval(Some("day"), 0, HOUR_MS).0, "day");
        assert_eq!(effective_interval(Some("month"), 0, HOUR_MS).0, "month");
        // 非法值回退为自动选择。
        assert_eq!(effective_interval(Some("week"), 0, HOUR_MS).0, "hour");
    }

    #[test]
    fn interval_coarsens_when_bucket_count_exceeds_limit() {
        let span = 60 * DAY_MS;
        let (interval, warning) = effective_interval(Some("hour"), 0, span);
        assert_eq!(interval, "day");
        assert!(warning.is_some());
        // 60 天按天约 61 桶，未超限，不再继续粗化。
        assert!(bucket_count("day", span) <= MAX_BUCKETS);
    }

    #[test]
    fn dimension_whitelist_rejects_unknown_identifiers() {
        for ok in [
            "model", "apikey", "ip", "type", "status", "backend", "client", "hour",
        ] {
            assert!(dimension_expr(ok).is_some(), "{ok} should be allowed");
        }
        assert!(dimension_expr("id").is_none());
        assert!(dimension_expr("Model").is_none());
        assert!(dimension_expr("1; DROP TABLE records").is_none());
        assert!(dimension_expr("").is_none());
    }

    #[test]
    fn summary_merge_sums_counts_and_tokens_not_averages() {
        let a = SummaryAgg {
            metrics: Metrics {
                requests: 10,
                success: 5,
                errors: 2,
                prompt_tokens: 100,
                completion_tokens: 50,
                total_tokens: 150,
            },
            models: 2,
            ips: 3,
        };
        let b = SummaryAgg {
            metrics: Metrics {
                requests: 30,
                success: 27,
                errors: 1,
                prompt_tokens: 400,
                completion_tokens: 100,
                total_tokens: 500,
            },
            models: 4,
            ips: 5,
        };
        let merged = merge_summaries(&[a, b]);
        assert_eq!(merged.metrics.requests, 40);
        assert_eq!(merged.metrics.success, 32);
        assert_eq!(merged.metrics.errors, 3);
        assert_eq!(merged.metrics.prompt_tokens, 500);
        assert_eq!(merged.metrics.completion_tokens, 150);
        assert_eq!(merged.metrics.total_tokens, 650);
        // DISTINCT 计数是各分片之和（上界语义）。
        assert_eq!(merged.models, 6);
        assert_eq!(merged.ips, 8);
        // 若误用平均，success 会是 16；确认不是平均值。
        assert_ne!(merged.metrics.success, (5 + 27) / 2);
    }

    #[test]
    fn error_group_merge_accumulates_same_key_across_shards() {
        let mut first: HashMap<DbErrorKey, i64> = HashMap::new();
        first.insert(
            (422, "m1".to_string(), "b1".to_string(), "bad".to_string()),
            3,
        );
        first.insert(
            (500, "m2".to_string(), "b2".to_string(), "boom".to_string()),
            1,
        );
        let mut second: HashMap<DbErrorKey, i64> = HashMap::new();
        second.insert(
            (422, "m1".to_string(), "b1".to_string(), "bad".to_string()),
            7,
        );
        let merged = merge_error_maps(vec![first, second]);
        assert_eq!(
            merged.get(&(422, "m1".to_string(), "b1".to_string(), "bad".to_string())),
            Some(&10)
        );
        assert_eq!(
            merged.get(&(500, "m2".to_string(), "b2".to_string(), "boom".to_string())),
            Some(&1)
        );
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn parses_nginx_error_log_timestamp() {
        assert_eq!(
            parse_log_time("17/Sep/2026:07:28:00 +0000"),
            Some(1_789_630_080_000)
        );
        // 带偏移的输入应按偏移换算到 UTC。
        assert_eq!(
            parse_log_time("17/Sep/2026:15:28:00 +0800"),
            Some(1_789_630_080_000)
        );
        assert!(parse_log_time("not a time").is_none());
        assert!(parse_log_time("").is_none());
    }

    #[test]
    fn order_metric_and_metric_value_map_whitelist() {
        let m = Metrics {
            requests: 9,
            errors: 4,
            total_tokens: 100,
            ..Default::default()
        };
        assert_eq!(metric_value(&m, order_metric("requests")), 9);
        assert_eq!(metric_value(&m, order_metric("errors")), 4);
        assert_eq!(metric_value(&m, order_metric("tokens")), 100);
        assert_eq!(metric_value(&m, order_metric("bogus")), 9);
    }

    #[test]
    fn filters_are_always_time_bounded_even_without_input_range() {
        let (from, to) = resolve_range(&AnalysisParams::default());
        assert!(to > from);
        assert_eq!(to - from, DEFAULT_WINDOW_MS);
        let (sql, _) =
            records_api::build_filters(&to_list_params(&AnalysisParams::default(), from, to));
        assert!(sql.contains("TimeMs >= ?"));
        assert!(sql.contains("TimeMs <= ?"));
    }

    #[test]
    fn bucket_label_expr_matches_interval_granularity() {
        assert!(bucket_expr("hour").contains("%Y-%m-%d %H"));
        assert!(bucket_expr("day").contains("%Y-%m-%d'"));
        assert!(bucket_expr("month").contains("%Y-%m"));
        assert_eq!(bucket_expr("bogus"), bucket_expr("day"));
    }

    async fn temp_db(tag: &str) -> (std::path::PathBuf, SqlitePool) {
        let nanos = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let path = std::env::temp_dir().join(format!(
            "qq_analysis_{}_{}_{}.db",
            std::process::id(),
            tag,
            nanos
        ));
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await.unwrap();
        sqlx::query(
            "CREATE TABLE records (\
                id INTEGER PRIMARY KEY, TimeMs INTEGER, Type TEXT, Model TEXT, Status INTEGER, \
                Backend TEXT, IP TEXT, ClientName TEXT, UserAgent TEXT, ApiKey TEXT, \
                PromptTokens INTEGER, CompletionTokens INTEGER, TotalTokens INTEGER, Error TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        (path, pool)
    }

    #[tokio::test]
    async fn sql_queries_execute_and_aggregate_on_temp_db() {
        let (path, pool) = temp_db("agg").await;
        sqlx::query(
            "INSERT INTO records (id, TimeMs, Type, Model, Status, Backend, IP, ClientName, \
             UserAgent, ApiKey, PromptTokens, CompletionTokens, TotalTokens, Error) \
             VALUES (1, 3600000, 'chat', 'm1', 200, 'b1', '1.1.1.1', 'c1', 'ua', 'k1', 10, 5, 15, NULL), \
                    (2, 7200000, 'chat', 'm1', 500, 'b1', '1.1.1.1', 'c1', 'ua', 'k1', 20, 8, 28, 'boom')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let shards = [ShardInput {
            id: ACTIVE_SHARD.to_string(),
            pool: pool.clone(),
        }];
        let (where_sql, binds) = records_api::build_filters(&records_api::ListParams::default());

        let summary_rows = fetch_all_shards(&shards, &summary_sql(&where_sql), &binds)
            .await
            .unwrap();
        let merged = merge_summaries(
            &summary_rows
                .iter()
                .flat_map(|rows| rows.iter().map(summary_from_row))
                .collect::<Vec<_>>(),
        );
        assert_eq!(merged.metrics.requests, 2);
        assert_eq!(merged.metrics.success, 1);
        assert_eq!(merged.metrics.errors, 1);
        assert_eq!(merged.metrics.total_tokens, 43);
        assert_eq!(merged.models, 1);
        assert_eq!(merged.ips, 1);

        let dim_rows = fetch_all_shards(
            &shards,
            &dims_sql(&where_sql, DEFAULT_DIM_EXPR, GROUP_CAP + 1),
            &binds,
        )
        .await
        .unwrap();
        let dim = metrics_from_row(&dim_rows[0][0]);
        assert_eq!(dim.requests, 2);
        assert_eq!(dim.total_tokens, 43);

        let err_rows = fetch_all_shards(&shards, &errors_db_sql(&where_sql), &binds)
            .await
            .unwrap();
        let row = &err_rows[0][0];
        assert_eq!(col_i64(row, "status"), 500);
        assert_eq!(col_text(row, "model").as_deref(), Some("m1"));
        assert_eq!(col_text(row, "error").as_deref(), Some("boom"));
        assert_eq!(col_i64(row, "c"), 1);

        let trend_rows = fetch_all_shards(&shards, &trend_sql("hour", &where_sql), &binds)
            .await
            .unwrap();
        let trend_total: i64 = trend_rows[0]
            .iter()
            .map(|r| col_i64(r, "totalTokens"))
            .sum();
        assert_eq!(trend_total, 43);
        assert!(!trend_rows[0].is_empty());

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }
}
