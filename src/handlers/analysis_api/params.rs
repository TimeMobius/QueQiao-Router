//! 参数解析与白名单：查询参数结构、时间区间、interval 粗化、维度/排序白名单。

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::handlers::records_api;

use super::aggregation::Metrics;

pub(super) const HOUR_MS: i64 = 3_600_000;
pub(super) const DAY_MS: i64 = 86_400_000;
pub(super) const MONTH_MS: i64 = 2_592_000_000;
pub(super) const DEFAULT_WINDOW_MS: i64 = 7 * DAY_MS;
pub(super) const MAX_BUCKETS: i64 = 400;
pub(super) const GROUP_CAP: usize = 5_000;
pub(super) const COUNT_CAP: i64 = 10_000;
pub(super) const DISTINCT_CAP: i64 = 10_000;
pub(super) const PAGE_SIZE_MAX: i64 = 200;
pub(super) const TOP_LIMIT_MAX: i64 = 50;
/// 时延分位统计使用固定宽度直方图近似：50ms 一桶，超过 60s 归入末桶。
pub(super) const LATENCY_BUCKET_MS: i64 = 50;
pub(super) const LATENCY_BUCKET_CAP_MS: i64 = 60_000;
pub(super) const LATENCY_COL: &str = "LatencyMs";
pub(super) const TTFT_COL: &str = "TtftMs";
pub(super) const ERRORS_LIMIT_MAX: i64 = 100;
pub(super) const DEFAULT_DIM_EXPR: &str = "COALESCE(NULLIF(Model, ''), 'Unknown')";
pub(super) const UNKNOWN: &str = "Unknown";

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

pub(super) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 始终有界：缺省 `to = now`、`from = now - 7d`。
pub(super) fn resolve_range(p: &AnalysisParams) -> (i64, i64) {
    let now = now_ms();
    let to = p.to.unwrap_or(now);
    let from = p.from.unwrap_or(to.saturating_sub(DEFAULT_WINDOW_MS));
    (from, to)
}

/// 把分析参数映射回 records 的过滤参数，复用完全一致的筛选语义。
///
/// `from`/`to` 传入已解析的区间，保证查询始终有时间上界（缺省 7 天窗口）。
pub(super) fn to_list_params(p: &AnalysisParams, from: i64, to: i64) -> records_api::ListParams {
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
pub(super) fn effective_interval(
    requested: Option<&str>,
    from: i64,
    to: i64,
) -> (String, Option<String>) {
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
pub(super) fn dimension_expr(dim: &str) -> Option<&'static str> {
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

pub(super) fn bucket_expr(interval: &str) -> &'static str {
    match interval {
        "hour" => "strftime('%Y-%m-%d %H', TimeMs/1000, 'unixepoch', 'localtime')",
        "month" => "strftime('%Y-%m', TimeMs/1000, 'unixepoch', 'localtime')",
        _ => "strftime('%Y-%m-%d', TimeMs/1000, 'unixepoch', 'localtime')",
    }
}

pub(super) fn order_metric(order_by: &str) -> &'static str {
    match order_by {
        "errors" => "errors",
        "tokens" => "totalTokens",
        _ => "requests",
    }
}

pub(super) fn metric_value(m: &Metrics, order: &str) -> i64 {
    match order {
        "errors" => m.errors,
        "totalTokens" => m.total_tokens,
        _ => m.requests,
    }
}

pub(super) fn clamp_page(v: Option<i64>) -> i64 {
    v.unwrap_or(1).max(1)
}

pub(super) fn clamp_page_size(v: Option<i64>) -> i64 {
    v.unwrap_or(20).clamp(1, PAGE_SIZE_MAX)
}

pub(super) fn clamp_top(v: Option<i64>) -> usize {
    v.unwrap_or(8).clamp(1, TOP_LIMIT_MAX) as usize
}

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
}
