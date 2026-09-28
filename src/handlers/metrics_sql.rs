use chrono::{Datelike, Local, TimeZone};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use std::collections::HashMap;

use super::metrics_cache::MetricValue;
use super::{GroupBy, MetricName, MetricPeriod, MetricQuery};
use crate::db::records_query::Bind;
use crate::handlers::records_api;

pub(super) fn resolve_query_range(query: &MetricQuery, now: chrono::DateTime<Local>) -> (i64, i64) {
    let now_ms = now.timestamp_millis();
    if let Some(period) = query.period {
        let start = now.date_naive();
        let date = match period {
            MetricPeriod::Week => {
                start - chrono::Days::new(u64::from(start.weekday().num_days_from_monday()))
            }
            MetricPeriod::Month => start.with_day(1).unwrap_or(start),
            MetricPeriod::Year => start
                .with_month(1)
                .and_then(|date| date.with_day(1))
                .unwrap_or(start),
        };
        let from = Local
            .from_local_datetime(&date.and_hms_opt(0, 0, 0).unwrap_or_default())
            .earliest()
            .map(|date| date.timestamp_millis())
            .unwrap_or(now_ms);
        return (from, now_ms);
    }
    let to = query.to.unwrap_or(now_ms);
    (query.from.unwrap_or(to.saturating_sub(7 * 86_400_000)), to)
}

pub(super) fn build_filters(
    query: &MetricQuery,
    from: i64,
    to: i64,
    full_shard: bool,
) -> (String, Vec<Bind>) {
    let params = records_api::ListParams {
        from: (!full_shard).then_some(from),
        to: (!full_shard).then_some(to),
        type_: query.filters.type_.clone(),
        model: query.filters.model.clone(),
        status: query.filters.status,
        backend: query.filters.backend.clone(),
        ip: query.filters.ip.clone(),
        client: query.filters.client.clone(),
        apikey: query.filters.apikey.clone(),
        ..Default::default()
    };
    let (mut where_sql, binds) = records_api::build_filters(&params);
    if full_shard {
        where_sql.push_str(" AND TimeMs IS NOT NULL");
    }
    (where_sql, binds)
}

pub(super) fn build_sql(group: &str, metrics: &[MetricName], where_sql: &str) -> String {
    let mut columns = vec![format!("{group} AS metric_group")];
    for metric in metrics.iter().copied() {
        if !matches!(
            metric,
            MetricName::AvgLatency
                | MetricName::AvgTtft
                | MetricName::SuccessRate
                | MetricName::ErrorRate
        ) {
            columns.push(metric.sql().to_string());
        }
    }
    format!(
        "SELECT {} FROM records{} GROUP BY 1",
        columns.join(", "),
        where_sql
    )
}

pub(super) fn group_expression(group_by: &GroupBy) -> String {
    match group_by {
        GroupBy::Single(name) => dimension_expr(name).to_string(),
        GroupBy::Multiple(names) if names.len() == 1 => {
            format!("json_array({})", dimension_expr(&names[0]))
        }
        GroupBy::Multiple(names) => format!(
            "json_array({}, {})",
            dimension_expr(&names[0]),
            dimension_expr(&names[1])
        ),
    }
}

fn dimension_expr(group_by: &str) -> &'static str {
    match group_by {
        "apikey" => "COALESCE(NULLIF(ApiKey, ''), 'Unknown')",
        "ip" => "COALESCE(NULLIF(IP, ''), 'Unknown')",
        "type" => "COALESCE(NULLIF(Type, ''), 'Unknown')",
        "backend" => "COALESCE(NULLIF(Backend, ''), 'Unknown')",
        "client" => "COALESCE(NULLIF(ClientName, ''), 'Unknown')",
        "status" => "COALESCE(CAST(Status AS TEXT), 'Unknown')",
        "hour" => "COALESCE(strftime('%H:00', TimeMs/1000, 'unixepoch', 'localtime'), 'Unknown')",
        _ => "COALESCE(NULLIF(Model, ''), 'Unknown')",
    }
}

pub(super) fn metric_value(values: &HashMap<MetricName, MetricValue>, metric: MetricName) -> f64 {
    match metric {
        MetricName::AvgLatency => {
            average(values, MetricName::LatencySum, MetricName::LatencyCount).unwrap_or(0.0)
        }
        MetricName::AvgTtft => {
            average(values, MetricName::TtftSum, MetricName::TtftCount).unwrap_or(0.0)
        }
        MetricName::SuccessRate => {
            average(values, MetricName::Success, MetricName::Requests).unwrap_or(0.0)
        }
        MetricName::ErrorRate => {
            average(values, MetricName::Errors, MetricName::Requests).unwrap_or(0.0)
        }
        _ => values
            .get(&metric)
            .copied()
            .map(MetricValue::number)
            .unwrap_or(0.0),
    }
}

pub(super) fn row_metric(row: &SqliteRow, metric: MetricName) -> Result<MetricValue, sqlx::Error> {
    match metric {
        MetricName::LatencySum | MetricName::MaxLatency | MetricName::TtftSum => {
            real_or_int(row, metric.alias()).map(MetricValue::Real)
        }
        MetricName::AvgLatency
        | MetricName::AvgTtft
        | MetricName::SuccessRate
        | MetricName::ErrorRate => Ok(MetricValue::Real(0.0)),
        _ => row
            .try_get::<i64, _>(metric.alias())
            .map(MetricValue::Integer),
    }
}

/// SQLite 同一列在不同分片可能以 `REAL` 或 `INTEGER` 存储（如 `SUM(LatencyMs)`），
/// 先按浮点读取，失败再按整数读取，避免整值列导致 `avgLatency` 解码失败。
fn real_or_int(row: &SqliteRow, alias: &str) -> Result<f64, sqlx::Error> {
    row.try_get::<f64, _>(alias)
        .or_else(|_| row.try_get::<i64, _>(alias).map(|value| value as f64))
}

pub(super) fn average(
    values: &HashMap<MetricName, MetricValue>,
    sum: MetricName,
    count: MetricName,
) -> Option<f64> {
    let count = values
        .get(&count)
        .copied()
        .map(MetricValue::number)
        .unwrap_or(0.0);
    (count > 0.0).then(|| {
        values
            .get(&sum)
            .copied()
            .map(MetricValue::number)
            .unwrap_or(0.0)
            / count
    })
}

impl MetricName {
    pub(super) fn alias(self) -> &'static str {
        match self {
            Self::Requests => "requests",
            Self::Success => "success",
            Self::Errors => "errors",
            Self::PromptTokens => "promptTokens",
            Self::CompletionTokens => "completionTokens",
            Self::TotalTokens => "totalTokens",
            Self::LatencySum => "latencySum",
            Self::LatencyCount => "latencyCount",
            Self::MaxLatency => "maxLatency",
            Self::TtftSum => "ttftSum",
            Self::TtftCount => "ttftCount",
            Self::AvgLatency => "avgLatency",
            Self::AvgTtft => "avgTtft",
            Self::SuccessRate => "successRate",
            Self::ErrorRate => "errorRate",
        }
    }

    fn sql(self) -> &'static str {
        match self {
            Self::Requests => "COUNT(CASE WHEN Status >= 200 THEN 1 END) AS requests",
            Self::Success => "COALESCE(SUM(CASE WHEN Status >= 200 AND Status < 400 THEN 1 ELSE 0 END), 0) AS success",
            Self::Errors => "COALESCE(SUM(CASE WHEN Status >= 400 THEN 1 ELSE 0 END), 0) AS errors",
            Self::PromptTokens => "COALESCE(SUM(COALESCE(PromptTokens, 0)), 0) AS promptTokens",
            Self::CompletionTokens => "COALESCE(SUM(COALESCE(CompletionTokens, 0)), 0) AS completionTokens",
            Self::TotalTokens => "COALESCE(SUM(COALESCE(TotalTokens, 0)), 0) AS totalTokens",
            Self::LatencySum => "COALESCE(SUM(LatencyMs), 0) AS latencySum",
            Self::LatencyCount => "COUNT(LatencyMs) AS latencyCount",
            Self::MaxLatency => "COALESCE(MAX(LatencyMs), 0) AS maxLatency",
            Self::TtftSum => "COALESCE(SUM(TtftMs), 0) AS ttftSum",
            Self::TtftCount => "COUNT(TtftMs) AS ttftCount",
            Self::AvgLatency | Self::AvgTtft | Self::SuccessRate | Self::ErrorRate => "0 AS unusedMetric",
        }
    }
}
