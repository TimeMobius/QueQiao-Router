use chrono::{Datelike, Local, TimeZone};
use serde_json::{json, Value};
use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use std::collections::HashMap;

use crate::db::records_query::Bind;
use crate::handlers::{
    analysis_api::shards::{collect_shards, fetch_one_shard},
    records_api,
};
use crate::state::app_state::AppState;

use super::{MetricName, MetricPeriod, MetricQuery};

pub(super) async fn execute_query(
    app_state: &std::sync::Arc<AppState>,
    query: &MetricQuery,
) -> Result<Value, sqlx::Error> {
    let (from, to) = resolve_query_range(query);
    let shards = collect_shards(app_state, from, to).await;
    let expression = group_expression(&query.group_by);
    let (filtered, binds) = build_filters(query, from, to, false);
    let sql = build_sql(expression, &query.metrics, &filtered);
    let mut tasks = Vec::with_capacity(shards.len());
    for shard in &shards {
        let pool = shard.pool.clone();
        let shard_sql = if shard_is_complete(shard, from, to) {
            let (where_sql, full_binds) = build_filters(query, from, to, true);
            (
                build_sql(expression, &query.metrics, &where_sql),
                full_binds,
            )
        } else {
            (sql.clone(), binds.clone())
        };
        tasks.push(tokio::spawn(async move {
            fetch_one_shard(&pool, &shard_sql.0, &shard_sql.1).await
        }));
    }

    let mut merged: HashMap<String, HashMap<MetricName, f64>> = HashMap::new();
    for task in tasks {
        for row in task
            .await
            .map_err(|error| sqlx::Error::Protocol(error.to_string()))??
        {
            let name = row.try_get::<String, _>("metric_group").unwrap_or_default();
            let entry = merged.entry(name).or_default();
            for metric in query.metrics.iter().copied() {
                let value = row_metric(&row, metric.alias());
                if metric == MetricName::MaxLatency {
                    let current = entry.entry(metric).or_insert(0.0);
                    *current = current.max(value);
                } else {
                    *entry.entry(metric).or_insert(0.0) += value;
                }
            }
        }
    }

    let mut items: Vec<(String, HashMap<MetricName, f64>)> = merged.into_iter().collect();
    items.sort_by(|a, b| {
        metric_value(&b.1, query.order_by)
            .total_cmp(&metric_value(&a.1, query.order_by))
            .then_with(|| a.0.cmp(&b.0))
    });
    let items = items
        .into_iter()
        .take(query.limit)
        .map(|(name, values)| {
            let mut object = serde_json::Map::new();
            object.insert(query.group_by.clone(), json!(name));
            for metric in query.metrics.iter().copied() {
                let value = match metric {
                    MetricName::AvgLatency => {
                        average(&values, MetricName::LatencySum, MetricName::LatencyCount)
                    }
                    MetricName::AvgTtft => {
                        average(&values, MetricName::TtftSum, MetricName::TtftCount)
                    }
                    _ => Some(values.get(&metric).copied().unwrap_or(0.0)),
                };
                object.insert(metric.alias().to_string(), json!(value));
            }
            Value::Object(object)
        })
        .collect::<Vec<_>>();

    Ok(json!({
        "from": from,
        "to": to,
        "groupBy": query.group_by,
        "items": items,
        "shards": shards.iter().map(|s| s.id.clone()).collect::<Vec<_>>()
    }))
}

fn resolve_query_range(query: &MetricQuery) -> (i64, i64) {
    let now = Local::now().timestamp_millis();
    if let Some(period) = query.period {
        let start = Local::now().date_naive();
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
            .unwrap_or(now);
        return (from, now);
    }
    let to = query.to.unwrap_or(now);
    (query.from.unwrap_or(to.saturating_sub(7 * 86_400_000)), to)
}

fn build_filters(query: &MetricQuery, from: i64, to: i64, full_shard: bool) -> (String, Vec<Bind>) {
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
    records_api::build_filters(&params)
}

fn build_sql(group: &str, metrics: &[MetricName], where_sql: &str) -> String {
    let mut columns = vec![format!("{group} AS metric_group")];
    for metric in metrics.iter().copied() {
        if !matches!(metric, MetricName::AvgLatency | MetricName::AvgTtft) {
            columns.push(metric.sql().to_string());
        }
    }
    format!(
        "SELECT {} FROM records{} GROUP BY 1",
        columns.join(", "),
        where_sql
    )
}

fn group_expression(group_by: &str) -> &'static str {
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

fn shard_is_complete(
    shard: &crate::handlers::analysis_api::shards::ShardInput,
    from: i64,
    to: i64,
) -> bool {
    shard.is_active && shard.active_month_covered(from, to)
        || !shard.is_active
            && shard.min_ms.is_some_and(|min| min >= from)
            && shard.max_ms.is_some_and(|max| max <= to)
}

fn metric_value(values: &HashMap<MetricName, f64>, metric: MetricName) -> f64 {
    match metric {
        MetricName::AvgLatency => {
            average(values, MetricName::LatencySum, MetricName::LatencyCount).unwrap_or(0.0)
        }
        MetricName::AvgTtft => {
            average(values, MetricName::TtftSum, MetricName::TtftCount).unwrap_or(0.0)
        }
        _ => values.get(&metric).copied().unwrap_or(0.0),
    }
}

fn row_metric(row: &SqliteRow, alias: &str) -> f64 {
    row.try_get::<f64, _>(alias)
        .or_else(|_| row.try_get::<i64, _>(alias).map(|value| value as f64))
        .unwrap_or(0.0)
}

fn average(values: &HashMap<MetricName, f64>, sum: MetricName, count: MetricName) -> Option<f64> {
    let count = values.get(&count).copied().unwrap_or(0.0);
    (count > 0.0).then(|| values.get(&sum).copied().unwrap_or(0.0) / count)
}

impl MetricName {
    fn alias(self) -> &'static str {
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
            Self::AvgLatency | Self::AvgTtft => "0 AS unusedMetric",
        }
    }
}
