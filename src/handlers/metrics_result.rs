use serde_json::{json, Value};
use std::collections::HashMap;

use super::metrics_cache::{GroupRows, MetricValue};
use super::metrics_engine::PlannedQuery;
use super::metrics_sql::{average, metric_value};
use super::{GroupBy, MetricName, MetricQuery};

pub(super) fn merge_groups(merged: &mut GroupRows, groups: &GroupRows, metrics: &[MetricName]) {
    for (name, values) in groups {
        let entry = merged.entry(name.clone()).or_default();
        for metric in metrics {
            if let Some(value) = values.get(metric) {
                entry
                    .entry(*metric)
                    .and_modify(|current| {
                        current.combine(*value, *metric == MetricName::MaxLatency)
                    })
                    .or_insert(*value);
            }
        }
    }
}

pub(super) fn render_result(query: &MetricQuery, plan: PlannedQuery, merged: GroupRows) -> Value {
    let mut items: Vec<(String, HashMap<MetricName, MetricValue>)> = merged.into_iter().collect();
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
            match &query.group_by {
                GroupBy::Single(group) => {
                    object.insert(group.clone(), json!(name));
                }
                GroupBy::Multiple(groups) => {
                    let names: Vec<String> = serde_json::from_str(&name).unwrap_or_default();
                    for (group, value) in groups.iter().zip(names) {
                        object.insert(group.clone(), json!(value));
                    }
                }
            }
            for metric in query.metrics.iter().copied() {
                let value = match metric {
                    MetricName::AvgLatency => {
                        average(&values, MetricName::LatencySum, MetricName::LatencyCount)
                    }
                    MetricName::AvgTtft => {
                        average(&values, MetricName::TtftSum, MetricName::TtftCount)
                    }
                    MetricName::SuccessRate => {
                        average(&values, MetricName::Success, MetricName::Requests)
                    }
                    MetricName::ErrorRate => {
                        average(&values, MetricName::Errors, MetricName::Requests)
                    }
                    _ => None,
                };
                let json_value = match metric {
                    MetricName::AvgLatency
                    | MetricName::AvgTtft
                    | MetricName::SuccessRate
                    | MetricName::ErrorRate => json!(value),
                    _ => match values.get(&metric) {
                        Some(MetricValue::Integer(value)) => json!(value),
                        Some(MetricValue::Real(value)) => json!(value),
                        None => json!(0),
                    },
                };
                object.insert(metric.alias().to_string(), json_value);
            }
            Value::Object(object)
        })
        .collect::<Vec<_>>();
    json!({"from": plan.from, "to": plan.to, "groupBy": query.group_by, "items": items, "shards": plan.shards})
}
