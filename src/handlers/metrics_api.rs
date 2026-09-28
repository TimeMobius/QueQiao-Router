use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::Arc;

use crate::state::app_state::AppState;

#[path = "metrics_cache.rs"]
mod metrics_cache;
#[path = "metrics_engine.rs"]
mod metrics_engine;
#[path = "metrics_result.rs"]
mod metrics_result;
#[path = "metrics_sql.rs"]
mod metrics_sql;
use metrics_engine::execute_queries;

const MAX_QUERIES: usize = 8;
const MAX_METRICS: usize = 16;
const MAX_ITEMS: usize = 10_000;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MetricsRequest {
    pub queries: Vec<MetricQuery>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MetricQuery {
    pub id: String,
    pub period: Option<MetricPeriod>,
    pub from: Option<i64>,
    pub to: Option<i64>,
    #[serde(rename = "groupBy", default = "default_group_by")]
    pub group_by: GroupBy,
    pub metrics: Vec<MetricName>,
    #[serde(default)]
    pub filters: MetricFilters,
    #[serde(rename = "orderBy", default = "default_order_by")]
    pub order_by: MetricName,
    #[serde(default = "default_limit")]
    pub limit: usize,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
pub enum MetricPeriod {
    Week,
    Month,
    Year,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum GroupBy {
    Single(String),
    Multiple(Vec<String>),
}

impl GroupBy {
    fn names(&self) -> Vec<&str> {
        match self {
            Self::Single(name) => vec![name],
            Self::Multiple(names) => names.iter().map(String::as_str).collect(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct MetricFilters {
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub model: Option<String>,
    pub status: Option<i64>,
    pub backend: Option<String>,
    pub ip: Option<String>,
    pub client: Option<String>,
    pub apikey: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "camelCase")]
pub enum MetricName {
    Requests,
    Success,
    Errors,
    PromptTokens,
    CompletionTokens,
    TotalTokens,
    LatencySum,
    LatencyCount,
    MaxLatency,
    TtftSum,
    TtftCount,
    AvgLatency,
    AvgTtft,
    SuccessRate,
    ErrorRate,
}

fn default_group_by() -> GroupBy {
    GroupBy::Single("model".to_string())
}

fn default_order_by() -> MetricName {
    MetricName::Requests
}

fn default_limit() -> usize {
    100
}

pub async fn metrics(
    State(app_state): State<Arc<AppState>>,
    Json(request): Json<MetricsRequest>,
) -> Result<Json<Value>, (StatusCode, String)> {
    validate_request(&request)?;
    let results = execute_queries(&app_state, &request.queries)
        .await
        .map_err(internal)?;
    Ok(Json(json!({"queries": results})))
}

fn validate_request(request: &MetricsRequest) -> Result<(), (StatusCode, String)> {
    if request.queries.is_empty() || request.queries.len() > MAX_QUERIES {
        return Err(bad_request("queries must contain 1 to 8 items"));
    }
    let mut ids = HashSet::with_capacity(request.queries.len());
    for query in &request.queries {
        if query.id.is_empty() || query.id.len() > 64 {
            return Err(bad_request("query id must contain 1 to 64 characters"));
        }
        if !ids.insert(&query.id) {
            return Err(bad_request("query ids must be unique"));
        }
        if query.metrics.is_empty() || query.metrics.len() > MAX_METRICS {
            return Err(bad_request("metrics must contain 1 to 16 items"));
        }
        let unique_metrics: HashSet<MetricName> = query.metrics.iter().copied().collect();
        if unique_metrics.len() != query.metrics.len() {
            return Err(bad_request("metrics must not contain duplicates"));
        }
        let names = query.group_by.names();
        if names.is_empty()
            || names.len() > 2
            || names.len() == 2 && names[0] == names[1]
            || names.iter().any(|name| {
                !matches!(
                    *name,
                    "model" | "apikey" | "ip" | "type" | "backend" | "client" | "status" | "hour"
                )
            })
        {
            return Err(bad_request("unsupported groupBy"));
        }
        if (query.from.is_some() || query.to.is_some()) && query.period.is_some() {
            return Err(bad_request("from/to and period cannot be combined"));
        }
        if query
            .from
            .is_some_and(|from| query.to.is_some_and(|to| from > to))
        {
            return Err(bad_request("from must not be greater than to"));
        }
        if query.limit == 0 || query.limit > MAX_ITEMS {
            return Err(bad_request("limit must be between 1 and 10000"));
        }
    }
    Ok(())
}

fn bad_request(message: &str) -> (StatusCode, String) {
    (StatusCode::BAD_REQUEST, message.to_string())
}

fn internal(error: impl std::fmt::Display) -> (StatusCode, String) {
    tracing::error!("metrics api error: {error}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "query failed".to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::{validate_request, GroupBy, MetricName, MetricPeriod, MetricQuery, MetricsRequest};

    fn query(metrics: Vec<MetricName>) -> MetricQuery {
        MetricQuery {
            id: "q".to_string(),
            period: Some(MetricPeriod::Month),
            from: None,
            to: None,
            group_by: GroupBy::Single("model".to_string()),
            metrics,
            filters: Default::default(),
            order_by: MetricName::Requests,
            limit: 100,
        }
    }

    #[test]
    fn accepts_combined_metrics_for_one_group_scan() {
        let request = MetricsRequest {
            queries: vec![query(vec![
                MetricName::Requests,
                MetricName::Success,
                MetricName::Errors,
            ])],
        };
        assert!(validate_request(&request).is_ok());
    }

    #[test]
    fn accepts_two_whitelisted_dimensions_and_rejects_duplicates() {
        let mut grouped = query(vec![MetricName::Requests]);
        grouped.group_by = GroupBy::Multiple(vec!["model".to_string(), "backend".to_string()]);
        assert!(validate_request(&MetricsRequest {
            queries: vec![grouped.clone()]
        })
        .is_ok());
        grouped.group_by = GroupBy::Multiple(vec!["model".to_string(), "model".to_string()]);
        assert!(validate_request(&MetricsRequest {
            queries: vec![grouped]
        })
        .is_err());
    }

    #[test]
    fn rejects_duplicate_query_ids() {
        let mut second = query(vec![MetricName::Requests]);
        second.id = "q".to_string();
        let request = MetricsRequest {
            queries: vec![query(vec![MetricName::Requests]), second],
        };
        assert!(validate_request(&request).is_err());
    }

    #[test]
    fn average_can_be_requested_without_base_metrics() {
        let mut query = query(vec![MetricName::AvgLatency]);
        query.order_by = MetricName::AvgLatency;
        let request = MetricsRequest {
            queries: vec![query],
        };
        assert!(validate_request(&request).is_ok());
    }
}
