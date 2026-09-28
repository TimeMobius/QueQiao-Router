use axum::{extract::State, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::state::app_state::AppState;

#[path = "metrics_engine.rs"]
mod metrics_engine;
use metrics_engine::execute_query;

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
    pub group_by: String,
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
}

fn default_group_by() -> String {
    "model".to_string()
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
    let mut results = serde_json::Map::new();
    let mut memo: HashMap<String, Value> = HashMap::new();
    for query in &request.queries {
        let mut memo_query = query.clone();
        memo_query.id.clear();
        let key = serde_json::to_string(&memo_query).map_err(internal)?;
        let value = if let Some(cached) = memo.get(&key) {
            cached.clone()
        } else {
            let value = execute_query(&app_state, query).await.map_err(internal)?;
            memo.insert(key, value.clone());
            value
        };
        results.insert(query.id.clone(), value);
    }
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
        if query.metrics.contains(&MetricName::AvgLatency)
            && !query.metrics.contains(&MetricName::LatencySum)
        {
            return Err(bad_request("avgLatency requires latencySum"));
        }
        if query.metrics.contains(&MetricName::AvgTtft)
            && !query.metrics.contains(&MetricName::TtftSum)
        {
            return Err(bad_request("avgTtft requires ttftSum"));
        }
        if !matches!(
            query.group_by.as_str(),
            "model" | "apikey" | "ip" | "type" | "backend" | "client" | "status" | "hour"
        ) {
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
        if !query.metrics.contains(&query.order_by) {
            return Err(bad_request("orderBy must be included in metrics"));
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
    use super::{validate_request, MetricName, MetricPeriod, MetricQuery, MetricsRequest};

    fn query(metrics: Vec<MetricName>) -> MetricQuery {
        MetricQuery {
            id: "q".to_string(),
            period: Some(MetricPeriod::Month),
            from: None,
            to: None,
            group_by: "model".to_string(),
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
    fn rejects_duplicate_query_ids() {
        let mut second = query(vec![MetricName::Requests]);
        second.id = "q".to_string();
        let request = MetricsRequest {
            queries: vec![query(vec![MetricName::Requests]), second],
        };
        assert!(validate_request(&request).is_err());
    }

    #[test]
    fn requires_base_metrics_for_average() {
        let request = MetricsRequest {
            queries: vec![query(vec![MetricName::AvgLatency])],
        };
        assert!(validate_request(&request).is_err());
    }
}
