use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use futures::future::join_all;
use reqwest::Client;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

use crate::config::types::{ClientConfig, ModelMatch};
use crate::services::models_cache::{CacheKey, CacheOutcome, ModelsCache};
use crate::state::app_state::AppState;

/// Per-upstream timeout for the `/v1/models` fetch (both TTL==0 and
/// cache-leader refresh paths).
const MODELS_FETCH_TIMEOUT: Duration = Duration::from_secs(15);

/// Short TTL used when re-serving a stale cached value after every upstream
/// failed, so follow-up requests serve the stale value quickly and retry
/// upstream at most every 30 seconds.
const STALE_RETRY_TTL: Duration = Duration::from_secs(30);

fn model_matches_filter(model_id: &str, model_match: &ModelMatch) -> bool {
    match model_match.match_type.as_str() {
        "keyword" => model_match
            .value
            .iter()
            .any(|keyword| model_id.contains(keyword)),
        "exact" => model_match.value.contains(&model_id.to_string()),
        _ => false,
    }
}

async fn fetch_models_from_client(
    client_config: &ClientConfig,
    http_client: &Client,
    api_key: &Option<String>,
) -> Result<Value, String> {
    let url = format!("{}/models", client_config.base_url.trim_end_matches('/'));

    let mut request_builder = http_client.get(&url);

    if let Some(key) = api_key {
        request_builder = request_builder.header("Authorization", format!("Bearer {}", key));
    }

    let response = request_builder
        .send()
        .await
        .map_err(|e| format!("request to {url} failed: {e}"))?;

    if !response.status().is_success() {
        return Err(format!(
            "upstream returned status {} from {url}",
            response.status()
        ));
    }

    response
        .json::<Value>()
        .await
        .map_err(|e| format!("invalid JSON from {url}: {e}"))
}

/// Aggregate, filter, and deduplicate models across every configured client.
/// Mirrors the original implementation verbatim so the external JSON contract
/// (`{"object":"list","data":[...]}`) is unchanged.
/// The returned bool is `any_success`: true iff at least one per-client fetch
/// returned `Ok`, i.e. the aggregated value is safe to cache.
async fn aggregate_models(
    openai_clients: &[ClientConfig],
    http_client: &Client,
    api_key: Option<String>,
) -> (Value, bool) {
    let mut tasks = Vec::new();
    for client_config in openai_clients {
        let client_config_clone = client_config.clone();
        let http_client_clone = http_client.clone();
        let api_key_clone = api_key.clone();

        let task = tokio::spawn(async move {
            fetch_models_from_client(&client_config_clone, &http_client_clone, &api_key_clone).await
        });
        tasks.push(task);
    }
    let results = join_all(tasks).await;

    let mut all_models = Vec::new();
    let mut any_success = false;
    for (result, client_config) in results.iter().zip(openai_clients.iter()) {
        match result {
            Ok(Ok(models)) => {
                any_success = true;
                if let Some(data) = models.get("data").and_then(|d| d.as_array()) {
                    for model in data {
                        if let Some(id) = model.get("id").and_then(|id| id.as_str()) {
                            if model_matches_filter(id, &client_config.model_match) {
                                all_models.push(model.clone());
                            }
                        }
                    }
                } else if let Some(id) = models.get("id").and_then(|id| id.as_str()) {
                    if model_matches_filter(id, &client_config.model_match) {
                        all_models.push(models.clone());
                    }
                }
            }
            Ok(Err(err)) => {
                tracing::warn!(
                    "fetch /v1/models from client '{}' failed: {err}",
                    client_config.name
                );
                continue;
            }
            Err(join_err) => {
                tracing::warn!(
                    "model list fetch task for client '{}' panicked: {join_err}",
                    client_config.name
                );
                continue;
            }
        }
    }

    let mut unique_models = Vec::new();
    let mut seen_ids = std::collections::HashSet::new();
    for model in all_models {
        if let Some(id) = model.get("id").and_then(|id| id.as_str()) {
            if !seen_ids.contains(id) {
                seen_ids.insert(id.to_string());
                unique_models.push(model);
            }
        } else {
            unique_models.push(model);
        }
    }

    (
        json!({
            "object": "list",
            "data": unique_models
        }),
        any_success,
    )
}

fn extract_credential_bytes(headers: &HeaderMap) -> Option<Vec<u8>> {
    if let Some(value) = headers.get("authorization") {
        if let Ok(s) = value.to_str() {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                let body = trimmed
                    .strip_prefix("Bearer ")
                    .or_else(|| trimmed.strip_prefix("bearer "))
                    .unwrap_or(trimmed);
                let body = body.trim();
                if !body.is_empty() {
                    return Some(body.as_bytes().to_vec());
                }
            }
        }
    }
    if let Some(value) = headers.get("x-api-key") {
        if let Ok(s) = value.to_str() {
            let trimmed = s.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.as_bytes().to_vec());
            }
        }
    }
    None
}

fn make_key(cache: &ModelsCache, generation: u64, credential: Option<&[u8]>) -> CacheKey {
    CacheKey {
        generation,
        credential: cache.fingerprint(credential),
    }
}

pub async fn list_models(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let (config, generation) = app_state.config_manager.get_config_with_generation().await;
    let ttl_seconds = config.models_cache.ttl_seconds;
    let credential_bytes = extract_credential_bytes(&headers);

    let build_http_client = || {
        Client::builder()
            .timeout(MODELS_FETCH_TIMEOUT)
            .build()
            .expect("Failed to build HTTP client")
    };
    let upstream_error_body = || {
        Json(crate::app_error::build_error_body(
            "failed to fetch model list from all upstreams",
            "upstream_error",
        ))
    };

    if ttl_seconds == 0 {
        let http_client = build_http_client();
        let key_str = credential_bytes.and_then(|b| String::from_utf8(b).ok());
        let (aggregated, any_success) =
            aggregate_models(&config.openai_clients, &http_client, key_str).await;
        return if any_success {
            (StatusCode::OK, Json(aggregated))
        } else {
            (StatusCode::BAD_GATEWAY, upstream_error_body())
        };
    }

    let key = make_key(
        &app_state.models_cache,
        generation,
        credential_bytes.as_deref(),
    );
    match app_state.models_cache.lookup(key) {
        CacheOutcome::Fresh(value) => return (StatusCode::OK, Json(value)),
        CacheOutcome::Empty => {}
    }

    let (is_leader, inflight) = app_state.models_cache.begin_refresh(key);
    if !is_leader {
        app_state.models_cache.wait_for_leader(key, &inflight).await;
        match app_state.models_cache.lookup(key) {
            CacheOutcome::Fresh(value) => return (StatusCode::OK, Json(value)),
            CacheOutcome::Empty => {
                if let Some(previous) = app_state.models_cache.previous_value(key) {
                    return (StatusCode::OK, Json(previous));
                }
            }
        }
        return (StatusCode::BAD_GATEWAY, upstream_error_body());
    }

    let http_client = build_http_client();
    let key_str = credential_bytes.and_then(|b| String::from_utf8(b).ok());
    let (aggregated, any_success) =
        aggregate_models(&config.openai_clients, &http_client, key_str).await;

    if any_success {
        app_state
            .models_cache
            .insert(key, aggregated.clone(), Duration::from_secs(ttl_seconds));
        app_state.models_cache.end_refresh(key, &inflight);
        return (StatusCode::OK, Json(aggregated));
    }

    // All upstreams failed: never cache the empty aggregation.
    tracing::warn!(
        "all upstream /v1/models fetches failed for generation {generation}; serving stale cache or 502"
    );
    if let Some(previous) = app_state.models_cache.previous_value(key) {
        // Stale fallback: re-insert with a short retry TTL so follow-up
        // requests serve the stale value quickly and retry upstream at most
        // every 30s.
        app_state
            .models_cache
            .insert(key, previous.clone(), STALE_RETRY_TTL);
        app_state.models_cache.end_refresh(key, &inflight);
        return (StatusCode::OK, Json(previous));
    }
    app_state.models_cache.end_refresh(key, &inflight);
    (StatusCode::BAD_GATEWAY, upstream_error_body())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unreachable_client_config() -> ClientConfig {
        ClientConfig {
            name: "unreachable".to_string(),
            base_url: "http://127.0.0.1:1/v1".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn fetch_models_from_client_returns_err_when_upstream_unreachable() {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("Failed to build HTTP client");
        let client_config = unreachable_client_config();
        let result = fetch_models_from_client(&client_config, &http_client, &None).await;
        assert!(
            result.is_err(),
            "unreachable upstream must produce Err, got {:?}",
            result
        );
    }

    #[tokio::test]
    async fn aggregate_models_all_failing_returns_empty_data_and_no_success() {
        let http_client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("Failed to build HTTP client");
        let openai_clients = vec![unreachable_client_config()];
        let (aggregated, any_success) = aggregate_models(&openai_clients, &http_client, None).await;
        assert!(
            !any_success,
            "all failing upstreams must yield any_success == false"
        );
        assert_eq!(
            aggregated["data"],
            Value::Array(Vec::new()),
            "data must be an empty array when every upstream fails"
        );
    }
}
