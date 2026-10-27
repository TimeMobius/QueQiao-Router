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
const MODELS_FETCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Upper bound on how long a follower waits for the leader's refresh. Slightly
/// above `MODELS_FETCH_TIMEOUT` so a healthy leader always wins the race; the
/// gap only matters if the refresh task itself is stuck.
const LEADER_WAIT_TIMEOUT: Duration = Duration::from_secs(35);

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

/// Client credential wins; backend `api_key` is the fallback. This is the
/// reverse of [`crate::client::proxy::get_api_key`], intentionally.
fn resolve_upstream_api_keys(
    openai_clients: &[ClientConfig],
    client_credential: Option<&str>,
) -> Vec<Option<String>> {
    openai_clients
        .iter()
        .map(|client_config| {
            client_credential
                .map(str::to_owned)
                .or_else(|| client_config.api_key.clone().filter(|key| !key.is_empty()))
        })
        .collect()
}

/// Length-prefixed so `["ab","c"]` and `["a","bc"]` cannot collide.
fn encode_effective_keys(keys: &[Option<String>]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(keys.len() * 32);
    buf.extend_from_slice(&(keys.len() as u64).to_le_bytes());
    for key in keys {
        match key {
            Some(key) => {
                buf.extend_from_slice(&(key.len() as u64).to_le_bytes());
                buf.extend_from_slice(key.as_bytes());
            }
            None => buf.extend_from_slice(&u64::MAX.to_le_bytes()),
        }
    }
    buf
}

async fn fetch_models_from_client(
    client_config: &ClientConfig,
    http_client: &Client,
    api_key: Option<&str>,
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
    api_keys: &[Option<String>],
) -> (Value, bool) {
    let mut tasks = Vec::new();
    for (client_config, api_key) in openai_clients.iter().zip(api_keys.iter()) {
        let client_config_clone = client_config.clone();
        let http_client_clone = http_client.clone();
        let api_key_clone = api_key.clone();

        let task = tokio::spawn(async move {
            fetch_models_from_client(
                &client_config_clone,
                &http_client_clone,
                api_key_clone.as_deref(),
            )
            .await
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

fn make_key(cache: &ModelsCache, generation: u64, keys: &[Option<String>]) -> CacheKey {
    CacheKey {
        generation,
        credential: cache.fingerprint(Some(&encode_effective_keys(keys))),
    }
}

pub async fn list_models(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    let (config, generation) = app_state.config_manager.get_config_with_generation().await;
    let ttl_seconds = config.models_cache.ttl_seconds;
    let client_credential =
        extract_credential_bytes(&headers).and_then(|bytes| String::from_utf8(bytes).ok());
    let api_keys = resolve_upstream_api_keys(&config.openai_clients, client_credential.as_deref());

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
        let (aggregated, any_success) =
            aggregate_models(&config.openai_clients, &http_client, &api_keys).await;
        return if any_success {
            (StatusCode::OK, Json(aggregated))
        } else {
            (StatusCode::BAD_GATEWAY, upstream_error_body())
        };
    }

    let key = make_key(&app_state.models_cache, generation, &api_keys);
    match app_state.models_cache.lookup(key) {
        CacheOutcome::Fresh(value) => return (StatusCode::OK, Json(value)),
        CacheOutcome::Empty => {}
    }

    let (is_leader, inflight) = app_state.models_cache.begin_refresh(key);
    if !is_leader {
        let waited = tokio::time::timeout(
            LEADER_WAIT_TIMEOUT,
            app_state.models_cache.wait_for_leader(key, &inflight),
        )
        .await;
        if waited.is_err() {
            tracing::warn!(
                "models refresh for generation {generation} did not finish within {LEADER_WAIT_TIMEOUT:?}; returning fallback"
            );
        }
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

    // The refresh runs in its own task so that dropping this handler future
    // (client timeout / disconnect) neither cancels the upstream fetch nor
    // strands the singleflight slot: the task owns the RAII guard and always
    // releases the key, even on panic.
    let cache = app_state.models_cache.clone();
    let guard = cache.refresh_guard(key, inflight);
    let clients = config.openai_clients.clone();
    let http_client = build_http_client();
    let refresh_keys = api_keys.clone();
    let ttl = Duration::from_secs(ttl_seconds);

    let refresh: tokio::task::JoinHandle<Result<Value, ()>> = tokio::spawn(async move {
        let _guard = guard;
        let (aggregated, any_success) =
            aggregate_models(&clients, &http_client, &refresh_keys).await;

        if any_success {
            cache.insert(key, aggregated.clone(), ttl);
            return Ok(aggregated);
        }

        // All upstreams failed: never cache the empty aggregation.
        tracing::warn!(
            "all upstream /v1/models fetches failed for generation {generation}; serving stale cache or 502"
        );
        match cache.previous_value(key) {
            Some(previous) => {
                // Stale fallback: re-insert with a short retry TTL so follow-up
                // requests serve the stale value quickly and retry upstream at
                // most every 30s.
                cache.insert(key, previous.clone(), STALE_RETRY_TTL);
                Ok(previous)
            }
            None => Err(()),
        }
    });

    match refresh.await {
        Ok(Ok(value)) => (StatusCode::OK, Json(value)),
        Ok(Err(())) => (StatusCode::BAD_GATEWAY, upstream_error_body()),
        Err(join_err) => {
            tracing::warn!("models refresh task for generation {generation} failed: {join_err}");
            (StatusCode::BAD_GATEWAY, upstream_error_body())
        }
    }
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
        let result = fetch_models_from_client(&client_config, &http_client, None).await;
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
        let (aggregated, any_success) =
            aggregate_models(&openai_clients, &http_client, &[None]).await;
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

    #[tokio::test]
    async fn fetch_models_from_client_sends_bearer_for_resolved_key() {
        use axum::{extract::State, http::HeaderMap, routing::get, Json as AxumJson, Router};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen: Arc<tokio::sync::Mutex<Option<String>>> = Arc::new(tokio::sync::Mutex::new(None));
        let seen_for_handler = seen.clone();

        let app = Router::new()
            .route(
                "/models",
                get(
                    move |State(seen): State<Arc<tokio::sync::Mutex<Option<String>>>>,
                          headers: HeaderMap| async move {
                        *seen.lock().await = headers
                            .get("authorization")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned);
                        AxumJson(json!({"data": []}))
                    },
                ),
            )
            .with_state(seen_for_handler);
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let http_client = Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .expect("Failed to build HTTP client");
        let client_config = ClientConfig {
            name: "upstream".to_string(),
            base_url: format!("http://{addr}"),
            ..Default::default()
        };

        let result =
            fetch_models_from_client(&client_config, &http_client, Some("sk-backend")).await;
        assert!(result.is_ok(), "fetch must succeed: {result:?}");
        let captured = seen.lock().await.clone();
        assert_eq!(
            captured.as_deref(),
            Some("Bearer sk-backend"),
            "upstream must receive the resolved key as a Bearer token"
        );

        let result = fetch_models_from_client(&client_config, &http_client, None).await;
        assert!(result.is_ok(), "fetch without a key must still succeed");
        assert!(
            seen.lock().await.is_none(),
            "no resolved key must mean no Authorization header"
        );
        server.abort();
    }

    #[test]
    fn resolve_prefers_client_credential_over_configured_key() {
        let mut config = unreachable_client_config();
        config.api_key = Some("sk-backend".to_string());

        let keys = resolve_upstream_api_keys(&[config], Some("sk-client"));
        assert_eq!(keys, vec![Some("sk-client".to_string())]);
    }

    #[test]
    fn resolve_falls_back_to_configured_key_without_client_credential() {
        let mut config = unreachable_client_config();
        config.api_key = Some("sk-backend".to_string());

        let keys = resolve_upstream_api_keys(&[config], None);
        assert_eq!(keys, vec![Some("sk-backend".to_string())]);
    }

    #[test]
    fn resolve_treats_empty_configured_key_as_absent() {
        let mut config = unreachable_client_config();
        config.api_key = Some(String::new());

        let keys = resolve_upstream_api_keys(&[config], None);
        assert_eq!(keys, vec![None]);
    }

    #[test]
    fn resolve_applies_priority_per_backend() {
        let mut with_key = unreachable_client_config();
        with_key.api_key = Some("sk-backend".to_string());
        let without_key = unreachable_client_config();

        let keys = resolve_upstream_api_keys(&[with_key, without_key], Some("sk-client"));
        assert_eq!(
            keys,
            vec![Some("sk-client".to_string()), Some("sk-client".to_string())]
        );

        let keys = resolve_upstream_api_keys(&[unreachable_client_config()], None);
        assert_eq!(keys, vec![None]);
    }

    #[test]
    fn effective_key_encoding_is_unambiguous() {
        let split_a = vec![Some("ab".to_string()), Some("c".to_string())];
        let split_b = vec![Some("a".to_string()), Some("bc".to_string())];
        assert_ne!(
            encode_effective_keys(&split_a),
            encode_effective_keys(&split_b)
        );

        let none_vs_empty = vec![None];
        assert_ne!(
            encode_effective_keys(&none_vs_empty),
            encode_effective_keys(&[Some(String::new())])
        );
    }

    #[test]
    fn cache_key_tracks_effective_keys() {
        let cache = ModelsCache::new();
        let with_client_key = vec![Some("sk-client".to_string())];
        let with_backend_key = vec![Some("sk-backend".to_string())];
        let no_key = vec![None];

        let k1 = make_key(&cache, 1, &with_client_key);
        let k2 = make_key(&cache, 1, &with_backend_key);
        let k3 = make_key(&cache, 1, &no_key);

        assert_ne!(k1.credential, k2.credential);
        assert_ne!(k1.credential, k3.credential);
        assert_ne!(k2.credential, k3.credential);
        assert_eq!(
            k1.credential,
            make_key(&cache, 1, &with_client_key).credential
        );
    }
}
