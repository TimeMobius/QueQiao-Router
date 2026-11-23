use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::sync::Arc;

use crate::state::app_state::AppState;

#[derive(Serialize)]
pub struct HealthResponse {
    status: String,
    /// `ok` while the on-disk config is usable; `degraded` when it is not.
    config: String,
    /// Config generation currently serving requests.
    #[serde(skip_serializing_if = "is_default")]
    config_generation: u64,
    /// Number of failed config loads since start (read/parse/validate).
    config_failures: u64,
    /// Why the config is unusable, if it is.
    #[serde(skip_serializing_if = "Option::is_none")]
    config_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    config_error_kind: Option<String>,
}

fn is_default(value: &u64) -> bool {
    *value == 0
}

/// Health check endpoint.
///
/// Liveness **and** config readiness: a deleted, unreadable, invalid or
/// backend-less config file no longer hides behind a 200. The in-memory
/// `Config` is never cleared on a failed reload (in-flight requests keep
/// working), but `/health` reports 503 so container health checks and
/// dashboards see the problem.
pub async fn health_check(State(app_state): State<Arc<AppState>>) -> Response {
    let health = app_state.config_manager.health().await;

    if health.ok {
        return Json(HealthResponse {
            status: "ok".to_string(),
            config: "ok".to_string(),
            config_generation: health.generation,
            config_failures: health.failures,
            config_error: None,
            config_error_kind: None,
        })
        .into_response();
    }

    let (kind, message) = health
        .last_error
        .as_ref()
        .map(|error| (error.kind.as_str().to_string(), error.message.clone()))
        .unwrap_or_else(|| ("unknown".to_string(), "config is unusable".to_string()));

    let mut response = (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(HealthResponse {
            status: "degraded".to_string(),
            config: "unusable".to_string(),
            config_generation: health.generation,
            config_failures: health.failures,
            config_error: Some(message),
            config_error_kind: Some(kind.clone()),
        }),
    )
        .into_response();
    // Cheap machine-readable signals for probes/dashboards that only look at headers.
    if let Ok(value) = kind.parse() {
        response.headers_mut().insert("x-config-error-kind", value);
    }
    if let Ok(value) = health.failures.to_string().parse() {
        response.headers_mut().insert("x-config-failures", value);
    }
    response
}
