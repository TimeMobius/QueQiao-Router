use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use prometheus::TextEncoder;
use std::sync::Arc;

use crate::state::app_state::AppState;

pub fn create_metrics_router() -> Router<Arc<AppState>> {
    Router::new().route("/metrics", get(metrics_endpoint))
}

async fn metrics_endpoint(State(app_state): State<Arc<AppState>>) -> Response {
    crate::metrics::db_writer::update(&app_state.audit_writer.snapshot());
    let encoder = TextEncoder::new();
    let metric_families = prometheus::gather();

    match encoder.encode_to_string(&metric_families) {
        Ok(output) => (StatusCode::OK, output).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}
