use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use once_cell::sync::Lazy;
use prometheus::TextEncoder;
use std::sync::{Arc, Mutex};

use crate::state::app_state::AppState;

/// 保护“重置/重建健康指标 + gather”这一段，避免并发抓取看到半成品序列。
static SCRAPE_LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

pub fn create_metrics_router() -> Router<Arc<AppState>> {
    Router::new().route("/metrics", get(metrics_endpoint))
}

async fn metrics_endpoint(State(app_state): State<Arc<AppState>>) -> Response {
    // await 放在锁外：std MutexGuard 不能跨 await 存活。
    let config_health = app_state.config_manager.health().await;
    let config = app_state.config_manager.get_config().await;
    let backend_health = app_state.dispatcher_service.backend_health_snapshot();

    let encoder = TextEncoder::new();
    let metric_families = {
        let _guard = SCRAPE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        crate::metrics::db_writer::update(&app_state.audit_writer.snapshot());
        crate::metrics::config_health::update(&config_health);
        crate::metrics::backend_health::update(&config, &backend_health);
        prometheus::gather()
    };

    match encoder.encode_to_string(&metric_families) {
        Ok(output) => (StatusCode::OK, output).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}
