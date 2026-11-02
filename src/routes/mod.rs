use crate::config::body_limit::{format_bytes, resolve_request_body_limit, RequestBodyLimit};
use crate::metrics::exporter::create_metrics_router;
use crate::metrics::middleware::metrics_middleware;
use crate::state::app_state::AppState;
use axum::{
    extract::DefaultBodyLimit,
    http::Method,
    middleware as axum_middleware,
    routing::{get, post},
    Extension, Router,
};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};

pub mod audio;
pub mod chat;
pub mod completions;
pub mod dashboard;
pub mod embeddings;
pub mod extras;
pub mod health;
pub mod messages;
pub mod models;
pub mod responses;

async fn favicon_redirect() -> axum::response::Redirect {
    axum::response::Redirect::permanent("/dashboard/favicon.svg")
}

pub fn create_router(app_state: Arc<AppState>) -> Router {
    // 网关请求体上限：默认 64 MiB，可用 `MAX_REQUEST_BODY_BYTES` 覆盖。
    // Axum 的 2 MiB 默认值会在多轮 Agent 会话（全量历史 + 内联 base64 图片）上
    // 提前返回 413，此时客户端自身的预算尚未生效，无法触发自动恢复。
    let request_body_limit = resolve_request_body_limit();
    tracing::info!(
        limit_bytes = request_body_limit,
        limit_human = %format_bytes(request_body_limit),
        "request body limit configured"
    );

    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_origin(Any)
        .allow_headers(Any);

    let mut router = Router::new()
        .route("/health", get(health::health_check))
        .route("/favicon.ico", get(favicon_redirect))
        .route("/v1/models", get(models::get_models))
        .route("/v1/chat/completions", post(chat::handle_chat_completion))
        .route("/v1/responses", post(responses::handle_responses))
        .route("/v1/messages", post(messages::handle_messages))
        .route("/v1/completions", post(completions::handle_completion))
        .route("/v1/embeddings", post(embeddings::handle_embeddings))
        .route("/v1/rerank", post(extras::handle_rerank))
        .route("/rerank", post(extras::handle_rerank))
        .route("/score", post(extras::handle_score))
        .route("/classify", post(extras::handle_classify))
        .route(
            "/v1/audio/transcriptions",
            post(audio::handle_audio_transcription),
        )
        .route(
            "/v1/audio/translations",
            post(audio::handle_audio_translation),
        )
        .merge(create_metrics_router())
        .nest("/dashboard", dashboard::dashboard_router());

    if std::env::var("ENABLE_TEST_API").as_deref() == Ok("1") {
        router = router.nest("/internal/test", crate::handlers::test_api::test_router());
    }

    router
        // 提高所有请求体提取器（Bytes/Json/Form/Multipart）的上限；
        // 同时把生效值放进请求扩展，供 `CustomJson` 生成可自解释的 413 错误体。
        .layer(DefaultBodyLimit::max(request_body_limit))
        .layer(Extension(RequestBodyLimit(request_body_limit)))
        .layer(cors)
        .layer(axum_middleware::from_fn(metrics_middleware))
        .with_state(app_state)
}
