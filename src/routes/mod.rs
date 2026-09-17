use crate::metrics::exporter::create_metrics_router;
use crate::metrics::middleware::metrics_middleware;
use crate::state::app_state::AppState;
use axum::{
    http::Method,
    middleware as axum_middleware,
    routing::{get, post},
    Router,
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
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS])
        .allow_origin(Any)
        .allow_headers(Any);

    Router::new()
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
        .nest("/dashboard", dashboard::dashboard_router())
        .layer(cors)
        .layer(axum_middleware::from_fn(metrics_middleware))
        .with_state(app_state)
}
