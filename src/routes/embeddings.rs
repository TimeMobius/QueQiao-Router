use axum::{
    extract::{ConnectInfo, State},
    http::HeaderMap,
    response::Response,
};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

use crate::{
    handlers::{common_handler::handle_request_logic, extractors::CustomJson},
    models::requests::{EmbeddingRequest, RequestPayload},
    state::app_state::AppState,
};

pub async fn handle_embeddings(
    state: State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    payload: CustomJson<EmbeddingRequest>,
) -> Response {
    info!("Handling embeddings request for model: {}", payload.0.model);
    handle_request_logic(
        state,
        headers,
        Some(addr),
        RequestPayload::Embedding(payload.0),
    )
    .await
}
