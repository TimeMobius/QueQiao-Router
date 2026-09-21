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
    models::requests::{ClassifyRequest, RequestPayload, RerankRequest, ScoreRequest},
    state::app_state::AppState,
};

pub async fn handle_rerank(
    state: State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    payload: CustomJson<RerankRequest>,
) -> Response {
    info!("Handling rerank request for model: {}", payload.0.model);
    handle_request_logic(
        state,
        headers,
        Some(addr),
        RequestPayload::Rerank(payload.0),
    )
    .await
}

pub async fn handle_score(
    state: State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    payload: CustomJson<ScoreRequest>,
) -> Response {
    info!("Handling score request for model: {}", payload.0.model);
    handle_request_logic(state, headers, Some(addr), RequestPayload::Score(payload.0)).await
}

pub async fn handle_classify(
    state: State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    payload: CustomJson<ClassifyRequest>,
) -> Response {
    info!("Handling classify request for model: {}", payload.0.model);
    handle_request_logic(
        state,
        headers,
        Some(addr),
        RequestPayload::Classify(payload.0),
    )
    .await
}
