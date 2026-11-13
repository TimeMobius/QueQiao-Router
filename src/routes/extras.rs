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
    models::requests::{
        ClassifyRequest, RequestPayload, RerankRequest, ScoreRequest, SystemOneRequest,
    },
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

/// System One 分类协议（TypeSafe / OpenRouter 的 Jev 等分类模型）。
/// 客户端路径 `/v1/systemone`（兼容裸路径 `/systemone`），上游 `{base_url}/systemone`。
pub async fn handle_systemone(
    state: State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    payload: CustomJson<SystemOneRequest>,
) -> Response {
    info!("Handling systemone request for model: {}", payload.0.model);
    handle_request_logic(
        state,
        headers,
        Some(addr),
        RequestPayload::SystemOne(payload.0),
    )
    .await
}
