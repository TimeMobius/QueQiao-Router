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
    models::requests::{RequestPayload, ResponsesRequest},
    state::app_state::AppState,
};

/// 处理 OpenAI Responses API 请求
pub async fn handle_responses(
    state: State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    payload: CustomJson<ResponsesRequest>,
) -> Response {
    info!("Handling responses request for model: {}", payload.0.model);
    handle_request_logic(
        state,
        headers,
        Some(addr),
        RequestPayload::Responses(payload.0),
    )
    .await
}
