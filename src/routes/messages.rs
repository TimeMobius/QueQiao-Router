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
    models::requests::{AnthropicMessagesRequest, RequestPayload},
    state::app_state::AppState,
};

/// 处理 Anthropic Messages API 请求
pub async fn handle_messages(
    state: State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    payload: CustomJson<AnthropicMessagesRequest>,
) -> Response {
    info!("Handling messages request for model: {}", payload.0.model);
    handle_request_logic(
        state,
        headers,
        Some(addr),
        RequestPayload::AnthropicMessages(payload.0),
    )
    .await
}
