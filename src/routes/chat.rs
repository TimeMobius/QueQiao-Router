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
    models::requests::{ChatCompletionRequest, RequestPayload},
    state::app_state::AppState,
};

/// 处理聊天完成请求
pub async fn handle_chat_completion(
    state: State<Arc<AppState>>,
    headers: HeaderMap,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    payload: CustomJson<ChatCompletionRequest>,
) -> Response {
    info!(
        "Handling chat completion request for model: {}",
        payload.0.model
    );
    handle_request_logic(state, headers, Some(addr), RequestPayload::Chat(payload.0)).await
}
