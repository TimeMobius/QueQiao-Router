use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    Json,
};
use serde_json::Value;
use std::sync::Arc;

use crate::{handlers::model_handler::list_models, state::app_state::AppState};

/// 获取所有模型的列表
pub async fn get_models(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> (StatusCode, Json<Value>) {
    list_models(State(app_state), headers).await
}
