use crate::db::engine::DbError;
use crate::models::AccessLogMeta;
use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use std::fmt;

/// 构建统一的双格式错误体：
/// - 嵌套 `error` 对象（`message`/`type`/`param`/`code`）对齐 OpenAI SDK 契约
/// - 顶层 `error_type` 保留，兼容读取该字段的旧客户端
pub fn build_error_body(message: &str, error_type: &str) -> Value {
    json!({
        "error": {
            "message": message,
            "type": error_type,
            "param": null,
            "code": null,
        },
        "error_type": error_type,
    })
}

#[derive(Debug)]
pub enum AppError {
    ReqwestError(reqwest::Error),
    DatabaseError(DbError),
    ClientNotFound(String),
    UpstreamError(String),
    InvalidHeader(String),
    InternalServerError(String),
    ApiError(StatusCode, String, String), // status_code, message, error_type
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AppError::ReqwestError(err) => write!(f, "External request failed: {}", err),
            AppError::DatabaseError(err) => write!(f, "Database operation failed: {}", err),
            AppError::ClientNotFound(model) => write!(f, "The model `{}` does not exist.", model),
            AppError::UpstreamError(msg) => write!(f, "Upstream error: {}", msg),
            AppError::InvalidHeader(msg) => write!(f, "Invalid header: {}", msg),
            AppError::InternalServerError(msg) => write!(f, "Internal server error: {}", msg),
            AppError::ApiError(_, message, error_type) => write!(f, "{}: {}", error_type, message),
        }
    }
}

impl std::error::Error for AppError {}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, error_message, error_type) = match self {
            AppError::ReqwestError(err) => {
                if err.is_timeout() {
                    (
                        StatusCode::GATEWAY_TIMEOUT,
                        format!("Request to upstream timed out: {}", err),
                        "timeout_error".to_string(),
                    )
                } else if err.is_connect() {
                    (
                        StatusCode::BAD_GATEWAY,
                        format!("Failed to connect to upstream: {}", err),
                        "connection_error".to_string(),
                    )
                } else if let Some(status) = err.status() {
                    // 如果上游返回了具体的 HTTP 错误状态码
                    (
                        status,
                        format!("Upstream returned error: {}", err),
                        "upstream_error".to_string(),
                    )
                } else {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("External request failed: {}", err),
                        "internal_error".to_string(),
                    )
                }
            }
            AppError::DatabaseError(err) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Database operation failed: {}", err),
                "database_error".to_string(),
            ),
            AppError::ClientNotFound(model) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("The model `{}` does not exist.", model),
                "Input Validation Error".to_string(),
            ),
            AppError::UpstreamError(msg) => {
                (StatusCode::BAD_GATEWAY, msg, "upstream_error".to_string())
            }
            AppError::InvalidHeader(msg) => (
                StatusCode::BAD_REQUEST,
                msg,
                "invalid_request_error".to_string(),
            ),
            AppError::InternalServerError(msg) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                msg,
                "internal_error".to_string(),
            ),
            AppError::ApiError(status, message, error_type) => (status, message, error_type),
        };

        let body = Json(build_error_body(&error_message, &error_type));

        let mut response = (status, body).into_response();

        // Inject error details for access logging
        response.extensions_mut().insert(AccessLogMeta {
            model: "-".to_string(),
            backend: "unknown".to_string(),
            error: Some(error_message),
            request_body: None,
        });

        response
    }
}

impl From<reqwest::Error> for AppError {
    fn from(err: reqwest::Error) -> Self {
        AppError::ReqwestError(err)
    }
}

impl From<DbError> for AppError {
    fn from(err: DbError) -> Self {
        AppError::DatabaseError(err)
    }
}
