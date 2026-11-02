use crate::config::body_limit::format_bytes;
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

/// 请求体超限（HTTP 413）时返回给调用方的错误消息。
///
/// 明确给出当前生效的上限，便于客户端压缩历史或降低图片载荷后重试。
/// 错误类型固定为 `request_too_large`，客户端可据此触发上下文压缩等自动恢复。
pub fn request_too_large_message(limit_bytes: Option<usize>) -> String {
    match limit_bytes {
        Some(limit) => format!(
            "Request body too large: the gateway accepts request bodies up to {} ({} bytes) and this request exceeded it. Reduce conversation history or inline image payloads and retry. The limit can be raised with the MAX_REQUEST_BODY_BYTES environment variable.",
            format_bytes(limit),
            limit
        ),
        None => "Request body too large: this request exceeded the gateway request body limit. Reduce conversation history or inline image payloads and retry.".to_string(),
    }
}

/// 构建请求体超限（HTTP 413）的 OpenAI 兼容错误体。
pub fn build_request_too_large_body(limit_bytes: Option<usize>) -> Value {
    build_error_body(&request_too_large_message(limit_bytes), "request_too_large")
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
