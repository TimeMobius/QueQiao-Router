use crate::config::body_limit::RequestBodyLimit;
use crate::models::AccessLogMeta;
use axum::{
    body::Bytes,
    extract::{
        rejection::{BytesRejection, FailedToBufferBody},
        FromRequest, Request,
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::de::DeserializeOwned;
use serde_json::Value;

/// 自定义 JSON 提取器，用于拦截反序列化错误并返回标准 JSON 格式的错误响应
pub struct CustomJson<T>(pub T);

#[axum::async_trait]
impl<T, S> FromRequest<S> for CustomJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        // 请求体上限由路由层以扩展形式注入，用于生成携带当前阈值的 413 错误体。
        let body_limit = req
            .extensions()
            .get::<RequestBodyLimit>()
            .map(|limit| limit.bytes());

        // 1. 先读取 Bytes
        let bytes = match Bytes::from_request(req, state).await {
            Ok(b) => b,
            Err(err) => {
                // 请求体超过网关上限：返回结构化 413，而不是 Axum 默认的纯文本
                // "Failed to buffer the request body: length limit exceeded"。
                // 客户端（如 DSH）可据 error.type=request_too_large 触发上下文压缩。
                if let BytesRejection::FailedToBufferBody(FailedToBufferBody::LengthLimitError(_)) =
                    &err
                {
                    let error_message = crate::app_error::request_too_large_message(body_limit);
                    let error_response = crate::app_error::build_request_too_large_body(body_limit);
                    let mut response =
                        (StatusCode::PAYLOAD_TOO_LARGE, Json(error_response)).into_response();
                    // 超限时不再把整个请求体写入错误日志（可能是数十 MiB）。
                    response.extensions_mut().insert(AccessLogMeta {
                        model: "-".to_string(),
                        backend: "unknown".to_string(),
                        error: Some(error_message),
                        request_body: None,
                    });
                    return Err(response);
                }

                let err_msg = err.to_string();
                if err_msg.contains("peer closed connection")
                    || err_msg.contains("closed connection")
                    || err_msg.contains("connection closed")
                {
                    let error_response = crate::app_error::build_error_body(
                        "Client disconnected before the request body was fully received. Please check network stability or increase the client-side timeout.",
                        "client_disconnect",
                    );
                    let mut response =
                        (StatusCode::BAD_REQUEST, Json(error_response)).into_response();
                    response.extensions_mut().insert(AccessLogMeta {
                        model: "-".to_string(),
                        backend: "unknown".to_string(),
                        error: Some(format!("Client disconnected prematurely: {}", err_msg)),
                        request_body: None,
                    });
                    return Err(response);
                }
                return Err(err.into_response());
            }
        };

        // 2. 尝试反序列化 (使用 simd-json 加速)
        // simd-json 需要可变 buffer，因此需要转换为 Vec<u8>
        let mut buf = bytes.to_vec();

        match simd_json::from_slice::<T>(&mut buf) {
            Ok(data) => Ok(CustomJson(data)),
            Err(e) => {
                // 3. 失败处理：记录日志元数据
                let error_message = e.to_string();
                // 将 bytes 转换为 string (lossy) 以便记录日志
                let body_str = String::from_utf8_lossy(&bytes).to_string();

                // 反序列化失败不代表 body 不是合法 JSON（可能只是字段不匹配），
                // 因此再尽力提取一次 model 用于失败归因；提取不到时保持 "-"。
                let model = serde_json::from_str::<Value>(&body_str)
                    .ok()
                    .and_then(|v| v.get("model").and_then(Value::as_str).map(str::to_string))
                    .unwrap_or_else(|| "-".to_string());

                // 统一错误消息
                let final_error_msg = format!("Request body validation failed: {}", error_message);

                let error_response =
                    crate::app_error::build_error_body(&final_error_msg, "InvalidRequest");

                let mut response =
                    (StatusCode::UNPROCESSABLE_ENTITY, Json(error_response)).into_response();

                // 注入 AccessLogMeta 到 Response extensions
                response.extensions_mut().insert(AccessLogMeta {
                    model,
                    backend: "unknown".to_string(),
                    error: Some(final_error_msg),
                    request_body: Some(body_str),
                });

                Err(response)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        extract::DefaultBodyLimit,
        http::{header, Request as HttpRequest, StatusCode},
        routing::post,
        Extension, Router,
    };
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;

    async fn echo(payload: CustomJson<Value>) -> Json<Value> {
        Json(payload.0)
    }

    fn test_router(limit_bytes: usize) -> Router {
        Router::new()
            .route("/echo", post(echo))
            .layer(DefaultBodyLimit::max(limit_bytes))
            .layer(Extension(RequestBodyLimit(limit_bytes)))
    }

    async fn post_json(app: Router, payload: Vec<u8>) -> (StatusCode, String, Value) {
        let request = HttpRequest::builder()
            .method("POST")
            .uri("/echo")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(payload))
            .unwrap();

        let response = app.oneshot(request).await.unwrap();
        let status = response.status();
        let content_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        (status, content_type, json)
    }

    #[tokio::test]
    async fn oversized_body_returns_structured_413() {
        let limit = 1024;
        // 合法 JSON，但超出 test router 设置的 1 KiB 上限
        let payload = format!("{{\"data\":\"{}\"}}", "a".repeat(2048)).into_bytes();

        let (status, content_type, body) = post_json(test_router(limit), payload).await;

        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert!(
            content_type.starts_with("application/json"),
            "413 should be JSON, got content-type: {content_type}"
        );
        assert_eq!(body["error"]["type"], "request_too_large");
        assert_eq!(body["error_type"], "request_too_large");

        let message = body["error"]["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("1 KiB") && message.contains("1024 bytes"),
            "413 message should include the effective limit, got: {message}"
        );
        assert!(
            message.contains("MAX_REQUEST_BODY_BYTES"),
            "413 message should point at the override knob, got: {message}"
        );
    }

    #[tokio::test]
    async fn body_within_limit_is_accepted() {
        let payload = br#"{"model":"test","messages":[]}"#.to_vec();

        let (status, content_type, body) = post_json(test_router(1024), payload).await;

        assert_eq!(status, StatusCode::OK);
        assert!(content_type.starts_with("application/json"));
        assert_eq!(body["model"], "test");
    }
}
