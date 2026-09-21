use crate::models::AccessLogMeta;
use axum::{
    body::Bytes,
    extract::{FromRequest, Request},
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
        // 1. 先读取 Bytes
        let bytes = match Bytes::from_request(req, state).await {
            Ok(b) => b,
            Err(err) => {
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
