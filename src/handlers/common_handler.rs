use crate::app_error::AppError;
use crate::metrics::middleware::get_metrics_sender;
use crate::metrics::prometheus::ERRORS_TOTAL;
use crate::metrics::worker::MetricEvent;
use crate::models::AccessLogMeta;
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tracing::warn;

use crate::{
    client::proxy::{build_and_send_request, get_api_key},
    config::types::ClientConfig,
    db::records::log_non_streaming_request,
    handlers::anthropic_stream_handler::process_anthropic_streaming_response,
    handlers::responses_stream_handler::process_responses_streaming_response,
    handlers::stream_handler::{extract_error_msg, process_streaming_response},
    handlers::utils::{
        apply_prefix_to_json, build_request_body_generic, filter_empty_messages, get_client_ip,
        process_messages, remove_think_tags, truncate_json,
    },
    metrics::active_requests::{ActiveRequestLabels, GuardedBody},
    models::requests::RequestPayload,
    state::app_state::AppState,
};

fn prepare_chat_request(payload: &mut RequestPayload) {
    if let RequestPayload::Chat(ref mut p) = payload {
        let messages = std::mem::take(&mut p.messages);
        let processed_messages = process_messages(messages);
        let filtered_messages = filter_empty_messages(processed_messages);
        p.messages = remove_think_tags(filtered_messages);
    }
}

fn is_empty_value(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Array(arr) => arr.is_empty(),
        Value::Object(obj) => obj.is_empty(),
        _ => false,
    }
}

// Helper to create validation error response with logging metadata
fn create_validation_error(msg: &str, payload: &RequestPayload) -> Response {
    let error_response = crate::app_error::build_error_body(msg, "Input Validation Error");

    let mut response = (StatusCode::UNPROCESSABLE_ENTITY, Json(error_response)).into_response();

    // Serialize payload for logging
    let payload_value =
        serde_json::to_value(payload).unwrap_or(json!({"error": "serialization failed"}));
    let log_body = serde_json::to_string(&truncate_json(&payload_value)).unwrap_or_default();

    response.extensions_mut().insert(AccessLogMeta {
        model: payload.get_model().to_string(),
        backend: "unknown".to_string(),
        error: Some(msg.to_string()),
        request_body: Some(log_body),
    });

    response
}

/// 统一处理所有请求的核心逻辑
///
/// 该函数实现了请求的完整生命周期管理，现已委托给 `DispatcherService` 处理路由和故障转移。
pub async fn handle_request_logic(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    addr: Option<SocketAddr>,
    mut payload: RequestPayload,
) -> Response {
    // 1. 输入验证
    match &payload {
        RequestPayload::Chat(p) => {
            if p.messages.is_empty() {
                return create_validation_error(
                    "Request param messages not arr or arr is empty",
                    &payload,
                );
            }
        }
        RequestPayload::Completion(p) => {
            if p.prompt.is_empty() {
                return create_validation_error("Request param prompt is empty", &payload);
            }
        }
        RequestPayload::Embedding(p) => {
            if is_empty_value(&p.input) {
                return create_validation_error("Request param input is empty", &payload);
            }
        }
        RequestPayload::Rerank(p) => {
            if p.query.is_empty() || p.documents.is_empty() {
                return create_validation_error(
                    "Request param query or documents is empty",
                    &payload,
                );
            }
        }
        RequestPayload::Score(p) => {
            if is_empty_value(&p.text_1) || is_empty_value(&p.text_2) {
                return create_validation_error(
                    "Request param text_1 or text_2 is empty",
                    &payload,
                );
            }
        }
        RequestPayload::Classify(p) => {
            if is_empty_value(&p.input) {
                return create_validation_error("Request param input is empty", &payload);
            }
        }
        RequestPayload::Responses(p) => {
            if is_empty_value(&p.input) {
                return create_validation_error("Request param input is empty", &payload);
            }
        }
        // Anthropic Messages API 透传模式，messages 数组校验留给上游做
        RequestPayload::AnthropicMessages(_) => {}
    }

    // 对 Chat 请求，预处理 messages
    prepare_chat_request(&mut payload);

    let initial_model = payload.get_model().to_string();
    let routing_keys = payload.get_routing_keys();
    let endpoint = payload.get_endpoint();
    let payload_clone = payload.clone();
    let app_state_clone = app_state.clone();
    let headers_clone = headers.clone();
    let addr_clone = addr;

    let mut response = app_state
        .dispatcher_service
        .execute(
            &initial_model,
            routing_keys,
            endpoint,
            move |client_config, model_name| {
                let app_state_inner = app_state_clone.clone();
                let headers_inner = headers_clone.clone();
                let addr_inner = addr_clone;

                let mut current_payload = payload_clone.clone();
                current_payload.set_model(model_name.to_string());
                let client_config = client_config.clone();

                async move {
                    dispatch_request(
                        &app_state_inner,
                        &headers_inner,
                        addr_inner,
                        &current_payload,
                        &client_config,
                    )
                    .await
                }
            },
        )
        .await;

    let _model = initial_model.as_str();
    let status = response.status().as_u16();

    // Record errors
    if status >= 400 {
        let error_type = if status == 401 || status == 403 {
            "auth"
        } else if status == 429 {
            "rate_limit"
        } else if status >= 500 {
            "server_error"
        } else {
            "client_error"
        };

        // Get backend from response extensions if available
        let backend_str = response
            .extensions()
            .get::<AccessLogMeta>()
            .map(|m| m.backend.as_str())
            .unwrap_or("unknown");

        ERRORS_TOTAL
            .with_label_values(&[error_type, &initial_model, backend_str])
            .inc();
    }

    // 如果响应中包含日志元数据但缺少 request_body（通常发生在所有上游都失败时），
    // 在此处补全 request_body 以便记录日志。
    if let Some(meta) = response.extensions_mut().get_mut::<AccessLogMeta>() {
        if meta.request_body.is_none() {
            let payload_value = serde_json::to_value(&payload)
                .unwrap_or(json!({"error": "failed to serialize payload"}));
            let log_body =
                serde_json::to_string(&truncate_json(&payload_value)).unwrap_or_default();
            meta.request_body = Some(log_body);
        }
    }

    response
}

// 仅重试连接层瞬时错误：连接失败、或响应体在传完前被对端截断（peer closed / reset / incomplete）。
// 不重试超时，避免在长请求上翻倍等待。
fn is_retryable_transient_error(err: &reqwest::Error) -> bool {
    if err.is_connect() {
        return true;
    }
    let msg = err.to_string().to_ascii_lowercase();
    msg.contains("peer closed connection")
        || msg.contains("connection closed")
        || msg.contains("connection reset")
        || msg.contains("incomplete message")
        || msg.contains("broken pipe")
}

fn classify_boxed_send_error(e: Box<dyn std::error::Error + Send + Sync>) -> (AppError, bool) {
    match e.downcast::<reqwest::Error>() {
        Ok(req_err) => {
            let retryable = is_retryable_transient_error(&req_err);
            let error_text = if req_err.is_timeout() {
                "Request timed out"
            } else if req_err.is_connect() {
                "Failed to connect to host"
            } else {
                "External request failed"
            };
            (
                AppError::InternalServerError(error_text.to_string()),
                retryable,
            )
        }
        Err(original_err) => (
            AppError::InternalServerError(original_err.to_string()),
            false,
        ),
    }
}

// 流式路径只重试到「收到响应头」为止；一旦开始向下游转发就不可重试。
async fn send_streaming_with_retry(
    app_state: &Arc<AppState>,
    client_config: &ClientConfig,
    api_key: &Option<String>,
    url: &str,
    request_body: &Value,
    api_endpoint: &str,
) -> Result<reqwest::Response, AppError> {
    for attempt in 1..=2u8 {
        match build_and_send_request(
            app_state,
            client_config,
            api_key,
            url,
            request_body,
            true,
            api_endpoint,
        )
        .await
        {
            Ok(r) => return Ok(r),
            Err(e) => {
                let (app_err, retryable) = classify_boxed_send_error(e);
                if retryable && attempt == 1 {
                    warn!(
                        "Transient error contacting {} (attempt {}), retrying with a fresh connection: {}",
                        client_config.name, attempt, app_err
                    );
                    continue;
                }
                return Err(app_err);
            }
        }
    }
    unreachable!("send_streaming_with_retry loop always returns")
}

// 非流式路径把 send 与 body 读取放在同一重试域：body 尚未下发给下游，重试完全安全。
async fn fetch_non_streaming_with_retry(
    app_state: &Arc<AppState>,
    client_config: &ClientConfig,
    api_key: &Option<String>,
    url: &str,
    request_body: &Value,
    api_endpoint: &str,
) -> Result<(StatusCode, Bytes), AppError> {
    for attempt in 1..=2u8 {
        let response = match build_and_send_request(
            app_state,
            client_config,
            api_key,
            url,
            request_body,
            false,
            api_endpoint,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                let (app_err, retryable) = classify_boxed_send_error(e);
                if retryable && attempt == 1 {
                    warn!(
                        "Transient error contacting {} (attempt {}), retrying with a fresh connection: {}",
                        client_config.name, attempt, app_err
                    );
                    continue;
                }
                return Err(app_err);
            }
        };

        let status = response.status();
        match response.bytes().await {
            Ok(bytes) => return Ok((status, bytes)),
            Err(e) => {
                if is_retryable_transient_error(&e) && attempt == 1 {
                    warn!(
                        "Upstream {} closed the response body prematurely (attempt {}), retrying: {}",
                        client_config.name, attempt, e
                    );
                    continue;
                }
                return Err(AppError::ReqwestError(e));
            }
        }
    }
    unreachable!("fetch_non_streaming_with_retry loop always returns")
}

/// 统一的请求派发函数
async fn dispatch_request(
    app_state: &Arc<AppState>,
    headers: &HeaderMap,
    addr: Option<SocketAddr>,
    payload: &RequestPayload,
    client_config: &ClientConfig,
) -> Result<Response, AppError> {
    let (endpoint_path, is_chat) = match payload {
        RequestPayload::Chat(_) => ("chat/completions", true),
        RequestPayload::Completion(_) => ("completions", false),
        RequestPayload::Embedding(_) => ("embeddings", false),
        RequestPayload::Rerank(_) => ("rerank", false),
        RequestPayload::Score(_) => ("score", false),
        RequestPayload::Classify(_) => ("classify", false),
        RequestPayload::Responses(_) => ("responses", false),
        RequestPayload::AnthropicMessages(_) => ("messages", false),
    };
    let url = format!(
        "{}/{}",
        client_config.base_url.trim_end_matches('/'),
        endpoint_path
    );
    let api_endpoint = format!("/v1/{}", endpoint_path);
    let api_key = get_api_key(client_config, headers);

    let request_body = build_request_body_generic(payload, client_config, payload.is_streaming());
    let is_streaming = payload.is_streaming();

    let active_labels = ActiveRequestLabels {
        endpoint: api_endpoint.clone(),
        model: request_body
            .get("model")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string(),
        backend: client_config.name.clone(),
    };

    let request_start = Instant::now();

    if is_streaming {
        let response = send_streaming_with_retry(
            app_state,
            client_config,
            &api_key,
            &url,
            &request_body,
            &api_endpoint,
        )
        .await?;

        let client_ip = get_client_ip(headers, addr);
        let mut resp = if matches!(payload, RequestPayload::Responses(_)) {
            process_responses_streaming_response(
                app_state.clone(),
                headers.clone(),
                payload.clone(),
                client_ip,
                response,
                client_config,
                &request_body,
            )
            .await?
        } else if matches!(payload, RequestPayload::AnthropicMessages(_)) {
            process_anthropic_streaming_response(
                app_state.clone(),
                headers.clone(),
                payload.clone(),
                client_ip,
                response,
                client_config,
                &request_body,
            )
            .await?
        } else {
            process_streaming_response(
                app_state.clone(),
                headers.clone(),
                payload.clone(),
                client_ip,
                response,
                client_config,
                is_chat,
                &request_body,
            )
            .await?
        };
        resp.extensions_mut().insert(());
        Ok(wrap_response_with_active_guard(resp, active_labels))
    } else {
        let (status, body_bytes) = fetch_non_streaming_with_retry(
            app_state,
            client_config,
            &api_key,
            &url,
            &request_body,
            &api_endpoint,
        )
        .await?;
        let request_elapsed = request_start.elapsed().as_secs_f64();

        let resp = process_non_streaming_response(
            app_state,
            headers,
            addr,
            payload,
            client_config,
            &request_body,
            status,
            body_bytes,
            request_elapsed,
            &api_endpoint,
        )
        .await?;
        Ok(wrap_response_with_active_guard(resp, active_labels))
    }
}

fn wrap_response_with_active_guard(resp: Response, labels: ActiveRequestLabels) -> Response {
    let (parts, body) = resp.into_parts();
    let guarded = GuardedBody::new(body, labels);
    Response::from_parts(parts, Body::new(guarded))
}

/// 处理非流式响应
async fn process_non_streaming_response(
    app_state: &Arc<AppState>,
    headers: &HeaderMap,
    addr: Option<SocketAddr>,
    payload: &RequestPayload,
    client_config: &ClientConfig,
    request_body: &Value,
    status: StatusCode,
    body_bytes: Bytes,
    request_elapsed: f64,
    api_endpoint: &str,
) -> Result<Response, AppError> {
    let model = payload.get_model().to_string();
    let backend = client_config.name.clone();

    let mut buf = body_bytes.to_vec();
    let mut response_body: Value = simd_json::from_slice(&mut buf).map_err(|e| {
        AppError::InternalServerError(format!(
            "Failed to parse upstream JSON with simd-json: {}",
            e
        ))
    })?;

    // 如果是错误响应，尝试提取错误信息并注入日志元数据
    let error_msg = if !status.is_success() {
        extract_error_msg(&response_body)
    } else {
        None
    };

    if status.is_success() {
        if let Some(special_prefix) = &client_config.special_prefix {
            apply_prefix_to_json(
                &mut response_body,
                special_prefix,
                matches!(payload, RequestPayload::Chat(_)),
            );
        }

        // Normalize thinking/reasoning format for chat completions only.
        if matches!(payload, RequestPayload::Chat(_)) {
            let target = {
                let config = app_state.config_manager.get_config_guard().await;
                config.resolve_thinking_format(client_config)
            };
            if !matches!(target, crate::config::types::ThinkingFormat::Passthrough) {
                crate::handlers::thinking_transformer::transform_response_body(
                    &mut response_body,
                    target,
                );
            }
        }

        // 在记录日志前检查并轮换数据库
        let app_state_clone = app_state.clone();
        let headers_clone = headers.clone();
        let payload_clone = payload.clone();
        let request_body_clone = request_body.clone();
        let response_body_clone = response_body.clone();
        let client_ip = get_client_ip(headers, addr);
        let log_backend = backend.clone();
        let log_endpoint = api_endpoint.to_string();
        let log_status = status.as_u16() as i64;
        let log_latency_ms = request_elapsed * 1000.0;

        tokio::spawn(async move {
            log_non_streaming_request(
                &app_state_clone,
                &headers_clone,
                &payload_clone,
                &request_body_clone,
                &response_body_clone,
                client_ip,
                crate::db::records::LogMeta {
                    latency_ms: Some(log_latency_ms),
                    status: Some(log_status),
                    backend: Some(log_backend),
                    endpoint: Some(log_endpoint),
                    ..Default::default()
                },
            )
            .await;
        });

        if let Some(usage) = response_body.get("usage") {
            let completion = usage
                .get("completion_tokens")
                .and_then(|v| v.as_u64())
                .or_else(|| usage.get("output_tokens").and_then(|v| v.as_u64()));
            let prompt = usage
                .get("prompt_tokens")
                .and_then(|v| v.as_u64())
                .or_else(|| usage.get("input_tokens").and_then(|v| v.as_u64()));
            if let (Some(completion), Some(prompt)) = (completion, prompt) {
                let endpoint = api_endpoint.to_string();
                let model_clone = model.clone();
                let backend_clone = backend.clone();
                let elapsed = request_elapsed;

                if let Some(sender) = get_metrics_sender() {
                    let event = MetricEvent {
                        endpoint,
                        status: status.as_u16().to_string(),
                        model: model_clone,
                        backend: backend_clone,
                        latency: elapsed,
                        is_success: true,
                        completion_tokens: Some(completion),
                        prompt_tokens: Some(prompt),
                        elapsed: Some(elapsed),
                    };
                    let _ = sender.try_send(event);
                }
            }
        }
    }

    // Create new response from JSON body
    let mut resp = Json(response_body).into_response();

    // Json extractor 默认设 200，这里手动把上游真实 status 设回去。
    *resp.status_mut() = status;

    if let Some(msg) = error_msg {
        let log_body = serde_json::to_string(&truncate_json(request_body)).unwrap_or_default();
        resp.extensions_mut().insert(AccessLogMeta {
            model: "-".to_string(),
            backend: "unknown".to_string(), // Placeholder, will be updated by handle_request_logic
            error: Some(msg),
            request_body: Some(log_body),
        });
    }

    Ok(resp)
}
