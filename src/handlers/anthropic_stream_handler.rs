use crate::app_error::AppError;
use crate::config::types::ClientConfig;
use crate::db::records::log_non_streaming_request;
use crate::handlers::stream_handler::extract_error_msg;
use crate::handlers::utils::{log_stream_interruption, truncate_json};
use crate::metrics::middleware::get_metrics_sender;
use crate::metrics::prometheus::{TTFT, TTFT_10M_MAX, TTFT_1H_MAX, TTFT_1M_MAX};
use crate::metrics::sliding_window;
use crate::metrics::worker::MetricEvent;
use crate::models::requests::RequestPayload;
use crate::models::AccessLogMeta;
use crate::state::app_state::AppState;
use axum::{
    http::HeaderMap,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse, Response,
    },
    Json,
};
use eventsource_stream::Eventsource;
use futures::stream::StreamExt;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::error;

/// Accumulates Anthropic SSE stream events into a final JSON response for audit logging.
#[derive(Debug)]
struct AnthropicStreamAccumulator {
    message_id: Option<String>,
    role: String,
    model: Option<String>,
    content_blocks: BTreeMap<u64, Value>,
    usage: Option<Value>,
    stop_reason: Option<String>,
    stop_sequence: Option<Value>,
}

impl AnthropicStreamAccumulator {
    fn new() -> Self {
        Self {
            message_id: None,
            role: "assistant".to_string(),
            model: None,
            content_blocks: BTreeMap::new(),
            usage: None,
            stop_reason: None,
            stop_sequence: None,
        }
    }

    fn handle_message_start(&mut self, data: &Value) {
        if let Some(msg) = data.get("message") {
            self.message_id = msg.get("id").and_then(|v| v.as_str()).map(String::from);
            self.model = msg.get("model").and_then(|v| v.as_str()).map(String::from);
            self.usage = msg.get("usage").cloned();
        }
    }

    fn handle_content_block_start(&mut self, data: &Value) {
        let index = data.get("index").and_then(|v| v.as_u64()).unwrap_or(0);
        let content_block = data.get("content_block").cloned().unwrap_or(json!({}));
        self.content_blocks.insert(index, content_block);
    }

    fn handle_content_block_delta(&mut self, data: &Value) {
        let index = data.get("index").and_then(|v| v.as_u64()).unwrap_or(0);
        let delta = match data.get("delta") {
            Some(d) => d,
            None => return,
        };
        let delta_type = delta.get("type").and_then(|v| v.as_str()).unwrap_or("");

        let block = self
            .content_blocks
            .entry(index)
            .or_insert_with(|| json!({}));

        match delta_type {
            "thinking_delta" => {
                if let Some(thinking) = delta.get("thinking").and_then(|v| v.as_str()) {
                    let existing = block.get("thinking").and_then(|v| v.as_str()).unwrap_or("");
                    block["thinking"] = json!(format!("{}{}", existing, thinking));
                }
            }
            "text_delta" => {
                if let Some(text) = delta.get("text").and_then(|v| v.as_str()) {
                    let existing = block.get("text").and_then(|v| v.as_str()).unwrap_or("");
                    block["text"] = json!(format!("{}{}", existing, text));
                }
            }
            "signature_delta" => {
                if let Some(sig) = delta.get("signature").and_then(|v| v.as_str()) {
                    block["signature"] = json!(sig);
                }
            }
            "input_json_delta" => {
                if let Some(partial) = delta.get("partial_json").and_then(|v| v.as_str()) {
                    let existing = block
                        .get("partial_json")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    block["partial_json"] = json!(format!("{}{}", existing, partial));
                }
            }
            _ => {}
        }
    }

    fn handle_message_delta(&mut self, data: &Value) {
        if let Some(delta) = data.get("delta") {
            self.stop_reason = delta
                .get("stop_reason")
                .and_then(|v| v.as_str())
                .map(String::from);
            self.stop_sequence = delta.get("stop_sequence").cloned();
        }
        if let Some(usage) = data.get("usage").cloned() {
            self.usage = Some(usage);
        }
    }

    fn to_final_response_json(&self) -> Value {
        let content: Vec<Value> = self.content_blocks.values().cloned().collect();

        json!({
            "id": self.message_id.as_deref().unwrap_or("unknown"),
            "type": "message",
            "role": self.role,
            "content": content,
            "model": self.model.as_deref().unwrap_or("unknown"),
            "stop_reason": self.stop_reason,
            "stop_sequence": self.stop_sequence,
            "usage": self.usage,
        })
    }
}

impl Default for AnthropicStreamAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

async fn anthropic_stream_logger_task(
    mut rx: mpsc::UnboundedReceiver<String>,
    app_state: Arc<AppState>,
    headers: HeaderMap,
    payload: RequestPayload,
    request_body: Value,
    client_ip: String,
    model: String,
    backend: String,
    start_time: Instant,
    endpoint: String,
    status: String,
) {
    let mut accumulator = AnthropicStreamAccumulator::new();
    let mut _ttft_recorded = false;
    let mut ttft_secs: Option<f64> = None;

    while let Some(chunk_str) = rx.recv().await {
        if !_ttft_recorded {
            _ttft_recorded = true;
            let ttft = start_time.elapsed().as_secs_f64();
            ttft_secs = Some(ttft);
            TTFT.with_label_values(&[&model, &backend]).observe(ttft);
            sliding_window::update_ttft_windows(ttft, &model, &backend);
            TTFT_1M_MAX
                .with_label_values(&[&model, &backend])
                .set(sliding_window::get_ttft_1m_max(&model, &backend));
            TTFT_10M_MAX
                .with_label_values(&[&model, &backend])
                .set(sliding_window::get_ttft_10m_max(&model, &backend));
            TTFT_1H_MAX
                .with_label_values(&[&model, &backend])
                .set(sliding_window::get_ttft_1h_max(&model, &backend));
        }

        let chunk: Value = match serde_json::from_str(&chunk_str) {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "Failed to deserialize chunk in anthropic stream logger task: {}",
                    e
                );
                continue;
            }
        };

        let event_type = chunk.get("type").and_then(|t| t.as_str()).unwrap_or("");

        match event_type {
            "message_start" => accumulator.handle_message_start(&chunk),
            "content_block_start" => accumulator.handle_content_block_start(&chunk),
            "content_block_delta" => accumulator.handle_content_block_delta(&chunk),
            "content_block_stop" => {
                // No special handling needed; block accumulation is already complete
            }
            "message_delta" => accumulator.handle_message_delta(&chunk),
            "message_stop" => {
                // Final event; break after processing to exit loop
                break;
            }
            _ => {}
        }
    }

    let (completion_tokens, prompt_tokens) = accumulator
        .usage
        .as_ref()
        .map(|u| {
            let comp = u
                .get("completion_tokens")
                .and_then(|v| v.as_u64())
                .or_else(|| u.get("output_tokens").and_then(|v| v.as_u64()));
            let prompt = u
                .get("prompt_tokens")
                .and_then(|v| v.as_u64())
                .or_else(|| u.get("input_tokens").and_then(|v| v.as_u64()));
            (comp, prompt)
        })
        .unwrap_or((None, None));

    // Log the final response (or a minimal fallback if truncated stream)
    let final_resp = accumulator.to_final_response_json();

    // Check if we received at least one valid content block or message metadata
    let has_content = !accumulator.content_blocks.is_empty()
        || accumulator.message_id.is_some()
        || accumulator.model.is_some();

    if has_content {
        let log_latency_ms = start_time.elapsed().as_secs_f64() * 1000.0;
        log_non_streaming_request(
            &app_state,
            &headers,
            &payload,
            &request_body,
            &final_resp,
            client_ip,
            crate::db::records::LogMeta {
                latency_ms: Some(log_latency_ms),
                ttft_ms: ttft_secs.map(|s| s * 1000.0),
                status: status.parse().ok(),
                backend: Some(backend.clone()),
                endpoint: Some(endpoint.clone()),
                ..Default::default()
            },
        )
        .await;
    } else {
        // Truncated stream - build minimal fallback response
        let fallback = json!({
            "id": "unknown",
            "type": "message",
            "role": "assistant",
            "content": [],
            "model": "unknown",
            "stop_reason": null,
            "stop_sequence": null,
            "usage": {
                "input_tokens": 0,
                "output_tokens": 0
            }
        });
        let log_latency_ms = start_time.elapsed().as_secs_f64() * 1000.0;
        log_non_streaming_request(
            &app_state,
            &headers,
            &payload,
            &request_body,
            &fallback,
            client_ip,
            crate::db::records::LogMeta {
                latency_ms: Some(log_latency_ms),
                ttft_ms: ttft_secs.map(|s| s * 1000.0),
                status: status.parse().ok(),
                backend: Some(backend.clone()),
                endpoint: Some(endpoint.clone()),
                ..Default::default()
            },
        )
        .await;
    }

    let total_elapsed = start_time.elapsed().as_secs_f64();
    if let Some(sender) = get_metrics_sender() {
        let event = MetricEvent {
            endpoint,
            status,
            model: model.clone(),
            backend: backend.clone(),
            latency: total_elapsed,
            is_success: true,
            completion_tokens,
            prompt_tokens,
            elapsed: Some(total_elapsed),
        };
        let _ = sender.try_send(event);
    }
}

pub async fn process_anthropic_streaming_response(
    app_state: Arc<AppState>,
    headers: HeaderMap,
    payload: RequestPayload,
    client_ip: String,
    response: reqwest::Response,
    client_config: &ClientConfig,
    request_body: &Value,
) -> Result<Response, AppError> {
    // Error status early-return: identical logic to chat streaming handler
    if !response.status().is_success() {
        let status = response.status();
        let body_bytes = response.bytes().await?;

        // Use simd-json for error parsing
        let mut buf = body_bytes.to_vec();
        let body_json: Value = simd_json::from_slice(&mut buf).unwrap_or_else(|_| {
            json!({
                "error": String::from_utf8_lossy(&body_bytes).to_string(),
                "error_type": "upstream_error"
            })
        });

        let error_msg = extract_error_msg(&body_json);

        let mut resp = (status, Json(body_json)).into_response();

        if let Some(msg) = error_msg {
            let log_body = serde_json::to_string(&truncate_json(request_body)).unwrap_or_default();
            resp.extensions_mut().insert(AccessLogMeta {
                model: "-".to_string(),
                backend: "unknown".to_string(),
                error: Some(msg),
                request_body: Some(log_body),
            });
        }

        return Ok(resp);
    }

    let stream = response.bytes_stream();

    // Setup logger channel
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let app_state_clone = app_state.clone();
    let headers_clone = headers.clone();
    let payload_clone = payload.clone();
    let request_body_clone = request_body.clone();
    let client_ip_clone = client_ip.clone();
    let model_name = payload.get_model().to_string();
    let backend_name = client_config.name.clone();
    let model_for_error = model_name.clone();
    let backend_for_error = backend_name.clone();
    let stream_start_time = Instant::now();

    // Spawn logger task
    tokio::spawn(async move {
        anthropic_stream_logger_task(
            rx,
            app_state_clone,
            headers_clone,
            payload_clone,
            request_body_clone,
            client_ip_clone,
            model_name,
            backend_name,
            stream_start_time,
            "/v1/messages".to_string(),
            "200".to_string(),
        )
        .await;
    });

    // SSE forwarding: preserve event type, forward data verbatim
    let sse_stream = stream.eventsource().map(move |result| {
        match result {
            Ok(event) => {
                // Send data to logger channel (best-effort, ignore send errors)
                let _ = tx.send(event.data.clone());

                // Preserve the SSE event type field
                let mut out = Event::default().data(event.data);
                if !event.event.is_empty() {
                    out = out.event(event.event);
                }
                Ok::<_, std::io::Error>(out)
            }
            Err(e) => {
                log_stream_interruption(
                    &client_ip,
                    "/v1/messages",
                    &model_for_error,
                    &backend_for_error,
                    &e.to_string(),
                );
                // Anthropic Messages API 标准: event:error + {"type":"error","error":{"type":"api_error","message":"..."}}
                let err_data = json!({
                    "type": "error",
                    "error": {
                        "type": "api_error",
                        "message": format!("upstream stream interrupted: {}", e)
                    }
                })
                .to_string();
                Ok::<_, std::io::Error>(Event::default().event("error").data(err_data))
            }
        }
    });

    // Create SSE response
    let mut response = Sse::new(sse_stream)
        .keep_alive(KeepAlive::default())
        .into_response();

    // Inject AccessLogMeta
    response.extensions_mut().insert(AccessLogMeta {
        model: payload.get_model().to_string(),
        backend: client_config.name.clone(),
        error: None,
        request_body: None,
    });

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_accumulator_full_thinking_stream_sequence() {
        let mut acc = AnthropicStreamAccumulator::new();

        // message_start
        acc.handle_message_start(&json!({
            "type": "message_start",
            "message": {
                "id": "msg_123",
                "model": "claude-sonnet-4-20250514",
                "usage": {"input_tokens": 50, "output_tokens": 0}
            }
        }));

        // content_block_start (thinking block at index 0)
        acc.handle_content_block_start(&json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "thinking", "thinking": ""}
        }));

        // content_block_delta (thinking_delta chunk 1)
        acc.handle_content_block_delta(&json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": "Let me think about this"}
        }));

        // content_block_delta (thinking_delta chunk 2)
        acc.handle_content_block_delta(&json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "thinking_delta", "thinking": " more carefully"}
        }));

        // content_block_delta (signature_delta)
        acc.handle_content_block_delta(&json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "signature_delta", "signature": "abc123signature"}
        }));

        // message_delta
        acc.handle_message_delta(&json!({
            "type": "message_delta",
            "delta": {"stop_reason": "end_turn", "stop_sequence": null},
            "usage": {"output_tokens": 123}
        }));

        let result = acc.to_final_response_json();

        // Verify id and model
        assert_eq!(result["id"], "msg_123");
        assert_eq!(result["model"], "claude-sonnet-4-20250514");

        // Verify thinking content is accumulated correctly
        assert_eq!(
            result["content"][0]["thinking"],
            "Let me think about this more carefully"
        );

        // Verify signature
        assert_eq!(result["content"][0]["signature"], "abc123signature");

        // Verify stop_reason
        assert_eq!(result["stop_reason"], "end_turn");

        // Verify usage output_tokens
        assert_eq!(result["usage"]["output_tokens"], 123);
    }

    #[test]
    fn test_accumulator_text_delta_accumulates() {
        let mut acc = AnthropicStreamAccumulator::new();

        // content_block_start (text block at index 0)
        acc.handle_content_block_start(&json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }));

        // text_delta chunk 1
        acc.handle_content_block_delta(&json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Hello "}
        }));

        // text_delta chunk 2
        acc.handle_content_block_delta(&json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "World!"}
        }));

        let result = acc.to_final_response_json();

        // Verify text is accumulated correctly
        assert_eq!(result["content"][0]["text"], "Hello World!");
    }

    #[test]
    fn test_accumulator_multiple_content_blocks_ordered_by_index() {
        let mut acc = AnthropicStreamAccumulator::new();

        // content_block_start at index 1 (thinking)
        acc.handle_content_block_start(&json!({
            "type": "content_block_start",
            "index": 1,
            "content_block": {"type": "thinking", "thinking": ""}
        }));

        // content_block_start at index 0 (text) - added second but has lower index
        acc.handle_content_block_start(&json!({
            "type": "content_block_start",
            "index": 0,
            "content_block": {"type": "text", "text": ""}
        }));

        // Add text to index 0
        acc.handle_content_block_delta(&json!({
            "type": "content_block_delta",
            "index": 0,
            "delta": {"type": "text_delta", "text": "Answer"}
        }));

        // Add thinking to index 1
        acc.handle_content_block_delta(&json!({
            "type": "content_block_delta",
            "index": 1,
            "delta": {"type": "thinking_delta", "thinking": "Reasoning"}
        }));

        let result = acc.to_final_response_json();

        // Verify content array is ordered by index (0 before 1)
        assert_eq!(result["content"].as_array().unwrap().len(), 2);
        assert_eq!(result["content"][0]["type"], "text");
        assert_eq!(result["content"][0]["text"], "Answer");
        assert_eq!(result["content"][1]["type"], "thinking");
        assert_eq!(result["content"][1]["thinking"], "Reasoning");
    }
}
