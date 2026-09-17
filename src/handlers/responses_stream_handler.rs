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
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::error;

async fn responses_stream_logger_task(
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
    let mut final_response: Option<Value> = None;
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

        // Try to parse the chunk
        let chunk: Value = match serde_json::from_str(&chunk_str) {
            Ok(v) => v,
            Err(e) => {
                error!(
                    "Failed to deserialize chunk in responses logger task: {}",
                    e
                );
                continue;
            }
        };

        // Watch for response.completed event to capture final response
        if chunk.get("type").and_then(|t| t.as_str()) == Some("response.completed") {
            if let Some(response_obj) = chunk.get("response") {
                final_response = Some(response_obj.clone());
            }
        }
    }

    let (completion_tokens, prompt_tokens) = final_response
        .as_ref()
        .and_then(|r| r.get("usage"))
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
    let log_latency_ms = start_time.elapsed().as_secs_f64() * 1000.0;
    if let Some(ref final_resp) = final_response {
        log_non_streaming_request(
            &app_state,
            &headers,
            &payload,
            &request_body,
            final_resp,
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
            "object": "response",
            "output": [],
            "usage": {
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0
            }
        });
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

pub async fn process_responses_streaming_response(
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
        responses_stream_logger_task(
            rx,
            app_state_clone,
            headers_clone,
            payload_clone,
            request_body_clone,
            client_ip_clone,
            model_name,
            backend_name,
            stream_start_time,
            "/v1/responses".to_string(),
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
                    "/v1/responses",
                    &model_for_error,
                    &backend_for_error,
                    &e.to_string(),
                );
                // Responses API 标准: type:"error" typed event 格式
                let err_data = json!({
                    "type": "error",
                    "code": "upstream_error",
                    "message": format!("upstream stream interrupted: {}", e),
                    "param": null
                })
                .to_string();
                return Ok::<_, std::io::Error>(Event::default().event("error").data(err_data));
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
