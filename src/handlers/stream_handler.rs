use crate::app_error::AppError;
use crate::config::types::ClientConfig;
use crate::db::records::log_non_streaming_request;
use crate::handlers::utils::truncate_json;
use crate::metrics::middleware::get_metrics_sender;
use crate::metrics::prometheus::{
    TOKENS_TOTAL, TPS, TPS_10M_AVG, TPS_1H_AVG, TPS_1M_AVG, TTFT, TTFT_10M_MAX, TTFT_1H_MAX,
    TTFT_1M_MAX,
};
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
use eventsource_stream::{Event as UpstreamEvent, Eventsource};
use futures::stream::{self, Stream, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::sync::mpsc;
use tracing::error;

struct ToolCallAccumulator {
    index: u64,
    id: Option<String>,
    r#type: Option<String>,
    name: Option<String>,
    arguments: String,
}

struct StreamAccumulator {
    content: String,
    reasoning: String,
    reasoning_content: String,
    role: String,
    tool_calls: HashMap<u64, ToolCallAccumulator>,
}

impl StreamAccumulator {
    fn new() -> Self {
        Self {
            content: String::new(),
            reasoning: String::new(),
            reasoning_content: String::new(),
            role: "assistant".to_string(),
            tool_calls: HashMap::new(),
        }
    }

    fn update(&mut self, delta: &Value) {
        if let Some(r) = delta.get("role").and_then(|s| s.as_str()) {
            self.role = r.to_string();
        }
        if let Some(c) = delta.get("content").and_then(|s| s.as_str()) {
            self.content.push_str(c);
        }
        if let Some(r) = delta.get("reasoning").and_then(|s| s.as_str()) {
            self.reasoning.push_str(r);
        }
        if let Some(rc) = delta.get("reasoning_content").and_then(|s| s.as_str()) {
            self.reasoning_content.push_str(rc);
        }
        if let Some(tcs) = delta.get("tool_calls").and_then(|v| v.as_array()) {
            for tc in tcs {
                if let Some(index) = tc.get("index").and_then(|i| i.as_u64()) {
                    let acc = self.tool_calls.entry(index).or_insert(ToolCallAccumulator {
                        index,
                        id: None,
                        r#type: None,
                        name: None,
                        arguments: String::new(),
                    });
                    if let Some(id) = tc.get("id").and_then(|s| s.as_str()) {
                        acc.id = Some(id.to_string());
                    }
                    if let Some(t) = tc.get("type").and_then(|s| s.as_str()) {
                        acc.r#type = Some(t.to_string());
                    }
                    if let Some(func) = tc.get("function") {
                        if let Some(name) = func.get("name").and_then(|s| s.as_str()) {
                            acc.name = Some(name.to_string());
                        }
                        if let Some(args) = func.get("arguments").and_then(|s| s.as_str()) {
                            acc.arguments.push_str(args);
                        }
                    }
                }
            }
        }
    }

    fn to_message_json(&self) -> Value {
        let mut msg = json!({ "role": self.role });
        if !self.content.is_empty() {
            msg["content"] = json!(self.content);
        }
        if !self.reasoning.is_empty() {
            msg["reasoning"] = json!(self.reasoning);
        }
        if !self.reasoning_content.is_empty() {
            msg["reasoning_content"] = json!(self.reasoning_content);
        }
        if !self.tool_calls.is_empty() {
            let mut tcs: Vec<_> = self.tool_calls.values().collect();
            tcs.sort_by_key(|tc| tc.index);
            let tool_calls_json: Vec<Value> = tcs
                .iter()
                .map(|tc| {
                    json!({
                        "id": tc.id,
                        "type": tc.r#type.clone().unwrap_or_else(|| "function".to_string()),
                        "function": { "name": tc.name, "arguments": tc.arguments }
                    })
                })
                .collect();
            msg["tool_calls"] = json!(tool_calls_json);
        }
        msg
    }
}

async fn stream_logger_task(
    mut rx: mpsc::UnboundedReceiver<String>,
    app_state: Arc<AppState>,
    headers: HeaderMap,
    payload: RequestPayload,
    request_body: Value,
    client_ip: String,
    is_chat: bool,
    model: String,
    backend: String,
    start_time: Instant,
    endpoint: String,
    status: String,
) {
    let mut accumulator = StreamAccumulator::new();
    let mut last_chunk: Option<Value> = None;
    let mut first_chunk: Option<Value> = None;
    let mut captured_usage: Option<Value> = None;
    let mut captured_finish_reason: Option<String> = None;
    let mut _ttft_recorded = false;

    while let Some(chunk_str) = rx.recv().await {
        // Deserialize in background task
        let chunk: Value = match serde_json::from_str(&chunk_str) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to deserialize chunk in logger task: {}", e);
                continue;
            }
        };

        if first_chunk.is_none() {
            first_chunk = Some(chunk.clone());
            // Record TTFT on first chunk with any delta (not just non-empty content)
            if let Some(choices) = chunk.get("choices").and_then(|c| c.as_array()) {
                if let Some(choice) = choices.first() {
                    // Record TTFT on first delta - whether content is empty or not
                    if is_chat && choice.get("delta").is_some() {
                        let ttft = start_time.elapsed().as_secs_f64();
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
                        _ttft_recorded = true;
                    } else if !is_chat && choice.get("text").is_some() {
                        let ttft = start_time.elapsed().as_secs_f64();
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
                        _ttft_recorded = true;
                    }
                }
            }
        }
        last_chunk = Some(chunk.clone());

        // Capture Usage (often in the last chunk)
        if let Some(u) = chunk.get("usage") {
            captured_usage = Some(u.clone());
            // Record TPS when we have usage info
            if let (Some(completion), Some(prompt)) = (
                u.get("completion_tokens").and_then(|v| v.as_u64()),
                u.get("prompt_tokens").and_then(|v| v.as_u64()),
            ) {
                let elapsed = start_time.elapsed().as_secs_f64();
                if completion > 0 && elapsed > 0.0 {
                    let tps = completion as f64 / elapsed;
                    TPS.with_label_values(&[&model, &backend]).set(tps);
                    sliding_window::update_tps_windows(tps, &model, &backend);
                    TPS_1M_AVG
                        .with_label_values(&[&model, &backend])
                        .set(sliding_window::get_tps_1m_avg(&model, &backend));
                    TPS_10M_AVG
                        .with_label_values(&[&model, &backend])
                        .set(sliding_window::get_tps_10m_avg(&model, &backend));
                    TPS_1H_AVG
                        .with_label_values(&[&model, &backend])
                        .set(sliding_window::get_tps_1h_avg(&model, &backend));
                }
                TOKENS_TOTAL
                    .with_label_values(&[&model, "completion"])
                    .inc_by(completion as f64);
                TOKENS_TOTAL
                    .with_label_values(&[&model, "prompt"])
                    .inc_by(prompt as f64);
            }
        }

        if let Some(choices) = chunk.get("choices").and_then(|c| c.as_array()) {
            if let Some(choice) = choices.first() {
                // Capture Finish Reason
                if let Some(fr) = choice.get("finish_reason").and_then(|s| s.as_str()) {
                    captured_finish_reason = Some(fr.to_string());
                }

                if is_chat {
                    if let Some(delta) = choice.get("delta") {
                        accumulator.update(delta);
                    }
                } else if let Some(text) = choice.get("text").and_then(|s| s.as_str()) {
                    accumulator.content.push_str(text);
                }
            }
        }
    }

    if let Some(mut final_chunk) = last_chunk {
        // If it's empty (e.g. only one chunk), fallback to first_chunk or mock
        if final_chunk.get("choices").is_none() {
            if let Some(first) = first_chunk {
                final_chunk = first;
            }
        }

        // Ensure 'choices' exists AND is not empty
        let choices_valid = final_chunk
            .get("choices")
            .and_then(|c| c.as_array())
            .map(|arr| !arr.is_empty())
            .unwrap_or(false);

        if !choices_valid {
            final_chunk["choices"] = json!([{ "index": 0 }]);
        }

        // Inject accumulated content
        if is_chat {
            final_chunk["choices"][0]["message"] = accumulator.to_message_json();
            if let Some(choice) = final_chunk["choices"][0].as_object_mut() {
                choice.remove("delta");
            }
        } else {
            final_chunk["choices"][0]["text"] = json!(accumulator.content);
        }

        // Inject captured Usage and Finish Reason
        if let Some(ref u) = captured_usage {
            final_chunk["usage"] = u.clone();
        }
        if let Some(fr) = captured_finish_reason {
            if let Some(choice) = final_chunk["choices"][0].as_object_mut() {
                choice.insert("finish_reason".to_string(), json!(fr));
            }
        }

        log_non_streaming_request(
            &app_state,
            &headers,
            &payload,
            &request_body,
            &final_chunk,
            client_ip,
        )
        .await;
    }

    // 发送完整流式延迟指标
    let total_elapsed = start_time.elapsed().as_secs_f64();

    // 提取 usage 信息
    let (completion_tokens, prompt_tokens) = captured_usage
        .as_ref()
        .map(|u| {
            let comp = u.get("completion_tokens").and_then(|v| v.as_u64());
            let prompt = u.get("prompt_tokens").and_then(|v| v.as_u64());
            (comp, prompt)
        })
        .unwrap_or((None, None));

    // 发送指标事件到 worker
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

/// 无分配预扫描，识别 `"choices":[]`（容错冒号/括号后空白）。
/// 让快速通道对正常 chunk 保持零拷贝，仅在命中时才付出解析成本。
fn has_empty_choices_marker(data: &str) -> bool {
    const KEY: &str = "\"choices\"";
    let mut search = data;
    while let Some(pos) = search.find(KEY) {
        let after = search[pos + KEY.len()..].trim_start();
        if let Some(after) = after.strip_prefix(':') {
            let after = after.trim_start();
            if let Some(after) = after.strip_prefix('[') {
                if after.trim_start().starts_with(']') {
                    return true;
                }
            }
        }
        search = &search[pos + KEY.len()..];
    }
    false
}

/// 为 vLLM 结尾的 `"choices":[]` usage chunk 注入占位 choice，
/// 使严格解析 `choices[0]` 的下游（如 OpenWebUI）不越界崩溃，且不丢 usage。
/// 仅改动"存在但为空"的数组；缺失或非空时不动。返回是否注入。
fn ensure_non_empty_choices(value: &mut Value, is_chat: bool) -> bool {
    let is_empty_array = value
        .get("choices")
        .and_then(|c| c.as_array())
        .map(|arr| arr.is_empty())
        .unwrap_or(false);
    if !is_empty_array {
        return false;
    }
    let placeholder = if is_chat {
        json!({ "index": 0, "delta": {}, "finish_reason": null })
    } else {
        json!({ "index": 0, "text": "", "finish_reason": null })
    };
    value["choices"] = json!([placeholder]);
    true
}

/// 在 SSE 流自然结束时补发 `[DONE]` 哨兵，防止客户端永远收不到流结束标记。
///
/// 背景：`eventsource-stream` 在以下两种情况下会**静默**丢弃最后一个不完整的 SSE 事件，
/// 不会发 Err 也不会发 events：
/// 1. 上游发送 `data: ...\n`（单换行）后关闭连接（缺少结尾的 `\n\n` 分隔符）。
/// 2. 上游 TCP 连接在写出最后一个 chunk 后被对端关闭，但 buffer 中仍有未解析的残余字节。
///
/// 这两种情况下 `eventsource-stream` 直接返回 `Poll::Ready(None)`，
/// `process_streaming_response` 的 flat_map 不会收到任何新的项，因此也不会
/// 触发现有的 `[DONE]` 发射路径 —— 客户端只能通过流量结束判断关闭，但 OpenAI 兼容
/// 客户端（如 OpenAI Python SDK）会因此在 `for chunk in stream` 处永久阻塞。
///
/// 行为约定：
/// - 上游已显式发送过 `[DONE]` → 直接结束（避免重复）
/// - 上游触发了错误解析路径（Err 分支已发 error chunk）→ 直接结束（保留原"错误不再发 [DONE]"语义）
/// - 上游流自然结束但未发 `[DONE]` → 补发一个
struct DoneSentinel {
    flag: Arc<AtomicBool>,
    emitted: bool,
}

impl Stream for DoneSentinel {
    type Item = Result<Event, std::io::Error>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.emitted {
            return Poll::Ready(None);
        }
        self.emitted = true;
        if self.flag.load(Ordering::Relaxed) {
            // 上游已发 [DONE]（Ok 分支）或已发 error chunk（Err 分支），无需补发
            Poll::Ready(None)
        } else {
            // 兜底：流自然结束但未发 [DONE]，补发以让客户端能正常退出
            Poll::Ready(Some(Ok(Event::default().data("[DONE]"))))
        }
    }
}

/// Forwards an upstream stream until its first error, then becomes terminal.
///
/// The upstream body must not be polled again after a transport/decode error:
/// some body implementations keep returning the same error, which otherwise
/// turns one failed request into a tight log and SSE error loop.
struct TerminateOnError<S> {
    inner: Pin<Box<S>>,
    terminated: bool,
}

impl<S> TerminateOnError<S> {
    fn new(inner: S) -> Self {
        Self {
            inner: Box::pin(inner),
            terminated: false,
        }
    }
}

impl<S, T, E> Stream for TerminateOnError<S>
where
    S: Stream<Item = Result<T, E>>,
{
    type Item = Result<T, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        let next = self.inner.as_mut().poll_next(cx);
        match next {
            Poll::Ready(Some(Err(error))) => {
                self.terminated = true;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                self.terminated = true;
                Poll::Ready(None)
            }
            other => other,
        }
    }
}

pub async fn process_streaming_response(
    app_state: Arc<AppState>,
    headers: HeaderMap,
    payload: RequestPayload,
    client_ip: String,
    response: reqwest::Response,
    client_config: &ClientConfig,
    is_chat: bool,
    request_body: &Value,
) -> Result<Response, AppError> {
    // 如果状态码不成功，直接作为普通 JSON 返回，不要包装成 SSE
    if !response.status().is_success() {
        let status = response.status();
        let body_bytes = response.bytes().await?;

        // 使用 simd-json 解析错误响应
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
    let special_prefix = client_config.special_prefix.clone().unwrap_or_default();
    let mut prefix_applied = false;

    // Resolve thinking format once for the streaming path
    let thinking_target = if is_chat {
        let config = app_state.config_manager.get_config_guard().await;
        config.resolve_thinking_format(client_config)
    } else {
        crate::config::types::ThinkingFormat::Passthrough
    };

    // Setup logger channel (String instead of Value)
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let app_state_clone = app_state.clone();
    let headers_clone = headers.clone();
    let payload_clone = payload.clone();
    let request_body_clone = request_body.clone();
    let client_ip_clone = client_ip.clone();

    // Extract model from payload
    let model_name = match &payload {
        RequestPayload::Chat(c) => c.model.clone(),
        RequestPayload::Completion(c) => c.model.clone(),
        RequestPayload::Embedding(e) => e.model.clone(),
        RequestPayload::Rerank(_) => "rerank".to_string(),
        RequestPayload::Score(_) => "score".to_string(),
        RequestPayload::Classify(_) => "classify".to_string(),
        RequestPayload::Responses(c) => c.model.clone(),
        RequestPayload::AnthropicMessages(c) => c.model.clone(),
    };
    let backend = client_config.name.clone();

    // Clone for AccessLogMeta (before they're moved into async block)
    let model_for_meta = model_name.clone();
    let backend_for_meta = backend.clone();
    let stream_start_time = Instant::now();

    tokio::spawn(async move {
        stream_logger_task(
            rx,
            app_state_clone,
            headers_clone,
            payload_clone,
            request_body_clone,
            client_ip_clone,
            is_chat,
            model_name,
            backend,
            stream_start_time,
            (if is_chat {
                "/v1/chat/completions"
            } else {
                "/v1/completions"
            })
            .to_string(),
            "200".to_string(),
        )
        .await;
    });

    let content_json_pointer = if is_chat {
        "/choices/0/delta/content"
    } else {
        "/choices/0/text"
    };

    let mut thinking_transformer =
        crate::handlers::thinking_transformer::ThinkingStreamTransformer::new(thinking_target);
    let apply_thinking = is_chat
        && !matches!(
            thinking_target,
            crate::config::types::ThinkingFormat::Passthrough
        );

    // Fast path: when no thinking transform and no special prefix,
    // skip the simd-json parse→transform→reserialize round-trip entirely.
    let needs_json_roundtrip = apply_thinking || !special_prefix.is_empty();
    let model_for_error = model_for_meta.clone();
    let backend_for_error = backend_for_meta.clone();

    // 共享标志：上游/错误路径需要通知尾巴处的 DoneSentinel 是否已经走过 [DONE]
    // 或 error chunk 发射路径，避免兜底时重复发送或破坏"错误不再发 [DONE]"语义。
    let done_flag = Arc::new(AtomicBool::new(false));
    let done_flag_in_flat_map = done_flag.clone();

    // 使用 eventsource-stream 进行鲁棒的 SSE 解析
    let sse_stream = TerminateOnError::new(stream.eventsource()).flat_map(move |result| {
        match result {
            Ok(event) => {
                if event.data == "[DONE]" {
                    // 上游显式发送 [DONE]：通知尾巴不再补发
                    done_flag_in_flat_map.store(true, Ordering::Relaxed);
                    if apply_thinking {
                        if let Some(flush_delta) = thinking_transformer.finalize() {
                            let flush_chunk =
                                json!({"choices": [{"index": 0, "delta": flush_delta}]});
                            if let Ok(flush_str) = simd_json::to_string(&flush_chunk) {
                                let _ = tx.send(flush_str.clone());
                                let flush_event = Event::default().data(flush_str);
                                return stream::iter(vec![
                                    Ok::<_, std::io::Error>(flush_event),
                                    Ok(Event::default().data("[DONE]")),
                                ]);
                            }
                        }
                    }
                    return stream::iter(vec![Ok(Event::default().data("[DONE]"))]);
                }

                if is_chat && is_chat_completion_ping(&event) {
                    return stream::iter(vec![Ok(Event::default().comment("keepalive"))]);
                }

                if needs_json_roundtrip {
                    // 需要转换：解析 JSON → 注入前缀 / thinking 转换 → 重序列化
                    let mut data_str = event.data;
                    if let Ok(mut value) = unsafe { simd_json::from_str::<Value>(&mut data_str) } {
                        if !prefix_applied && !special_prefix.is_empty() {
                            if let Some(delta_content) = value.pointer_mut(content_json_pointer) {
                                if let Some(s) = delta_content.as_str() {
                                    if !s.is_empty() {
                                        *delta_content = json!(format!("{}{}", special_prefix, s));
                                        prefix_applied = true;
                                    }
                                }
                            }
                        }

                        if apply_thinking {
                            if let Some(delta) = value.pointer_mut("/choices/0/delta") {
                                thinking_transformer.transform_delta(delta);
                            }
                        }

                        ensure_non_empty_choices(&mut value, is_chat);

                        let json_str = simd_json::to_string(&value).unwrap_or_default();
                        let _ = tx.send(json_str.clone());
                        stream::iter(vec![Ok(Event::default().data(json_str))])
                    } else {
                        stream::iter(vec![Ok(Event::default().data(data_str))])
                    }
                } else if has_empty_choices_marker(&event.data) {
                    // Fast path 例外：结尾 usage chunk 的空 choices 需补占位，
                    // 否则严格取下标的下游客户端会越界崩溃。此分支才付出解析成本。
                    let mut data_str = event.data;
                    if let Ok(mut value) = unsafe { simd_json::from_str::<Value>(&mut data_str) } {
                        ensure_non_empty_choices(&mut value, is_chat);
                        let json_str = simd_json::to_string(&value).unwrap_or(data_str);
                        let _ = tx.send(json_str.clone());
                        stream::iter(vec![Ok(Event::default().data(json_str))])
                    } else {
                        let _ = tx.send(data_str.clone());
                        stream::iter(vec![Ok(Event::default().data(data_str))])
                    }
                } else {
                    // Fast path: 无需任何转换，直接透传 event.data
                    let _ = tx.send(event.data.clone());
                    stream::iter(vec![Ok(Event::default().data(event.data))])
                }
            }
            Err(e) => {
                error!(
                    error = %e,
                    error_source = "upstream_sse_stream",
                    backend = %backend_for_error,
                    model = %model_for_error,
                    is_chat = %is_chat,
                    "upstream SSE stream terminated after first error"
                );
                // 错误路径不再发 [DONE]（保持 OpenAI 兼容客户端既有期望），
                // 通知尾巴处的 DoneSentinel 也不要补发。
                done_flag_in_flat_map.store(true, Ordering::Relaxed);
                // OpenAI 标准: 以标准 chat.completion.chunk 格式下发错误，
                // 顶层 error 字段 + choices 带 finish_reason:"error"，不再发 [DONE]。
                let err_chunk = json!({
                    "object": "chat.completion.chunk",
                    "model": model_for_error,
                    "error": {
                        "code": "upstream_error",
                        "message": format!("upstream stream interrupted: {}", e)
                    },
                    "choices": [{
                        "index": 0,
                        "delta": {},
                        "finish_reason": "error"
                    }]
                });
                let err_str = simd_json::to_string(&err_chunk).unwrap_or_else(|_| {
                    "{\"object\":\"chat.completion.chunk\",\"error\":{\"code\":\"upstream_error\",\"message\":\"upstream stream interrupted\"},\"choices\":[{\"index\":0,\"delta\":{},\"finish_reason\":\"error\"}]}".to_string()
                });
                stream::iter(vec![
                    Ok(Event::default().data(err_str)),
                ])
            }
        }
    });

    // 在流尾部连接 DoneSentinel：当上游因 eventsource-stream 静默丢弃最后一个
    // 不完整事件而自然结束（Poll::Ready(None)）时，兜底补发 `[DONE]`，避免
    // 客户端永远收不到流结束标记。上游已显式发过 [DONE] 或触发了错误路径
    // 时，DoneSentinel 看到 flag 后会直接结束，不会重复也不破坏既有语义。
    let appended = sse_stream.chain(DoneSentinel {
        flag: done_flag.clone(),
        emitted: false,
    });

    // Create response and add AccessLogMeta for metrics tracking
    let mut response = Sse::new(appended)
        .keep_alive(KeepAlive::default())
        .into_response();

    response.extensions_mut().insert(AccessLogMeta {
        model: model_for_meta,
        backend: backend_for_meta,
        error: None,
        request_body: None,
    });

    Ok(response)
}

pub fn extract_error_msg(body: &Value) -> Option<String> {
    if let Some(error) = body.get("error") {
        if let Some(msg) = error.get("message").and_then(|v| v.as_str()) {
            return Some(msg.to_string());
        }
        if let Some(msg) = error.as_str() {
            return Some(msg.to_string());
        }
    }
    // 如果是扁平结构 {"error": "msg", ...}
    if let Some(msg) = body.get("error").and_then(|v| v.as_str()) {
        return Some(msg.to_string());
    }
    // 尝试直接把 body 转字符串（如果是简单的 {"error": ...}）
    // 或者如果不包含 error 字段，但 status 错了，就返回 body 的紧凑字符串
    Some(body.to_string())
}

fn is_chat_completion_ping(event: &UpstreamEvent) -> bool {
    if event.event == "ping" {
        return true;
    }

    if !event.data.contains("\"type\"") {
        return false;
    }

    serde_json::from_str::<Value>(&event.data)
        .ok()
        .is_some_and(|value| value.get("type").and_then(Value::as_str) == Some("ping"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn marker_detects_empty_choices_variants() {
        assert!(has_empty_choices_marker(r#"{"choices":[],"usage":{}}"#));
        assert!(has_empty_choices_marker(r#"{"choices": [ ] }"#));
        assert!(has_empty_choices_marker(r#"{"usage":{},"choices":[]}"#));
    }

    #[test]
    fn marker_ignores_non_empty_and_absent_choices() {
        assert!(!has_empty_choices_marker(
            r#"{"choices":[{"index":0,"delta":{"content":"hi"}}]}"#
        ));
        assert!(!has_empty_choices_marker(r#"{"usage":{}}"#));
        assert!(!has_empty_choices_marker(r#"{"choices":[{}],"note":"[]"}"#));
    }

    #[test]
    fn chat_empty_choices_gets_placeholder() {
        let mut v = json!({ "choices": [], "usage": { "total_tokens": 5 } });
        assert!(ensure_non_empty_choices(&mut v, true));
        assert_eq!(v["choices"][0]["index"], json!(0));
        assert!(v["choices"][0]["delta"].is_object());
        assert_eq!(v["usage"]["total_tokens"], json!(5));
    }

    #[test]
    fn completion_empty_choices_gets_text_placeholder() {
        let mut v = json!({ "choices": [] });
        assert!(ensure_non_empty_choices(&mut v, false));
        assert_eq!(v["choices"][0]["text"], json!(""));
    }

    #[test]
    fn non_empty_or_absent_choices_untouched() {
        let mut with = json!({ "choices": [{ "index": 0, "delta": {} }] });
        assert!(!ensure_non_empty_choices(&mut with, true));
        assert_eq!(with["choices"].as_array().unwrap().len(), 1);

        let mut without = json!({ "usage": {} });
        assert!(!ensure_non_empty_choices(&mut without, true));
        assert!(without.get("choices").is_none());
    }

    #[test]
    fn chat_ping_event_is_detected() {
        let event = UpstreamEvent {
            event: "ping".to_string(),
            data: "{}".to_string(),
            ..Default::default()
        };

        assert!(is_chat_completion_ping(&event));
    }

    #[test]
    fn chat_ping_payload_is_detected() {
        let event = UpstreamEvent {
            data: r#"{"type":"ping"}"#.to_string(),
            ..Default::default()
        };

        assert!(is_chat_completion_ping(&event));
    }

    #[test]
    fn normal_choices_and_error_events_are_not_detected_as_ping() {
        let choices = UpstreamEvent {
            data: r#"{"choices":[{"delta":{"content":"hi"}}]}"#.to_string(),
            ..Default::default()
        };
        let error = UpstreamEvent {
            event: "error".to_string(),
            data: r#"{"error":{"message":"upstream"}}"#.to_string(),
            ..Default::default()
        };

        assert!(!is_chat_completion_ping(&choices));
        assert!(!is_chat_completion_ping(&error));
    }

    use futures::executor::block_on;
    use futures::StreamExt;

    fn collect_done_sentinel_count(flag: Arc<AtomicBool>) -> usize {
        let mut s = DoneSentinel {
            flag,
            emitted: false,
        };
        let mut count = 0;
        block_on(async {
            while let Some(item) = s.next().await {
                let _ = item.expect("DoneSentinel never emits Err");
                count += 1;
            }
        });
        count
    }

    #[test]
    fn done_sentinel_emits_done_when_flag_unset() {
        let flag = Arc::new(AtomicBool::new(false));
        assert_eq!(collect_done_sentinel_count(flag), 1);
    }

    #[test]
    fn done_sentinel_skips_when_done_already_seen() {
        let flag = Arc::new(AtomicBool::new(true));
        assert_eq!(collect_done_sentinel_count(flag), 0);
    }

    #[test]
    fn done_sentinel_skips_after_error_path() {
        let flag = Arc::new(AtomicBool::new(true));
        assert_eq!(collect_done_sentinel_count(flag), 0);
    }

    #[test]
    fn done_sentinel_emits_at_most_once() {
        let flag = Arc::new(AtomicBool::new(false));
        let mut s = DoneSentinel {
            flag,
            emitted: false,
        };
        block_on(async {
            let first = s.next().await;
            let second = s.next().await;
            let third = s.next().await;
            assert!(first.is_some(), "first poll must emit");
            assert!(second.is_none(), "second poll must end");
            assert!(third.is_none(), "third poll must end");
        });
    }

    #[test]
    fn terminate_on_error_stops_polling_after_first_error() {
        struct PollCountingStream {
            items: std::collections::VecDeque<Result<i32, &'static str>>,
            polls: Arc<std::sync::atomic::AtomicUsize>,
        }

        impl Stream for PollCountingStream {
            type Item = Result<i32, &'static str>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _cx: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                self.polls.fetch_add(1, Ordering::Relaxed);
                Poll::Ready(self.items.pop_front())
            }
        }

        let polls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source = PollCountingStream {
            items: std::collections::VecDeque::from([Ok(1), Err("first"), Err("second"), Ok(2)]),
            polls: polls.clone(),
        };
        let mut stream = TerminateOnError::new(source);

        let items = block_on(async { stream.by_ref().collect::<Vec<_>>().await });

        assert_eq!(items, vec![Ok(1), Err("first")]);
        assert_eq!(polls.load(Ordering::Relaxed), 2);
        assert!(block_on(async { stream.next().await }).is_none());
        assert_eq!(polls.load(Ordering::Relaxed), 2);
    }
}
