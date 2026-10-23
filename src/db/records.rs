use axum::http::HeaderMap;
use chrono::Local;
use serde_json::Value;
use std::sync::Arc;
use tracing::error;

use crate::db::engine::{DbError, Value as DbValue};
use crate::db::extract::{extract_request, extract_response, header_meta};
use crate::db::store::{opt_int, opt_real, opt_text, AuditStore};
use crate::models::requests::{MessageContent, RequestPayload};
use crate::state::app_state::AppState;

/// Returns a static string label for the request type based on the payload variant.
fn request_type_label(payload: &RequestPayload) -> &'static str {
    match payload {
        RequestPayload::Chat(_) => "chat.completions",
        RequestPayload::Completion(_) => "text_completion",
        RequestPayload::Embedding(_) => "embeddings",
        RequestPayload::Rerank(_) => "rerank",
        RequestPayload::Score(_) => "score",
        RequestPayload::Classify(_) => "classify",
        RequestPayload::Responses(_) => "responses",
        RequestPayload::AnthropicMessages(_) => "anthropic.messages",
    }
}

/// 由调用点提供的运行期审计信息（时延、状态、后端等）。
#[derive(Debug, Default, Clone)]
pub struct LogMeta {
    pub latency_ms: Option<f64>,
    pub ttft_ms: Option<f64>,
    pub upstream_ms: Option<f64>,
    pub stream_ms: Option<f64>,
    pub status: Option<i64>,
    pub backend: Option<String>,
    pub endpoint: Option<String>,
    pub error: Option<String>,
    pub retry_count: Option<i64>,
}

#[derive(Debug, Default)]
pub struct Record {
    pub time: String,
    pub time_ms: i64,
    pub ip: String,
    pub method: Option<String>,
    pub endpoint: Option<String>,
    pub model: String,
    pub r#type: String,
    pub backend: Option<String>,
    pub session_id: Option<String>,
    pub parent_session_id: Option<String>,
    pub request_id: Option<String>,
    pub session_affinity: Option<String>,
    pub user_agent: Option<String>,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub api_key: Option<String>,
    pub status: Option<i64>,
    pub error: Option<String>,
    pub retry_count: Option<i64>,
    pub finish_reason: Option<String>,
    pub latency_ms: Option<f64>,
    pub ttft_ms: Option<f64>,
    pub upstream_ms: Option<f64>,
    pub stream_ms: Option<f64>,
    pub completion_tokens: i32,
    pub prompt_tokens: i32,
    pub total_tokens: i32,
    pub tool: bool,
    pub multimodal: bool,
    pub request_bytes: i64,
    pub response_bytes: i64,
    pub prompt_bytes: i64,
    pub request_tail_bytes: i64,
    pub answer_bytes: i64,
    pub message_count: i64,
    pub system_count: i64,
    pub tool_count: i64,
    pub assistant_count: i64,
    pub tool_result_count: i64,
    pub image_count: i64,
    pub prompt: String,
    pub request_tail: String,
    pub answer: String,
    pub tool_names: String,
    pub headers: String,
    pub request: String,
    pub response: String,
}

/// 记录请求到数据库（入有界队列，由单写任务批量提交）
pub async fn log_request(app_state: &Arc<AppState>, record: Record) -> Result<(), DbError> {
    crate::db::rotate_if_needed(app_state, record.time_ms).await;
    let request = crate::db::payload::compress(record.request.as_bytes());
    let response = crate::db::payload::compress(record.response.as_bytes());
    let headers = crate::db::payload::compress(record.headers.as_bytes());
    let item = crate::db::writer::AuditItem {
        record,
        request,
        response,
        headers,
    };
    if app_state.audit_writer.enqueue(item).await {
        Ok(())
    } else {
        Err(DbError::Shape(
            "audit write queue is full; record dropped".to_string(),
        ))
    }
}

fn search_text(record: &Record) -> String {
    build_search_text(&record.prompt, &record.request_tail, &record.answer)
}

/// Concatenated text used by the FTS index.
///
/// `AUDIT_FTS_PROMPT_BYTES` / `_TAIL_BYTES` / `_ANSWER_BYTES` bound each field
/// (0 disables the cap); `AUDIT_FTS_MAX_BYTES` adds a global cap. Indexing the
/// full prompt/answer is the dominant write cost on large audit rows.
pub fn build_search_text(prompt: &str, request_tail: &str, answer: &str) -> String {
    static LIMITS: once_cell::sync::Lazy<FtsLimits> =
        once_cell::sync::Lazy::new(FtsLimits::from_env);
    let limits = &*LIMITS;
    let prompt = head_bytes(prompt, limits.prompt);
    let tail = tail_bytes(request_tail, limits.tail);
    let answer = head_bytes(answer, limits.answer);
    let mut text = format!("{prompt}\n{tail}\n{answer}");
    if limits.global > 0 && text.len() > limits.global {
        let mut end = limits.global;
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        text.truncate(end);
    }
    text
}

struct FtsLimits {
    prompt: usize,
    tail: usize,
    answer: usize,
    global: usize,
}

impl FtsLimits {
    fn from_env() -> Self {
        Self {
            prompt: env_bytes("AUDIT_FTS_PROMPT_BYTES", 512),
            tail: env_bytes("AUDIT_FTS_TAIL_BYTES", 512),
            answer: env_bytes("AUDIT_FTS_ANSWER_BYTES", 512),
            global: env_bytes("AUDIT_FTS_MAX_BYTES", 0),
        }
    }
}

fn env_bytes(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn head_bytes(value: &str, limit: usize) -> &str {
    if limit == 0 || value.len() <= limit {
        return value;
    }
    let mut end = limit;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

fn tail_bytes(value: &str, limit: usize) -> &str {
    if limit == 0 || value.len() <= limit {
        return value;
    }
    let mut start = value.len() - limit;
    while !value.is_char_boundary(start) {
        start += 1;
    }
    &value[start..]
}

pub(crate) fn record_params(record: &Record, id: i64) -> Vec<DbValue> {
    vec![
        DbValue::Integer(id),
        DbValue::Text(record.time.clone()),
        DbValue::Integer(record.time_ms),
        DbValue::Text(record.ip.clone()),
        opt_text(record.method.as_deref()),
        opt_text(record.endpoint.as_deref()),
        DbValue::Text(record.r#type.clone()),
        DbValue::Text(record.model.clone()),
        opt_text(record.backend.as_deref()),
        opt_text(record.session_id.as_deref()),
        opt_text(record.parent_session_id.as_deref()),
        opt_text(record.request_id.as_deref()),
        opt_text(record.session_affinity.as_deref()),
        opt_text(record.user_agent.as_deref()),
        opt_text(record.client_name.as_deref()),
        opt_text(record.client_version.as_deref()),
        opt_text(record.api_key.as_deref()),
        opt_int(record.status),
        opt_text(record.error.as_deref()),
        opt_int(record.retry_count),
        opt_text(record.finish_reason.as_deref()),
        opt_real(record.latency_ms),
        opt_real(record.ttft_ms),
        opt_real(record.upstream_ms),
        opt_real(record.stream_ms),
        DbValue::Integer(record.completion_tokens as i64),
        DbValue::Integer(record.prompt_tokens as i64),
        DbValue::Integer(record.total_tokens as i64),
        DbValue::Integer(i64::from(record.tool)),
        DbValue::Integer(i64::from(record.multimodal)),
        DbValue::Integer(record.request_bytes),
        DbValue::Integer(record.response_bytes),
        DbValue::Integer(record.prompt_bytes),
        DbValue::Integer(record.request_tail_bytes),
        DbValue::Integer(record.answer_bytes),
        DbValue::Integer(record.message_count),
        DbValue::Integer(record.system_count),
        DbValue::Integer(record.tool_count),
        DbValue::Integer(record.assistant_count),
        DbValue::Integer(record.tool_result_count),
        DbValue::Integer(record.image_count),
        DbValue::Text(record.prompt.clone()),
        DbValue::Text(record.request_tail.clone()),
        DbValue::Text(record.answer.clone()),
        DbValue::Text(record.tool_names.clone()),
        DbValue::Text(search_text(record)),
    ]
}

pub(crate) fn payload_params(
    record_id: i64,
    request: &crate::db::payload::Compressed,
    response: &crate::db::payload::Compressed,
    headers: &crate::db::payload::Compressed,
) -> Vec<DbValue> {
    vec![
        DbValue::Integer(record_id),
        DbValue::Text(request.codec.clone()),
        opt_text(request.dict_id.as_deref()),
        DbValue::Blob(request.bytes.clone()),
        DbValue::Blob(response.bytes.clone()),
        DbValue::Blob(headers.bytes.clone()),
        DbValue::Integer(request.raw_len),
        DbValue::Integer(response.raw_len),
        DbValue::Integer(headers.raw_len),
    ]
}

#[derive(Debug, Default)]
pub struct StoredPayload {
    pub request: String,
    pub response: String,
    pub headers: String,
}

pub async fn load_payload(app_state: &Arc<AppState>, record_id: i64) -> Option<StoredPayload> {
    let store = app_state.active_db().await;
    load_payload_from(&store, record_id).await
}

/// 在指定库上加载并解压记录正文（供跨月归档详情复用）。
pub async fn load_payload_from(store: &AuditStore, record_id: i64) -> Option<StoredPayload> {
    let row = store
        .query_one(
            "SELECT codec, dict_id, request, response, headers FROM payloads WHERE record_id = ?",
            vec![DbValue::Integer(record_id)],
        )
        .await
        .ok()??;

    let codec = row.get_str(0).ok()?.to_string();
    let dict_id = row.get_opt_str(1).ok()?.map(str::to_string);
    let decode = |idx: usize| -> String {
        row.get_opt_blob(idx)
            .ok()
            .flatten()
            .and_then(|b| crate::db::payload::decompress(&codec, dict_id.as_deref(), b))
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .unwrap_or_default()
    };

    Some(StoredPayload {
        request: decode(2),
        response: decode(3),
        headers: decode(4),
    })
}

/// 为非流式请求记录日志
pub async fn log_non_streaming_request(
    app_state: &Arc<AppState>,
    headers: &HeaderMap,
    payload: &RequestPayload,
    request_body: &Value,
    response_body: &Value,
    client_ip: String,
    meta: LogMeta,
) {
    let header = header_meta(headers);
    let req = extract_request(payload);
    let resp = extract_response(response_body);

    let headers_json = serde_json::to_string(
        &headers
            .iter()
            .map(|(k, v)| {
                (
                    k.to_string(),
                    serde_json::Value::String(v.to_str().unwrap_or("").to_string()),
                )
            })
            .collect::<serde_json::Map<_, _>>(),
    )
    .unwrap_or_default();

    let usage = response_body.get("usage");
    let prompt_tokens = usage
        .and_then(|u| u.get("prompt_tokens").and_then(|t| t.as_u64()))
        .or_else(|| usage.and_then(|u| u.get("input_tokens").and_then(|t| t.as_u64())))
        .unwrap_or(0);
    let completion_tokens = usage
        .and_then(|u| u.get("completion_tokens").and_then(|t| t.as_u64()))
        .or_else(|| usage.and_then(|u| u.get("output_tokens").and_then(|t| t.as_u64())))
        .unwrap_or(0);
    let total_tokens = usage
        .and_then(|u| u.get("total_tokens"))
        .and_then(|t| t.as_u64())
        .unwrap_or(0);

    let request_type = request_type_label(payload);

    let tool_used = request_body.get("tools").is_some();

    let is_multimodal = if let RequestPayload::Chat(p) = payload {
        p.messages.iter().any(|m| {
            if let Some(MessageContent::Array(content_array)) = &m.content {
                content_array.iter().any(|item| item.r#type == "image_url")
            } else {
                false
            }
        })
    } else {
        false
    };

    let request_str = serde_json::to_string(request_body).unwrap_or_default();
    let response_str = serde_json::to_string(response_body).unwrap_or_default();

    let now = Local::now();
    let record = Record {
        time: now.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        time_ms: now.timestamp_millis(),
        ip: client_ip.clone(),
        method: None,
        endpoint: meta
            .endpoint
            .clone()
            .or_else(|| payload.get_endpoint().map(str::to_string)),
        model: payload.get_model().to_string(),
        r#type: request_type.to_string(),
        backend: meta.backend.clone(),
        session_id: header.session_id,
        parent_session_id: header.parent_session_id,
        request_id: header.request_id,
        session_affinity: header.session_affinity,
        user_agent: header.user_agent,
        client_name: header.client_name,
        client_version: header.client_version,
        api_key: header.api_key,
        status: meta.status,
        error: meta.error.clone(),
        retry_count: meta.retry_count,
        finish_reason: resp.finish_reason,
        latency_ms: meta.latency_ms,
        ttft_ms: meta.ttft_ms,
        upstream_ms: meta.upstream_ms,
        stream_ms: meta.stream_ms,
        completion_tokens: completion_tokens.try_into().unwrap_or_default(),
        prompt_tokens: prompt_tokens.try_into().unwrap_or_default(),
        total_tokens: total_tokens.try_into().unwrap_or_default(),
        tool: tool_used,
        multimodal: is_multimodal,
        request_bytes: request_str.len() as i64,
        response_bytes: response_str.len() as i64,
        prompt_bytes: req.prompt_bytes,
        request_tail_bytes: req.request_tail_bytes,
        answer_bytes: resp.answer_bytes,
        message_count: req.message_count,
        system_count: req.system_count,
        tool_count: req.tool_count,
        assistant_count: req.assistant_count,
        tool_result_count: req.tool_result_count,
        image_count: req.image_count,
        prompt: req.prompt,
        request_tail: req.request_tail,
        answer: resp.answer,
        tool_names: resp.tool_names,
        headers: headers_json,
        request: request_str,
        response: response_str,
    };

    if let Err(e) = log_request(app_state, record).await {
        error!("Failed to log request to database: {}", e);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::requests::{
        AnthropicMessagesRequest, ChatCompletionRequest, ClassifyRequest, CompletionRequest,
        EmbeddingRequest, RerankRequest, ResponsesRequest, ScoreRequest,
    };

    fn make_chat_payload() -> RequestPayload {
        RequestPayload::Chat(ChatCompletionRequest {
            model: "gpt-4".to_string(),
            messages: vec![],
            stream: None,
            temperature: None,
            max_tokens: None,
            stop: None,
            tools: None,
            chat_template_kwargs: None,
            stream_options: None,
            logprobs: None,
            top_logprobs: None,
            top_p: None,
            frequency_penalty: None,
            presence_penalty: None,
            repetition_penalty: None,
            seed: None,
        })
    }

    fn make_completion_payload() -> RequestPayload {
        RequestPayload::Completion(CompletionRequest {
            model: "gpt-3.5-turbo".to_string(),
            prompt: "Hello".to_string(),
            stream: None,
            temperature: None,
            max_tokens: None,
            stop: None,
            stream_options: None,
            logprobs: None,
            prompt_logprobs: None,
            echo: None,
        })
    }

    fn make_embedding_payload() -> RequestPayload {
        RequestPayload::Embedding(EmbeddingRequest {
            model: "text-embedding-ada-002".to_string(),
            input: serde_json::json!("Hello world"),
            encoding_format: None,
            dimensions: None,
            user: None,
        })
    }

    fn make_rerank_payload() -> RequestPayload {
        RequestPayload::Rerank(RerankRequest {
            model: "rerank-model".to_string(),
            query: "test query".to_string(),
            documents: vec!["doc1".to_string(), "doc2".to_string()],
            top_n: None,
        })
    }

    fn make_score_payload() -> RequestPayload {
        RequestPayload::Score(ScoreRequest {
            model: "score-model".to_string(),
            text_1: serde_json::json!("text1"),
            text_2: serde_json::json!("text2"),
        })
    }

    fn make_classify_payload() -> RequestPayload {
        RequestPayload::Classify(ClassifyRequest {
            model: "classify-model".to_string(),
            input: serde_json::json!("input text"),
        })
    }

    fn make_responses_payload() -> RequestPayload {
        RequestPayload::Responses(ResponsesRequest {
            model: "gpt-4".to_string(),
            input: serde_json::json!("Hello"),
            stream: None,
            extra: serde_json::Map::new(),
        })
    }

    fn make_anthropic_messages_payload() -> RequestPayload {
        RequestPayload::AnthropicMessages(AnthropicMessagesRequest {
            model: "claude-sonnet-4-20250514".to_string(),
            stream: None,
            extra: serde_json::Map::new(),
        })
    }

    #[test]
    fn test_request_type_label_chat() {
        assert_eq!(request_type_label(&make_chat_payload()), "chat.completions");
    }

    #[test]
    fn test_request_type_label_completion() {
        assert_eq!(
            request_type_label(&make_completion_payload()),
            "text_completion"
        );
    }

    #[test]
    fn test_request_type_label_embedding() {
        assert_eq!(request_type_label(&make_embedding_payload()), "embeddings");
    }

    #[test]
    fn test_request_type_label_rerank() {
        assert_eq!(request_type_label(&make_rerank_payload()), "rerank");
    }

    #[test]
    fn test_request_type_label_score() {
        assert_eq!(request_type_label(&make_score_payload()), "score");
    }

    #[test]
    fn test_request_type_label_classify() {
        assert_eq!(request_type_label(&make_classify_payload()), "classify");
    }

    #[test]
    fn test_request_type_label_responses() {
        assert_eq!(request_type_label(&make_responses_payload()), "responses");
    }

    #[test]
    fn test_request_type_label_anthropic_messages() {
        assert_eq!(
            request_type_label(&make_anthropic_messages_payload()),
            "anthropic.messages"
        );
    }
}
