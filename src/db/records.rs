use axum::http::HeaderMap;
use chrono::Local;
use serde_json::Value;
use sqlx::Row;
use std::sync::Arc;
use tracing::error;

use crate::db::extract::{extract_request, extract_response, header_meta};
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

/// 记录请求到数据库
pub async fn log_request(app_state: &Arc<AppState>, record: Record) -> Result<(), sqlx::Error> {
    crate::db::rotate_if_needed(app_state, record.time_ms).await;
    let pool = app_state.db_pool.read().await;
    let req = crate::db::payload::compress(record.request.as_bytes());
    let resp = crate::db::payload::compress(record.response.as_bytes());
    let hdr = crate::db::payload::compress(record.headers.as_bytes());

    let mut tx = pool.begin().await?;
    let inserted = sqlx::query(
        r#"
        INSERT INTO records (
            Time, TimeMs, IP, Method, Endpoint, Type, Model, Backend,
            SessionId, ParentSessionId, RequestId, SessionAffinity, UserAgent, ClientName, ClientVersion, ApiKey,
            Status, Error, RetryCount, FinishReason,
            LatencyMs, TtftMs, UpstreamMs, StreamMs,
            CompletionTokens, PromptTokens, TotalTokens, Tool, Multimodal,
            RequestBytes, ResponseBytes, PromptBytes, RequestTailBytes, AnswerBytes,
            MessageCount, SystemCount, ToolCount, AssistantCount, ToolResultCount, ImageCount,
            Prompt, RequestTail, Answer, ToolNames,
            Headers, Request, Response
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?
        )
        "#,
    )
    .bind(&record.time)
    .bind(record.time_ms)
    .bind(&record.ip)
    .bind(record.method.as_deref())
    .bind(record.endpoint.as_deref())
    .bind(&record.r#type)
    .bind(&record.model)
    .bind(record.backend.as_deref())
    .bind(record.session_id.as_deref())
    .bind(record.parent_session_id.as_deref())
    .bind(record.request_id.as_deref())
    .bind(record.session_affinity.as_deref())
    .bind(record.user_agent.as_deref())
    .bind(record.client_name.as_deref())
    .bind(record.client_version.as_deref())
    .bind(record.api_key.as_deref())
    .bind(record.status)
    .bind(record.error.as_deref())
    .bind(record.retry_count)
    .bind(record.finish_reason.as_deref())
    .bind(record.latency_ms)
    .bind(record.ttft_ms)
    .bind(record.upstream_ms)
    .bind(record.stream_ms)
    .bind(record.completion_tokens)
    .bind(record.prompt_tokens)
    .bind(record.total_tokens)
    .bind(record.tool)
    .bind(record.multimodal)
    .bind(record.request_bytes)
    .bind(record.response_bytes)
    .bind(record.prompt_bytes)
    .bind(record.request_tail_bytes)
    .bind(record.answer_bytes)
    .bind(record.message_count)
    .bind(record.system_count)
    .bind(record.tool_count)
    .bind(record.assistant_count)
    .bind(record.tool_result_count)
    .bind(record.image_count)
    .bind(&record.prompt)
    .bind(&record.request_tail)
    .bind(&record.answer)
    .bind(&record.tool_names)
    .bind("")
    .bind("")
    .bind("")
    .execute(&mut *tx)
    .await?;

    let record_id = inserted.last_insert_rowid();
    sqlx::query(
        r#"
        INSERT INTO payloads (
            record_id, codec, dict_id,
            request, response, headers,
            request_raw_len, response_raw_len, headers_raw_len
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(record_id)
    .bind(&req.codec)
    .bind(req.dict_id.as_deref())
    .bind(&req.bytes)
    .bind(&resp.bytes)
    .bind(&hdr.bytes)
    .bind(req.raw_len)
    .bind(resp.raw_len)
    .bind(hdr.raw_len)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(())
}

#[derive(Debug, Default)]
pub struct StoredPayload {
    pub request: String,
    pub response: String,
    pub headers: String,
}

pub async fn load_payload(app_state: &Arc<AppState>, record_id: i64) -> Option<StoredPayload> {
    let pool = app_state.db_pool.read().await;
    load_payload_from(&pool, record_id).await
}

/// 在指定连接池上加载并解压记录正文（供跨月归档详情复用）。
pub async fn load_payload_from(pool: &sqlx::SqlitePool, record_id: i64) -> Option<StoredPayload> {
    let row = sqlx::query(
        "SELECT codec, dict_id, request, response, headers FROM payloads WHERE record_id = ?",
    )
    .bind(record_id)
    .fetch_optional(pool)
    .await
    .ok()??;

    let codec: String = row.get("codec");
    let dict_id: Option<String> = row.get("dict_id");
    let decode = |blob: Option<Vec<u8>>| -> String {
        blob.and_then(|b| crate::db::payload::decompress(&codec, dict_id.as_deref(), &b))
            .and_then(|bytes| String::from_utf8(bytes).ok())
            .unwrap_or_default()
    };

    Some(StoredPayload {
        request: decode(row.get("request")),
        response: decode(row.get("response")),
        headers: decode(row.get("headers")),
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
