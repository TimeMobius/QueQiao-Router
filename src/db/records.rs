use chrono::Local;

use axum::http::HeaderMap;
use serde_json::Value;
use std::sync::Arc;
use tracing::error;

use crate::{
    models::requests::{MessageContent, RequestPayload},
    state::app_state::AppState,
};

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

/// 数据库记录结构
#[derive(Debug)]
pub struct Record {
    pub time: String,
    pub ip: String,
    pub model: String,
    pub r#type: String,
    pub completion_tokens: i32,
    pub prompt_tokens: i32,
    pub total_tokens: i32,
    pub tool: bool,
    pub multimodal: bool,
    pub headers: String,
    pub request: String,
    pub response: String,
}

/// 记录请求到数据库
pub async fn log_request(app_state: &Arc<AppState>, record: Record) -> Result<(), sqlx::Error> {
    let pool = app_state.db_pool.read().await;
    sqlx::query(
        r#"
        INSERT INTO records (
            Time, IP, Model, Type, CompletionTokens, PromptTokens, TotalTokens,
            tool, multimodal, headers, request, response
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&record.time)
    .bind(&record.ip)
    .bind(&record.model)
    .bind(&record.r#type)
    .bind(record.completion_tokens)
    .bind(record.prompt_tokens)
    .bind(record.total_tokens)
    .bind(record.tool)
    .bind(record.multimodal)
    .bind(&record.headers)
    .bind(&record.request)
    .bind(&record.response)
    .execute(&*pool)
    .await?;

    Ok(())
}

/// 为非流式请求记录日志
pub async fn log_non_streaming_request(
    app_state: &Arc<AppState>,
    headers: &HeaderMap,
    payload: &RequestPayload,
    request_body: &Value,
    response_body: &Value,
    client_ip: String,
) {
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

    let record = Record {
        time: Local::now().format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        ip: client_ip.clone(),
        model: payload.get_model().to_string(),
        r#type: request_type.to_string(),
        completion_tokens: completion_tokens.try_into().unwrap_or_default(),
        prompt_tokens: prompt_tokens.try_into().unwrap_or_default(),
        total_tokens: total_tokens.try_into().unwrap_or_default(),
        tool: tool_used,
        multimodal: is_multimodal,
        headers: headers_json,
        request: serde_json::to_string(request_body).unwrap_or_default(),
        response: serde_json::to_string(response_body).unwrap_or_default(),
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
