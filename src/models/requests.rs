use axum::body::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::Value;

// Part of a multi-modal message
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContentPart {
    pub r#type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub image_url: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    String(String),
    Array(Vec<ContentPart>),
}

// --- 1. 抽象请求类型 ---
// 使用 enum 来统一不同类型的请求，这是很好的实践。
#[derive(Debug, Clone, Serialize, Deserialize)] // Add Debug and Clone traits
pub enum RequestPayload {
    Chat(ChatCompletionRequest),
    Completion(CompletionRequest),
    Embedding(EmbeddingRequest),
    Rerank(RerankRequest),
    Score(ScoreRequest),
    Classify(ClassifyRequest),
    Responses(ResponsesRequest),
    /// Anthropic Messages API 透传模式（暂不接入路由/流式处理）
    AnthropicMessages(AnthropicMessagesRequest),
}

impl RequestPayload {
    pub fn get_model(&self) -> &str {
        match self {
            RequestPayload::Chat(p) => &p.model,
            RequestPayload::Completion(p) => &p.model,
            RequestPayload::Embedding(p) => &p.model,
            RequestPayload::Rerank(p) => &p.model,
            RequestPayload::Score(p) => &p.model,
            RequestPayload::Classify(p) => &p.model,
            RequestPayload::Responses(p) => &p.model,
            RequestPayload::AnthropicMessages(p) => &p.model,
        }
    }

    pub fn set_model(&mut self, model_name: String) {
        match self {
            RequestPayload::Chat(p) => p.model = model_name,
            RequestPayload::Completion(p) => p.model = model_name,
            RequestPayload::Embedding(p) => p.model = model_name,
            RequestPayload::Rerank(p) => p.model = model_name,
            RequestPayload::Score(p) => p.model = model_name,
            RequestPayload::Classify(p) => p.model = model_name,
            RequestPayload::Responses(p) => p.model = model_name,
            RequestPayload::AnthropicMessages(p) => p.model = model_name,
        }
    }

    pub fn is_streaming(&self) -> bool {
        match self {
            RequestPayload::Chat(p) => p.stream.unwrap_or(false),
            RequestPayload::Completion(p) => p.stream.unwrap_or(false),
            RequestPayload::Responses(p) => p.stream.unwrap_or(false),
            RequestPayload::AnthropicMessages(p) => p.stream.unwrap_or(false),
            // 其他类型暂时不支持流式
            _ => false,
        }
    }

    pub fn get_endpoint(&self) -> Option<&'static str> {
        match self {
            RequestPayload::Chat(_) => Some("chat/completions"),
            RequestPayload::Completion(_) => Some("completions"),
            RequestPayload::Embedding(_) => Some("embeddings"),
            RequestPayload::Rerank(_) => Some("rerank"),
            RequestPayload::Score(_) => Some("score"),
            RequestPayload::Classify(_) => Some("classify"),
            RequestPayload::Responses(_) => Some("responses"),
            RequestPayload::AnthropicMessages(_) => Some("messages"),
        }
    }

    /// 提取用于路由决策的特征键值对 (key_content, weight)
    /// 返回 None 表示不使用确定性路由，回退到随机模式。
    pub fn get_routing_keys(&self) -> Option<Vec<(String, usize)>> {
        match self {
            RequestPayload::Chat(p) => {
                let mut keys = Vec::new();
                for msg in &p.messages {
                    if msg.role == "user" {
                        if let Some(content) = &msg.content {
                            let text = match content {
                                MessageContent::String(s) => s.clone(),
                                MessageContent::Array(parts) => parts
                                    .iter()
                                    .filter_map(|part| part.text.clone())
                                    .collect::<Vec<String>>()
                                    .join(""),
                            };

                            if !text.is_empty() {
                                // 提取前 64 个字符作为哈希锚点，完整长度作为权重
                                let key = text.chars().take(64).collect::<String>();
                                let weight = text.len();
                                keys.push((key, weight));
                            }
                        }
                    }
                }
                if keys.is_empty() {
                    None
                } else {
                    Some(keys)
                }
            }
            RequestPayload::Completion(p) => {
                if !p.prompt.is_empty() {
                    let key = p.prompt.chars().take(64).collect::<String>();
                    let weight = p.prompt.len();
                    Some(vec![(key, weight)])
                } else {
                    None
                }
            }
            // Embedding, Rerank, Score, Classify 等不需要内容路由，返回 None 回退随机
            _ => None,
        }
    }
}

// Data structures for requests
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub role: String,
    #[serde(default)]
    pub content: Option<MessageContent>,
    /// 思考字段，新版 vLLM 用 `reasoning`、旧版用 `reasoning_content`，二者等价，原样透传
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning_content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<Message>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frequency_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repetition_penalty: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seed: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chat_template_kwargs: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<Value>,
    // Logprobs parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_logprobs: Option<u8>,
    // Add other fields as needed
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionRequest {
    pub model: String,
    pub prompt: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stop: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream_options: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logprobs: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_logprobs: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub echo: Option<bool>,
}

// --- New vLLM Compatible Requests ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbeddingRequest {
    pub model: String,
    pub input: Value, // String or Vec<String> or Tokens
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dimensions: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RerankRequest {
    pub model: String,
    pub query: String,
    pub documents: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_n: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreRequest {
    pub model: String,
    pub text_1: Value, // String or Vec<String>
    pub text_2: Value, // String or Vec<String>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassifyRequest {
    pub model: String,
    pub input: Value, // String or Vec<String>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponsesRequest {
    pub model: String,
    pub input: Value, // string or array (OpenAI Responses API)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stream: Option<bool>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

/// Anthropic Messages API 请求结构（透传模式）
/// 必填字段 model、max_tokens、messages 通过 extra 透传，仅 model 和 stream 显式提取用于路由决策
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnthropicMessagesRequest {
    pub model: String,
    #[serde(default)]
    pub stream: Option<bool>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

// --- Internal Structures for Multipart/Audio Handling ---

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MultipartPart {
    pub name: String,
    pub data: Bytes,
    pub file_name: Option<String>,
    pub content_type: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct AudioRequest {
    pub model: String,
    pub parts: Vec<MultipartPart>,
    pub endpoint: String, // To distinguish between transcriptions and translations internally
}

#[cfg(test)]
mod tests {
    use super::*;

    // -------------------------------------------------------------------------
    // RequestPayload::AnthropicMessages tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_anthropic_messages_get_model() {
        let payload = RequestPayload::AnthropicMessages(AnthropicMessagesRequest {
            model: "claude-sonnet-4-20250514".to_string(),
            stream: None,
            extra: serde_json::Map::new(),
        });
        assert_eq!(payload.get_model(), "claude-sonnet-4-20250514");
    }

    #[test]
    fn test_anthropic_messages_set_model() {
        let mut payload = RequestPayload::AnthropicMessages(AnthropicMessagesRequest {
            model: "claude-3-haiku".to_string(),
            stream: None,
            extra: serde_json::Map::new(),
        });
        payload.set_model("claude-3-opus".to_string());
        assert_eq!(payload.get_model(), "claude-3-opus");
    }

    #[test]
    fn test_anthropic_messages_is_streaming_true() {
        let payload = RequestPayload::AnthropicMessages(AnthropicMessagesRequest {
            model: "claude-sonnet-4-20250514".to_string(),
            stream: Some(true),
            extra: serde_json::Map::new(),
        });
        assert!(payload.is_streaming());
    }

    #[test]
    fn test_anthropic_messages_is_streaming_false() {
        let payload = RequestPayload::AnthropicMessages(AnthropicMessagesRequest {
            model: "claude-sonnet-4-20250514".to_string(),
            stream: Some(false),
            extra: serde_json::Map::new(),
        });
        assert!(!payload.is_streaming());
    }

    #[test]
    fn test_anthropic_messages_is_streaming_none() {
        let payload = RequestPayload::AnthropicMessages(AnthropicMessagesRequest {
            model: "claude-sonnet-4-20250514".to_string(),
            stream: None,
            extra: serde_json::Map::new(),
        });
        assert!(!payload.is_streaming());
    }

    #[test]
    fn test_anthropic_messages_get_endpoint() {
        let payload = RequestPayload::AnthropicMessages(AnthropicMessagesRequest {
            model: "claude-sonnet-4-20250514".to_string(),
            stream: None,
            extra: serde_json::Map::new(),
        });
        assert_eq!(payload.get_endpoint(), Some("messages"));
    }

    #[test]
    fn test_anthropic_messages_extra_flattens_additional_fields() {
        // Verify that #[serde(flatten)] extra field correctly captures additional fields
        let json_str = r#"{
            "model": "claude-sonnet-4-20250514",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": "Hello"}]
        }"#;
        let request: AnthropicMessagesRequest = serde_json::from_str(json_str).unwrap();

        assert_eq!(request.model, "claude-sonnet-4-20250514");
        assert!(request.stream.is_none());

        // Extra should contain max_tokens and messages
        assert!(request.extra.contains_key("max_tokens"));
        assert!(request.extra.contains_key("messages"));

        // Verify the values are correctly parsed
        assert_eq!(
            request.extra.get("max_tokens").and_then(|v| v.as_i64()),
            Some(1024)
        );
    }
}
