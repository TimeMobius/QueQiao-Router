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
    /// TypeSafe / OpenRouter System One 分类协议（Jev 等分类模型）
    SystemOne(SystemOneRequest),
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
            RequestPayload::SystemOne(p) => &p.model,
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
            RequestPayload::SystemOne(p) => p.model = model_name,
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
            RequestPayload::SystemOne(_) => Some("systemone"),
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
            // Embedding, Rerank, Score, Classify, SystemOne 等不需要内容路由，返回 None 回退随机
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
    /// OpenAI 的会话级 prompt 缓存键。pi-ai 在 `cacheRetention: long` 时会把它
    /// 当作会话标识塞进 body（见 `dsh-llm-pi-ai`），因此它既是给上游的缓存提示，
    /// 也是**没有会话标识头时**审计库 `SessionId` 的回退来源之一。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_key: Option<String>,
    /// 与 `prompt_cache_key` 配套的缓存保留策略（如 `"24h"`），原样透传。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt_cache_retention: Option<String>,
    /// 未在结构体中声明的字段，原样透传给上游。
    ///
    /// 网关是透传代理，不应逐字段猜测客户端会用哪些扩展参数：vLLM / SGLang /
    /// OpenAI 生态持续新增采样与路由字段（`top_k`、`guided_json`、
    /// `response_format`、`prompt_cache_key`…），逐字段声明必然滞后，而丢弃时
    /// **不会报错**，排查成本极高。
    ///
    /// 重建请求体时必须把它**铺在最底层**，再由已知字段覆盖同名键——这样
    /// `model` / `stream` 等网关拥有最终决定权的字段不会被客户端经此绕过。
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
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
    /// 未在结构体中声明的字段，原样透传给上游。
    ///
    /// 网关是透传代理，不应逐字段猜测客户端会用哪些扩展参数：vLLM / SGLang /
    /// OpenAI 生态持续新增采样与路由字段（`top_k`、`guided_json`、
    /// `response_format`、`prompt_cache_key`…），逐字段声明必然滞后，而丢弃时
    /// **不会报错**，排查成本极高。
    ///
    /// 重建请求体时必须把它**铺在最底层**，再由已知字段覆盖同名键——这样
    /// `model` / `stream` 等网关拥有最终决定权的字段不会被客户端经此绕过。
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
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
    /// 未在结构体中声明的字段，原样透传给上游。
    ///
    /// 网关是透传代理，不应逐字段猜测客户端会用哪些扩展参数：vLLM / SGLang /
    /// OpenAI 生态持续新增采样与路由字段（`top_k`、`guided_json`、
    /// `response_format`、`prompt_cache_key`…），逐字段声明必然滞后，而丢弃时
    /// **不会报错**，排查成本极高。
    ///
    /// 重建请求体时必须把它**铺在最底层**，再由已知字段覆盖同名键——这样
    /// `model` / `stream` 等网关拥有最终决定权的字段不会被客户端经此绕过。
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RerankRequest {
    pub model: String,
    pub query: String,
    pub documents: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_n: Option<usize>,
    /// 未在结构体中声明的字段，原样透传给上游。
    ///
    /// 网关是透传代理，不应逐字段猜测客户端会用哪些扩展参数：vLLM / SGLang /
    /// OpenAI 生态持续新增采样与路由字段（`top_k`、`guided_json`、
    /// `response_format`、`prompt_cache_key`…），逐字段声明必然滞后，而丢弃时
    /// **不会报错**，排查成本极高。
    ///
    /// 重建请求体时必须把它**铺在最底层**，再由已知字段覆盖同名键——这样
    /// `model` / `stream` 等网关拥有最终决定权的字段不会被客户端经此绕过。
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreRequest {
    pub model: String,
    pub text_1: Value, // String or Vec<String>
    pub text_2: Value, // String or Vec<String>
    /// 未在结构体中声明的字段，原样透传给上游。
    ///
    /// 网关是透传代理，不应逐字段猜测客户端会用哪些扩展参数：vLLM / SGLang /
    /// OpenAI 生态持续新增采样与路由字段（`top_k`、`guided_json`、
    /// `response_format`、`prompt_cache_key`…），逐字段声明必然滞后，而丢弃时
    /// **不会报错**，排查成本极高。
    ///
    /// 重建请求体时必须把它**铺在最底层**，再由已知字段覆盖同名键——这样
    /// `model` / `stream` 等网关拥有最终决定权的字段不会被客户端经此绕过。
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassifyRequest {
    pub model: String,
    pub input: Value, // String or Vec<String>
    /// 未在结构体中声明的字段，原样透传给上游。
    ///
    /// 网关是透传代理，不应逐字段猜测客户端会用哪些扩展参数：vLLM / SGLang /
    /// OpenAI 生态持续新增采样与路由字段（`top_k`、`guided_json`、
    /// `response_format`、`prompt_cache_key`…），逐字段声明必然滞后，而丢弃时
    /// **不会报错**，排查成本极高。
    ///
    /// 重建请求体时必须把它**铺在最底层**，再由已知字段覆盖同名键——这样
    /// `model` / `stream` 等网关拥有最终决定权的字段不会被客户端经此绕过。
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
}

/// TypeSafe System One 分类协议请求（Jev 等分类模型，OpenRouter 也转售同一协议）。
///
/// 协议要点：
/// - 端点：客户端 `POST /v1/systemone`（也接受裸路径 `/systemone`），上游 `{base_url}/systemone`；
/// - body：`{"model": "...", "state": "...", "questions": {...}}`，`state` 是待分类
///   文本，带图片时演变为 `[{"type":"image_url",...}, "文本"]` 的 parts 数组；
///   TypeSafe 原生协议用 `assets`，部分服务用 `images`；
/// - 响应：`{"answers": {...}, "usage": {"input_tokens": N, "output_tokens": N}}`，
///   按题型（`noul` / `choice` / `score`）返回结构化答案，**不是**文本生成。
///
/// 网关只负责路由、鉴权、透传与审计，不解析题型语义：除 `model` 外全部按原样透传，
/// 未声明的字段（`images` / `assets` / 未来的扩展）经 `extra` 保留。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemOneRequest {
    pub model: String,
    /// 待分类文本；带图片时为 `[{type: "image_url", ...}, "文本"]` parts 数组。
    pub state: Value,
    /// 题型定义：id → `{type: "noul" | "choice" | "score", instructions, criteria?}`。
    pub questions: Value,
    /// 未在结构体中声明的字段，原样透传给上游（`images`、`assets` 等）。
    #[serde(flatten)]
    pub extra: serde_json::Map<String, Value>,
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

    // -------------------------------------------------------------------------
    // RequestPayload::SystemOne tests
    // -------------------------------------------------------------------------

    fn make_systemone_payload() -> RequestPayload {
        RequestPayload::SystemOne(SystemOneRequest {
            model: "typesafe/jev-1.13".to_string(),
            state: serde_json::json!("I was charged twice for my subscription."),
            questions: serde_json::json!({
                "refund": {"type": "noul", "instructions": "Is the customer asking for money back?"}
            }),
            extra: serde_json::Map::new(),
        })
    }

    #[test]
    fn test_systemone_get_model() {
        assert_eq!(make_systemone_payload().get_model(), "typesafe/jev-1.13");
    }

    #[test]
    fn test_systemone_set_model() {
        let mut payload = make_systemone_payload();
        payload.set_model("~typesafe/jev-latest".to_string());
        assert_eq!(payload.get_model(), "~typesafe/jev-latest");
    }

    #[test]
    fn test_systemone_is_not_streaming() {
        assert!(!make_systemone_payload().is_streaming());
    }

    #[test]
    fn test_systemone_get_endpoint() {
        assert_eq!(make_systemone_payload().get_endpoint(), Some("systemone"));
    }

    #[test]
    fn test_systemone_no_content_routing() {
        assert!(make_systemone_payload().get_routing_keys().is_none());
    }

    #[test]
    fn test_systemone_extra_flattens_additional_fields() {
        // `images`（Clef 风格）与 `assets`（TypeSafe 原生）都不在结构体声明里，
        // 必须经 flatten 原样保留，否则带图请求会被网关静默丢字段。
        let json_str = r#"{
            "model": "typesafe/jev-1.13",
            "state": "some text",
            "questions": {"refund": {"type": "noul"}},
            "images": ["data:image/png;base64,AAA"],
            "assets": {"<image-1>": {"type": "image", "format": "png", "data_base64": "AAA"}}
        }"#;
        let request: SystemOneRequest = serde_json::from_str(json_str).unwrap();

        assert_eq!(request.model, "typesafe/jev-1.13");
        assert_eq!(request.state, serde_json::json!("some text"));
        assert!(request.extra.contains_key("images"));
        assert!(request.extra.contains_key("assets"));

        // 重建请求体走的是内层结构体的序列化（见 build_request_body_generic），
        // 未声明字段必须原样回到顶层。
        let body = serde_json::to_value(&request).unwrap();
        assert_eq!(
            body["images"][0],
            serde_json::json!("data:image/png;base64,AAA")
        );
        assert!(body["assets"].get("<image-1>").is_some());
    }

    #[test]
    fn test_systemone_state_can_be_parts_array() {
        // 带图片时 state 变成 parts 数组（OpenRouter: image_url parts 在文本之前）。
        let request: SystemOneRequest = serde_json::from_str(
            r#"{
                "model": "typesafe/jev-1.13",
                "state": [
                    {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAA"}},
                    "what is shown?"
                ],
                "questions": {"is_chart": {"type": "noul"}}
            }"#,
        )
        .unwrap();
        assert_eq!(request.state.as_array().map(|a| a.len()), Some(2));
    }
}
