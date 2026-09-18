use crate::config::types::{ClientConfig, ExtraBodyCached};
use crate::models::requests::{Message, MessageContent, RequestPayload};
use rayon::prelude::*;
use regex::Regex;
use serde_json::{json, Value};
use std::net::SocketAddr;

use axum::http::HeaderMap;

const HEADER_X_FORWARDED_FOR: &str = "x-forwarded-for";
const HEADER_X_REAL_IP: &str = "x-real-ip";

/// 从请求头中提取客户端真实 IP
///
/// 尝试顺序：
/// 1. `X-Forwarded-For`: 标准代理头，取第一个 IP
/// 2. `X-Real-IP`: Nginx 等常用头
/// 3. `SocketAddr`: TCP 连接的远端地址
pub fn get_client_ip(headers: &HeaderMap, addr: Option<SocketAddr>) -> String {
    if let Some(xff) = headers.get(HEADER_X_FORWARDED_FOR) {
        if let Ok(xff_str) = xff.to_str() {
            let raw_ip = xff_str.split(',').next().unwrap_or(xff_str).trim();
            return clean_ip(raw_ip);
        }
    }

    if let Some(xri) = headers.get(HEADER_X_REAL_IP) {
        if let Ok(xri_str) = xri.to_str() {
            return clean_ip(xri_str.trim());
        }
    }

    if let Some(addr) = addr {
        return clean_ip(&addr.ip().to_string());
    }

    "unknown".to_string()
}

/// 辅助函数：清洗 IP 地址（移除 IPv4-mapped IPv6 前缀）
fn clean_ip(ip: &str) -> String {
    if let Some(ipv4) = ip.strip_prefix("::ffff:") {
        ipv4.to_string()
    } else {
        ip.to_string()
    }
}

fn sanitize_msg(msg: &mut Message) {
    if let Some(content) = &mut msg.content {
        match content {
            MessageContent::String(c) => {
                let trimmed = c.trim().to_string();
                *c = trimmed;
            }
            MessageContent::Array(parts) => {
                for part in parts.iter_mut() {
                    if part.r#type == "text" {
                        if let Some(text) = &mut part.text {
                            *text = text.trim().to_string();
                        }
                    }
                }
            }
        }
    }
}

fn msg_is_empty(msg: &Message) -> bool {
    match &msg.content {
        Some(MessageContent::String(c)) => c.is_empty(),
        Some(MessageContent::Array(parts)) => parts.is_empty(),
        None => true,
    }
}

/// 处理消息：清理空白字符和合并连续的用户消息
pub fn process_messages(mut messages: Vec<Message>) -> Vec<Message> {
    if messages.is_empty() {
        return vec![];
    }

    messages.par_iter_mut().for_each(|msg| sanitize_msg(msg));

    let mut result: Vec<Message> = Vec::new();
    for msg in messages {
        if msg_is_empty(&msg) && msg.tool_calls.is_none() && !has_reasoning(&msg) {
            continue;
        }
        if let Some(last_message) = result.last_mut() {
            if last_message.role == "user" && msg.role == "user" {
                *last_message = msg;
            } else {
                result.push(msg);
            }
        } else {
            result.push(msg);
        }
    }
    result
}

/// 过滤空消息：content 为空时，仍保留携带工具调用或思考内容的消息
pub fn filter_empty_messages(messages: Vec<Message>) -> Vec<Message> {
    messages
        .into_iter()
        .filter(|message| {
            has_content(message) || message.tool_calls.is_some() || has_reasoning(message)
        })
        .collect()
}

fn has_content(message: &Message) -> bool {
    match &message.content {
        Some(MessageContent::String(c)) => !c.trim().is_empty(),
        Some(MessageContent::Array(parts)) => !parts.is_empty(),
        None => false,
    }
}

/// 判断消息是否携带思考内容（reasoning 或 reasoning_content，二者为 vLLM 不同版本的等价字段）
fn has_reasoning(message: &Message) -> bool {
    let non_empty = |s: &Option<String>| s.as_ref().is_some_and(|v| !v.trim().is_empty());
    non_empty(&message.reasoning) || non_empty(&message.reasoning_content)
}

use once_cell::sync::Lazy;

static THINK_TAG_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"<think>.*?</think>").unwrap());

/// 移除助手消息中的思考标签
pub fn remove_think_tags(messages: Vec<Message>) -> Vec<Message> {
    messages
        .into_par_iter()
        .map(|mut message| {
            if message.role == "assistant" {
                if let Some(MessageContent::String(content)) = &message.content {
                    let new_content = THINK_TAG_RE.replace_all(content, "").to_string();
                    message.content = Some(MessageContent::String(new_content));
                }
            }
            message
        })
        .collect()
}

/// 合并停止词
pub fn merge_stop_words(
    client_stop: Option<&Vec<String>>,
    request_stop: Option<Vec<String>>,
) -> Option<Vec<String>> {
    match (client_stop, request_stop) {
        (Some(client_stop_words), Some(request_stop_words)) => {
            let mut merged: Vec<String> = client_stop_words.clone();
            for word in request_stop_words {
                if !merged.contains(&word) {
                    merged.push(word);
                }
            }
            Some(merged)
        }
        (Some(client_stop_words), None) => Some(client_stop_words.clone()),
        (None, Some(request_stop_words)) => Some(request_stop_words),
        (None, None) => None,
    }
}

/// 智能调整 max_tokens
pub fn adjust_max_tokens(
    client_max_tokens: Option<u32>,
    request_max_tokens: Option<u32>,
) -> Option<u32> {
    match (client_max_tokens, request_max_tokens) {
        (Some(client_limit), Some(requested)) => {
            if requested > client_limit {
                Some(client_limit)
            } else {
                Some(requested)
            }
        }
        (Some(client_limit), None) => Some(client_limit),
        (None, Some(requested)) => Some(requested),
        (None, None) => None,
    }
}

/// 通用函数：为各种请求类型构建请求体
pub fn build_request_body_generic(
    payload: &RequestPayload,
    client_config: &ClientConfig,
    stream: bool,
) -> Value {
    let mut body = build_request_body_inner(payload, client_config, stream);
    apply_extra_body_cached(
        &mut body,
        &client_config.extra_body_cached,
        &client_config.name,
    );
    body
}

fn apply_extra_body_cached(body: &mut Value, extra: &ExtraBodyCached, _client_name: &str) {
    let ExtraBodyCached(Some(ref map)) = extra else {
        return;
    };
    let Some(target) = body.as_object_mut() else {
        return;
    };
    for (key, value) in map {
        if !target.contains_key(key) {
            target.insert(key.clone(), value.clone());
        }
    }
}

fn build_request_body_inner(
    payload: &RequestPayload,
    client_config: &ClientConfig,
    stream: bool,
) -> Value {
    match payload {
        RequestPayload::Chat(p) => {
            let adjusted_max_tokens = adjust_max_tokens(client_config.max_tokens, p.max_tokens);
            let merged_stop = merge_stop_words(client_config.stop.as_ref(), p.stop.clone());

            let mut body = json!({
                "model": p.model,
                "messages": p.messages,
                "stream": stream,
            });

            if let Some(temp) = p.temperature {
                body["temperature"] = json!(temp);
            }
            if let Some(top_p) = p.top_p {
                body["top_p"] = json!(top_p);
            }
            if let Some(frequency_penalty) = p.frequency_penalty {
                body["frequency_penalty"] = json!(frequency_penalty);
            }
            if let Some(presence_penalty) = p.presence_penalty {
                body["presence_penalty"] = json!(presence_penalty);
            }
            if let Some(repetition_penalty) = p.repetition_penalty {
                body["repetition_penalty"] = json!(repetition_penalty);
            }
            if let Some(seed) = p.seed {
                body["seed"] = json!(seed);
            }
            if let Some(tokens) = adjusted_max_tokens {
                body["max_tokens"] = json!(tokens);
            }
            if let Some(stop) = merged_stop {
                body["stop"] = json!(stop);
            }
            if let Some(tools) = &p.tools {
                body["tools"] = tools.clone();
            }
            if let Some(kwargs) = &p.chat_template_kwargs {
                body["chat_template_kwargs"] = kwargs.clone();
            }
            if stream {
                if let Some(opts) = &p.stream_options {
                    body["stream_options"] = opts.clone();
                } else {
                    body["stream_options"] = json!({"include_usage": true});
                }
            }
            if let Some(logprobs) = p.logprobs {
                body["logprobs"] = json!(logprobs);
            }
            if let Some(top_logprobs) = p.top_logprobs {
                body["top_logprobs"] = json!(top_logprobs);
            }
            body
        }
        RequestPayload::Completion(p) => {
            let adjusted_max_tokens = adjust_max_tokens(client_config.max_tokens, p.max_tokens);
            let merged_stop = merge_stop_words(client_config.stop.as_ref(), p.stop.clone());

            let mut body = json!({
                "model": p.model,
                "prompt": p.prompt,
                "stream": stream,
            });

            if let Some(temp) = p.temperature {
                body["temperature"] = json!(temp);
            }
            if let Some(tokens) = adjusted_max_tokens {
                body["max_tokens"] = json!(tokens);
            }
            if let Some(stop) = merged_stop {
                body["stop"] = json!(stop);
            }
            if stream {
                if let Some(opts) = &p.stream_options {
                    body["stream_options"] = opts.clone();
                } else {
                    body["stream_options"] = json!({"include_usage": true});
                }
            }
            if let Some(logprobs) = p.logprobs {
                body["logprobs"] = json!(logprobs);
            }
            if let Some(prompt_logprobs) = p.prompt_logprobs {
                body["prompt_logprobs"] = json!(prompt_logprobs);
            }
            if let Some(echo) = p.echo {
                body["echo"] = json!(echo);
            }
            body
        }
        RequestPayload::Embedding(p) => serde_json::to_value(p).unwrap_or(json!({})),
        RequestPayload::Rerank(p) => serde_json::to_value(p).unwrap_or(json!({})),
        RequestPayload::Score(p) => serde_json::to_value(p).unwrap_or(json!({})),
        RequestPayload::Classify(p) => serde_json::to_value(p).unwrap_or(json!({})),
        RequestPayload::Responses(p) => {
            let mut body = serde_json::to_value(p).unwrap_or(json!({}));
            if let Some(obj) = body.as_object_mut() {
                obj.insert("stream".to_string(), json!(stream));
            }
            body
        }
        RequestPayload::AnthropicMessages(p) => {
            let mut body = serde_json::to_value(p).unwrap_or(json!({}));
            if let Some(obj) = body.as_object_mut() {
                obj.insert("stream".to_string(), json!(stream));
            }
            body
        }
    }
}

/// 通用函数：为非流式响应的 JSON 体添加特殊前缀
pub fn apply_prefix_to_json(response_body: &mut Value, prefix: &str, is_chat: bool) {
    if prefix.is_empty() {
        return;
    }

    if let Some(choices) = response_body
        .get_mut("choices")
        .and_then(|c| c.as_array_mut())
    {
        for choice in choices {
            let text_node = if is_chat {
                choice.get_mut("message").and_then(|m| m.get_mut("content"))
            } else {
                choice.get_mut("text")
            };

            if let Some(content_val) = text_node {
                if let Some(content_str) = content_val.as_str() {
                    *content_val = json!(format!("{}{}", prefix, content_str));
                }
            }
        }
    }
}

/// 递归截断 JSON 对象，用于日志记录
pub fn truncate_json(value: &Value) -> Value {
    match value {
        Value::String(s) => {
            if s.len() > 500 {
                let mut end = 500;
                while !s.is_char_boundary(end) {
                    end -= 1;
                }
                json!(format!("{}...[TRUNCATED]", &s[..end]))
            } else {
                value.clone()
            }
        }
        Value::Array(arr) => {
            if arr.len() > 10 {
                let mut new_arr: Vec<Value> = arr.iter().take(10).map(truncate_json).collect();
                new_arr.push(json!(format!("...[TRUNCATED: {} items]", arr.len())));
                Value::Array(new_arr)
            } else {
                Value::Array(arr.iter().map(truncate_json).collect())
            }
        }
        Value::Object(map) => {
            let new_map = map
                .iter()
                .map(|(k, v)| (k.clone(), truncate_json(v)))
                .collect();
            Value::Object(new_map)
        }
        _ => value.clone(),
    }
}

use crate::models::AccessLogMeta;
use axum::{
    body::Bytes,
    extract::{FromRequest, Request},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::de::DeserializeOwned;

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

/// Records a mid-stream SSE interruption in `error.log`.
///
/// SSE responses are committed as `200` before the body is polled, so the
/// access-log middleware can never classify these failures as errors. The
/// `access_log` target is what routes the record to `error.log`; its layers keep
/// only the message, so all fields are folded into the formatted line.
pub fn log_stream_interruption(
    client_ip: &str,
    endpoint: &str,
    model: &str,
    backend: &str,
    error: &str,
) {
    let time_str = chrono::Local::now().format("%d/%b/%Y:%H:%M:%S %z");
    tracing::error!(
        target: "access_log",
        "[{}] STREAM_INTERRUPTED client={} endpoint={} model={} backend={} error={:?}",
        time_str,
        client_ip,
        endpoint,
        model,
        backend,
        error
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::requests::{Message, MessageContent};
    use axum::http::HeaderMap;

    fn create_message(role: &str, content: &str) -> Message {
        Message {
            role: role.to_string(),
            content: Some(MessageContent::String(content.to_string())),
            reasoning: None,
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: None,
        }
    }

    #[test]
    fn test_get_client_ip_from_x_forwarded_for() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "203.0.113.195, 70.41.3.18".parse().unwrap(),
        );

        let result = get_client_ip(&headers, None);
        assert_eq!(result, "203.0.113.195");
    }

    #[test]
    fn test_get_client_ip_from_x_real_ip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", "192.168.1.100".parse().unwrap());

        let result = get_client_ip(&headers, None);
        assert_eq!(result, "192.168.1.100");
    }

    #[test]
    fn test_get_client_ip_ipv4_mapped() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "::ffff:192.168.1.1".parse().unwrap());

        let result = get_client_ip(&headers, None);
        assert_eq!(result, "192.168.1.1");
    }

    #[test]
    fn test_get_client_ip_unknown() {
        let headers = HeaderMap::new();

        let result = get_client_ip(&headers, None);
        assert_eq!(result, "unknown");
    }

    #[test]
    fn test_process_messages_empty() {
        let messages: Vec<Message> = vec![];
        let result = process_messages(messages);
        assert!(result.is_empty());
    }

    #[test]
    fn test_process_messages_merge_consecutive_user() {
        let messages = vec![
            create_message("user", "First message"),
            create_message("user", "Second message"),
            create_message("assistant", "Response"),
        ];

        let result = process_messages(messages);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].role, "user");
        assert_eq!(
            result[0].content.as_ref().unwrap(),
            &MessageContent::String("Second message".to_string())
        );
    }

    #[test]
    fn test_process_messages_filter_empty() {
        let messages = vec![
            create_message("user", "Valid message"),
            create_message("user", "   "),
        ];

        let result = process_messages(messages);
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].content.as_ref().unwrap(),
            &MessageContent::String("Valid message".to_string())
        );
    }

    #[test]
    fn test_filter_empty_messages_keeps_tool_calls() {
        let messages = vec![Message {
            role: "assistant".to_string(),
            content: None,
            reasoning: None,
            reasoning_content: None,
            tool_calls: Some(serde_json::json!([{"id": "call_123"}])),
            tool_call_id: None,
        }];

        let result = filter_empty_messages(messages);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_filter_empty_messages_keeps_empty_string_with_tool_calls() {
        let messages = vec![Message {
            role: "assistant".to_string(),
            content: Some(MessageContent::String(String::new())),
            reasoning: None,
            reasoning_content: None,
            tool_calls: Some(serde_json::json!([{"id": "call_123"}])),
            tool_call_id: None,
        }];

        let result = filter_empty_messages(messages);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_filter_empty_messages_keeps_empty_string_with_reasoning() {
        let messages = vec![Message {
            role: "assistant".to_string(),
            content: Some(MessageContent::String(String::new())),
            reasoning: Some("thinking...".to_string()),
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: None,
        }];

        let result = filter_empty_messages(messages);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_process_then_filter_keeps_assistant_toolcall_with_whitespace_content() {
        let messages = vec![
            create_message("user", "现在的时间是多少"),
            Message {
                role: "assistant".to_string(),
                content: Some(MessageContent::String("\n".to_string())),
                reasoning: Some("用户想知道现在的时间".to_string()),
                reasoning_content: None,
                tool_calls: Some(serde_json::json!([{"id": "call_1", "type": "function"}])),
                tool_call_id: None,
            },
        ];

        let result = filter_empty_messages(process_messages(messages));
        assert_eq!(result.len(), 2);
        assert_eq!(result[1].role, "assistant");
        assert!(result[1].tool_calls.is_some());
    }

    #[test]
    fn test_remove_think_tags() {
        let messages = vec![Message {
            role: "assistant".to_string(),
            content: Some(MessageContent::String(
                "<think> thinking process</think> actual response".to_string(),
            )),
            reasoning: None,
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: None,
        }];

        let result = remove_think_tags(messages);
        assert_eq!(
            result[0].content.as_ref().unwrap(),
            &MessageContent::String(" actual response".to_string())
        );
    }

    #[test]
    fn test_remove_think_tags_multiple() {
        let messages = vec![Message {
            role: "assistant".to_string(),
            content: Some(MessageContent::String(
                "<think> think 1</think> response 1<think> think 2</think> response 2".to_string(),
            )),
            reasoning: None,
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: None,
        }];

        let result = remove_think_tags(messages);
        assert_eq!(
            result[0].content.as_ref().unwrap(),
            &MessageContent::String(" response 1 response 2".to_string())
        );
    }

    #[test]
    fn test_merge_stop_words_both_present() {
        let client_stop = vec!["<STOP1>".to_string(), "<STOP2>".to_string()];
        let request_stop = vec!["<STOP2>".to_string(), "<STOP3>".to_string()];

        let result = merge_stop_words(Some(&client_stop), Some(request_stop));
        let result = result.unwrap();
        assert_eq!(result.len(), 3);
        assert!(result.contains(&"<STOP1>".to_string()));
        assert!(result.contains(&"<STOP2>".to_string()));
        assert!(result.contains(&"<STOP3>".to_string()));
    }

    #[test]
    fn test_merge_stop_words_client_only() {
        let client_stop = vec!["<STOP1>".to_string()];

        let result = merge_stop_words(Some(&client_stop), None);
        assert_eq!(result, Some(vec!["<STOP1>".to_string()]));
    }

    #[test]
    fn test_merge_stop_words_request_only() {
        let request_stop = vec!["<STOP3>".to_string()];

        let result = merge_stop_words(None, Some(request_stop));
        assert_eq!(result, Some(vec!["<STOP3>".to_string()]));
    }

    #[test]
    fn test_merge_stop_words_none() {
        let result = merge_stop_words(None, None);
        assert!(result.is_none());
    }

    #[test]
    fn test_adjust_max_tokens_both_present_request_exceeds() {
        let result = adjust_max_tokens(Some(1024), Some(8000));
        assert_eq!(result, Some(1024));
    }

    #[test]
    fn test_adjust_max_tokens_both_present_request_within_limit() {
        let result = adjust_max_tokens(Some(1024), Some(500));
        assert_eq!(result, Some(500));
    }

    #[test]
    fn test_adjust_max_tokens_client_only() {
        let result = adjust_max_tokens(Some(1024), None);
        assert_eq!(result, Some(1024));
    }

    #[test]
    fn test_adjust_max_tokens_request_only() {
        let result = adjust_max_tokens(None, Some(500));
        assert_eq!(result, Some(500));
    }

    #[test]
    fn test_adjust_max_tokens_none() {
        let result = adjust_max_tokens(None, None);
        assert!(result.is_none());
    }

    #[test]
    fn test_truncate_json_string_short() {
        let value = json!("short string");
        let result = truncate_json(&value);
        assert_eq!(result, "short string");
    }

    #[test]
    fn test_truncate_json_string_long() {
        let long_string = "a".repeat(600);
        let value = json!(long_string);
        let result = truncate_json(&value);
        let result_str = result.as_str().unwrap();
        assert!(result_str.ends_with("...[TRUNCATED]"));
        assert!(result_str.len() < 550);
    }

    #[test]
    fn test_truncate_json_array() {
        let arr: Vec<Value> = (0..15).map(|i| json!(i)).collect();
        let value = Value::Array(arr);
        let result = truncate_json(&value);
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 11);
        assert!(arr[10].as_str().unwrap().contains("TRUNCATED"));
    }

    #[test]
    fn test_apply_prefix_to_json_chat() {
        let mut body = json!({
            "choices": [
                {"message": {"content": "Hello"}}
            ]
        });

        apply_prefix_to_json(&mut body, "<PREFIX>", true);

        assert_eq!(body["choices"][0]["message"]["content"], "<PREFIX>Hello");
    }

    #[test]
    fn test_apply_prefix_to_json_completion() {
        let mut body = json!({
            "choices": [
                {"text": "Hello"}
            ]
        });

        apply_prefix_to_json(&mut body, "<PREFIX>", false);

        assert_eq!(body["choices"][0]["text"], "<PREFIX>Hello");
    }

    #[test]
    fn test_apply_prefix_empty() {
        let mut body = json!({
            "choices": [
                {"message": {"content": "Hello"}}
            ]
        });

        apply_prefix_to_json(&mut body, "", true);

        assert_eq!(body["choices"][0]["message"]["content"], "Hello");
    }

    #[test]
    fn test_apply_extra_body_injects_absent_keys() {
        let mut body = json!({"model": "m", "messages": []});
        let extra: serde_json::Map<_, _> =
            serde_json::from_str(r#"{"frequency_penalty": 1, "presence_penalty": 0.91}"#).unwrap();
        apply_extra_body_cached(&mut body, &ExtraBodyCached(Some(extra)), "test");

        assert_eq!(body["frequency_penalty"], json!(1));
        assert_eq!(body["presence_penalty"], json!(0.91));
    }

    #[test]
    fn test_apply_extra_body_does_not_override_existing() {
        let mut body = json!({"model": "m", "frequency_penalty": 2});
        let extra: serde_json::Map<_, _> =
            serde_json::from_str(r#"{"frequency_penalty": 1, "presence_penalty": 0.91}"#).unwrap();
        apply_extra_body_cached(&mut body, &ExtraBodyCached(Some(extra)), "test");

        assert_eq!(body["frequency_penalty"], json!(2));
        assert_eq!(body["presence_penalty"], json!(0.91));
    }

    #[test]
    fn test_apply_extra_body_nested_object() {
        let mut body = json!({"model": "m"});
        let extra: serde_json::Map<_, _> = serde_json::from_str(
            r#"{"chat_template_kwargs": {"enable_thinking": true, "reasoning_effort": "max"}}"#,
        )
        .unwrap();
        apply_extra_body_cached(&mut body, &ExtraBodyCached(Some(extra)), "test");

        assert_eq!(body["chat_template_kwargs"]["enable_thinking"], json!(true));
        assert_eq!(
            body["chat_template_kwargs"]["reasoning_effort"],
            json!("max")
        );
    }

    #[test]
    fn test_apply_extra_body_invalid_json_is_skipped() {
        let mut body = json!({"model": "m"});
        let before = body.clone();

        apply_extra_body_cached(&mut body, &ExtraBodyCached(None), "test");

        assert_eq!(body, before);
    }

    #[test]
    fn test_apply_extra_body_none_and_empty() {
        let mut body = json!({"model": "m"});
        let before = body.clone();

        apply_extra_body_cached(&mut body, &ExtraBodyCached(None), "test");
        apply_extra_body_cached(&mut body, &ExtraBodyCached(None), "test");

        assert_eq!(body, before);
    }

    #[test]
    fn test_apply_extra_body_non_object_is_skipped() {
        let mut body = json!({"model": "m"});
        let before = body.clone();

        apply_extra_body_cached(&mut body, &ExtraBodyCached(None), "test");

        assert_eq!(body, before);
    }

    #[test]
    fn test_build_chat_request_preserves_sampling_parameters() {
        let request =
            serde_json::from_value::<crate::models::requests::ChatCompletionRequest>(json!({
                "model": "xiaoke-5",
                "messages": [],
                "stream": false,
                "top_p": 0.37,
                "frequency_penalty": 0.11,
                "presence_penalty": 0.22,
                "repetition_penalty": 1.15,
                "seed": 42
            }))
            .unwrap();
        let payload = RequestPayload::Chat(request);
        let client_config = ClientConfig {
            name: "test".to_string(),
            base_url: "http://localhost/v1".to_string(),
            api_key: None,
            model_match: crate::config::types::ModelMatch {
                match_type: "exact".to_string(),
                value: vec!["xiaoke-5".to_string()],
            },
            priority: None,
            fallback: None,
            special_prefix: None,
            stop: None,
            max_tokens: None,
            extra_body: None,
            thinking_format: None,
            extra_body_cached: ExtraBodyCached::default(),
        };

        let body = build_request_body_generic(&payload, &client_config, false);

        assert_eq!(body["top_p"], json!(0.37_f32));
        assert_eq!(body["frequency_penalty"], json!(0.11_f32));
        assert_eq!(body["presence_penalty"], json!(0.22_f32));
        assert_eq!(body["repetition_penalty"], json!(1.15_f32));
        assert_eq!(body["seed"], json!(42));
    }
}
