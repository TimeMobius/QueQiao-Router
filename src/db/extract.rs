use axum::http::HeaderMap;
use once_cell::sync::Lazy;
use serde_json::Value;

use crate::models::requests::{ChatCompletionRequest, MessageContent, RequestPayload};

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_bool(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

static PROMPT_MAX: Lazy<usize> = Lazy::new(|| env_usize("RECORD_PROMPT_MAX_BYTES", 8 * 1024));
static REQUEST_TAIL_MAX: Lazy<usize> =
    Lazy::new(|| env_usize("RECORD_REQUEST_TAIL_MAX_BYTES", 32 * 1024));
static ANSWER_MAX: Lazy<usize> = Lazy::new(|| env_usize("RECORD_ANSWER_MAX_BYTES", 32 * 1024));

/// 凭据默认明文入库（经确认）；`RECORD_STORE_RAW_CREDENTIALS=false` 时改为不落库。
static STORE_RAW_CREDENTIALS: Lazy<bool> =
    Lazy::new(|| env_bool("RECORD_STORE_RAW_CREDENTIALS", true));

#[derive(Debug, Default, Clone)]
pub struct RequestExtract {
    pub prompt: String,
    pub prompt_bytes: i64,
    pub request_tail: String,
    pub request_tail_bytes: i64,
    pub message_count: i64,
    pub system_count: i64,
    pub tool_count: i64,
    pub assistant_count: i64,
    pub tool_result_count: i64,
    pub image_count: i64,
}

#[derive(Debug, Default, Clone)]
pub struct ResponseExtract {
    pub answer: String,
    pub answer_bytes: i64,
    pub tool_names: String,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct HeaderMeta {
    pub session_id: Option<String>,
    pub parent_session_id: Option<String>,
    pub session_affinity: Option<String>,
    pub user_agent: Option<String>,
    pub client_name: Option<String>,
    pub client_version: Option<String>,
    pub api_key: Option<String>,
}

fn truncate_head(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…[+{} bytes]", &s[..end], s.len() - end)
}

fn truncate_head_tail(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let head = max * 3 / 4;
    let tail = max - head;
    let mut head_end = head;
    while head_end > 0 && !s.is_char_boundary(head_end) {
        head_end -= 1;
    }
    let mut tail_start = s.len() - tail;
    while tail_start < s.len() && !s.is_char_boundary(tail_start) {
        tail_start += 1;
    }
    let omitted = tail_start - head_end;
    format!(
        "{}…[+{} bytes]…{}",
        &s[..head_end],
        omitted,
        &s[tail_start..]
    )
}

fn content_text(c: &MessageContent) -> String {
    match c {
        MessageContent::String(s) => s.clone(),
        MessageContent::Array(parts) => parts
            .iter()
            .filter_map(|p| {
                if p.r#type == "text" {
                    p.text.clone()
                } else if p.r#type == "image_url" {
                    Some("[image]".to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn content_images(c: &MessageContent) -> i64 {
    match c {
        MessageContent::String(_) => 0,
        MessageContent::Array(parts) => {
            parts.iter().filter(|p| p.r#type == "image_url").count() as i64
        }
    }
}

fn value_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Array(items) => items
            .iter()
            .map(value_text)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("\n"),
        Value::Object(map) => map
            .get("text")
            .or_else(|| map.get("content"))
            .map(value_text)
            .unwrap_or_default(),
        _ => String::new(),
    }
}

fn chat_extract(p: &ChatCompletionRequest) -> RequestExtract {
    let mut r = RequestExtract {
        message_count: p.messages.len() as i64,
        ..Default::default()
    };
    for m in &p.messages {
        match m.role.as_str() {
            "system" | "developer" => r.system_count += 1,
            "assistant" => r.assistant_count += 1,
            "tool" => r.tool_result_count += 1,
            _ => {}
        }
        if let Some(c) = &m.content {
            r.image_count += content_images(c);
        }
    }
    r.tool_count = p
        .tools
        .as_ref()
        .and_then(|v| v.as_array())
        .map(|a| a.len() as i64)
        .unwrap_or(0);

    let last_user = p
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .and_then(|m| m.content.as_ref())
        .map(content_text)
        .unwrap_or_default();
    let last_msg = p
        .messages
        .last()
        .and_then(|m| m.content.as_ref())
        .map(content_text)
        .unwrap_or_default();

    r.prompt_bytes = last_user.len() as i64;
    r.request_tail_bytes = last_msg.len() as i64;
    r.prompt = truncate_head(&last_user, *PROMPT_MAX);
    r.request_tail = truncate_head_tail(&last_msg, *REQUEST_TAIL_MAX);
    r
}

fn responses_input_extract(input: &Value) -> RequestExtract {
    if let Value::String(s) = input {
        return RequestExtract {
            message_count: 1,
            prompt_bytes: s.len() as i64,
            request_tail_bytes: s.len() as i64,
            prompt: truncate_head(s, *PROMPT_MAX),
            request_tail: truncate_head_tail(s, *REQUEST_TAIL_MAX),
            ..Default::default()
        };
    }
    let Some(items) = input.as_array() else {
        return RequestExtract::default();
    };
    let mut r = RequestExtract {
        message_count: items.len() as i64,
        ..Default::default()
    };
    let mut last_user = String::new();
    let mut last_any = String::new();
    for item in items {
        let role = item.get("role").and_then(|v| v.as_str()).unwrap_or("");
        let typ = item.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match role {
            "system" | "developer" => r.system_count += 1,
            "assistant" => r.assistant_count += 1,
            "tool" => r.tool_result_count += 1,
            _ => {}
        }
        if typ == "function_call_output" {
            r.tool_result_count += 1;
        }
        let text = item.get("content").map(value_text).unwrap_or_default();
        if !text.is_empty() {
            r.image_count += count_input_images(item.get("content"));
            if role == "user" {
                last_user = text.clone();
            }
            last_any = text;
        }
    }
    r.prompt_bytes = last_user.len() as i64;
    r.request_tail_bytes = last_any.len() as i64;
    r.prompt = truncate_head(&last_user, *PROMPT_MAX);
    r.request_tail = truncate_head_tail(&last_any, *REQUEST_TAIL_MAX);
    r
}

fn count_input_images(content: Option<&Value>) -> i64 {
    content
        .and_then(|c| c.as_array())
        .map(|parts| {
            parts
                .iter()
                .filter(|p| {
                    matches!(
                        p.get("type").and_then(|v| v.as_str()),
                        Some("input_image") | Some("image_url")
                    )
                })
                .count() as i64
        })
        .unwrap_or(0)
}

fn anthropic_extract(p: &crate::models::requests::AnthropicMessagesRequest) -> RequestExtract {
    let mut r = RequestExtract::default();
    r.system_count = match p.extra.get("system") {
        Some(Value::Array(a)) => a.len() as i64,
        Some(_) => 1,
        None => 0,
    };
    r.tool_count = p
        .extra
        .get("tools")
        .and_then(|v| v.as_array())
        .map(|a| a.len() as i64)
        .unwrap_or(0);

    let Some(msgs) = p.extra.get("messages").and_then(|v| v.as_array()) else {
        return r;
    };
    r.message_count = msgs.len() as i64;

    let mut last_user = String::new();
    let mut last_any = String::new();
    for m in msgs {
        let role = m.get("role").and_then(|v| v.as_str()).unwrap_or("");
        match role {
            "assistant" => r.assistant_count += 1,
            "user" => {}
            _ => {}
        }
        if m.get("content").map(has_tool_result).unwrap_or(false) {
            r.tool_result_count += 1;
        }
        let text = m.get("content").map(value_text).unwrap_or_default();
        if !text.is_empty() {
            if role == "user" {
                last_user = text.clone();
            }
            last_any = text;
        }
    }
    r.prompt_bytes = last_user.len() as i64;
    r.request_tail_bytes = last_any.len() as i64;
    r.prompt = truncate_head(&last_user, *PROMPT_MAX);
    r.request_tail = truncate_head_tail(&last_any, *REQUEST_TAIL_MAX);
    r
}

fn has_tool_result(content: &Value) -> bool {
    content
        .as_array()
        .map(|parts| {
            parts
                .iter()
                .any(|p| p.get("type").and_then(|v| v.as_str()) == Some("tool_result"))
        })
        .unwrap_or(false)
}

pub fn extract_request(payload: &RequestPayload) -> RequestExtract {
    match payload {
        RequestPayload::Chat(p) => chat_extract(p),
        RequestPayload::Completion(p) => {
            let text = p.prompt.clone();
            let mut r = RequestExtract {
                prompt_bytes: text.len() as i64,
                request_tail_bytes: text.len() as i64,
                prompt: truncate_head(&text, *PROMPT_MAX),
                request_tail: truncate_head_tail(&text, *REQUEST_TAIL_MAX),
                ..Default::default()
            };
            r.message_count = 0;
            r
        }
        RequestPayload::Embedding(p) => value_prompt(&p.input),
        RequestPayload::Classify(p) => value_prompt(&p.input),
        RequestPayload::Rerank(p) => {
            let text = p.query.clone();
            RequestExtract {
                prompt_bytes: text.len() as i64,
                request_tail_bytes: text.len() as i64,
                prompt: truncate_head(&text, *PROMPT_MAX),
                request_tail: truncate_head_tail(&text, *REQUEST_TAIL_MAX),
                ..Default::default()
            }
        }
        RequestPayload::Score(p) => value_prompt(&p.text_1),
        RequestPayload::Responses(p) => responses_input_extract(&p.input),
        RequestPayload::AnthropicMessages(p) => anthropic_extract(p),
    }
}

fn value_prompt(v: &Value) -> RequestExtract {
    let text = value_text(v);
    RequestExtract {
        prompt_bytes: text.len() as i64,
        request_tail_bytes: text.len() as i64,
        prompt: truncate_head(&text, *PROMPT_MAX),
        request_tail: truncate_head_tail(&text, *REQUEST_TAIL_MAX),
        ..Default::default()
    }
}

fn push_nonempty(parts: &mut Vec<String>, s: Option<&str>) {
    if let Some(s) = s {
        if !s.is_empty() {
            parts.push(s.to_string());
        }
    }
}

fn push_reasoning(parts: &mut Vec<String>, s: Option<&str>, label: &str) {
    if let Some(s) = s {
        if !s.is_empty() {
            parts.push(format!("[{}]{}", label, s));
        }
    }
}

fn finish_response(
    parts: Vec<String>,
    tools: Vec<String>,
    finish_reason: Option<String>,
) -> ResponseExtract {
    let answer = parts.join("\n");
    ResponseExtract {
        answer_bytes: answer.len() as i64,
        answer: truncate_head_tail(&answer, *ANSWER_MAX),
        tool_names: tools.join(","),
        finish_reason,
    }
}

pub fn extract_response(response: &Value) -> ResponseExtract {
    if let Some(choices) = response.get("choices").and_then(|c| c.as_array()) {
        if let Some(choice) = choices.first() {
            let finish_reason = choice
                .get("finish_reason")
                .and_then(|v| v.as_str())
                .map(str::to_string);
            let mut parts = Vec::new();
            let mut tools = Vec::new();
            if let Some(m) = choice.get("message") {
                push_nonempty(&mut parts, m.get("content").and_then(|v| v.as_str()));
                push_reasoning(
                    &mut parts,
                    m.get("reasoning").and_then(|v| v.as_str()),
                    "reasoning",
                );
                push_reasoning(
                    &mut parts,
                    m.get("reasoning_content").and_then(|v| v.as_str()),
                    "reasoning_content",
                );
                if let Some(tcs) = m.get("tool_calls").and_then(|v| v.as_array()) {
                    for t in tcs {
                        if let Some(n) = t
                            .get("function")
                            .and_then(|f| f.get("name"))
                            .and_then(|v| v.as_str())
                        {
                            tools.push(n.to_string());
                        }
                    }
                }
            }
            if parts.is_empty() {
                push_nonempty(&mut parts, choice.get("text").and_then(|v| v.as_str()));
            }
            return finish_response(parts, tools, finish_reason);
        }
    }

    if let Some(content) = response.get("content").and_then(|v| v.as_array()) {
        let mut parts = Vec::new();
        let mut tools = Vec::new();
        for block in content {
            match block.get("type").and_then(|v| v.as_str()) {
                Some("text") => {
                    push_nonempty(&mut parts, block.get("text").and_then(|v| v.as_str()))
                }
                Some("thinking") => push_reasoning(
                    &mut parts,
                    block.get("thinking").and_then(|v| v.as_str()),
                    "thinking",
                ),
                Some("tool_use") => {
                    if let Some(n) = block.get("name").and_then(|v| v.as_str()) {
                        tools.push(n.to_string());
                    }
                }
                _ => {}
            }
        }
        let finish_reason = response
            .get("stop_reason")
            .and_then(|v| v.as_str())
            .map(str::to_string);
        return finish_response(parts, tools, finish_reason);
    }

    if let Some(output) = response.get("output").and_then(|v| v.as_array()) {
        let mut parts = Vec::new();
        let mut tools = Vec::new();
        for item in output {
            match item.get("type").and_then(|v| v.as_str()) {
                Some("message") => {
                    if let Some(content) = item.get("content").and_then(|v| v.as_array()) {
                        for c in content {
                            if c.get("type").and_then(|v| v.as_str()) == Some("output_text") {
                                push_nonempty(&mut parts, c.get("text").and_then(|v| v.as_str()));
                            }
                        }
                    }
                }
                Some("function_call") => {
                    if let Some(n) = item.get("name").and_then(|v| v.as_str()) {
                        tools.push(n.to_string());
                    }
                }
                _ => {}
            }
        }
        return finish_response(parts, tools, None);
    }

    ResponseExtract::default()
}

fn parse_client(user_agent: &Option<String>) -> (Option<String>, Option<String>) {
    let Some(ua) = user_agent else {
        return (None, None);
    };
    let first = ua.split_whitespace().next().unwrap_or("");
    match first.split_once('/') {
        Some((name, version)) => (Some(name.to_string()), Some(version.to_string())),
        None if !first.is_empty() => (Some(first.to_string()), None),
        _ => (None, None),
    }
}

pub fn header_meta(headers: &HeaderMap) -> HeaderMeta {
    let get = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let user_agent = get("user-agent");
    let (client_name, client_version) = parse_client(&user_agent);

    let api_key = if *STORE_RAW_CREDENTIALS {
        get("authorization")
            .map(|s| s.strip_prefix("Bearer ").unwrap_or(&s).to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| get("x-api-key"))
    } else {
        None
    };

    HeaderMeta {
        session_id: get("x-session-id"),
        parent_session_id: get("x-parent-session-id"),
        session_affinity: get("x-session-affinity"),
        user_agent,
        client_name,
        client_version,
        api_key,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::requests::{Message, MessageContent};

    fn chat(messages: Vec<Message>) -> RequestPayload {
        RequestPayload::Chat(ChatCompletionRequest {
            model: "m".to_string(),
            messages,
            stream: None,
            temperature: None,
            top_p: None,
            frequency_penalty: None,
            presence_penalty: None,
            repetition_penalty: None,
            seed: None,
            max_tokens: None,
            stop: None,
            tools: None,
            chat_template_kwargs: None,
            stream_options: None,
            logprobs: None,
            top_logprobs: None,
        })
    }

    fn msg(role: &str, text: &str) -> Message {
        Message {
            role: role.to_string(),
            content: Some(MessageContent::String(text.to_string())),
            reasoning: None,
            reasoning_content: None,
            tool_calls: None,
            tool_call_id: None,
        }
    }

    #[test]
    fn chat_prompt_is_last_user_and_tail_is_last_message() {
        let payload = chat(vec![
            msg("system", "sys"),
            msg("user", "first"),
            msg("assistant", "reply"),
            msg("tool", "tool-result"),
        ]);
        let r = extract_request(&payload);
        assert_eq!(r.prompt, "first");
        assert_eq!(r.request_tail, "tool-result");
        assert_eq!(r.message_count, 4);
        assert_eq!(r.system_count, 1);
        assert_eq!(r.assistant_count, 1);
        assert_eq!(r.tool_result_count, 1);
    }

    #[test]
    fn truncation_is_utf8_boundary_safe() {
        let long = "模".repeat(100);
        let out = truncate_head(&long, 10);
        assert!(out.len() <= 10 + 32);
        assert!(out.starts_with("模"));
        let out2 = truncate_head_tail(&long, 11);
        assert!(out2.contains("bytes"));
    }

    #[test]
    fn chat_answer_includes_content_and_tool_names() {
        let resp = serde_json::json!({
            "choices": [{
                "finish_reason": "tool_calls",
                "message": {
                    "content": "hello",
                    "reasoning": "think",
                    "tool_calls": [{"function": {"name": "read_file"}}]
                }
            }]
        });
        let a = extract_response(&resp);
        assert!(a.answer.contains("hello"));
        assert!(a.answer.contains("[reasoning]think"));
        assert_eq!(a.tool_names, "read_file");
        assert_eq!(a.finish_reason.as_deref(), Some("tool_calls"));
    }

    #[test]
    fn completion_prompt_from_prompt_field() {
        let payload = RequestPayload::Completion(crate::models::requests::CompletionRequest {
            model: "m".to_string(),
            prompt: "hello completion".to_string(),
            stream: None,
            temperature: None,
            max_tokens: None,
            stop: None,
            stream_options: None,
            logprobs: None,
            prompt_logprobs: None,
            echo: None,
        });
        assert_eq!(extract_request(&payload).prompt, "hello completion");
    }
}
