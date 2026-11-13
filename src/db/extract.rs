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
    pub request_id: Option<String>,
}

/// 会话标识的请求头候选，按优先级排列。
///
/// 网关原生约定是 `x-session-id`，但不同客户端各有各的名字。DSH
/// （deepseek-harness）的 Messages 适配器发 `x-deepseek-harness-session-id`
/// （见 `dsh-llm-deepseek` 的请求头构造），pi-ai 的 `openai-responses` 协议发
/// `session_id` / `x-client-request-id`。网关此前只认第一个名字，导致这些客户端
/// 的审计记录 `SessionId` 恒为空——数据其实一直在请求头里，只是没被读。
///
/// 顺序即优先级：原生名优先，避免客户端同时发多个头时行为漂移。
const SESSION_ID_HEADERS: &[&str] = &[
    "x-session-id",
    "x-deepseek-harness-session-id",
    "session_id",
    "x-client-request-id",
];

/// 父会话标识的请求头候选。目前没有任何已知客户端会发后者，属于前瞻性预留：
/// 父会话 id 的现实来源是请求体里的 `dsh_session_log.session.parentSession`
/// （见 [`body_session_meta`]），而不是请求头。
const PARENT_SESSION_ID_HEADERS: &[&str] = &[
    "x-parent-session-id",
    "x-deepseek-harness-parent-session-id",
];

/// 按给定顺序返回第一个**非空**请求头值。
///
/// 注意不能用 `find_map(..).filter(..)`：那样遇到一个空白值的头就会停下并返回
/// `None`，而不是继续尝试后面的候选。必须逐项过滤。
fn first_header(get: &dyn Fn(&str) -> Option<String>, names: &[&str]) -> Option<String> {
    names.iter().find_map(|name| {
        get(name)
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
    })
}

/// DSH 官方路由写在**请求体**里的会话元数据。
#[derive(Debug, Default, Clone, PartialEq)]
pub struct BodySessionMeta {
    pub session_id: Option<String>,
    pub parent_session_id: Option<String>,
}

fn pick_str(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// 从请求体提取会话标识，作为请求头的**回退来源**。
///
/// 两条独立通道：
///
/// 1. `dsh_session_log.session`（DSH 官方 Messages 路由）。其中 `parentSession`
///    是**子智能体唯一能到达网关的父会话线索**——DSH 不会为此发送任何请求头。
///    `origin`/`delegationDepth` 也在同一对象里，但审计库没有对应列，故不提取。
/// 2. `prompt_cache_key`（OpenAI 兼容端点）。pi-ai 在 `cacheRetention: long` 时
///    把它当会话标识写进 body，值就是会话 id；上游不认该字段时会忽略，不影响转发。
///
/// 两者都只写进**现有**的 `SessionId` / `ParentSessionId` 列，不新增表结构。
pub fn body_session_meta(request_body: &Value) -> BodySessionMeta {
    let dsh = request_body
        .get("dsh_session_log")
        .and_then(|log| log.get("session"));

    let dsh_session = dsh.and_then(|s| pick_str(s, "id"));
    let dsh_parent = dsh.and_then(|s| pick_str(s, "parentSession"));
    let cache_key = pick_str(request_body, "prompt_cache_key");

    BodySessionMeta {
        session_id: dsh_session.or(cache_key),
        parent_session_id: dsh_parent,
    }
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
        // System One：`state` 是待分类文本；带图时是 parts 数组，
        // `value_text` 只取带 text 的 part，image_url 自然被忽略。
        RequestPayload::SystemOne(p) => value_prompt(&p.state),
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
        RequestPayload::Responses(p) => {
            let mut r = responses_input_extract(&p.input);
            r.tool_count = p
                .extra
                .get("tools")
                .and_then(|v| v.as_array())
                .map(|a| a.len() as i64)
                .unwrap_or(0);
            r
        }
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

    // System One 分类响应：`answers` 是结构化答案（noul/choice/score），
    // 不是文本生成；序列化后落到审计库 answer 列，看板可直接看到分类结果。
    if let Some(answers) = response.get("answers") {
        let answer = serde_json::to_string(answers).unwrap_or_default();
        return finish_response(vec![answer], Vec::new(), None);
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

    let raw_header = |name: &str| {
        headers
            .get(name)
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
    };
    let request_id = [
        "x-request-id",
        "x-trace-id",
        "request-id",
        "x-correlation-id",
    ]
    .iter()
    .filter_map(|name| raw_header(name))
    .map(|v| v.trim().to_string())
    .find(|v| !v.is_empty())
    .map(|v| v.chars().take(200).collect::<String>());

    HeaderMeta {
        session_id: first_header(&get, SESSION_ID_HEADERS),
        parent_session_id: first_header(&get, PARENT_SESSION_ID_HEADERS),
        session_affinity: get("x-session-affinity"),
        user_agent,
        client_name,
        client_version,
        api_key,
        request_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::requests::{Message, MessageContent};
    use axum::http::HeaderValue;

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
            prompt_cache_key: None,
            prompt_cache_retention: None,
            extra: serde_json::Map::new(),
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
    fn responses_request_counts_declared_tools() {
        let with_tools = RequestPayload::Responses(crate::models::requests::ResponsesRequest {
            model: "gpt-5.6-sol".to_string(),
            input: serde_json::json!("hi"),
            stream: None,
            extra: serde_json::json!({
                "tools": [
                    {"type": "function", "name": "bash"},
                    {"type": "function", "name": "read_file"},
                ]
            })
            .as_object()
            .cloned()
            .unwrap(),
        });
        assert_eq!(extract_request(&with_tools).tool_count, 2);

        let without_tools = RequestPayload::Responses(crate::models::requests::ResponsesRequest {
            model: "gpt-5.6-sol".to_string(),
            input: serde_json::json!("hi"),
            stream: None,
            extra: serde_json::Map::new(),
        });
        assert_eq!(extract_request(&without_tools).tool_count, 0);
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
            extra: serde_json::Map::new(),
        });
        assert_eq!(extract_request(&payload).prompt, "hello completion");
    }

    #[test]
    fn request_id_prefers_first_present_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-correlation-id", HeaderValue::from_static("corr"));
        headers.insert("request-id", HeaderValue::from_static("req"));
        headers.insert("x-trace-id", HeaderValue::from_static("trace"));
        headers.insert("x-request-id", HeaderValue::from_static("xreq"));
        assert_eq!(header_meta(&headers).request_id.as_deref(), Some("xreq"));

        let mut only_corr = HeaderMap::new();
        only_corr.insert("x-correlation-id", HeaderValue::from_static("corr"));
        assert_eq!(header_meta(&only_corr).request_id.as_deref(), Some("corr"));
    }

    #[test]
    fn request_id_ignores_empty_and_truncates_to_200_chars() {
        let mut empty = HeaderMap::new();
        empty.insert("x-request-id", HeaderValue::from_static("   "));
        empty.insert("x-trace-id", HeaderValue::from_static("  trace  "));
        assert_eq!(header_meta(&empty).request_id.as_deref(), Some("trace"));

        let long = "模".repeat(250);
        let mut headers = HeaderMap::new();
        headers.insert("x-request-id", HeaderValue::from_str(&long).unwrap());
        let got = header_meta(&headers).request_id.unwrap();
        assert_eq!(got.chars().count(), 200);
        assert!(got.starts_with("模"));
    }

    // ---- 会话标识：多别名回退 ----

    #[test]
    fn session_id_falls_back_to_harness_header() {
        // DSH 的 Messages 路由只发这个头；网关此前只认 x-session-id，导致
        // 审计库 SessionId 恒为空。
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-deepseek-harness-session-id",
            HeaderValue::from_static("session-abc"),
        );
        assert_eq!(
            header_meta(&headers).session_id.as_deref(),
            Some("session-abc")
        );
    }

    #[test]
    fn native_session_header_wins_over_alias() {
        let mut headers = HeaderMap::new();
        headers.insert("x-session-id", HeaderValue::from_static("native"));
        headers.insert(
            "x-deepseek-harness-session-id",
            HeaderValue::from_static("harness"),
        );
        assert_eq!(header_meta(&headers).session_id.as_deref(), Some("native"));
    }

    #[test]
    fn session_id_accepts_pi_responses_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("session_id", HeaderValue::from_static("pi-sess"));
        assert_eq!(header_meta(&headers).session_id.as_deref(), Some("pi-sess"));

        let mut headers = HeaderMap::new();
        headers.insert("x-client-request-id", HeaderValue::from_static("pi-req"));
        assert_eq!(header_meta(&headers).session_id.as_deref(), Some("pi-req"));
    }

    #[test]
    fn empty_session_header_is_ignored() {
        let mut headers = HeaderMap::new();
        headers.insert("x-session-id", HeaderValue::from_static("   "));
        headers.insert(
            "x-deepseek-harness-session-id",
            HeaderValue::from_static("fallback"),
        );
        assert_eq!(
            header_meta(&headers).session_id.as_deref(),
            Some("fallback")
        );
    }

    // ---- 请求体会话元数据 ----

    #[test]
    fn body_meta_reads_dsh_session_log() {
        let body = serde_json::json!({
            "model": "deepseek-flash",
            "dsh_session_log": {
                "session": {
                    "id": "session-child",
                    "parentSession": "session-parent",
                    "origin": "subagent",
                    "delegationDepth": 1
                }
            }
        });
        let meta = body_session_meta(&body);
        assert_eq!(meta.session_id.as_deref(), Some("session-child"));
        assert_eq!(meta.parent_session_id.as_deref(), Some("session-parent"));
    }

    #[test]
    fn body_meta_reads_prompt_cache_key() {
        let body = serde_json::json!({
            "model": "m",
            "messages": [],
            "prompt_cache_key": "session-cache-key"
        });
        let meta = body_session_meta(&body);
        assert_eq!(meta.session_id.as_deref(), Some("session-cache-key"));
        assert_eq!(meta.parent_session_id, None);
    }

    #[test]
    fn body_meta_prefers_dsh_session_over_cache_key() {
        let body = serde_json::json!({
            "dsh_session_log": {"session": {"id": "dsh-id", "parentSession": "p"}},
            "prompt_cache_key": "cache-id"
        });
        assert_eq!(
            body_session_meta(&body).session_id.as_deref(),
            Some("dsh-id")
        );
    }

    #[test]
    fn body_meta_is_empty_for_plain_body() {
        let body = serde_json::json!({"model": "m", "messages": []});
        let meta = body_session_meta(&body);
        assert_eq!(meta, BodySessionMeta::default());
    }

    #[test]
    fn body_meta_ignores_blank_values() {
        let body = serde_json::json!({
            "dsh_session_log": {"session": {"id": "   ", "parentSession": ""}},
            "prompt_cache_key": "  "
        });
        assert_eq!(body_session_meta(&body), BodySessionMeta::default());
    }

    #[test]
    fn body_meta_tolerates_wrong_shapes() {
        // dsh_session_log 不是对象、session 是字符串等畸形输入都不得 panic。
        for body in [
            serde_json::json!({"dsh_session_log": "not-an-object"}),
            serde_json::json!({"dsh_session_log": {"session": "nope"}}),
            serde_json::json!({"dsh_session_log": null}),
        ] {
            assert_eq!(body_session_meta(&body), BodySessionMeta::default());
        }
    }

    // ---- System One 分类（Jev）审计字段 ----

    fn systemone(state: serde_json::Value) -> RequestPayload {
        RequestPayload::SystemOne(crate::models::requests::SystemOneRequest {
            model: "typesafe/jev-1.13".to_string(),
            state,
            questions: serde_json::json!({"refund": {"type": "noul"}}),
            extra: serde_json::Map::new(),
        })
    }

    #[test]
    fn systemone_prompt_is_state_text() {
        let r = extract_request(&systemone(serde_json::json!("charged twice")));
        assert_eq!(r.prompt, "charged twice");
        assert_eq!(r.request_tail, "charged twice");
        assert_eq!(r.prompt_bytes, "charged twice".len() as i64);
    }

    #[test]
    fn systemone_parts_array_skips_image_url() {
        // 带图时 state 是 parts 数组：文本 part 进 prompt，image_url 不产生文本。
        let r = extract_request(&systemone(serde_json::json!([
            {"type": "image_url", "image_url": {"url": "data:image/png;base64,AAA"}},
            "what is shown?"
        ])));
        assert_eq!(r.prompt, "what is shown?");
    }

    #[test]
    fn systemone_response_answers_are_recorded() {
        // 分类结果不是文本生成，但审计库 answer 列应能看到结构化答案。
        let response = serde_json::json!({
            "answers": {"refund": {"type": "noul", "noul": 0.98}},
            "usage": {"input_tokens": 12, "output_tokens": 0}
        });
        let r = extract_response(&response);
        assert!(r.answer.contains("\"noul\":0.98"));
        assert_eq!(r.answer_bytes, r.answer.len() as i64);
    }
}
