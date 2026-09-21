use crate::models::requests::{Message, MessageContent};
use once_cell::sync::Lazy;
use rayon::prelude::*;
use regex::Regex;

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

static THINK_TAG_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?s)<think>.*?</think>").unwrap());

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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
            tool_calls: Some(json!([{"id": "call_123"}])),
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
            tool_calls: Some(json!([{"id": "call_123"}])),
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
                tool_calls: Some(json!([{"id": "call_1", "type": "function"}])),
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
    fn test_remove_think_tags_multiline() {
        let messages = vec![create_message(
            "assistant",
            "<think>first line\nsecond line</think>actual response",
        )];

        let result = remove_think_tags(messages);

        assert_eq!(
            result[0].content.as_ref().unwrap(),
            &MessageContent::String("actual response".to_string())
        );
    }
}
