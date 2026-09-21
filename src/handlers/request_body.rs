use crate::config::types::{ClientConfig, ExtraBodyCached};
use crate::models::requests::RequestPayload;
use serde_json::{json, Value};

/// 合并停止词
fn merge_stop_words(
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
fn adjust_max_tokens(
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
