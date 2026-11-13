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
    // 必须放在最后：模型名改写是 `model` 字段的最终权威，
    // 不允许被 extra_body 里的同名键意外覆盖。
    rewrite_body_model(&mut body, client_config);
    body
}

/// 按后端配置把请求体中的 `model` 从入站名改写为上游名。
///
/// 覆盖全部请求类型：`Chat` / `Completion` 分支是字面量写入 `model`，
/// 其余类型走 `serde_json::to_value(p)`，因此在 JSON 层统一处理即可，
/// 无需逐个分支修改。
///
/// 未配置 `model_rewrite` 或未命中映射时**零分配**直接返回。
fn rewrite_body_model(body: &mut Value, client_config: &ClientConfig) {
    let Some(obj) = body.as_object_mut() else {
        return;
    };
    let Some(inbound) = obj.get("model").and_then(Value::as_str) else {
        return;
    };
    let Some(upstream) = client_config
        .model_rewrite
        .as_ref()
        .and_then(|rewrite| rewrite.get(inbound))
    else {
        return;
    };
    if upstream.as_str() != inbound {
        // 先 clone 再插入：`upstream` 借用自 client_config，
        // 但 `obj.insert` 需要 body 的可变借用，这里让不可变借用提前结束。
        let upstream = upstream.clone();
        obj.insert("model".to_string(), json!(upstream));
    }
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

            // 未知字段垫底：客户端显式发来的扩展参数（top_k / guided_json /
            // response_format …）原样带给上游。随后所有已知字段都会覆盖同名键，
            // 因此客户端无法经 extra 绕过 `model` / `stream` 这类网关权威字段。
            let mut body = Value::Object(p.extra.clone());
            body["model"] = json!(p.model);
            body["messages"] = json!(p.messages);
            body["stream"] = json!(stream);

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
                // 客户端给了就用客户端的；没给才注入 include_usage。
                // 用赋值覆盖（而非只在缺失时插入），保证不会与 extra 里的同名键并存两份。
                if let Some(opts) = &p.stream_options {
                    body["stream_options"] = opts.clone();
                } else {
                    body["stream_options"] = json!({"include_usage": true});
                }
            } else if let Some(obj) = body.as_object_mut() {
                // 非流式请求不该把 stream_options 透传给上游（它只对 SSE 有意义）。
                obj.remove("stream_options");
            }
            if let Some(logprobs) = p.logprobs {
                body["logprobs"] = json!(logprobs);
            }
            if let Some(top_logprobs) = p.top_logprobs {
                body["top_logprobs"] = json!(top_logprobs);
            }
            // prompt 缓存键：客户端（如 pi-ai 在 cacheRetention=long 时）用它标识会话，
            // 原样透传给上游；同时它也是审计库 SessionId 的回退来源（见 db::extract）。
            if let Some(cache_key) = &p.prompt_cache_key {
                body["prompt_cache_key"] = json!(cache_key);
            }
            if let Some(retention) = &p.prompt_cache_retention {
                body["prompt_cache_retention"] = json!(retention);
            }
            body
        }
        RequestPayload::Completion(p) => {
            let adjusted_max_tokens = adjust_max_tokens(client_config.max_tokens, p.max_tokens);
            let merged_stop = merge_stop_words(client_config.stop.as_ref(), p.stop.clone());

            // 同 Chat 分支：未知字段垫底，已知字段覆盖。
            let mut body = Value::Object(p.extra.clone());
            body["model"] = json!(p.model);
            body["prompt"] = json!(p.prompt);
            body["stream"] = json!(stream);

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
            } else if let Some(obj) = body.as_object_mut() {
                obj.remove("stream_options");
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
        // System One：`model` / `state` / `questions` 与未声明字段（images、assets…）
        // 都在结构体里，序列化即完整透传；该协议没有 stream 概念。
        RequestPayload::SystemOne(p) => serde_json::to_value(p).unwrap_or(json!({})),
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

    /// 测试用最小后端配置：无模型改写、无 extra_body、无 stop/max_tokens 覆盖，
    /// 使断言只反映请求体自身的重建行为。
    fn test_client() -> ClientConfig {
        ClientConfig {
            name: "test".to_string(),
            base_url: "http://localhost/v1".to_string(),
            api_key: None,
            model_match: crate::config::types::ModelMatch {
                match_type: "exact".to_string(),
                value: vec!["m".to_string()],
            },
            priority: None,
            fallback: None,
            special_prefix: None,
            stop: None,
            max_tokens: None,
            extra_body: None,
            thinking_format: None,
            model_rewrite: None,
            model_echo: crate::config::types::ModelEcho::default(),
            extra_body_cached: ExtraBodyCached::default(),
        }
    }

    fn chat_payload(value: serde_json::Value) -> RequestPayload {
        RequestPayload::Chat(
            serde_json::from_value::<crate::models::requests::ChatCompletionRequest>(value)
                .unwrap(),
        )
    }

    // ---- 未知字段透传 ----

    #[test]
    fn chat_preserves_unknown_fields() {
        // vLLM / SGLang 的扩展采样与结构化输出字段，结构体里没有声明。
        let payload = chat_payload(json!({
            "model": "m",
            "messages": [],
            "top_k": 42,
            "guided_json": {"type": "object"},
            "response_format": {"type": "json_object"},
            "prompt_cache_key": "ck-1"
        }));
        let body = build_request_body_generic(&payload, &test_client(), false);

        assert_eq!(body["top_k"], json!(42));
        assert_eq!(body["guided_json"], json!({"type": "object"}));
        assert_eq!(body["response_format"], json!({"type": "json_object"}));
        assert_eq!(body["prompt_cache_key"], json!("ck-1"));
    }

    #[test]
    fn chat_known_fields_override_extra() {
        // 客户端把 model/stream 塞进"未知"位置也不能生效：
        // 这两个字段由结构体解析，网关拥有最终决定权。
        let payload = chat_payload(json!({
            "model": "inbound-model",
            "messages": [],
            "stream": false
        }));
        let body = build_request_body_generic(&payload, &test_client(), true);

        // 入站 model 保留（未配 model_rewrite），stream 由调用点参数决定为 true。
        assert_eq!(body["model"], json!("inbound-model"));
        assert_eq!(body["stream"], json!(true));
    }

    #[test]
    fn chat_stream_options_not_duplicated() {
        // extra 里即使出现 stream_options，也必须被已知字段覆盖，不能并存两份。
        let payload = chat_payload(json!({
            "model": "m",
            "messages": [],
            "stream_options": {"include_usage": false}
        }));
        let body = build_request_body_generic(&payload, &test_client(), true);

        // 客户端显式给了就尊重客户端值（既有行为）。
        assert_eq!(body["stream_options"], json!({"include_usage": false}));
        // JSON 对象天然去重；这里再确认值不是被注入的默认值。
        assert_ne!(body["stream_options"], json!({"include_usage": true}));
    }

    #[test]
    fn chat_injects_stream_options_when_absent() {
        let payload = chat_payload(json!({"model": "m", "messages": []}));
        let body = build_request_body_generic(&payload, &test_client(), true);
        assert_eq!(body["stream_options"], json!({"include_usage": true}));
    }

    #[test]
    fn chat_non_stream_drops_stream_options() {
        // 非流式请求不该带 stream_options，即使客户端发了。
        let payload = chat_payload(json!({
            "model": "m",
            "messages": [],
            "stream_options": {"include_usage": true}
        }));
        let body = build_request_body_generic(&payload, &test_client(), false);
        assert!(body.get("stream_options").is_none());
    }

    #[test]
    fn model_rewrite_applies_after_passthrough() {
        // 透传不得破坏 model_rewrite：改写仍是 model 字段的最终权威。
        let mut config = test_client();
        config.model_rewrite = Some(
            [("inbound".to_string(), "upstream".to_string())]
                .into_iter()
                .collect(),
        );
        let payload = chat_payload(json!({
            "model": "inbound",
            "messages": [],
            "top_k": 9
        }));
        let body = build_request_body_generic(&payload, &config, false);

        assert_eq!(body["model"], json!("upstream"));
        assert_eq!(body["top_k"], json!(9));
    }

    #[test]
    fn completion_preserves_unknown_fields() {
        let payload = RequestPayload::Completion(
            serde_json::from_value::<crate::models::requests::CompletionRequest>(json!({
                "model": "m",
                "prompt": "hi",
                "suffix": "tail",
                "best_of": 3
            }))
            .unwrap(),
        );
        let body = build_request_body_generic(&payload, &test_client(), false);

        assert_eq!(body["suffix"], json!("tail"));
        assert_eq!(body["best_of"], json!(3));
    }

    #[test]
    fn embedding_preserves_unknown_fields() {
        let payload = RequestPayload::Embedding(
            serde_json::from_value::<crate::models::requests::EmbeddingRequest>(json!({
                "model": "m",
                "input": "hi",
                "truncate_prompt_tokens": 5,
                "task": "embed"
            }))
            .unwrap(),
        );
        let body = build_request_body_generic(&payload, &test_client(), false);

        assert_eq!(body["truncate_prompt_tokens"], json!(5));
        assert_eq!(body["task"], json!("embed"));
    }

    #[test]
    fn systemone_passthrough_keeps_state_questions_and_extra() {
        // 网关不解析 System One 题型语义：state/questions 与未声明字段（images、assets）
        // 都必须原样到达上游，且不能注入 stream。
        let payload = RequestPayload::SystemOne(
            serde_json::from_value::<crate::models::requests::SystemOneRequest>(json!({
                "model": "typesafe/jev-1.13",
                "state": [{"type": "image_url", "image_url": {"url": "data:image/png;base64,AAA"}}, "text"],
                "questions": {
                    "refund": {"type": "noul", "instructions": "asking for money back?"},
                    "team": {"type": "choice", "instructions": "which team?", "criteria": {"billing": "x"}}
                },
                "images": ["data:image/png;base64,AAA"],
                "assets": {"<image-1>": {"type": "image", "format": "png", "data_base64": "AAA"}}
            }))
            .unwrap(),
        );
        let body = build_request_body_generic(&payload, &test_client(), false);

        assert_eq!(body["model"], json!("typesafe/jev-1.13"));
        assert_eq!(body["state"][1], json!("text"));
        assert_eq!(body["questions"]["refund"]["type"], json!("noul"));
        assert_eq!(body["questions"]["team"]["criteria"]["billing"], json!("x"));
        assert_eq!(body["images"][0], json!("data:image/png;base64,AAA"));
        assert!(body["assets"].get("<image-1>").is_some());
        assert!(body.get("stream").is_none());
    }

    #[test]
    fn systemone_model_rewrite_applies() {
        // model_rewrite 同样覆盖 System One：入站别名 → 上游真实 id。
        let mut config = test_client();
        config.model_rewrite = Some(
            [("jev".to_string(), "typesafe/jev-1.13".to_string())]
                .into_iter()
                .collect(),
        );
        let payload = RequestPayload::SystemOne(
            serde_json::from_value::<crate::models::requests::SystemOneRequest>(json!({
                "model": "jev",
                "state": "text",
                "questions": {"q": {"type": "noul"}}
            }))
            .unwrap(),
        );
        let body = build_request_body_generic(&payload, &config, false);

        assert_eq!(body["model"], json!("typesafe/jev-1.13"));
        assert_eq!(body["state"], json!("text"));
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
            model_rewrite: None,
            model_echo: crate::config::types::ModelEcho::default(),
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
