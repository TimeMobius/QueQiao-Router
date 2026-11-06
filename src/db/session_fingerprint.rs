//! 会话时间线重建：给**没有会话标识**的记录推导一个稳定的会话键。
//!
//! ## 为什么需要
//!
//! 网关的 `SessionId` 列来自请求头（`x-session-id` 及其别名）与请求体
//! （`dsh_session_log.session`、`prompt_cache_key`，见 [`crate::db::extract::body_session_meta`]）。
//! 但并非所有客户端都给得出标识：DSH 的 pi-ai / OpenAI chat-completions 路由在
//! 非 OpenRouter 目标上**一个会话头都不发**，body 里也没有缓存键（除非用户显式
//! 配 `cacheRetention: long`）。这些记录的 `SessionId` 为空，面板的「会话时间线」
//! 按钮因此被禁用。
//!
//! ## 怎么做
//!
//! Agent 类客户端（DSH / pi / Cline 等）每一轮都把**完整历史**重发，且历史前缀
//! 在会话内保持不变：system 提示词逐字节稳定，首条提问也不会被改写。因此
//! 「system + 首条 message 的哈希」在同一个会话内恒定，在会话之间几乎必然不同
//! （不同会话的 system 提示词通常不同，工具清单/指令随会话内容变化）。
//!
//! **只取首条、不取更多**是必须的：历史只增不减，第 1 轮只有 1 条 message，
//! 第 2 轮才有多条。取固定的 N>1 会让同一会话在不同轮次得到不同指纹，
//! 时间线会碎成一堆单条记录（这个缺陷被 `growing_history_keeps_the_same_fingerprint`
//! 回归用例锁住）。
//!
//! 指纹是**查询时现场计算**的派生量：不写数据库、不加列。代价是每条记录都要从
//! `payloads` 表解压请求体，因此配一层进程内 LRU 缓存（记录一旦落库就不再变化，
//! 指纹可以安全缓存）。
//!
//! ## 局限（必须在 UI 上说明）
//!
//! - 会话被**压缩**（compaction）后前缀改变，时间线会断成多段。
//! - 前缀完全相同的并发 fork（如同一 system + 同一首条提问）会被合并成一个会话。
//! - 只对「每轮重发全量历史」的客户端有效；一次性单轮请求会各自成为独立会话。
//!
//! 因此它**只作为回退**：记录已有 `SessionId` 时永远以真实 id 为准。

use dashmap::DashMap;
use once_cell::sync::Lazy;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, Ordering};

/// 单个字段参与哈希的字节上限，避免超大 message（内联 base64 图片）拖慢计算。
const FIELD_BYTE_CAP: usize = 8 * 1024;

/// 缓存容量（记录条数）。每条只存 16 字节十六进制串，内存可忽略。
const CACHE_CAPACITY: usize = 50_000;

/// `record_id -> fingerprint` 的进程内缓存。
///
/// 记录写入后不再变化，所以缓存无需失效逻辑；容量满时整体清空（比实现真正的
/// LRU 简单得多，且重建成本只是重新解压一次请求体）。
static CACHE: Lazy<DashMap<i64, String>> = Lazy::new(DashMap::new);
static HITS: AtomicU64 = AtomicU64::new(0);
static MISSES: AtomicU64 = AtomicU64::new(0);

/// 取一个字段的稳定文本表示并施加字节上限。
fn capped_text(value: &Value) -> String {
    let raw = match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    if raw.len() <= FIELD_BYTE_CAP {
        return raw;
    }
    let mut end = FIELD_BYTE_CAP;
    while end > 0 && !raw.is_char_boundary(end) {
        end -= 1;
    }
    raw[..end].to_string()
}

/// 从**已解析的请求体**计算对话前缀指纹。
///
/// 返回 16 位十六进制串；请求体里没有可用 message 时返回 `None`
/// （例如 embeddings / rerank，或正文缺失的历史记录）。
///
/// 哈希输入只有三样：归一化后的 system 文本、首条非 system message 的 role 与
/// content。三者都是会话内恒定、会话间易变的量。
///
/// system 的两种形态（顶层 `system` 字段 / 首条 `system` message）会被归一化到
/// 同一个槽位，否则同一会话在 Anthropic 与 OpenAI 两种形态下会算出不同指纹。
pub fn conversation_fingerprint(body: &Value) -> Option<String> {
    let messages = body
        .get("messages")
        .or_else(|| body.get("input"))
        .and_then(Value::as_array)?;

    // 归一化：顶层 `system` 优先；否则取首条 system/developer message 的内容。
    // 该 message 不计入前缀 message 名额。
    let top_level_system = body.get("system").map(capped_text);
    let leading_system = messages
        .iter()
        .take_while(|m| {
            matches!(
                m.get("role").and_then(Value::as_str),
                Some("system") | Some("developer")
            )
        })
        .last()
        .map(|m| capped_text(m.get("content").unwrap_or(&Value::Null)));
    let system = top_level_system.or(leading_system).unwrap_or_default();

    // 第一条非 system message 才是会话的「开场白」。
    let first = messages.iter().find(|m| {
        !matches!(
            m.get("role").and_then(Value::as_str),
            Some("system") | Some("developer")
        )
    })?;

    let mut hasher = Sha256::new();
    hasher.update(system.as_bytes());
    hasher.update(b"\x1f");
    hasher.update(
        first
            .get("role")
            .and_then(Value::as_str)
            .unwrap_or("")
            .as_bytes(),
    );
    hasher.update(b"\x1e");
    hasher.update(capped_text(first.get("content").unwrap_or(&Value::Null)).as_bytes());

    Some(
        hasher.finalize()[..8]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect(),
    )
}

/// 带缓存的版本：按 `record_id` 缓存结果。
///
/// `body` 是**已经解压好的**请求体（调用方负责从 `payloads` 取），本函数只做
/// 哈希与缓存。`None` 结果不缓存（正文可能因归档/修复而补上，且负缓存收益低）。
pub fn fingerprint_cached(record_id: i64, body: &Value) -> Option<String> {
    if let Some(hit) = CACHE.get(&record_id) {
        HITS.fetch_add(1, Ordering::Relaxed);
        return Some(hit.clone());
    }
    let computed = conversation_fingerprint(body)?;
    MISSES.fetch_add(1, Ordering::Relaxed);
    if CACHE.len() >= CACHE_CAPACITY {
        CACHE.clear();
    }
    CACHE.insert(record_id, computed.clone());
    Some(computed)
}

/// 缓存统计 `(hits, misses, entries)`，供排障使用。
pub fn cache_stats() -> (u64, u64, usize) {
    (
        HITS.load(Ordering::Relaxed),
        MISSES.load(Ordering::Relaxed),
        CACHE.len(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn chat(history: Value, system: Option<&str>) -> Value {
        let mut body = json!({ "model": "m", "messages": history });
        if let Some(system) = system {
            body["system"] = json!(system);
        }
        body
    }

    #[test]
    fn growing_history_keeps_the_same_fingerprint() {
        // 这是最关键的回归用例：历史只增不减，同一会话的每一轮都必须算出同一指纹。
        // 第 1 轮只有 1 条 message，后续轮次有 3、5 条。
        let round1 = chat(
            json!([
                {"role": "system", "content": "you are a bot"},
                {"role": "user", "content": "first question"},
            ]),
            None,
        );
        let round2 = chat(
            json!([
                {"role": "system", "content": "you are a bot"},
                {"role": "user", "content": "first question"},
                {"role": "assistant", "content": "answer"},
                {"role": "user", "content": "second question"},
            ]),
            None,
        );
        let round3 = chat(
            json!([
                {"role": "system", "content": "you are a bot"},
                {"role": "user", "content": "first question"},
                {"role": "assistant", "content": "answer"},
                {"role": "user", "content": "second question"},
                {"role": "assistant", "content": "answer 2"},
                {"role": "user", "content": "third question"},
            ]),
            None,
        );
        let a = conversation_fingerprint(&round1);
        assert!(a.is_some());
        assert_eq!(a, conversation_fingerprint(&round2));
        assert_eq!(a, conversation_fingerprint(&round3));
    }

    #[test]
    fn system_in_top_level_matches_system_message() {
        // 同一份内容，放在顶层 `system` 或作为首条 message，应得同一指纹
        // （否则 Anthropic 与 OpenAI 两种形态的同一会话会分裂）。
        let as_top_level = chat(
            json!([{"role": "user", "content": "q1"}]),
            Some("you are a bot"),
        );
        let as_message = chat(
            json!([
                {"role": "system", "content": "you are a bot"},
                {"role": "user", "content": "q1"},
            ]),
            None,
        );
        assert_eq!(
            conversation_fingerprint(&as_top_level),
            conversation_fingerprint(&as_message)
        );
    }

    #[test]
    fn different_system_yields_different_fingerprint() {
        let history = json!([{"role": "user", "content": "same opening line"}]);
        let a = chat(history.clone(), Some("system A"));
        let b = chat(history, Some("system B"));
        assert_ne!(conversation_fingerprint(&a), conversation_fingerprint(&b));
    }

    #[test]
    fn different_first_question_yields_different_fingerprint() {
        let a = chat(
            json!([{"role": "user", "content": "question A"}]),
            Some("s"),
        );
        let b = chat(
            json!([{"role": "user", "content": "question B"}]),
            Some("s"),
        );
        assert_ne!(conversation_fingerprint(&a), conversation_fingerprint(&b));
    }

    #[test]
    fn content_beyond_first_message_does_not_matter() {
        let base: Vec<Value> = vec![
            json!({"role": "user", "content": "q1"}),
            json!({"role": "assistant", "content": "a1"}),
        ];
        let mut extended = base.clone();
        extended.push(json!({"role": "user", "content": "q2"}));
        extended.push(json!({"role": "assistant", "content": "a2"}));
        assert_eq!(
            conversation_fingerprint(&chat(json!(base), Some("s"))),
            conversation_fingerprint(&chat(json!(extended), Some("s")))
        );
    }

    #[test]
    fn real_world_bodies_from_live_gateway_match() {
        // 这两份 body 直接取自 e2e 实例入库的请求体（id=5 / id=6）：
        // 同一会话的第 1 轮与第 2 轮，首条 message 相同。
        let turn1 = json!({
            "messages": [{"content": "start task alpha", "role": "user"}],
            "model": "mock-model",
            "stream": false
        });
        let turn2 = json!({
            "messages": [
                {"content": "start task alpha", "role": "user"},
                {"content": "ok", "role": "assistant"},
                {"content": "step 2", "role": "user"}
            ],
            "model": "mock-model",
            "stream": false
        });
        assert_eq!(
            conversation_fingerprint(&turn1),
            conversation_fingerprint(&turn2)
        );
    }

    #[test]
    fn missing_messages_returns_none() {
        assert_eq!(conversation_fingerprint(&json!({"model": "m"})), None);
        assert_eq!(
            conversation_fingerprint(&json!({"model": "m", "messages": []})),
            None
        );
    }

    #[test]
    fn empty_history_of_only_system_returns_none() {
        let body = chat(json!([{"role": "system", "content": "only system"}]), None);
        assert_eq!(conversation_fingerprint(&body), None);
    }

    #[test]
    fn responses_input_array_is_supported() {
        let body = json!({
            "model": "m",
            "input": [{"role": "user", "content": "hello"}],
        });
        assert!(conversation_fingerprint(&body).is_some());
    }

    #[test]
    fn cache_returns_stable_value_per_record() {
        let body = chat(json!([{"role": "user", "content": "cached"}] ), Some("s"));
        let first = fingerprint_cached(9_000_001, &body).unwrap();
        let second = fingerprint_cached(9_000_001, &body).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.len(), 16);
    }

    #[test]
    fn cache_ignores_body_change_for_same_record() {
        // 记录一旦落库就不再变化；缓存按 record_id 命中是预期行为。
        let a = chat(json!([{"role": "user", "content": "a"}]), Some("s"));
        let b = chat(json!([{"role": "user", "content": "b"}]), Some("s"));
        let first = fingerprint_cached(9_000_002, &a).unwrap();
        let second = fingerprint_cached(9_000_002, &b).unwrap();
        assert_eq!(first, second);
    }

    #[test]
    fn large_content_is_capped_but_still_hashes() {
        let huge = "x".repeat(FIELD_BYTE_CAP * 3);
        let body = chat(json!([{"role": "user", "content": huge}]), Some("s"));
        let fp = conversation_fingerprint(&body).unwrap();
        assert_eq!(fp.len(), 16);
    }

    #[test]
    fn multibyte_content_truncates_on_char_boundary() {
        // 让截断点落在多字节字符中间，验证不会 panic。
        let body = chat(
            json!([{"role": "user", "content": "中".repeat(FIELD_BYTE_CAP)}]),
            Some("系统提示"),
        );
        assert!(conversation_fingerprint(&body).is_some());
    }
}
