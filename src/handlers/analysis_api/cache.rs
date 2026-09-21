//! 缓存：15s TTL 的进程内结果缓存与统一错误响应构造。

use axum::http::StatusCode;
use once_cell::sync::Lazy;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::params::AnalysisParams;

const CACHE_TTL: Duration = Duration::from_secs(15);
const CACHE_MAX_ENTRIES: usize = 256;

/// 15s TTL 的进程内结果缓存；键为规范化查询串，插入时清理过期项并设上限。
static CACHE: Lazy<Mutex<HashMap<String, (Instant, Value)>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub(super) fn cache_key(p: &AnalysisParams, kind: &str) -> String {
    format!("{kind}:{}", serde_json::to_string(p).unwrap_or_default())
}

pub(super) fn cache_get(key: &str) -> Option<Value> {
    let mut cache = CACHE.lock().ok()?;
    let expired = cache
        .get(key)
        .map(|(t, _)| t.elapsed() >= CACHE_TTL)
        .unwrap_or(false);
    if expired {
        cache.remove(key);
        return None;
    }
    cache.get(key).map(|(_, v)| v.clone())
}

pub(super) fn cache_put(key: String, value: Value) {
    if let Ok(mut cache) = CACHE.lock() {
        cache.retain(|_, (t, _)| t.elapsed() < CACHE_TTL);
        if cache.len() >= CACHE_MAX_ENTRIES {
            cache.clear();
        }
        cache.insert(key, (Instant::now(), value));
    }
}

pub(super) fn internal<E: std::fmt::Display>(e: E) -> (StatusCode, String) {
    tracing::error!("analysis api error: {e}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "query failed".to_string(),
    )
}
