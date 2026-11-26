//! 运行期观测到的“入站模型 → 后端”组合。
//!
//! `/metrics` 需要 `model` 维度才能把后端健康状态挂到具体模型上，但配置里的
//! `model_match: keyword` 无法反推出具体模型名（只要模型名包含关键字即可路由），
//! 因此这里在路由解析阶段记录实际出现过的组合——既覆盖 keyword，也覆盖配置里
//! 未显式声明的模型。
//!
//! 热路径开销：每个候选后端一次 `DashMap` 查找；已记录的组合零分配。

use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tracing::warn;

/// 上限用于防止异常/任意的入站模型名把内存与 Prometheus 基数撑爆。
const MAX_MODELS: usize = 512;
const MAX_BACKENDS_PER_MODEL: usize = 64;

static MODEL_BACKENDS: Lazy<DashMap<String, DashMap<String, ()>>> = Lazy::new(DashMap::new);
static MODEL_COUNT: AtomicUsize = AtomicUsize::new(0);
static CAP_WARNED: AtomicBool = AtomicBool::new(false);

/// 记录一次路由解析中出现的（模型，后端）组合。
pub fn record(model: &str, backend: &str) {
    if model.is_empty() || backend.is_empty() {
        return;
    }

    if let Some(backends) = MODEL_BACKENDS.get(model) {
        if !backends.contains_key(backend) && backends.len() < MAX_BACKENDS_PER_MODEL {
            backends.insert(backend.to_string(), ());
        }
        return;
    }

    if MODEL_COUNT.load(Ordering::Relaxed) >= MAX_MODELS {
        if !CAP_WARNED.swap(true, Ordering::Relaxed) {
            warn!(
                max_models = MAX_MODELS,
                "observed model/backend cardinality cap reached; new models are not exported in /metrics"
            );
        }
        return;
    }

    let backends = DashMap::new();
    backends.insert(backend.to_string(), ());
    // 并发下两个线程可能同时为同一个模型建表，只有先插入成功者计数。
    if MODEL_BACKENDS.insert(model.to_string(), backends).is_none() {
        MODEL_COUNT.fetch_add(1, Ordering::Relaxed);
    }
}

/// 导出当前观测到的所有组合（按 model、backend 排序，便于稳定输出与测试）。
pub fn snapshot() -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    for model in MODEL_BACKENDS.iter() {
        for backend in model.value().iter() {
            pairs.push((model.key().clone(), backend.key().clone()));
        }
    }
    pairs.sort();
    pairs.dedup();
    pairs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_pairs_without_duplicates() {
        let model = "test-model-backends-record";
        record(model, "backend-a");
        record(model, "backend-a");
        record(model, "backend-b");

        let pairs = snapshot();
        assert!(pairs.contains(&(model.to_string(), "backend-a".to_string())));
        assert!(pairs.contains(&(model.to_string(), "backend-b".to_string())));
        assert_eq!(
            pairs
                .iter()
                .filter(|(name, backend)| name == model && backend == "backend-a")
                .count(),
            1
        );
    }

    #[test]
    fn ignores_empty_names() {
        record("", "backend");
        record("model", "");
        assert!(snapshot()
            .iter()
            .all(|(model, backend)| !model.is_empty() && !backend.is_empty()));
    }
}
