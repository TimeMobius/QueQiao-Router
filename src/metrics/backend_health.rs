//! 按“模型 × 后端”导出熔断健康状态的 Prometheus 指标。
//!
//! 指标族：
//!
//! | 指标 | 标签 | 含义 |
//! | :--- | :--- | :--- |
//! | `gpt_api_model_backend_health` | `model,backend` | `1` 可接流量（closed/disabled），`0` 冷却中或半开 |
//! | `gpt_api_model_backend_circuit_state` | `model,backend` | `0` closed / `1` open / `2` half_open / `3` disabled |
//! | `gpt_api_model_backend_cooldown_remaining_seconds` | `model,backend` | 距可再次探测的剩余秒数 |
//! | `gpt_api_backend_health` | `backend` | 同上，不依赖模型维度（keyword 冷启动兜底） |
//! | `gpt_api_backend_circuit_state` | `backend` | 同上 |
//! | `gpt_api_backend_cooldown_remaining_seconds` | `backend` | 同上 |
//!
//! 模型维度取并集：
//! 1. 配置中 `model_match.type: exact` 的模型名与 `model_rewrite` 的别名（配置即承诺）；
//! 2. 运行期实际路由过的（模型，后端）组合（见 [`crate::metrics::model_backends`]），
//!    覆盖 `keyword` 匹配出、配置里无法枚举的模型名。

use std::collections::{BTreeSet, HashMap};

use once_cell::sync::Lazy;
use prometheus::{register_gauge_vec, GaugeVec};

use crate::config::types::{ClientConfig, Config};
use crate::services::circuit_breaker::{BackendCircuitState, BackendHealth};

static MODEL_BACKEND_HEALTH: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_model_backend_health",
        "Backend health for a model (1 = can take traffic, 0 = cooling down or half-open)",
        &["model", "backend"]
    )
    .unwrap()
});

static MODEL_BACKEND_CIRCUIT_STATE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_model_backend_circuit_state",
        "Circuit state of the backend serving a model: 0=closed, 1=open, 2=half_open, 3=disabled",
        &["model", "backend"]
    )
    .unwrap()
});

static MODEL_BACKEND_COOLDOWN_REMAINING: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_model_backend_cooldown_remaining_seconds",
        "Seconds until the backend serving a model is eligible for a half-open probe",
        &["model", "backend"]
    )
    .unwrap()
});

static BACKEND_HEALTH: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_backend_health",
        "Backend health without the model dimension (1 = can take traffic, 0 = cooling down or half-open)",
        &["backend"]
    )
    .unwrap()
});

static BACKEND_CIRCUIT_STATE: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_backend_circuit_state",
        "Circuit state of a backend: 0=closed, 1=open, 2=half_open, 3=disabled",
        &["backend"]
    )
    .unwrap()
});

static BACKEND_COOLDOWN_REMAINING: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_backend_cooldown_remaining_seconds",
        "Seconds until a backend is eligible for a half-open probe",
        &["backend"]
    )
    .unwrap()
});

/// 以当前配置与熔断快照刷新所有健康指标。
///
/// 调用方负责与 `prometheus::gather()` 串行执行（见 [`crate::metrics::exporter`]），
/// 避免 `reset()` 重建序列时被并发抓取到半成品。
pub fn update(config: &Config, health: &[BackendHealth]) {
    let by_backend: HashMap<&str, &BackendHealth> = health
        .iter()
        .map(|item| (item.name.as_str(), item))
        .collect();
    let observed = crate::metrics::model_backends::snapshot();
    let pairs = collect_pairs(config, &observed);

    reset();

    // 后端维度：覆盖所有配置后端（哪怕从未失败、注册表里没有条目）。
    for client in &config.openai_clients {
        write_series(
            None,
            &client.name,
            by_backend.get(client.name.as_str()).copied(),
            config.health_check.enabled,
        );
    }

    // 模型 × 后端维度。
    for (model, backend) in &pairs {
        write_series(
            Some(model),
            backend,
            by_backend.get(backend.as_str()).copied(),
            config.health_check.enabled,
        );
    }
}

fn reset() {
    MODEL_BACKEND_HEALTH.reset();
    MODEL_BACKEND_CIRCUIT_STATE.reset();
    MODEL_BACKEND_COOLDOWN_REMAINING.reset();
    BACKEND_HEALTH.reset();
    BACKEND_CIRCUIT_STATE.reset();
    BACKEND_COOLDOWN_REMAINING.reset();
}

fn write_series(
    model: Option<&str>,
    backend: &str,
    health: Option<&BackendHealth>,
    breaker_enabled: bool,
) {
    let (state, cooldown_seconds) = match health {
        Some(item) => (item.state, item.cooldown_remaining.as_secs_f64()),
        None => (default_state(breaker_enabled), 0.0),
    };
    let health_value = health_value(state);
    let state_code = f64::from(state.code());

    match model {
        Some(model) => {
            MODEL_BACKEND_HEALTH
                .with_label_values(&[model, backend])
                .set(health_value);
            MODEL_BACKEND_CIRCUIT_STATE
                .with_label_values(&[model, backend])
                .set(state_code);
            MODEL_BACKEND_COOLDOWN_REMAINING
                .with_label_values(&[model, backend])
                .set(cooldown_seconds);
        }
        None => {
            BACKEND_HEALTH
                .with_label_values(&[backend])
                .set(health_value);
            BACKEND_CIRCUIT_STATE
                .with_label_values(&[backend])
                .set(state_code);
            BACKEND_COOLDOWN_REMAINING
                .with_label_values(&[backend])
                .set(cooldown_seconds);
        }
    }
}

/// 注册表里没有条目 = 从未失败；熔断关闭时没有健康判定，统一记为 disabled。
fn default_state(breaker_enabled: bool) -> BackendCircuitState {
    if breaker_enabled {
        BackendCircuitState::Closed
    } else {
        BackendCircuitState::Disabled
    }
}

/// `1` 只代表“现在可以接流量”：closed 正常、disabled 不做判定；
/// open（冷却中）与 half_open（等待探测/探测中）都记 `0`。
fn health_value(state: BackendCircuitState) -> f64 {
    match state {
        BackendCircuitState::Closed | BackendCircuitState::Disabled => 1.0,
        BackendCircuitState::Open | BackendCircuitState::HalfOpen => 0.0,
    }
}

/// 配置里能确定的模型名：`exact` 的精确值 + `model_rewrite` 的别名键。
///
/// `keyword` 的值是匹配片段而非模型名，不参与枚举；这类模型靠运行期观测补全。
fn declared_models(client: &ClientConfig) -> Vec<&str> {
    let mut models: Vec<&str> = Vec::new();
    if client.model_match.match_type == "exact" {
        models.extend(
            client
                .model_match
                .value
                .iter()
                .map(String::as_str)
                .filter(|model| !model.is_empty()),
        );
    }
    if let Some(rewrite) = &client.model_rewrite {
        models.extend(
            rewrite
                .keys()
                .map(String::as_str)
                .filter(|model| !model.is_empty()),
        );
    }
    models
}

/// 合并“配置声明”和“运行期观测”，并丢弃已失效的组合：
/// 后端不在当前配置里、或该模型已不再能路由到该后端（配置热重载后）时排除。
fn collect_pairs(config: &Config, observed: &[(String, String)]) -> BTreeSet<(String, String)> {
    let clients: HashMap<&str, &ClientConfig> = config
        .openai_clients
        .iter()
        .map(|client| (client.name.as_str(), client))
        .collect();

    let mut pairs = BTreeSet::new();
    for client in &config.openai_clients {
        for model in declared_models(client) {
            pairs.insert((model.to_string(), client.name.clone()));
        }
    }

    for (model, backend) in observed {
        let Some(client) = clients.get(backend.as_str()) else {
            continue;
        };
        if client.matches_model(model) {
            pairs.insert((model.clone(), backend.clone()));
        }
    }

    pairs
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::connection_pool::ConnectionPoolConfig;
    use crate::config::types::{HealthCheckConfig, ModelsCacheConfig};
    use crate::config::types::{ModelMatch, ThinkingFormat};

    fn client(
        name: &str,
        match_type: &str,
        values: &[&str],
        rewrites: &[(&str, &str)],
    ) -> ClientConfig {
        ClientConfig {
            name: name.to_string(),
            base_url: format!("https://{name}.example.com/v1"),
            model_match: ModelMatch {
                match_type: match_type.to_string(),
                value: values.iter().map(|value| value.to_string()).collect(),
            },
            model_rewrite: (!rewrites.is_empty()).then(|| {
                rewrites
                    .iter()
                    .map(|(from, to)| (from.to_string(), to.to_string()))
                    .collect()
            }),
            ..ClientConfig::default()
        }
    }

    fn config(clients: Vec<ClientConfig>) -> Config {
        Config {
            openai_clients: clients,
            load_balancing: Default::default(),
            thinking_format: None::<ThinkingFormat>,
            models_cache: ModelsCacheConfig::default(),
            health_check: HealthCheckConfig::default(),
            tools_proxy: Default::default(),
            connection_pool: ConnectionPoolConfig::default(),
            config_generation: 1,
        }
    }

    #[test]
    fn declared_models_includes_exact_and_aliases_but_not_keywords() {
        let exact = client("a", "exact", &["m1", "m2"], &[("alias", "upstream")]);
        assert_eq!(declared_models(&exact), vec!["m1", "m2", "alias"]);

        let keyword = client("b", "keyword", &["gpt-4"], &[]);
        assert!(declared_models(&keyword).is_empty());
    }

    #[test]
    fn collect_pairs_unions_config_and_observed_and_filters_stale() {
        let config = config(vec![
            client("a", "exact", &["m1"], &[("alias", "upstream")]),
            client("b", "keyword", &["kw"], &[]),
        ]);
        let observed = vec![
            ("m1".to_string(), "a".to_string()),
            ("kw-model".to_string(), "b".to_string()),
            ("stale-model".to_string(), "b".to_string()),
            ("alias".to_string(), "removed-backend".to_string()),
        ];

        let pairs = collect_pairs(&config, &observed);

        assert!(pairs.contains(&("m1".to_string(), "a".to_string())));
        assert!(pairs.contains(&("alias".to_string(), "a".to_string())));
        // keyword 后端靠运行期观测出现。
        assert!(pairs.contains(&("kw-model".to_string(), "b".to_string())));
        // 不匹配该后端、或后端已不在配置里，都不导出。
        assert!(!pairs.contains(&("stale-model".to_string(), "b".to_string())));
        assert!(!pairs
            .iter()
            .any(|(_, backend)| backend == "removed-backend"));
    }

    #[test]
    fn health_value_matches_documented_semantics() {
        assert_eq!(health_value(BackendCircuitState::Closed), 1.0);
        assert_eq!(health_value(BackendCircuitState::Disabled), 1.0);
        assert_eq!(health_value(BackendCircuitState::Open), 0.0);
        assert_eq!(health_value(BackendCircuitState::HalfOpen), 0.0);
        assert_eq!(BackendCircuitState::Open.code(), 1);
    }
}
