use super::connection_pool::ConnectionPoolConfig;
use serde::{Deserialize, Serialize};

/// Pre-parsed extra_body cache (set at config load, used at runtime).
#[derive(Debug, Clone, Default)]
pub struct ExtraBodyCached(pub Option<serde_json::Map<String, serde_json::Value>>);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LoadBalancingStrategy {
    /// 基于内容的确定性路由 (Rendezvous Hashing)
    /// 当请求提供 routing_keys 时使用确定性 voting,
    /// 无 routing_keys 时自动回退到 random。
    Deterministic,
    /// 加权随机路由 (Efraimidis-Spirakis 采样)
    Random,
    /// 加权最少连接路由，仅在该策略下读取 ACTIVE_REQUESTS metric
    LeastConnections,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadBalancingConfig {
    #[serde(default)]
    pub strategy: LoadBalancingStrategy,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub name: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub model_match: ModelMatch,
    pub priority: Option<u32>,
    pub fallback: Option<String>,
    pub special_prefix: Option<String>,
    pub stop: Option<Vec<String>>,
    pub max_tokens: Option<u32>,
    /// JSON 字符串，仅当请求未提供同名字段时注入进请求体。
    pub extra_body: Option<String>,
    /// 每后端覆盖全局 thinking_format；None 时回退全局配置
    #[serde(default)]
    pub thinking_format: Option<ThinkingFormat>,

    /// 模型名改写：入站名 → 上游名，仅精确映射。
    /// 映射的键自动并入本后端的可路由模型集（与 `model_match` 取并集），
    /// 因此纯别名后端可以把 `model_match.value` 留空。
    /// 未命中时原样透传入站名。
    #[serde(default)]
    pub model_rewrite: Option<std::collections::HashMap<String, String>>,

    /// 响应体 `model` 字段的回显策略，默认透传上游原名。
    /// `inbound` 会把流式热路径从"逐 chunk 原样透传"变成"逐 chunk JSON 往返"，
    /// 仅在确有必要时开启。
    #[serde(default)]
    pub model_echo: ModelEcho,

    /// 预解析的 extra_body，配置加载时由字符串解析为 Value 树。
    /// 运行时直接使用此缓存，避免每个请求重复 parse。
    #[serde(skip)]
    pub extra_body_cached: ExtraBodyCached,
}

/// 响应体中 `model` 字段的回显策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelEcho {
    /// 原样透传上游返回的模型名（默认，零额外开销）。
    #[default]
    Upstream,
    /// 改回客户端请求时使用的入站名。
    Inbound,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            name: Default::default(),
            base_url: Default::default(),
            api_key: Default::default(),
            model_match: ModelMatch {
                match_type: "exact".to_string(),
                value: Default::default(),
            },
            priority: Default::default(),
            fallback: Default::default(),
            special_prefix: Default::default(),
            stop: Default::default(),
            max_tokens: Default::default(),
            extra_body: Default::default(),
            thinking_format: Default::default(),
            model_rewrite: Default::default(),
            model_echo: Default::default(),
            extra_body_cached: ExtraBodyCached::default(),
        }
    }
}

impl ClientConfig {
    /// 入站模型名是否可路由到本后端。
    ///
    /// 判定为 `model_match` 命中，**或**该名字存在于 `model_rewrite` 的键中。
    /// 后者让"配置了别名却忘记同步 `model_match.value`"不会变成静默不路由。
    pub fn matches_model(&self, inbound: &str) -> bool {
        let by_match = match self.model_match.match_type.as_str() {
            "keyword" => self
                .model_match
                .value
                .iter()
                .any(|keyword| inbound.contains(keyword.as_str())),
            "exact" => self
                .model_match
                .value
                .iter()
                .any(|value| value.as_str() == inbound),
            _ => false,
        };

        by_match
            || self
                .model_rewrite
                .as_ref()
                .is_some_and(|rewrite| rewrite.contains_key(inbound))
    }

    /// 把入站模型名解析为本后端的上游模型名；未配置或未命中时原样返回。
    pub fn resolve_upstream_model<'a>(&'a self, inbound: &'a str) -> &'a str {
        self.model_rewrite
            .as_ref()
            .and_then(|rewrite| rewrite.get(inbound))
            .map(String::as_str)
            .unwrap_or(inbound)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelMatch {
    #[serde(rename = "type")]
    pub match_type: String,
    pub value: Vec<String>,
}

/// Cache configuration for the /v1/models endpoint.
///
/// Default TTL is 600 seconds. Set `ttl_seconds: 0` to disable caching entirely.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ModelsCacheConfig {
    /// Time-to-live in seconds for cached model lists (default: 600).
    /// A value of 0 disables both cache reads and writes.
    #[serde(default = "default_models_cache_ttl")]
    pub ttl_seconds: u64,
}

/// Global backend health and circuit-breaker policy.
///
/// Recovery uses the next real user request as the half-open probe, so this
/// configuration never causes an extra model request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct HealthCheckConfig {
    #[serde(default = "default_health_check_enabled")]
    pub enabled: bool,
    #[serde(default = "default_health_check_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_health_check_cooldown_seconds")]
    pub cooldown_seconds: u64,
    #[serde(default = "default_health_check_max_cooldown_seconds")]
    pub max_cooldown_seconds: u64,
    #[serde(default = "default_health_check_backoff_multiplier")]
    pub backoff_multiplier: f64,
}

const fn default_health_check_enabled() -> bool {
    true
}

const fn default_health_check_failure_threshold() -> u32 {
    3
}

const fn default_health_check_cooldown_seconds() -> u64 {
    30
}

const fn default_health_check_max_cooldown_seconds() -> u64 {
    300
}

const fn default_health_check_backoff_multiplier() -> f64 {
    2.0
}

impl Default for HealthCheckConfig {
    fn default() -> Self {
        Self {
            enabled: default_health_check_enabled(),
            failure_threshold: default_health_check_failure_threshold(),
            cooldown_seconds: default_health_check_cooldown_seconds(),
            max_cooldown_seconds: default_health_check_max_cooldown_seconds(),
            backoff_multiplier: default_health_check_backoff_multiplier(),
        }
    }
}

const fn default_models_cache_ttl() -> u64 {
    600
}

impl Default for ModelsCacheConfig {
    fn default() -> Self {
        Self {
            ttl_seconds: default_models_cache_ttl(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub openai_clients: Vec<ClientConfig>,
    #[serde(default)]
    pub load_balancing: LoadBalancingConfig,
    #[serde(default)]
    pub thinking_format: Option<ThinkingFormat>,
    /// Cache configuration for the /v1/models endpoint.
    /// Set ttl_seconds to 0 to disable caching.
    #[serde(default)]
    pub models_cache: ModelsCacheConfig,
    /// Global backend health and circuit-breaker policy.
    #[serde(default)]
    pub health_check: HealthCheckConfig,
    /// reqwest connection-pool settings, applied once when the shared HTTP client
    /// is built at startup. Hot reload does NOT rebuild the client; restart to apply.
    #[serde(default)]
    pub connection_pool: ConnectionPoolConfig,
    /// Internal generation counter, incremented on each successful hot reload.
    /// Skipped during serialization/deserialization; initialized to 1 by ConfigManager.
    #[serde(skip)]
    pub config_generation: u64,
}

impl Default for LoadBalancingConfig {
    fn default() -> Self {
        Self {
            strategy: LoadBalancingStrategy::Deterministic,
        }
    }
}

impl Default for LoadBalancingStrategy {
    fn default() -> Self {
        Self::Deterministic
    }
}

/// 思考内容归一化目标格式。
/// - `Passthrough`（默认）：不做任何转换，原样透传上游思考内容
/// - `ThinkTag`：统一封装为 `<think>...</think>` 包裹在 content 中
/// - `Reasoning`：统一放入 `reasoning` 字段
/// - `ReasoningContent`：统一放入 `reasoning_content` 字段
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ThinkingFormat {
    Passthrough,
    ThinkTag,
    Reasoning,
    ReasoningContent,
}

impl Default for ThinkingFormat {
    fn default() -> Self {
        Self::Passthrough
    }
}

impl Config {
    /// 解析某后端最终生效的思考格式：优先 per-backend，其次全局，最后 Passthrough。
    pub fn resolve_thinking_format(&self, client: &ClientConfig) -> ThinkingFormat {
        client
            .thinking_format
            .or(self.thinking_format)
            .unwrap_or(ThinkingFormat::Passthrough)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_client_config(thinking_format: Option<ThinkingFormat>) -> ClientConfig {
        ClientConfig {
            name: "test".to_string(),
            base_url: "http://localhost".to_string(),
            api_key: None,
            model_match: ModelMatch {
                match_type: "exact".to_string(),
                value: vec!["test".to_string()],
            },
            priority: None,
            fallback: None,
            special_prefix: None,
            stop: None,
            max_tokens: None,
            extra_body: None,
            thinking_format,
            model_rewrite: None,
            model_echo: ModelEcho::default(),
            extra_body_cached: ExtraBodyCached::default(),
        }
    }

    fn make_config(global_format: Option<ThinkingFormat>) -> Config {
        Config {
            openai_clients: vec![],
            load_balancing: LoadBalancingConfig::default(),
            thinking_format: global_format,
            models_cache: ModelsCacheConfig::default(),
            health_check: HealthCheckConfig::default(),
            connection_pool: ConnectionPoolConfig::default(),
            config_generation: 1,
        }
    }

    #[test]
    fn test_models_cache_default_is_600_seconds() {
        let yaml = r#"
openai_clients: []
"#;
        let cfg: Config = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(cfg.models_cache.ttl_seconds, 600);
    }

    #[test]
    fn test_models_cache_explicit_zero_disables() {
        let yaml = r#"
openai_clients: []
models_cache:
  ttl_seconds: 0
"#;
        let cfg: Config = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(cfg.models_cache.ttl_seconds, 0);
    }

    #[test]
    fn test_health_check_defaults_when_omitted_from_yaml() {
        let config: Config = serde_yaml::from_str("openai_clients: []").unwrap();

        assert!(config.health_check.enabled);
        assert_eq!(config.health_check.failure_threshold, 3);
        assert_eq!(config.health_check.cooldown_seconds, 30);
        assert_eq!(config.health_check.max_cooldown_seconds, 300);
        assert_eq!(config.health_check.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_models_cache_custom_value() {
        let yaml = r#"
openai_clients: []
models_cache:
  ttl_seconds: 42
"#;
        let cfg: Config = serde_yaml::from_str(yaml).expect("parse");
        assert_eq!(cfg.models_cache.ttl_seconds, 42);
    }

    #[test]
    fn test_resolve_per_backend_overrides_global() {
        let cfg = make_config(Some(ThinkingFormat::ThinkTag));
        let client = make_client_config(Some(ThinkingFormat::Reasoning));
        assert_eq!(
            cfg.resolve_thinking_format(&client),
            ThinkingFormat::Reasoning
        );
    }

    #[test]
    fn test_resolve_global_when_per_backend_none() {
        let cfg = make_config(Some(ThinkingFormat::ReasoningContent));
        let client = make_client_config(None);
        assert_eq!(
            cfg.resolve_thinking_format(&client),
            ThinkingFormat::ReasoningContent
        );
    }

    #[test]
    fn test_resolve_passthrough_when_both_none() {
        let cfg = make_config(None);
        let client = make_client_config(None);
        assert_eq!(
            cfg.resolve_thinking_format(&client),
            ThinkingFormat::Passthrough
        );
    }

    // --- 模型名改写 (model_rewrite) ---

    fn client_with_rewrite(
        match_type: &str,
        matched: &[&str],
        rewrite: &[(&str, &str)],
    ) -> ClientConfig {
        ClientConfig {
            model_match: ModelMatch {
                match_type: match_type.to_string(),
                value: matched.iter().map(|s| s.to_string()).collect(),
            },
            model_rewrite: Some(
                rewrite
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            ..ClientConfig::default()
        }
    }

    #[test]
    fn test_model_rewrite_defaults_to_none_and_echo_upstream() {
        let yaml = r#"
openai_clients:
  - name: plain
    base_url: "https://example.com/v1"
    model_match:
      type: exact
      value: ["m"]
"#;
        let cfg: Config = serde_yaml::from_str(yaml).expect("parse");
        let client = &cfg.openai_clients[0];
        assert!(client.model_rewrite.is_none());
        assert_eq!(client.model_echo, ModelEcho::Upstream);
    }

    #[test]
    fn test_model_rewrite_parsed_from_yaml() {
        let yaml = r#"
openai_clients:
  - name: hapiopen
    base_url: "https://hapiopen.cc/v1"
    model_match:
      type: exact
      value: []
    model_rewrite:
      "deepseek-flash": "deepseek-v4.1-flash"
    model_echo: inbound
"#;
        let cfg: Config = serde_yaml::from_str(yaml).expect("parse");
        let client = &cfg.openai_clients[0];
        let rewrite = client.model_rewrite.as_ref().expect("rewrite present");
        assert_eq!(
            rewrite.get("deepseek-flash").map(String::as_str),
            Some("deepseek-v4.1-flash")
        );
        assert_eq!(client.model_echo, ModelEcho::Inbound);
    }

    #[test]
    fn test_resolve_upstream_model_hit_miss_and_absent() {
        let client =
            client_with_rewrite("exact", &[], &[("deepseek-flash", "deepseek-v4.1-flash")]);
        assert_eq!(
            client.resolve_upstream_model("deepseek-flash"),
            "deepseek-v4.1-flash"
        );
        // 未命中映射：原样返回
        assert_eq!(
            client.resolve_upstream_model("deepseek-v4.1-flash"),
            "deepseek-v4.1-flash"
        );

        // 完全没有配置 model_rewrite：原样返回
        let plain = ClientConfig::default();
        assert!(plain.model_rewrite.is_none());
        assert_eq!(plain.resolve_upstream_model("any-model"), "any-model");
    }

    #[test]
    fn test_matches_model_exact_and_alias_union() {
        // model_match 为空，别名键仍应可路由
        let alias_only =
            client_with_rewrite("exact", &[], &[("deepseek-flash", "deepseek-v4.1-flash")]);
        assert!(alias_only.matches_model("deepseek-flash"));
        assert!(!alias_only.matches_model("unknown-model"));
        // 上游真名既不在 model_match 也不在映射键中 → 不可路由
        assert!(!alias_only.matches_model("deepseek-v4.1-flash"));

        // 显式列出的真名 + 别名，两者都可路由
        let both = client_with_rewrite(
            "exact",
            &["deepseek-v4.1-flash"],
            &[("deepseek-flash", "deepseek-v4.1-flash")],
        );
        assert!(both.matches_model("deepseek-flash"));
        assert!(both.matches_model("deepseek-v4.1-flash"));
    }

    #[test]
    fn test_matches_model_keyword_and_unknown_match_type() {
        let keyword = client_with_rewrite("keyword", &["deepseek"], &[]);
        assert!(keyword.matches_model("deepseek-flash"));
        assert!(keyword.matches_model("my-deepseek-v4"));
        assert!(!keyword.matches_model("gpt-4"));

        // 未知 match_type 恒为 false（与既有实现一致），但别名键仍生效
        let unknown = client_with_rewrite("regex", &["deepseek"], &[("alias", "target")]);
        assert!(!unknown.matches_model("deepseek-flash"));
        assert!(unknown.matches_model("alias"));
    }
}
