use serde::{Deserialize, Serialize};
use std::time::Duration;

/// reqwest connection-pool settings, applied once when the shared HTTP client is
/// built at startup.
///
/// `ClientManager` is built exactly once, so pool changes only take effect after
/// a restart. Hot reload does not rebuild the client.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ConnectionPoolConfig {
    /// Enable pooled keep-alive connections (default: true).
    /// When `false`, pooling is disabled by forcing `max_idle_per_host` to 0.
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    /// Maximum idle connections kept alive per host (default: 64).
    #[serde(default = "default_max_idle_per_host")]
    pub max_idle_per_host: usize,
    /// Idle timeout in seconds before a pooled connection is evicted (default: 15).
    /// `0` disables the idle timeout (reqwest keeps idle connections indefinitely).
    #[serde(default = "default_idle_timeout_seconds")]
    pub idle_timeout_seconds: u64,
    /// TCP keepalive probe interval in seconds (default: 30).
    /// `0` disables TCP keepalive entirely.
    #[serde(default = "default_tcp_keepalive_seconds")]
    pub tcp_keepalive_seconds: u64,
}

const fn default_enabled() -> bool {
    true
}

const fn default_max_idle_per_host() -> usize {
    64
}

const fn default_idle_timeout_seconds() -> u64 {
    15
}

const fn default_tcp_keepalive_seconds() -> u64 {
    30
}

impl Default for ConnectionPoolConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            max_idle_per_host: default_max_idle_per_host(),
            idle_timeout_seconds: default_idle_timeout_seconds(),
            tcp_keepalive_seconds: default_tcp_keepalive_seconds(),
        }
    }
}

impl ConnectionPoolConfig {
    pub const fn effective_max_idle_per_host(&self) -> usize {
        if self.enabled {
            self.max_idle_per_host
        } else {
            0
        }
    }

    pub const fn effective_idle_timeout(&self) -> Option<Duration> {
        if self.idle_timeout_seconds == 0 {
            None
        } else {
            Some(Duration::from_secs(self.idle_timeout_seconds))
        }
    }

    pub const fn effective_tcp_keepalive(&self) -> Option<Duration> {
        if self.tcp_keepalive_seconds == 0 {
            None
        } else {
            Some(Duration::from_secs(self.tcp_keepalive_seconds))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::Config;

    #[test]
    fn connection_pool_defaults_when_omitted_from_yaml() {
        let config: Config = serde_yaml::from_str("openai_clients: []").unwrap();
        let pool = config.connection_pool;

        assert!(pool.enabled);
        assert_eq!(pool.max_idle_per_host, 64);
        assert_eq!(pool.idle_timeout_seconds, 15);
        assert_eq!(pool.tcp_keepalive_seconds, 30);
    }

    #[test]
    fn connection_pool_explicit_yaml_maps_values() {
        let yaml = r#"
openai_clients: []
connection_pool:
  enabled: true
  max_idle_per_host: 128
  idle_timeout_seconds: 7
  tcp_keepalive_seconds: 9
"#;
        let config: Config = serde_yaml::from_str(yaml).unwrap();
        let pool = config.connection_pool;

        assert_eq!(pool.effective_max_idle_per_host(), 128);
        assert_eq!(pool.effective_idle_timeout(), Some(Duration::from_secs(7)));
        assert_eq!(pool.effective_tcp_keepalive(), Some(Duration::from_secs(9)));
    }

    #[test]
    fn disabled_connection_pool_maps_max_idle_to_zero() {
        let pool = ConnectionPoolConfig {
            enabled: false,
            ..ConnectionPoolConfig::default()
        };

        assert_eq!(pool.effective_max_idle_per_host(), 0);
    }

    #[test]
    fn zero_timeouts_map_to_none() {
        let pool = ConnectionPoolConfig {
            idle_timeout_seconds: 0,
            tcp_keepalive_seconds: 0,
            ..ConnectionPoolConfig::default()
        };

        assert_eq!(pool.effective_idle_timeout(), None);
        assert_eq!(pool.effective_tcp_keepalive(), None);
    }

    #[test]
    fn negative_unsigned_yaml_value_is_rejected() {
        let yaml = r#"
openai_clients: []
connection_pool:
  idle_timeout_seconds: -5
"#;

        assert!(serde_yaml::from_str::<Config>(yaml).is_err());
    }
}
