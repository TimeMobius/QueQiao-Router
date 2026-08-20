use crate::config::types::{ClientConfig, HealthCheckConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptPermission {
    Allowed,
    HalfOpen,
    Blocked { retry_after: Duration },
}

#[derive(Debug, Clone)]
struct BackendState {
    base_url: String,
    consecutive_failures: u32,
    opened_at: Option<Instant>,
    backoff_level: u32,
    probe_in_flight: bool,
}

impl BackendState {
    fn new(client: &ClientConfig) -> Self {
        Self {
            base_url: client.base_url.clone(),
            consecutive_failures: 0,
            opened_at: None,
            backoff_level: 0,
            probe_in_flight: false,
        }
    }
}

#[derive(Debug)]
struct RegistryState {
    config: HealthCheckConfig,
    backends: HashMap<String, BackendState>,
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerRegistry {
    state: Arc<Mutex<RegistryState>>,
}

impl CircuitBreakerRegistry {
    pub fn new(config: HealthCheckConfig) -> Self {
        Self {
            state: Arc::new(Mutex::new(RegistryState {
                config: normalize_config(config),
                backends: HashMap::new(),
            })),
        }
    }

    pub fn configure(&self, config: HealthCheckConfig) {
        let mut state = lock_state(&self.state);
        state.config = normalize_config(config);
    }

    pub fn allow(&self, client: &ClientConfig) -> AttemptPermission {
        let mut state = lock_state(&self.state);
        if !state.config.enabled {
            return AttemptPermission::Allowed;
        }

        let config = state.config;
        let backend = state
            .backends
            .entry(client.name.clone())
            .or_insert_with(|| BackendState::new(client));

        if backend.base_url != client.base_url {
            *backend = BackendState::new(client);
            return AttemptPermission::Allowed;
        }

        let Some(opened_at) = backend.opened_at else {
            return AttemptPermission::Allowed;
        };

        let cooldown = cooldown_for_state(&config, backend);
        let elapsed = opened_at.elapsed();
        if elapsed < cooldown {
            return AttemptPermission::Blocked {
                retry_after: cooldown.saturating_sub(elapsed),
            };
        }

        if backend.probe_in_flight {
            return AttemptPermission::Blocked {
                retry_after: Duration::ZERO,
            };
        }

        backend.probe_in_flight = true;
        AttemptPermission::HalfOpen
    }

    pub fn record_success(&self, client: &ClientConfig) {
        let mut state = lock_state(&self.state);
        if !state.config.enabled {
            return;
        }

        let backend = state
            .backends
            .entry(client.name.clone())
            .or_insert_with(|| BackendState::new(client));
        if backend.base_url != client.base_url {
            *backend = BackendState::new(client);
            return;
        }

        backend.consecutive_failures = 0;
        backend.opened_at = None;
        backend.backoff_level = 0;
        backend.probe_in_flight = false;
    }

    pub fn record_failure(&self, client: &ClientConfig) {
        let mut state = lock_state(&self.state);
        if !state.config.enabled {
            return;
        }

        let config = state.config;
        let backend = state
            .backends
            .entry(client.name.clone())
            .or_insert_with(|| BackendState::new(client));
        if backend.base_url != client.base_url {
            *backend = BackendState::new(client);
        }

        if backend.opened_at.is_some() || backend.probe_in_flight {
            backend.backoff_level = backend.backoff_level.saturating_add(1);
        } else {
            backend.consecutive_failures = backend.consecutive_failures.saturating_add(1);
        }

        backend.probe_in_flight = false;
        if backend.opened_at.is_some() || backend.consecutive_failures >= config.failure_threshold {
            backend.opened_at = Some(Instant::now());
        }
    }

    #[cfg(test)]
    pub fn cooldown_for(&self, client: &ClientConfig) -> Duration {
        let state = lock_state(&self.state);
        let Some(backend) = state.backends.get(&client.name) else {
            return Duration::ZERO;
        };
        cooldown_for_state(&state.config, backend)
    }
}

fn normalize_config(mut config: HealthCheckConfig) -> HealthCheckConfig {
    config.failure_threshold = config.failure_threshold.max(1);
    config.max_cooldown_seconds = config.max_cooldown_seconds.max(config.cooldown_seconds);
    if !config.backoff_multiplier.is_finite() || config.backoff_multiplier < 1.0 {
        config.backoff_multiplier = 1.0;
    }
    config
}

fn cooldown_for_state(config: &HealthCheckConfig, backend: &BackendState) -> Duration {
    let max = Duration::from_secs(config.max_cooldown_seconds);
    let mut cooldown = Duration::from_secs(config.cooldown_seconds);
    for _ in 0..backend.backoff_level {
        cooldown = cooldown.mul_f64(config.backoff_multiplier).min(max);
    }
    cooldown
}

fn lock_state(state: &Mutex<RegistryState>) -> MutexGuard<'_, RegistryState> {
    state
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
#[path = "circuit_breaker_tests.rs"]
mod tests;
