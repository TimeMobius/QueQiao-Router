use crate::config::types::{ClientConfig, HealthCheckConfig};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};
use tracing::warn;

/// 半开探测租约超时。
///
/// 探测请求若在此时限内仍未归还许可（例如上游挂住、任务没有被取消但也永远不会
/// 回调），就回收旧许可并允许下一次请求重新探测。避免一个卡住的探测把所有后续
/// 请求永久锁死在“冷却中”。
const PROBE_LEASE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug)]
pub enum AttemptPermission {
    Allowed,
    HalfOpen(ProbeLease),
    Blocked { retry_after: Duration },
}

/// 熔断器视角的后端状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendCircuitState {
    /// 健康，正常接流量。
    Closed,
    /// 熔断打开，冷却中，暂时不会接到新请求。
    Open,
    /// 冷却已到期：等待探测，或探测正在进行。
    HalfOpen,
    /// `health_check.enabled == false`，不做健康判定。
    Disabled,
}

impl BackendCircuitState {
    /// 稳定的短标识，用于日志。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
            Self::Disabled => "disabled",
        }
    }

    /// 数值编码，用于 Prometheus gauge：
    /// `0 = closed`、`1 = open`、`2 = half_open`、`3 = disabled`。
    pub fn code(self) -> u8 {
        match self {
            Self::Closed => 0,
            Self::Open => 1,
            Self::HalfOpen => 2,
            Self::Disabled => 3,
        }
    }
}

/// 单个后端的健康快照（只读副本，供指标导出使用）。
#[derive(Debug, Clone)]
pub struct BackendHealth {
    pub name: String,
    pub state: BackendCircuitState,
    /// 距离可再次探测还剩多久；`Closed` / `HalfOpen` / `Disabled` 时为 0。
    pub cooldown_remaining: Duration,
    pub consecutive_failures: u32,
    pub backoff_level: u32,
    pub probe_in_flight: bool,
}

/// 半开探测的所有权凭据。
///
/// `allow()` 返回 `HalfOpen(lease)` 时，本次请求独占该后端的探测许可；只有调用
/// [`ProbeLease::record_success`] / [`ProbeLease::record_failure`] 才会归还许可。
///
/// 若请求在归还前被丢弃（客户端断开、调用方取消、任务 panic 等），`Drop` 会主动
/// 回收许可。否则一个被取消的探测会让后端永远停在“有人正在探测”状态，冷却到期
/// 后再也不会发新探测，表现为后端被永久熔断。
pub struct ProbeLease {
    registry: CircuitBreakerRegistry,
    name: String,
    base_url: String,
    epoch: u64,
    resolved: bool,
}

impl ProbeLease {
    fn new(registry: CircuitBreakerRegistry, client: &ClientConfig, epoch: u64) -> Self {
        Self {
            registry,
            name: client.name.clone(),
            base_url: client.base_url.clone(),
            epoch,
            resolved: false,
        }
    }

    /// 探测成功：归还许可，关闭熔断并清零退避。
    pub fn record_success(mut self) {
        self.resolved = true;
        self.registry
            .probe_success(&self.name, &self.base_url, self.epoch);
    }

    /// 探测失败：归还许可，重新打开熔断并提升退避级别。
    pub fn record_failure(mut self) {
        self.resolved = true;
        self.registry
            .probe_failure(&self.name, &self.base_url, self.epoch);
    }
}

impl fmt::Debug for ProbeLease {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProbeLease")
            .field("name", &self.name)
            .field("epoch", &self.epoch)
            .field("resolved", &self.resolved)
            .finish()
    }
}

impl Drop for ProbeLease {
    fn drop(&mut self) {
        if !self.resolved {
            self.registry
                .probe_abandoned(&self.name, &self.base_url, self.epoch);
        }
    }
}

#[derive(Debug, Clone)]
struct BackendState {
    base_url: String,
    consecutive_failures: u32,
    opened_at: Option<Instant>,
    backoff_level: u32,
    probe_in_flight: bool,
    probe_started_at: Option<Instant>,
    /// 探测许可的代次。回收旧许可后再授予新许可时自增，迟到的旧探测结果据此被忽略。
    probe_epoch: u64,
}

impl BackendState {
    fn new(client: &ClientConfig) -> Self {
        Self {
            base_url: client.base_url.clone(),
            consecutive_failures: 0,
            opened_at: None,
            backoff_level: 0,
            probe_in_flight: false,
            probe_started_at: None,
            probe_epoch: 0,
        }
    }
}

#[derive(Debug)]
struct RegistryState {
    config: HealthCheckConfig,
    backends: HashMap<String, BackendState>,
    /// 测试里可以缩短租约超时，避免真的等 120 秒。
    #[cfg(test)]
    probe_lease_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerRegistry {
    state: Arc<Mutex<RegistryState>>,
}

impl CircuitBreakerRegistry {
    pub fn new(config: HealthCheckConfig) -> Self {
        #[cfg(test)]
        let state = RegistryState {
            config: normalize_config(config),
            backends: HashMap::new(),
            probe_lease_timeout: PROBE_LEASE_TIMEOUT,
        };
        #[cfg(not(test))]
        let state = RegistryState {
            config: normalize_config(config),
            backends: HashMap::new(),
        };
        Self {
            state: Arc::new(Mutex::new(state)),
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

        #[cfg(test)]
        let lease_timeout = state.probe_lease_timeout;
        #[cfg(not(test))]
        let lease_timeout = PROBE_LEASE_TIMEOUT;

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

        // 冷却已到期。若上一次探测的许可仍被占用，先看它是否已超出租约：
        // 超时说明持有者大概率已经丢失（请求被丢弃、任务卡死但未被取消等），
        // 必须回收，否则后端会永久停留在“探测中”，再也不会被选择。
        if backend.probe_in_flight {
            let lease_expired = match backend.probe_started_at {
                Some(started) => started.elapsed() >= lease_timeout,
                // 状态不完整（有标志无起始时间）时按超时处理，避免永久卡死。
                None => true,
            };
            if !lease_expired {
                return AttemptPermission::Blocked {
                    retry_after: Duration::ZERO,
                };
            }
            warn!(
                backend = %client.name,
                lease_timeout_secs = lease_timeout.as_secs(),
                "half-open probe lease expired without a verdict; reclaiming it"
            );
            backend.probe_in_flight = false;
            backend.probe_started_at = None;
        }

        let epoch = backend.probe_epoch.wrapping_add(1);
        backend.probe_epoch = epoch;
        backend.probe_in_flight = true;
        backend.probe_started_at = Some(Instant::now());
        AttemptPermission::HalfOpen(ProbeLease::new(self.clone(), client, epoch))
    }

    /// 导出所有已跟踪后端的健康状态快照（只读，不影响路由状态）。
    ///
    /// 从未失败过的后端不会出现在这里（注册表按需创建条目），调用方应把缺失的
    /// 后端视为 `Closed`；`health_check.enabled == false` 时统一返回 `Disabled`。
    pub fn snapshot(&self) -> Vec<BackendHealth> {
        let state = lock_state(&self.state);
        let enabled = state.config.enabled;
        let mut snapshot: Vec<BackendHealth> = state
            .backends
            .iter()
            .map(|(name, backend)| {
                let (circuit_state, cooldown_remaining) = if !enabled {
                    (BackendCircuitState::Disabled, Duration::ZERO)
                } else {
                    match backend.opened_at {
                        None => (BackendCircuitState::Closed, Duration::ZERO),
                        Some(opened_at) => {
                            let cooldown = cooldown_for_state(&state.config, backend);
                            let elapsed = opened_at.elapsed();
                            if elapsed < cooldown {
                                (BackendCircuitState::Open, cooldown.saturating_sub(elapsed))
                            } else {
                                (BackendCircuitState::HalfOpen, Duration::ZERO)
                            }
                        }
                    }
                };
                BackendHealth {
                    name: name.clone(),
                    state: circuit_state,
                    cooldown_remaining,
                    consecutive_failures: backend.consecutive_failures,
                    backoff_level: backend.backoff_level,
                    probe_in_flight: backend.probe_in_flight,
                }
            })
            .collect();
        snapshot.sort_by(|left, right| left.name.cmp(&right.name));
        snapshot
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
        backend.probe_started_at = None;
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
        backend.probe_started_at = None;
        if backend.opened_at.is_some() || backend.consecutive_failures >= config.failure_threshold {
            backend.opened_at = Some(Instant::now());
        }
    }

    /// 半开探测成功：归还许可并关闭熔断。仅当本次探测仍是最新许可时生效，
    /// 避免被回收后的迟到探测污染新状态。
    fn probe_success(&self, name: &str, base_url: &str, epoch: u64) {
        let mut state = lock_state(&self.state);
        let enabled = state.config.enabled;
        let Some(backend) = state.backends.get_mut(name) else {
            return;
        };
        if backend.base_url != base_url || backend.probe_epoch != epoch {
            return;
        }

        backend.probe_in_flight = false;
        backend.probe_started_at = None;
        if !enabled {
            return;
        }

        backend.consecutive_failures = 0;
        backend.opened_at = None;
        backend.backoff_level = 0;
    }

    /// 半开探测失败：归还许可，重新打开熔断并提升退避级别。
    fn probe_failure(&self, name: &str, base_url: &str, epoch: u64) {
        let mut state = lock_state(&self.state);
        let enabled = state.config.enabled;
        let Some(backend) = state.backends.get_mut(name) else {
            return;
        };
        if backend.base_url != base_url || backend.probe_epoch != epoch {
            return;
        }

        backend.probe_in_flight = false;
        backend.probe_started_at = None;
        if !enabled {
            return;
        }

        backend.backoff_level = backend.backoff_level.saturating_add(1);
        backend.opened_at = Some(Instant::now());
    }

    /// 探测请求在给出结论前被丢弃：立即归还许可，让下一次请求可以重新探测。
    ///
    /// 这不是后端的故障信号（客户端主动取消、网关侧任务被取消都可能发生），
    /// 因此不提升退避级别、也不延长冷却，避免健康后端因为调用方反复取消而饿死。
    fn probe_abandoned(&self, name: &str, base_url: &str, epoch: u64) {
        let mut state = lock_state(&self.state);
        let enabled = state.config.enabled;
        let Some(backend) = state.backends.get_mut(name) else {
            return;
        };
        if backend.base_url != base_url || backend.probe_epoch != epoch {
            return;
        }

        backend.probe_in_flight = false;
        backend.probe_started_at = None;
        if !enabled {
            return;
        }

        warn!(
            backend = %name,
            "half-open probe abandoned before a verdict (request cancelled or dropped); backend stays eligible for a new probe"
        );
    }

    #[cfg(test)]
    pub fn cooldown_for(&self, client: &ClientConfig) -> Duration {
        let state = lock_state(&self.state);
        let Some(backend) = state.backends.get(&client.name) else {
            return Duration::ZERO;
        };
        cooldown_for_state(&state.config, backend)
    }

    #[cfg(test)]
    fn set_probe_lease_timeout(&self, timeout: Duration) {
        lock_state(&self.state).probe_lease_timeout = timeout;
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
