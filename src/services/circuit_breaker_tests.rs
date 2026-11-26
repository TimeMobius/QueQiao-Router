use super::*;
use std::time::Duration;

fn client(name: &str) -> ClientConfig {
    ClientConfig {
        name: name.to_string(),
        base_url: format!("https://{name}.example.com/v1"),
        ..ClientConfig::default()
    }
}

fn registry_with(cooldown_seconds: u64, failure_threshold: u32) -> CircuitBreakerRegistry {
    CircuitBreakerRegistry::new(HealthCheckConfig {
        failure_threshold,
        cooldown_seconds,
        ..HealthCheckConfig::default()
    })
}

fn expect_half_open(registry: &CircuitBreakerRegistry, backend: &ClientConfig) -> ProbeLease {
    match registry.allow(backend) {
        AttemptPermission::HalfOpen(lease) => lease,
        other => panic!("expected half-open probe, got {other:?}"),
    }
}

#[test]
fn backend_opens_after_consecutive_hard_failures() {
    let registry = registry_with(30, 2);
    let backend = client("primary");

    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::Allowed
    ));
    registry.record_failure(&backend);
    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::Allowed
    ));
    registry.record_failure(&backend);

    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::Blocked { .. }
    ));
}

#[test]
fn successful_probe_closes_backend() {
    let registry = registry_with(0, 1);
    let backend = client("primary");

    registry.record_failure(&backend);
    let lease = expect_half_open(&registry, &backend);
    lease.record_success();

    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::Allowed
    ));
}

#[test]
fn only_one_probe_lease_is_granted() {
    let registry = registry_with(0, 1);
    let backend = client("primary");

    registry.record_failure(&backend);
    let lease = expect_half_open(&registry, &backend);

    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::Blocked { .. }
    ));
    drop(lease);
}

#[test]
fn dropped_probe_lease_does_not_wedge_backend() {
    // 回归：探测请求被取消时不会调用 record_*，许可必须由 Drop 归还；
    // 否则后端会永久停在“有人在探测”状态，冷却到期后再也不发新探测。
    let registry = registry_with(0, 1);
    let backend = client("primary");

    registry.record_failure(&backend);
    let lease = expect_half_open(&registry, &backend);
    drop(lease); // 模拟客户端断开 / 任务取消

    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::HalfOpen(_)
    ));
}

#[test]
fn probe_failure_reopens_with_escalated_backoff() {
    // 冷却 0 时先打开熔断并拿到探测许可，再把配置换成 10s/×2，
    // 这样无需等待即可验证探测失败后的退避曲线。
    let registry = registry_with(0, 1);
    let backend = client("primary");

    registry.record_failure(&backend);
    let lease = expect_half_open(&registry, &backend);
    registry.configure(HealthCheckConfig {
        failure_threshold: 1,
        cooldown_seconds: 10,
        max_cooldown_seconds: 100,
        backoff_multiplier: 2.0,
        ..HealthCheckConfig::default()
    });
    lease.record_failure();

    assert_eq!(registry.cooldown_for(&backend), Duration::from_secs(20));
    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::Blocked { .. }
    ));
}

#[test]
fn expired_probe_lease_is_reclaimed() {
    // 回归：探测请求还活着但永远不会回调（上游挂住），超时后必须允许新探测。
    let registry = registry_with(0, 1);
    registry.set_probe_lease_timeout(Duration::ZERO);
    let backend = client("primary");

    registry.record_failure(&backend);
    let hung = expect_half_open(&registry, &backend);
    std::mem::forget(hung); // 模拟仍在执行、没有归还许可的请求

    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::HalfOpen(_)
    ));
}

#[test]
fn stale_probe_result_is_ignored_after_reclaim() {
    let registry = registry_with(0, 1);
    registry.set_probe_lease_timeout(Duration::ZERO);
    let backend = client("primary");

    registry.record_failure(&backend);
    let stale = expect_half_open(&registry, &backend);
    let fresh = expect_half_open(&registry, &backend); // 回收旧租约并授予新租约
    registry.set_probe_lease_timeout(Duration::from_secs(3600));

    // 迟到的旧探测结果必须被忽略，不能污染新租约的状态。
    stale.record_failure();
    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::Blocked { .. }
    ));

    fresh.record_success();
    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::Allowed
    ));
}

#[test]
fn backoff_increases_after_repeated_open_state_failure() {
    let registry = CircuitBreakerRegistry::new(HealthCheckConfig {
        failure_threshold: 1,
        cooldown_seconds: 10,
        max_cooldown_seconds: 100,
        backoff_multiplier: 2.0,
        ..HealthCheckConfig::default()
    });
    let backend = client("primary");

    registry.record_failure(&backend);
    registry.record_failure(&backend);
    assert_eq!(registry.cooldown_for(&backend), Duration::from_secs(20));
}

#[test]
fn snapshot_reports_closed_open_and_half_open_states() {
    // 从未失败过的后端不会出现在快照里，调用方按 Closed 兜底。
    let registry = registry_with(0, 1);
    assert!(registry.snapshot().is_empty());

    let healthy = client("healthy");
    registry.record_success(&healthy);
    let snapshot = registry.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].name, "healthy");
    assert_eq!(snapshot[0].state, BackendCircuitState::Closed);
    assert_eq!(snapshot[0].cooldown_remaining, Duration::ZERO);

    // open：冷却 60s，剩余时间应落在 (0, 60]。
    let registry = CircuitBreakerRegistry::new(HealthCheckConfig {
        failure_threshold: 1,
        cooldown_seconds: 60,
        ..HealthCheckConfig::default()
    });
    let cooling = client("cooling");
    registry.record_failure(&cooling);
    let snapshot = registry.snapshot();
    assert_eq!(snapshot[0].state, BackendCircuitState::Open);
    assert!(snapshot[0].cooldown_remaining > Duration::ZERO);
    assert!(snapshot[0].cooldown_remaining <= Duration::from_secs(60));

    // half_open：冷却 0，拿到探测租约后状态为 HalfOpen。
    let registry = registry_with(0, 1);
    let probing = client("probing");
    registry.record_failure(&probing);
    let lease = expect_half_open(&registry, &probing);
    let snapshot = registry.snapshot();
    assert_eq!(snapshot[0].state, BackendCircuitState::HalfOpen);
    assert!(snapshot[0].probe_in_flight);
    drop(lease);
}

#[test]
fn snapshot_reports_disabled_when_breaker_is_disabled() {
    let registry = registry_with(30, 1);
    let backend = client("primary");
    registry.record_failure(&backend);
    assert_eq!(registry.snapshot()[0].state, BackendCircuitState::Open);

    // 热加载关闭熔断后，已有条目统一报告 Disabled（不再做健康判定）。
    registry.configure(HealthCheckConfig {
        enabled: false,
        ..HealthCheckConfig::default()
    });
    let snapshot = registry.snapshot();
    assert_eq!(snapshot[0].state, BackendCircuitState::Disabled);
    assert_eq!(snapshot[0].cooldown_remaining, Duration::ZERO);
}
