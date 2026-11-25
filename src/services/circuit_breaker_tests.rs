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
