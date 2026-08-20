use super::*;
use std::time::Duration;

fn client(name: &str) -> ClientConfig {
    ClientConfig {
        name: name.to_string(),
        base_url: format!("https://{name}.example.com/v1"),
        ..ClientConfig::default()
    }
}

#[test]
fn backend_opens_after_consecutive_hard_failures() {
    let registry = CircuitBreakerRegistry::new(HealthCheckConfig {
        failure_threshold: 2,
        cooldown_seconds: 30,
        ..HealthCheckConfig::default()
    });
    let backend = client("primary");

    assert_eq!(registry.allow(&backend), AttemptPermission::Allowed);
    registry.record_failure(&backend);
    assert_eq!(registry.allow(&backend), AttemptPermission::Allowed);
    registry.record_failure(&backend);

    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::Blocked { .. }
    ));
}

#[test]
fn successful_request_closes_half_open_backend() {
    let registry = CircuitBreakerRegistry::new(HealthCheckConfig {
        failure_threshold: 1,
        cooldown_seconds: 0,
        ..HealthCheckConfig::default()
    });
    let backend = client("primary");

    registry.record_failure(&backend);
    assert_eq!(registry.allow(&backend), AttemptPermission::HalfOpen);
    registry.record_success(&backend);

    assert_eq!(registry.allow(&backend), AttemptPermission::Allowed);
}

#[test]
fn only_one_request_enters_half_open_state() {
    let registry = CircuitBreakerRegistry::new(HealthCheckConfig {
        failure_threshold: 1,
        cooldown_seconds: 0,
        ..HealthCheckConfig::default()
    });
    let backend = client("primary");

    registry.record_failure(&backend);
    assert_eq!(registry.allow(&backend), AttemptPermission::HalfOpen);
    assert!(matches!(
        registry.allow(&backend),
        AttemptPermission::Blocked { .. }
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
