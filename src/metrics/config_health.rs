//! Prometheus mirrors of the config file's health.
//!
//! `/health` already turns an unusable config into 503. These gauges make the
//! same fact scrapeable, so a deleted/invalid config shows up in dashboards and
//! alerts without polling the health endpoint's body.

use crate::config::config_manager::ConfigHealth;
use crate::metrics::prometheus::{CONFIG_GENERATION, CONFIG_HEALTHY, CONFIG_LOAD_FAILURES};

/// Publish a health snapshot.
pub fn update(health: &ConfigHealth) {
    CONFIG_HEALTHY.set(i64::from(health.ok));
    CONFIG_GENERATION.set(i64::try_from(health.generation).unwrap_or(i64::MAX));

    // Cumulative failures by kind: read/parse/validate never decrease, so a
    // recovered config keeps its history (use gpt_api_config_healthy for state).
    let kind = health
        .last_error
        .as_ref()
        .map(|error| error.kind.as_str())
        .unwrap_or("none");
    for label in ["read", "parse", "validate"] {
        let observed = if label == kind { health.failures } else { 0 };
        CONFIG_LOAD_FAILURES
            .with_label_values(&[label])
            .set(i64::try_from(observed).unwrap_or(i64::MAX));
    }
}
