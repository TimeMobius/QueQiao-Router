use prometheus::{
    exponential_buckets, register_counter_vec, register_gauge_vec, register_histogram_vec,
    register_int_gauge, register_int_gauge_vec, CounterVec, GaugeVec, HistogramVec, IntGauge,
    IntGaugeVec,
};

pub static REQUESTS_TOTAL: once_cell::sync::Lazy<CounterVec> = once_cell::sync::Lazy::new(|| {
    register_counter_vec!(
        "gpt_api_requests_total",
        "Total number of HTTP requests",
        &["endpoint", "status", "model", "backend"]
    )
    .unwrap()
});

pub static ACTIVE_REQUESTS: once_cell::sync::Lazy<IntGaugeVec> = once_cell::sync::Lazy::new(|| {
    register_int_gauge_vec!(
        "gpt_api_active_requests",
        "Current active requests",
        &["endpoint", "model", "backend"]
    )
    .unwrap()
});

pub static ACTIVE_REQUESTS_1M_MAX: once_cell::sync::Lazy<IntGaugeVec> =
    once_cell::sync::Lazy::new(|| {
        register_int_gauge_vec!(
            "gpt_api_active_requests_1m_max",
            "Max active requests in 1 minute",
            &["endpoint", "model", "backend"]
        )
        .unwrap()
    });

pub static ACTIVE_REQUESTS_10M_MAX: once_cell::sync::Lazy<IntGaugeVec> =
    once_cell::sync::Lazy::new(|| {
        register_int_gauge_vec!(
            "gpt_api_active_requests_10m_max",
            "Max active requests in 10 minutes",
            &["endpoint", "model", "backend"]
        )
        .unwrap()
    });

pub static ACTIVE_REQUESTS_1H_MAX: once_cell::sync::Lazy<IntGaugeVec> =
    once_cell::sync::Lazy::new(|| {
        register_int_gauge_vec!(
            "gpt_api_active_requests_1h_max",
            "Max active requests in 1 hour",
            &["endpoint", "model", "backend"]
        )
        .unwrap()
    });

pub static SUCCESS_RATE: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_success_rate",
        "Overall success rate",
        &["endpoint", "model", "backend"]
    )
    .unwrap()
});

pub static SUCCESS_RATE_1M: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_success_rate_1m",
        "Success rate in last 1 minute",
        &["endpoint", "model", "backend"]
    )
    .unwrap()
});

pub static SUCCESS_RATE_10M: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_success_rate_10m",
        "Success rate in last 10 minutes",
        &["endpoint", "model", "backend"]
    )
    .unwrap()
});

pub static SUCCESS_RATE_1H: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_success_rate_1h",
        "Success rate in last 1 hour",
        &["endpoint", "model", "backend"]
    )
    .unwrap()
});

pub static TTFT: once_cell::sync::Lazy<HistogramVec> = once_cell::sync::Lazy::new(|| {
    register_histogram_vec!(
        "gpt_api_ttft_seconds",
        "Time to First Token in seconds",
        &["model", "backend"],
        exponential_buckets(0.01, 2.0, 20).unwrap()
    )
    .unwrap()
});

pub static TTFT_1M_MAX: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_ttft_1m_max",
        "Max TTFT in 1 minute",
        &["model", "backend"]
    )
    .unwrap()
});

pub static TTFT_10M_MAX: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_ttft_10m_max",
        "Max TTFT in 10 minutes",
        &["model", "backend"]
    )
    .unwrap()
});

pub static TTFT_1H_MAX: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_ttft_1h_max",
        "Max TTFT in 1 hour",
        &["model", "backend"]
    )
    .unwrap()
});

pub static LATENCY: once_cell::sync::Lazy<HistogramVec> = once_cell::sync::Lazy::new(|| {
    register_histogram_vec!(
        "gpt_api_latency_seconds",
        "End-to-end latency in seconds",
        &["model", "backend"],
        exponential_buckets(0.01, 2.0, 20).unwrap()
    )
    .unwrap()
});

pub static LATENCY_1M_MAX: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_latency_1m_max",
        "Max latency in 1 minute",
        &["model", "backend"]
    )
    .unwrap()
});

pub static LATENCY_10M_MAX: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_latency_10m_max",
        "Max latency in 10 minutes",
        &["model", "backend"]
    )
    .unwrap()
});

pub static LATENCY_1H_MAX: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_latency_1h_max",
        "Max latency in 1 hour",
        &["model", "backend"]
    )
    .unwrap()
});

pub static TPS: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_tps",
        "Real-time TPS (tokens per second)",
        &["model", "backend"]
    )
    .unwrap()
});

pub static TPS_1M_AVG: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_tps_1m_avg",
        "Average TPS in 1 minute",
        &["model", "backend"]
    )
    .unwrap()
});

pub static TPS_10M_AVG: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_tps_10m_avg",
        "Average TPS in 10 minutes",
        &["model", "backend"]
    )
    .unwrap()
});

pub static TPS_1H_AVG: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!(
        "gpt_api_tps_1h_avg",
        "Average TPS in 1 hour",
        &["model", "backend"]
    )
    .unwrap()
});

pub static RPS: once_cell::sync::Lazy<GaugeVec> = once_cell::sync::Lazy::new(|| {
    register_gauge_vec!("gpt_api_rps", "Requests per second", &["endpoint"]).unwrap()
});

pub static TOKENS_TOTAL: once_cell::sync::Lazy<CounterVec> = once_cell::sync::Lazy::new(|| {
    register_counter_vec!(
        "gpt_api_tokens_total",
        "Total tokens processed",
        &["model", "type"]
    )
    .unwrap()
});

pub static TOKEN_DISTRIBUTION: once_cell::sync::Lazy<HistogramVec> =
    once_cell::sync::Lazy::new(|| {
        register_histogram_vec!(
            "gpt_api_token_distribution",
            "Token count distribution per request",
            &["model", "backend", "type"],
            exponential_buckets(256.0, 2.0, 13).unwrap()
        )
        .unwrap()
    });

pub static FAILOVER_TOTAL: once_cell::sync::Lazy<CounterVec> = once_cell::sync::Lazy::new(|| {
    register_counter_vec!(
        "gpt_api_failover_total",
        "Total failover events",
        &["model"]
    )
    .unwrap()
});

pub static ERRORS_TOTAL: once_cell::sync::Lazy<CounterVec> = once_cell::sync::Lazy::new(|| {
    register_counter_vec!(
        "gpt_api_errors_total",
        "Total errors",
        &["error_type", "model", "backend"]
    )
    .unwrap()
});

pub static TOKENS_MISSING_TOTAL: once_cell::sync::Lazy<CounterVec> =
    once_cell::sync::Lazy::new(|| {
        register_counter_vec!(
            "gpt_api_tokens_missing_total",
            "Total requests without token usage info (embeddings, rerank, etc.)",
            &["endpoint", "model", "backend"]
        )
        .unwrap()
    });

/// `1` while the config on disk is usable, `0` when it is not
/// (missing/unreadable/invalid/backend-less). Mirrors the `/health` status.
pub static CONFIG_HEALTHY: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    register_int_gauge!(
        "gpt_api_config_healthy",
        "Config file usability (1 = ok, 0 = unusable)"
    )
    .unwrap()
});

/// Config generation currently serving requests (bumped by each successful reload).
pub static CONFIG_GENERATION: once_cell::sync::Lazy<IntGauge> = once_cell::sync::Lazy::new(|| {
    register_int_gauge!(
        "gpt_api_config_generation",
        "Config generation currently serving requests"
    )
    .unwrap()
});

/// Failed config loads since start, split by failure kind (read/parse/validate).
pub static CONFIG_LOAD_FAILURES: once_cell::sync::Lazy<IntGaugeVec> =
    once_cell::sync::Lazy::new(|| {
        register_int_gauge_vec!(
            "gpt_api_config_load_failures_total",
            "Config load failures since start",
            &["kind"]
        )
        .unwrap()
    });
