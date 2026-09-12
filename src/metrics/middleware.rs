// Copyright 2024 QueQiao-Router Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Metrics middleware - 异步发送指标到独立 worker 线程
//!
//! 架构：
//! 请求处理线程 ──(channel)──> 独立监控线程
//!      │                              │
//!      │                              ▼
//!      │                       滑动窗口计算
//!      │                       Prometheus 指标更新
//!      ▼                       
//!   路由转发

use axum::{body::Body, extract::Request, middleware::Next, response::Response};
use std::time::Instant;

use crate::metrics::worker::{MetricEvent, MetricsSender};

// 全局指标发送端 - 由 main.rs 在启动时设置
static METRICS_SENDER: once_cell::sync::OnceCell<MetricsSender> = once_cell::sync::OnceCell::new();

/// 设置全局指标发送端（必须在服务启动前调用）
pub fn set_metrics_sender(sender: MetricsSender) {
    METRICS_SENDER
        .set(sender)
        .expect("Metrics sender already set");
}

/// 获取全局指标发送端（用于健康检查等）
pub fn get_metrics_sender() -> Option<&'static MetricsSender> {
    METRICS_SENDER.get()
}

/// Endpoints that should be excluded from metrics
const METRICS_SKIP_ENDPOINTS: &[&str] = &["/metrics", "/health", "/v1/models"];

fn should_skip_metrics(endpoint: &str) -> bool {
    endpoint == "/"
        || endpoint == "/favicon.ico"
        || endpoint == "/dashboard"
        || endpoint.starts_with("/dashboard/")
        || METRICS_SKIP_ENDPOINTS.iter().any(|&skip| endpoint == skip)
}

/// Helper to extract model from request body
fn extract_model_from_request(req: &Request<Body>) -> String {
    // Try to get model from query params first
    if let Some(query) = req.uri().query() {
        for param in query.split('&') {
            if param.starts_with("model=") {
                return param[6..].to_string();
            }
        }
    }
    // Default to unknown - actual model will be set from response extensions
    "unknown".to_string()
}

/// Metrics middleware - 将指标发送到独立 worker 线程处理
///
/// 关键优化：不再在请求路径中直接更新 Prometheus 指标和滑动窗口
/// 而是只提取数据并通过 channel 异步发送，让 worker 线程处理耗时的指标更新
pub async fn metrics_middleware(req: Request<Body>, next: Next) -> Response {
    let start = Instant::now();
    let endpoint = req.uri().path().to_string();

    // Skip metrics for non-API endpoints
    if should_skip_metrics(&endpoint) {
        return next.run(req).await;
    }

    // Extract model from request at the start (may be updated later from response)
    let initial_model = extract_model_from_request(&req);
    let pending_backend = "pending";

    let response = next.run(req).await;

    // Skip metrics if response has streaming marker (set by stream_handler)
    // This handles cases where stream=true is in request body not query string
    if response.extensions().get::<()>().is_some() {
        return response;
    }

    let elapsed = start.elapsed().as_secs_f64();
    let status = response.status().as_u16();
    let status_str = status.to_string();
    let is_success = status == 200;

    // Get actual model/backend from response extensions
    let access_log_meta = response.extensions().get::<crate::models::AccessLogMeta>();
    let (model_str, backend_str) = access_log_meta
        .map(|meta| (meta.model.as_str(), meta.backend.as_str()))
        .unwrap_or_else(|| (&initial_model.as_str(), pending_backend));

    // 构建指标事件并发送到 worker（异步，不阻塞请求）
    if let Some(sender) = METRICS_SENDER.get() {
        let event = MetricEvent {
            endpoint: endpoint.clone(),
            status: status_str,
            model: model_str.to_string(),
            backend: backend_str.to_string(),
            latency: elapsed,
            is_success,
            completion_tokens: None,
            prompt_tokens: None,
            elapsed: None,
        };

        // 使用 try_send 是非阻塞的，即使 channel 满了也不会影响请求处理
        // 丢失一些指标总比影响请求延迟好
        let _ = sender.try_send(event);
    }

    response
}

#[cfg(test)]
mod tests {
    use super::should_skip_metrics;

    #[test]
    fn skips_dashboard_root_and_static_asset_requests() {
        assert!(should_skip_metrics("/"));
        assert!(should_skip_metrics("/favicon.ico"));
        assert!(should_skip_metrics("/dashboard"));
        assert!(should_skip_metrics("/dashboard/"));
        assert!(should_skip_metrics("/dashboard/vendor/echarts.min.js"));
    }

    #[test]
    fn keeps_api_requests_in_metrics() {
        assert!(!should_skip_metrics("/v1/chat/completions"));
        assert!(!should_skip_metrics("/rerank"));
        assert!(!should_skip_metrics("/score"));
    }
}
