use axum::{body::Body, extract::ConnectInfo, http::Request, middleware::Next, response::Response};
use chrono::Local;
use std::net::SocketAddr;
use std::time::Instant;
use tracing::{error, info};

use crate::middleware::client_ip::get_client_ip;
use crate::models::AccessLogMeta;

fn should_log_full_token_on_error() -> bool {
    // Security default: do NOT log full credentials.
    // Set LOG_FULL_TOKEN_ON_ERROR=true (or 1/yes/on) to opt-in for incident debugging.
    std::env::var("LOG_FULL_TOKEN_ON_ERROR")
        .ok()
        .map(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn truncate_token_for_log(token: &str) -> String {
    if token.len() > 10 {
        format!("{}...", &token[..8])
    } else {
        token.to_string()
    }
}

/// 面板请求被有意排除在访问日志之外（轮询频繁，记录只产生无谓磁盘 I/O）
fn should_skip_access_log(path: &str) -> bool {
    path == "/dashboard" || path.starts_with("/dashboard/")
}

/// 访问日志中间件
///
/// 记录详细的请求和响应信息，格式模仿 Nginx Combined Log Format。
/// 并在发生错误时记录请求体以便排查。
pub async fn access_log_middleware(req: Request<Body>, next: Next) -> Response {
    if should_skip_access_log(req.uri().path()) {
        return next.run(req).await;
    }

    let start = Instant::now();

    // 1. 提取请求信息
    let method = req.method().clone();
    let uri = req.uri().clone();
    let version = req.version();
    let headers = req.headers().clone();

    // 提取 IP
    let addr = req
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0);
    let client_ip = get_client_ip(&headers, addr);

    // 提取 User-Agent
    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");

    // 提取 Bearer Token：
    // - 正常情况下只记录缩略（避免泄露）
    // - 仅在 error 日志中记录完整 token（便于排障定位具体 key）
    let bearer_token_raw = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.strip_prefix("Bearer ").unwrap_or(s).to_string())
        .filter(|s| !s.is_empty());

    // 2. 执行后续处理
    let response = next.run(req).await;

    // 3. 提取响应信息
    let latency = start.elapsed();
    let status = response.status();

    // 尝试获取 Content-Length (并不总是存在，尤其是流式响应)
    let body_bytes = response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("-");

    // 4. 提取 Handler 注入的 Model, Error 和 Request Body 信息
    let (model, error_msg, req_body_option) =
        if let Some(meta) = response.extensions().get::<AccessLogMeta>() {
            (
                meta.model.as_str(),
                meta.error.clone().unwrap_or_else(|| "-".to_string()),
                meta.request_body.clone(),
            )
        } else {
            ("-", "-".to_string(), None)
        };

    // 5. 构造 Nginx Combined 风格的日志字符串
    // 格式: IP - - [Time] "Method URI Version" Status Bytes "Referer" "UserAgent" Latency "Model" "ApiKey" "Error" "RequestBody"
    let time_str = Local::now().format("%d/%b/%Y:%H:%M:%S %z");

    // 6. 根据状态码决定日志级别
    if status.is_server_error() || status.is_client_error() {
        // 错误日志：默认仍然脱敏；仅在显式开启开关时输出完整 token
        let api_key_for_error = bearer_token_raw
            .as_deref()
            .map(|t| {
                if should_log_full_token_on_error() {
                    t.to_string()
                } else {
                    truncate_token_for_log(t)
                }
            })
            .unwrap_or_else(|| "-".to_string());
        let mut log_line = format!(
            "{} - - [{}] \"{} {} {:?}\" {} {} \"-\" \"{}\" {:.3}s \"{}\" \"{}\" {:?}",
            client_ip,
            time_str,
            method,
            uri,
            version,
            status.as_u16(),
            body_bytes,
            user_agent,
            latency.as_secs_f64(),
            model,
            api_key_for_error,
            error_msg
        );

        // 仅在错误时记录请求体
        // 不再进行中间件层面的截断，完全信任上层逻辑 (handler) 传递的内容
        // 即使上层传递的是原始的长字符串(例如 json 解析失败)，也完整记录
        if let Some(body) = req_body_option {
            log_line.push_str(&format!(" {:?}", body));
        } else {
            log_line.push_str(" \"-\"");
        }
        error!(target: "access_log", "{}", log_line);
    } else {
        // 成功日志：输出缩略 token（若存在）
        let api_key_for_info = bearer_token_raw
            .as_deref()
            .map(truncate_token_for_log)
            .unwrap_or_else(|| "-".to_string());

        let log_line = format!(
            "{} - - [{}] \"{} {} {:?}\" {} {} \"-\" \"{}\" {:.3}s \"{}\" \"{}\" {:?}",
            client_ip,
            time_str,
            method,
            uri,
            version,
            status.as_u16(),
            body_bytes,
            user_agent,
            latency.as_secs_f64(),
            model,
            api_key_for_info,
            error_msg
        );
        // 成功请求不记录请求体
        info!(target: "access_log", "{}", log_line);
    }

    response
}

#[cfg(test)]
mod tests {
    use super::should_skip_access_log;

    #[test]
    fn skips_dashboard_requests_including_subpaths() {
        assert!(should_skip_access_log("/dashboard"));
        assert!(should_skip_access_log("/dashboard/"));
        assert!(should_skip_access_log("/dashboard/records"));
        assert!(should_skip_access_log("/dashboard/api/records"));
        assert!(should_skip_access_log("/dashboard/vendor/app.css"));
    }

    #[test]
    fn keeps_logging_other_routes() {
        assert!(!should_skip_access_log("/"));
        assert!(!should_skip_access_log("/health"));
        assert!(!should_skip_access_log("/metrics"));
        assert!(!should_skip_access_log("/v1/chat/completions"));
        assert!(!should_skip_access_log("/dashboardx"));
    }
}
