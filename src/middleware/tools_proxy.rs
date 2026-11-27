//! `/v1/tools/fetch` 与 `/v1/tools/search` 的直通代理。
//!
//! 这两个端点在**中间件层**就被转发到上游：不进入 handler、不做模型匹配、
//! 不解析请求体、不写审计库、不产生 Prometheus 指标。
//!
//! 转发语义：
//! - 请求方法、path（含 query）、end-to-end 请求头原样上行；逐跳头
//!   （`connection` / `transfer-encoding` 等）与 `host` / `content-length`
//!   由本进程按实际传输重建。
//! - 请求体、响应体都按字节流透传，不做 JSON 解析或改写。
//! - 响应状态码与响应头（除逐跳头外）原样返回。
//! - 上游缺省为 DeepSeek 官方端点，可用 `tools_proxy.base_url` 替换。

use crate::state::app_state::AppState;
use axum::{
    body::Body,
    extract::State,
    http::{HeaderMap, HeaderName, Method, Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};
use reqwest::Client;
use std::sync::Arc;

/// 仅这两个路径进入直通代理；不做任何模型名或请求体匹配。
pub fn is_tools_proxy_path(path: &str) -> bool {
    matches!(path, "/v1/tools/fetch" | "/v1/tools/search")
}

/// RFC 9110 §7.6.1 定义的逐跳头：只对单段连接有意义，代理必须丢弃。
fn is_hop_by_hop(name: &HeaderName) -> bool {
    matches!(
        name.as_str(),
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

/// 请求侧需要剔除的头：逐跳头，以及由 reqwest/hyper 按实际连接与体长重建的头。
fn should_skip_request_header(name: &HeaderName) -> bool {
    is_hop_by_hop(name) || matches!(name.as_str(), "host" | "content-length" | "expect")
}

/// 拼出上游 URL。
///
/// `base_url` 被当作上游根地址（可带结尾 `/`，也可写成 `https://host/v1`）；
/// 原请求的 path 与 query 原样拼接。写成 `.../v1` 时自动避免出现 `/v1/v1/...`。
fn build_upstream_url(base_url: &str, path_and_query: &str) -> String {
    let base = base_url.trim().trim_end_matches('/');
    let path = path_and_query.trim_start_matches('/');
    let path = if base.ends_with("/v1") {
        path.strip_prefix("v1/").unwrap_or(path)
    } else {
        path
    };
    format!("{base}/{path}")
}

/// 复制客户端请求头（保留同名多值），剔除逐跳头与需要重建的头。
fn forwarded_request_headers(headers: &HeaderMap) -> HeaderMap {
    let mut forwarded = HeaderMap::new();
    for (name, value) in headers {
        if should_skip_request_header(name) {
            continue;
        }
        forwarded.append(name.clone(), value.clone());
    }
    forwarded
}

/// 执行一次直通转发：请求体、响应体均为流式，不缓冲、不解析。
pub async fn forward_tools_request(
    http: &Client,
    base_url: &str,
    req: Request<Body>,
) -> Result<Response, reqwest::Error> {
    let (parts, body) = req.into_parts();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map(|value| value.as_str())
        .unwrap_or("/");
    let url = build_upstream_url(base_url, path_and_query);
    let headers = forwarded_request_headers(&parts.headers);

    let upstream = http
        .request(parts.method, url)
        .headers(headers)
        .body(reqwest::Body::wrap_stream(body.into_data_stream()))
        .send()
        .await?;

    let status = upstream.status();
    let headers = upstream.headers().clone();
    let mut response = Response::new(Body::from_stream(upstream.bytes_stream()));
    *response.status_mut() = status;
    for (name, value) in &headers {
        if is_hop_by_hop(name) {
            continue;
        }
        response.headers_mut().append(name.clone(), value.clone());
    }
    Ok(response)
}

/// 把直通失败转成网关统一错误体；超时映射 504，其余上游错误映射 502。
fn tools_proxy_error_response(base_url: &str, error: &reqwest::Error) -> Response {
    let status = if error.is_timeout() {
        StatusCode::GATEWAY_TIMEOUT
    } else {
        StatusCode::BAD_GATEWAY
    };
    tracing::warn!(
        %error,
        base_url,
        status = status.as_u16(),
        "tools proxy upstream request failed"
    );
    let body = crate::app_error::build_error_body(
        &format!("Tools proxy upstream request failed: {error}"),
        "upstream_error",
    );
    (status, axum::Json(body)).into_response()
}

/// `/v1/tools/*` 直通中间件。
///
/// 命中路径后直接转发并返回，业务路由不会被执行。它位于 CORS 层内侧，因此
/// 响应仍会带上网关的 CORS 头，预检请求也由 CORS 层先行处理。外层 metrics
/// 中间件的端点白名单不含这两个路径，因此不会记录指标；`OPTIONS` 与未命中
/// 路径继续走 `next`，保持原有行为。
pub async fn tools_proxy_middleware(
    State(state): State<Arc<AppState>>,
    req: Request<Body>,
    next: Next,
) -> Response {
    if req.method() == Method::OPTIONS || !is_tools_proxy_path(req.uri().path()) {
        return next.run(req).await;
    }

    let base_url = {
        let config = state.config_manager.get_config_guard().await;
        config.tools_proxy.resolved_base_url().to_owned()
    };
    let http = state.client_manager.get_client();

    match forward_tools_request(&http, &base_url, req).await {
        Ok(response) => response,
        Err(error) => tools_proxy_error_response(&base_url, &error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::ToolsProxyConfig;
    use axum::http::{header, HeaderValue};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    #[test]
    fn only_tools_paths_are_proxied() {
        assert!(is_tools_proxy_path("/v1/tools/fetch"));
        assert!(is_tools_proxy_path("/v1/tools/search"));

        assert!(!is_tools_proxy_path("/v1/tools"));
        assert!(!is_tools_proxy_path("/v1/tools/fetch/"));
        assert!(!is_tools_proxy_path("/v1/tools/fetch/extra"));
        assert!(!is_tools_proxy_path("/v1/chat/completions"));
        assert!(!is_tools_proxy_path("/health"));
    }

    #[test]
    fn default_base_url_is_deepseek() {
        assert_eq!(
            ToolsProxyConfig::default().resolved_base_url(),
            "https://api.deepseek.com"
        );
    }

    #[test]
    fn joins_paths_without_double_slashes_or_v1() {
        assert_eq!(
            build_upstream_url("https://api.deepseek.com", "/v1/tools/fetch"),
            "https://api.deepseek.com/v1/tools/fetch"
        );
        assert_eq!(
            build_upstream_url("https://api.deepseek.com/", "/v1/tools/search?q=x"),
            "https://api.deepseek.com/v1/tools/search?q=x"
        );
        // base_url 自带 `/v1` 时不重复拼接
        assert_eq!(
            build_upstream_url("https://proxy.example.com/v1", "/v1/tools/fetch"),
            "https://proxy.example.com/v1/tools/fetch"
        );
        assert_eq!(
            build_upstream_url("https://proxy.example.com/v1/", "/v1/tools/search"),
            "https://proxy.example.com/v1/tools/search"
        );
        assert_eq!(
            build_upstream_url("http://127.0.0.1:9000/api", "/v1/tools/fetch"),
            "http://127.0.0.1:9000/api/v1/tools/fetch"
        );
    }

    #[test]
    fn strips_transport_headers_but_keeps_end_to_end_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("gateway.local"));
        headers.insert(header::CONTENT_LENGTH, HeaderValue::from_static("42"));
        headers.insert(header::CONNECTION, HeaderValue::from_static("keep-alive"));
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.insert(header::EXPECT, HeaderValue::from_static("100-continue"));
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer sk-upstream"),
        );
        headers.insert("x-request-id", HeaderValue::from_static("req-1"));
        headers.insert("x-api-key", HeaderValue::from_static("sk-raw"));
        headers.insert("x-custom", HeaderValue::from_static("keep-me"));

        let forwarded = forwarded_request_headers(&headers);

        assert!(forwarded.get(header::HOST).is_none());
        assert!(forwarded.get(header::CONTENT_LENGTH).is_none());
        assert!(forwarded.get(header::CONNECTION).is_none());
        assert!(forwarded.get(header::TRANSFER_ENCODING).is_none());
        assert!(forwarded.get(header::EXPECT).is_none());

        // 鉴权头与自定义头都原样透传（不做 key 替换）
        assert_eq!(
            forwarded.get(header::AUTHORIZATION).unwrap(),
            "Bearer sk-upstream"
        );
        assert_eq!(forwarded.get("x-api-key").unwrap(), "sk-raw");
        assert_eq!(forwarded.get("x-request-id").unwrap(), "req-1");
        assert_eq!(forwarded.get("x-custom").unwrap(), "keep-me");
    }

    struct CapturedRequest {
        method: String,
        path: String,
        headers: Vec<(String, String)>,
        body: Vec<u8>,
    }

    impl CapturedRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        }
    }

    fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window == needle)
    }

    async fn read_request(socket: &mut TcpStream) -> CapturedRequest {
        let mut buf = Vec::new();
        let mut chunk = [0u8; 4096];
        let header_end = loop {
            let read = socket.read(&mut chunk).await.expect("read request");
            assert!(read > 0, "upstream closed before headers");
            buf.extend_from_slice(&chunk[..read]);
            if let Some(pos) = find(&buf, b"\r\n\r\n") {
                break pos;
            }
        };

        let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
        let mut lines = head.split("\r\n");
        let request_line = lines.next().expect("request line");
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts.next().unwrap_or_default().to_string();
        let path = request_parts.next().unwrap_or_default().to_string();
        let headers: Vec<(String, String)> = lines
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                Some((name.trim().to_string(), value.trim().to_string()))
            })
            .collect();

        let mut body = buf[header_end + 4..].to_vec();
        let chunked = headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("transfer-encoding")
                && value.to_ascii_lowercase().contains("chunked")
        });
        if chunked {
            // 读到最后一个 `0\r\n\r\n` 为止，再解码 chunked 体。
            while find(&body, b"\r\n0\r\n\r\n").is_none() {
                let read = socket.read(&mut chunk).await.expect("read chunked body");
                assert!(read > 0, "upstream closed before the final chunk");
                body.extend_from_slice(&chunk[..read]);
            }
            body = decode_chunked(&body);
        } else if let Some(length) = headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case("content-length"))
            .and_then(|(_, value)| value.parse::<usize>().ok())
        {
            while body.len() < length {
                let read = socket.read(&mut chunk).await.expect("read body");
                assert!(read > 0, "upstream closed before the declared body");
                body.extend_from_slice(&chunk[..read]);
            }
            body.truncate(length);
        }

        CapturedRequest {
            method,
            path,
            headers,
            body,
        }
    }

    fn decode_chunked(mut data: &[u8]) -> Vec<u8> {
        let mut decoded = Vec::new();
        while let Some(line_end) = find(data, b"\r\n") {
            let size_line = String::from_utf8_lossy(&data[..line_end]);
            let size = usize::from_str_radix(size_line.trim(), 16).expect("chunk size");
            data = &data[line_end + 2..];
            if size == 0 {
                break;
            }
            decoded.extend_from_slice(&data[..size]);
            data = &data[size + 2..];
        }
        decoded
    }

    /// 起一个只处理一次请求的假上游，返回其 base_url 与抓取到的请求。
    async fn spawn_upstream() -> (String, tokio::task::JoinHandle<CapturedRequest>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let handle = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let captured = read_request(&mut socket).await;
            socket
                .write_all(
                    b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: 11\r\n\r\n{\"ok\":true}",
                )
                .await
                .expect("write response");
            socket.flush().await.expect("flush");
            captured
        });
        (format!("http://{addr}"), handle)
    }

    #[tokio::test]
    async fn forwards_request_unchanged_and_returns_upstream_response() {
        let (base_url, upstream) = spawn_upstream().await;
        let http = Client::builder().build().expect("client");
        let request = Request::builder()
            .method(Method::POST)
            .uri("/v1/tools/search?q=deepseek")
            .header(header::AUTHORIZATION, "Bearer sk-upstream")
            .header(header::CONTENT_TYPE, "application/json")
            .header("x-request-id", "req-42")
            .body(Body::from(r#"{"query":"deepseek"}"#))
            .expect("request");

        let response = forward_tools_request(&http, &base_url, request)
            .await
            .expect("forward");

        assert_eq!(response.status(), StatusCode::CREATED);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok()),
            Some("application/json")
        );
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("read response body");
        assert_eq!(&body[..], br#"{"ok":true}"#);

        let captured = upstream.await.expect("upstream task");
        let upstream_addr = base_url
            .strip_prefix("http://")
            .expect("test base url")
            .to_string();
        assert_eq!(captured.method, "POST");
        assert_eq!(captured.path, "/v1/tools/search?q=deepseek");
        assert_eq!(captured.header("authorization"), Some("Bearer sk-upstream"));
        assert_eq!(captured.header("x-request-id"), Some("req-42"));
        assert_eq!(captured.header("content-type"), Some("application/json"));
        // 逐跳头不会上行；host 由 reqwest 按上游地址重建。
        assert_eq!(captured.header("connection"), None);
        assert_eq!(captured.header("host"), Some(upstream_addr.as_str()));
        assert_eq!(captured.body, br#"{"query":"deepseek"}"#);
    }
}
