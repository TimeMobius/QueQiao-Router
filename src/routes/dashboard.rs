use axum::{
    body::Body,
    extract::Path,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use rust_embed::RustEmbed;
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::handlers::{analysis_api, error_log_api, records_api};
use crate::state::app_state::AppState;

/// 内嵌的监控面板静态资源（web/ 目录，编译进二进制）
#[derive(RustEmbed)]
#[folder = "web/"]
struct DashboardAssets;

/// 监控面板路由：`/dashboard` 返回页面，`/dashboard/*path` 返回静态资源
pub fn dashboard_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/", get(index_handler))
        .route("/records", get(records_page))
        .route("/analysis", get(analysis_page))
        .route("/api/records", get(records_api::list_records))
        .route("/api/records/facets", get(records_api::record_facets))
        .route("/api/analysis", get(analysis_api::analysis))
        .route("/api/analysis/errors", get(analysis_api::analysis_errors))
        .route("/api/error-log", get(error_log_api::error_log_tail))
        .route("/api/records/:id", get(records_api::record_detail))
        .route("/*path", get(assets_handler))
}

async fn index_handler(headers: HeaderMap) -> Response {
    serve_asset("index.html", &headers)
}

async fn records_page(headers: HeaderMap) -> Response {
    serve_asset("records.html", &headers)
}

async fn analysis_page(headers: HeaderMap) -> Response {
    serve_asset("analysis.html", &headers)
}

async fn assets_handler(Path(path): Path<String>, headers: HeaderMap) -> Response {
    serve_asset(&path, &headers)
}

/// Assets are `no-cache` rather than `immutable` because filenames are not
/// content-hashed; the ETag makes that revalidation a cheap 304.
fn etag_for(bytes: &[u8]) -> String {
    format!("\"{:x}\"", Sha256::digest(bytes))
}

fn serve_asset(path: &str, request_headers: &HeaderMap) -> Response {
    let path = path.trim_start_matches('/');
    match DashboardAssets::get(path) {
        Some(content) => {
            let mime_type = mime_for(path);
            let cache_control = if path.starts_with("vendor/") {
                "public, max-age=31536000, immutable"
            } else {
                "no-cache"
            };
            let bytes = content.data.into_owned();
            let etag = etag_for(&bytes);

            let not_modified = request_headers
                .get(header::IF_NONE_MATCH)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| {
                    value
                        .split(',')
                        .any(|candidate| candidate.trim().trim_start_matches("W/") == etag)
                });

            if not_modified {
                let mut headers = HeaderMap::new();
                headers.insert(
                    header::CACHE_CONTROL,
                    HeaderValue::from_static(cache_control),
                );
                if let Ok(value) = HeaderValue::from_str(&etag) {
                    headers.insert(header::ETAG, value);
                }
                return (StatusCode::NOT_MODIFIED, headers).into_response();
            }

            let mut headers = HeaderMap::new();
            headers.insert(header::CONTENT_TYPE, HeaderValue::from_static(mime_type));
            headers.insert(
                header::CACHE_CONTROL,
                HeaderValue::from_static(cache_control),
            );
            if let Ok(value) = HeaderValue::from_str(&etag) {
                headers.insert(header::ETAG, value);
            }
            (StatusCode::OK, headers, Body::from(bytes)).into_response()
        }
        None => (StatusCode::NOT_FOUND, "404 Not Found").into_response(),
    }
}

fn mime_for(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or_default();
    match ext {
        "html" => "text/html; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json; charset=utf-8",
        "woff2" => "font/woff2",
        "woff" => "font/woff",
        "ttf" => "font/ttf",
        "eot" => "application/vnd.ms-fontobject",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "txt" => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}
