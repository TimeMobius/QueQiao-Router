use axum::{
    body::Body,
    extract::Path,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use rust_embed::RustEmbed;
use std::sync::Arc;

use crate::handlers::records_api;
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
        .route("/api/records", get(records_api::list_records))
        .route("/api/records/facets", get(records_api::record_facets))
        .route("/api/records/:id", get(records_api::record_detail))
        .route("/*path", get(assets_handler))
}

async fn index_handler() -> Response {
    serve_asset("index.html")
}

async fn records_page() -> Response {
    serve_asset("records.html")
}

async fn assets_handler(Path(path): Path<String>) -> Response {
    serve_asset(&path)
}

fn serve_asset(path: &str) -> Response {
    let path = path.trim_start_matches('/');
    match DashboardAssets::get(path) {
        Some(content) => {
            let mime_type = mime_for(path);
            let cache_control = if path.starts_with("vendor/") {
                "public, max-age=31536000, immutable"
            } else {
                "no-cache"
            };
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, mime_type),
                    (header::CACHE_CONTROL, cache_control),
                ],
                Body::from(content.data.into_owned()),
            )
                .into_response()
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
