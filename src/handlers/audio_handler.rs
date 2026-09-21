use crate::app_error::AppError;
use axum::{
    extract::{Multipart, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use reqwest::multipart::{Form, Part};
use std::sync::Arc;
use tracing::error;

use crate::{
    client::proxy::{build_and_send_request_multipart, get_api_key},
    state::app_state::AppState,
};

pub async fn handle_audio_transcription(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    handle_audio_request(app_state, headers, multipart, "audio/transcriptions").await
}

pub async fn handle_audio_translation(
    State(app_state): State<Arc<AppState>>,
    headers: HeaderMap,
    multipart: Multipart,
) -> Response {
    handle_audio_request(app_state, headers, multipart, "audio/translations").await
}

// 用于在内存中缓存 Multipart 数据的结构体
struct CachedPart {
    name: String,
    data: Vec<u8>,
    file_name: Option<String>,
    content_type: Option<String>,
}

async fn handle_audio_request(
    app_state: Arc<AppState>,
    headers: HeaderMap,
    mut multipart: Multipart,
    endpoint_path: &str,
) -> Response {
    let mut model_name = String::new();
    let mut cached_parts: Vec<CachedPart> = Vec::new();

    // 1. 解析 Multipart，提取 model 并缓存所有部分
    while let Ok(Some(field)) = multipart.next_field().await {
        let name = field.name().unwrap_or("").to_string();
        let file_name = field.file_name().map(|s| s.to_string());
        let content_type = field.content_type().map(|s| s.to_string());

        // 读取数据到内存
        let data = match field.bytes().await {
            Ok(bytes) => bytes.to_vec(),
            Err(e) => {
                return AppError::ApiError(
                    StatusCode::BAD_REQUEST,
                    format!("Failed to read field data: {}", e),
                    "multipart_error".to_string(),
                )
                .into_response();
            }
        };

        if name == "model" {
            model_name = String::from_utf8_lossy(&data).to_string();
        }

        cached_parts.push(CachedPart {
            name,
            data,
            file_name,
            content_type,
        });
    }

    if model_name.is_empty() {
        return AppError::ApiError(
            StatusCode::BAD_REQUEST,
            "Missing 'model' field in multipart form data".to_string(),
            "validation_error".to_string(),
        )
        .into_response();
    }

    let endpoint_path_owned = endpoint_path.to_string();
    let cached_parts_clone = cached_parts;
    let app_state_for_closure = app_state.clone();

    // 2. 委托给 DispatcherService 处理路由和重试
    app_state
        .dispatcher_service
        .execute(
            &model_name,
            None,
            Some(endpoint_path),
            move |client_config: &crate::config::types::ClientConfig, _current_model| {
                let app_state = app_state_for_closure.clone();
                let headers = headers.clone();

                let parts_clone: Vec<CachedPart> = cached_parts_clone
                    .iter()
                    .map(|p| CachedPart {
                        name: p.name.clone(),
                        data: p.data.clone(),
                        file_name: p.file_name.clone(),
                        content_type: p.content_type.clone(),
                    })
                    .collect();

                let client_config = client_config.clone();
                let endpoint_path = endpoint_path_owned.clone();
                let current_model = _current_model.to_string();

                async move {
                    // 3. 为每个客户端重新构建 Form
                    let mut form = Form::new();
                    for part in parts_clone {
                        let mut req_part = Part::bytes(part.data);
                        if let Some(fn_str) = part.file_name {
                            req_part = req_part.file_name(fn_str);
                        }
                        if let Some(ct_str) = part.content_type {
                            if let Ok(_) = ct_str.parse::<mime::Mime>() {
                                req_part =
                                    req_part.mime_str(&ct_str).expect("Mime confirmed valid");
                            } else {
                                error!("Invalid mime type in audio part: {}", ct_str);
                            }
                        }
                        form = form.part(part.name, req_part);
                    }

                    let url = format!(
                        "{}/{}",
                        client_config.base_url.trim_end_matches('/'),
                        endpoint_path
                    );
                    let api_endpoint = format!("/v1/{}", endpoint_path);
                    let api_key = get_api_key(&client_config, &headers);

                    // 4. 发送请求（音频请求不支持流式）
                    match build_and_send_request_multipart(
                        &app_state,
                        &client_config,
                        &api_key,
                        &url,
                        form,
                        false,
                        &api_endpoint,
                        current_model.as_str(),
                        &headers,
                    )
                    .await
                    {
                        Ok(resp) => {
                            let status = resp.status();
                            let headers = resp.headers().clone();
                            let bytes = match resp.bytes().await {
                                Ok(b) => b,
                                Err(e) => {
                                    error!("Failed to read response bytes: {}", e);
                                    return Err(AppError::from(e));
                                }
                            };

                            let mut response = bytes.into_response();
                            *response.status_mut() = status;
                            if let Some(ct) = headers.get("content-type") {
                                response.headers_mut().insert("content-type", ct.clone());
                            }

                            Ok(response)
                        }
                        Err(e) => {
                            error!(
                                "Failed to process audio request with client {}: {:?}",
                                client_config.name, e
                            );
                            Err(AppError::InternalServerError(e.to_string()))
                        }
                    }
                }
            },
        )
        .await
}
