//! Test-only endpoints used by the external acceptance suite.
//!
//! Registered only when `ENABLE_TEST_API=1`; never part of the public router.

use axum::{
    extract::State,
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::TimeZone;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

use crate::db::engine::Value as DbValue;
use crate::db::payload::compress;
use crate::db::records::Record;
use crate::db::writer::AuditItem;
use crate::state::app_state::AppState;

#[derive(Debug, Deserialize)]
pub struct TestRecord {
    pub time_ms: Option<i64>,
    #[serde(default = "default_model")]
    pub model: String,
    pub status: Option<i64>,
    #[serde(default = "default_prompt")]
    pub prompt: String,
    #[serde(default = "default_answer")]
    pub answer: String,
    #[serde(default = "default_type")]
    pub r#type: String,
    pub request_id: Option<String>,
    #[serde(default = "default_ip")]
    pub ip: String,
}

fn default_model() -> String {
    "gpt-4".to_string()
}
fn default_prompt() -> String {
    "test prompt".to_string()
}
fn default_answer() -> String {
    "test answer".to_string()
}
fn default_type() -> String {
    "chat.completions".to_string()
}
fn default_ip() -> String {
    "127.0.0.1".to_string()
}

pub fn test_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/records", post(insert_record))
        .route("/reset", post(reset_records))
        .route("/stats", get(stats))
}

pub async fn insert_record(
    State(app_state): State<Arc<AppState>>,
    Json(payload): Json<TestRecord>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let now = chrono::Local::now();
    let time_ms = payload.time_ms.unwrap_or_else(|| now.timestamp_millis());
    let time = chrono::Local
        .timestamp_millis_opt(time_ms)
        .single()
        .map(|value| value.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
        .unwrap_or_else(|| now.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
    let request_id = payload.request_id.unwrap_or_else(|| {
        format!(
            "test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|value| value.as_nanos())
                .unwrap_or_default()
        )
    });
    let request_body = json!({
        "model": payload.model,
        "messages": [{"role": "user", "content": payload.prompt}],
    })
    .to_string();
    let response_body = json!({
        "choices": [{"message": {"role": "assistant", "content": payload.answer}}],
    })
    .to_string();

    let record = Record {
        time,
        time_ms,
        ip: payload.ip,
        method: Some("POST".to_string()),
        endpoint: Some("/v1/chat/completions".to_string()),
        model: payload.model,
        r#type: payload.r#type,
        backend: Some("test".to_string()),
        session_id: None,
        parent_session_id: None,
        request_id: Some(request_id.clone()),
        session_affinity: None,
        user_agent: Some("queqiao-test".to_string()),
        client_name: Some("test-client".to_string()),
        client_version: None,
        api_key: None,
        status: Some(payload.status.unwrap_or(200)),
        error: None,
        retry_count: Some(0),
        finish_reason: Some("stop".to_string()),
        latency_ms: Some(1.0),
        ttft_ms: None,
        upstream_ms: None,
        stream_ms: None,
        completion_tokens: 2,
        prompt_tokens: 1,
        total_tokens: 3,
        tool: false,
        multimodal: false,
        request_bytes: request_body.len() as i64,
        response_bytes: response_body.len() as i64,
        prompt_bytes: payload.prompt.len() as i64,
        request_tail_bytes: payload.prompt.len() as i64,
        answer_bytes: payload.answer.len() as i64,
        message_count: 1,
        system_count: 0,
        tool_count: 0,
        assistant_count: 0,
        tool_result_count: 0,
        image_count: 0,
        prompt: payload.prompt.clone(),
        request_tail: payload.prompt,
        answer: payload.answer.clone(),
        tool_names: String::new(),
        headers: "{}".to_string(),
        request: request_body,
        response: response_body,
    };
    let request = compress(record.request.as_bytes());
    let response = compress(record.response.as_bytes());
    let headers = compress(record.headers.as_bytes());
    let item = AuditItem {
        record,
        request,
        response,
        headers,
    };
    if !app_state.audit_writer.enqueue(item).await {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "audit queue is full".to_string(),
        ));
    }
    app_state
        .audit_writer
        .flush()
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;

    let store = app_state.active_db().await;
    let row = store
        .query_one(
            "SELECT id FROM records WHERE RequestId = ?",
            vec![DbValue::Text(request_id)],
        )
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
        .ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "record not found after flush".to_string(),
            )
        })?;
    Ok(Json(json!({ "id": row.get_i64(0).unwrap_or_default() })))
}

pub async fn reset_records(
    State(app_state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let deleted = app_state
        .audit_writer
        .reset()
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(json!({ "deleted": deleted })))
}

pub async fn stats(
    State(app_state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let store = app_state.active_db().await;
    let row = store
        .query_one(
            "SELECT COUNT(*), MIN(TimeMs), MAX(TimeMs) FROM records",
            Vec::new(),
        )
        .await
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    let (count, min_ms, max_ms) = match row {
        Some(row) => (
            row.get_i64(0).unwrap_or(0),
            row.get_opt_i64(1).unwrap_or(None),
            row.get_opt_i64(2).unwrap_or(None),
        ),
        None => (0, None, None),
    };
    let wal_bytes = std::fs::metadata(format!("{}-wal", store.path().display()))
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    let writer = app_state.audit_writer.snapshot();
    Ok(Json(json!({
        "count": count,
        "minTimeMs": min_ms,
        "maxTimeMs": max_ms,
        "walBytes": wal_bytes,
        "writer": writer,
    })))
}
