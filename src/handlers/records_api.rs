use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, SqlitePool};

use crate::state::app_state::AppState;
use std::sync::Arc;

const LIST_COLS: &str = "id, Time, TimeMs, Type, Model, Status, Backend, IP, Method, Endpoint, \
    SessionId, ParentSessionId, RequestId, ClientName, ClientVersion, UserAgent, \
    LatencyMs, TtftMs, UpstreamMs, StreamMs, PromptTokens, CompletionTokens, TotalTokens, \
    MessageCount, SystemCount, ToolCount, AssistantCount, ToolResultCount, ImageCount, \
    Tool, Multimodal, RequestBytes, ResponseBytes, FinishReason, Error, RetryCount, \
    substr(Prompt,1,201) AS PromptPreview";

const DETAIL_COLS: &str = "id, Time, TimeMs, Type, Model, Status, Backend, IP, Method, Endpoint, \
    SessionId, ParentSessionId, RequestId, ClientName, ClientVersion, UserAgent, \
    LatencyMs, TtftMs, UpstreamMs, StreamMs, PromptTokens, CompletionTokens, TotalTokens, \
    MessageCount, SystemCount, ToolCount, AssistantCount, ToolResultCount, ImageCount, \
    Tool, Multimodal, RequestBytes, ResponseBytes, FinishReason, Error, RetryCount, \
    Prompt, RequestTail, Answer, ToolNames, ApiKey";

const COUNT_CAP: i64 = 10_000;

#[derive(Debug, Default, Deserialize)]
pub struct ListParams {
    pub from: Option<i64>,
    pub to: Option<i64>,
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub model: Option<String>,
    pub status: Option<i64>,
    pub backend: Option<String>,
    pub ip: Option<String>,
    pub client: Option<String>,
    pub session_id: Option<String>,
    pub parent_session_id: Option<String>,
    pub request_id: Option<String>,
    pub apikey: Option<String>,
    pub q: Option<String>,
    pub errors: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<i64>,
}

#[derive(Debug, Default, Deserialize)]
pub struct DetailParams {
    pub include: Option<String>,
}

#[derive(Debug, Clone)]
enum Bind {
    Int(i64),
    Text(String),
}

fn like_contains(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('%');
    for ch in value.chars() {
        if matches!(ch, '%' | '_' | '\\') {
            out.push('\\');
        }
        out.push(ch);
    }
    out.push('%');
    out
}

fn fts_phrase(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 2);
    out.push('"');
    for ch in value.chars() {
        if ch == '"' {
            out.push('"');
        }
        out.push(ch);
    }
    out.push('"');
    out
}

/// Upper bound for a prefix scan: appending the largest code point sorts after every
/// string that starts with `value`, so `col >= value AND col < bound` is a half-open
/// range a B-tree can seek, unlike `LIKE 'value%'` on a BINARY column.
fn prefix_upper(value: &str) -> String {
    let mut out = String::with_capacity(value.len() + 4);
    out.push_str(value);
    out.push('\u{10FFFF}');
    out
}

fn push_opt_text(sql: &mut String, binds: &mut Vec<Bind>, column: &str, value: Option<&String>) {
    if let Some(v) = value {
        if !v.is_empty() {
            sql.push_str(&format!(" AND {column} LIKE ? ESCAPE '\\'"));
            binds.push(Bind::Text(like_contains(v)));
        }
    }
}

fn build_filters(p: &ListParams) -> (String, Vec<Bind>) {
    let mut sql = String::from(" WHERE 1=1");
    let mut binds: Vec<Bind> = Vec::new();

    if let Some(v) = p.from {
        sql.push_str(" AND TimeMs >= ?");
        binds.push(Bind::Int(v));
    }
    if let Some(v) = p.to {
        sql.push_str(" AND TimeMs <= ?");
        binds.push(Bind::Int(v));
    }
    if let Some(v) = p.type_.as_ref().filter(|v| !v.is_empty()) {
        sql.push_str(" AND Type = ?");
        binds.push(Bind::Text(v.clone()));
    }
    if let Some(v) = p.model.as_ref().filter(|v| !v.is_empty()) {
        sql.push_str(" AND Model COLLATE NOCASE >= ? AND Model COLLATE NOCASE < ?");
        binds.push(Bind::Text(v.clone()));
        binds.push(Bind::Text(prefix_upper(v)));
    }
    if let Some(v) = p.status {
        sql.push_str(" AND Status = ?");
        binds.push(Bind::Int(v));
    }
    if let Some(v) = p.backend.as_ref().filter(|v| !v.is_empty()) {
        sql.push_str(" AND Backend = ?");
        binds.push(Bind::Text(v.clone()));
    }
    if let Some(v) = p.ip.as_ref().filter(|v| !v.is_empty()) {
        sql.push_str(" AND IP COLLATE NOCASE >= ? AND IP COLLATE NOCASE < ?");
        binds.push(Bind::Text(v.clone()));
        binds.push(Bind::Text(prefix_upper(v)));
    }
    if let Some(v) = p.client.as_ref().filter(|v| !v.is_empty()) {
        let pat = like_contains(v);
        sql.push_str(" AND (ClientName LIKE ? ESCAPE '\\' OR UserAgent LIKE ? ESCAPE '\\')");
        binds.push(Bind::Text(pat.clone()));
        binds.push(Bind::Text(pat));
    }
    push_opt_text(&mut sql, &mut binds, "SessionId", p.session_id.as_ref());
    push_opt_text(
        &mut sql,
        &mut binds,
        "ParentSessionId",
        p.parent_session_id.as_ref(),
    );
    push_opt_text(&mut sql, &mut binds, "RequestId", p.request_id.as_ref());
    if let Some(v) = p.apikey.as_ref().filter(|v| !v.is_empty()) {
        sql.push_str(" AND ApiKey = ?");
        binds.push(Bind::Text(v.clone()));
    }

    let error_mode = p
        .errors
        .as_deref()
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if error_mode {
        sql.push_str(" AND Status >= 400");
    }

    if let Some(v) = p.q.as_ref().map(|v| v.trim()).filter(|v| !v.is_empty()) {
        let char_count = v.chars().count();
        if char_count >= 3 {
            sql.push_str(" AND id IN (SELECT rowid FROM records_fts WHERE records_fts MATCH ?)");
            binds.push(Bind::Text(fts_phrase(v)));
        } else {
            let pat = like_contains(v);
            sql.push_str(
                " AND (Prompt LIKE ? ESCAPE '\\' OR RequestTail LIKE ? ESCAPE '\\' OR Answer LIKE ? ESCAPE '\\')",
            );
            binds.push(Bind::Text(pat.clone()));
            binds.push(Bind::Text(pat.clone()));
            binds.push(Bind::Text(pat));
        }
    }

    (sql, binds)
}

fn with_cursor(sql: &mut String, binds: &mut Vec<Bind>, cursor: Option<&String>) -> bool {
    let Some((time_ms, id)) = cursor.and_then(|c| {
        let (a, b) = c.split_once(':')?;
        Some((a.parse::<i64>().ok()?, b.parse::<i64>().ok()?))
    }) else {
        return false;
    };
    sql.push_str(" AND (TimeMs, id) < (?, ?)");
    binds.push(Bind::Int(time_ms));
    binds.push(Bind::Int(id));
    true
}

fn bind_all<'q>(
    mut query: sqlx::query::Query<'q, Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    binds: &'q [Bind],
) -> sqlx::query::Query<'q, Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    for b in binds {
        query = match b {
            Bind::Int(v) => query.bind(*v),
            Bind::Text(v) => query.bind(v.as_str()),
        };
    }
    query
}

fn text(row: &sqlx::sqlite::SqliteRow, name: &str) -> Option<String> {
    row.try_get::<Option<String>, _>(name).ok().flatten()
}

fn int(row: &sqlx::sqlite::SqliteRow, name: &str) -> Option<i64> {
    row.try_get::<Option<i64>, _>(name).ok().flatten()
}

fn real(row: &sqlx::sqlite::SqliteRow, name: &str) -> Option<f64> {
    row.try_get::<Option<f64>, _>(name).ok().flatten()
}

fn flag(row: &sqlx::sqlite::SqliteRow, name: &str) -> bool {
    int(row, name).unwrap_or(0) != 0
}

fn preview(row: &sqlx::sqlite::SqliteRow) -> Option<String> {
    let raw = text(row, "PromptPreview")?;
    let mut chars = raw.chars();
    let head: String = chars.by_ref().take(200).collect();
    if chars.next().is_some() {
        Some(format!("{head}…"))
    } else {
        Some(head)
    }
}

fn tokens(row: &sqlx::sqlite::SqliteRow) -> (i64, i64, i64) {
    let prompt = int(row, "PromptTokens").unwrap_or(0);
    let completion = int(row, "CompletionTokens").unwrap_or(0);
    let mut total = int(row, "TotalTokens").unwrap_or(0);
    if total == 0 {
        total = prompt + completion;
    }
    (prompt, completion, total)
}

fn row_to_item(row: &sqlx::sqlite::SqliteRow) -> Value {
    let (prompt_tokens, completion_tokens, total_tokens) = tokens(row);
    json!({
        "id": int(row, "id").unwrap_or(0),
        "time": text(row, "Time"),
        "timeMs": int(row, "TimeMs"),
        "type": text(row, "Type"),
        "model": text(row, "Model"),
        "status": int(row, "Status"),
        "backend": text(row, "Backend"),
        "ip": text(row, "IP"),
        "method": text(row, "Method"),
        "endpoint": text(row, "Endpoint"),
        "sessionId": text(row, "SessionId"),
        "parentSessionId": text(row, "ParentSessionId"),
        "requestId": text(row, "RequestId"),
        "clientName": text(row, "ClientName"),
        "clientVersion": text(row, "ClientVersion"),
        "userAgent": text(row, "UserAgent"),
        "latencyMs": real(row, "LatencyMs"),
        "ttftMs": real(row, "TtftMs"),
        "upstreamMs": real(row, "UpstreamMs"),
        "streamMs": real(row, "StreamMs"),
        "promptTokens": prompt_tokens,
        "completionTokens": completion_tokens,
        "totalTokens": total_tokens,
        "messageCount": int(row, "MessageCount"),
        "systemCount": int(row, "SystemCount"),
        "toolCount": int(row, "ToolCount"),
        "assistantCount": int(row, "AssistantCount"),
        "toolResultCount": int(row, "ToolResultCount"),
        "imageCount": int(row, "ImageCount"),
        "tool": flag(row, "Tool"),
        "multimodal": flag(row, "Multimodal"),
        "requestBytes": int(row, "RequestBytes"),
        "responseBytes": int(row, "ResponseBytes"),
        "finishReason": text(row, "FinishReason"),
        "error": text(row, "Error"),
        "retryCount": int(row, "RetryCount"),
        "promptPreview": preview(row),
    })
}

pub async fn list_records(
    State(app_state): State<Arc<AppState>>,
    Query(params): Query<ListParams>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let pool: tokio::sync::RwLockReadGuard<'_, SqlitePool> = app_state.db_pool.read().await;

    let (where_sql, base_binds) = build_filters(&params);
    let limit = params.limit.unwrap_or(50).clamp(1, 500);

    let mut count_binds = base_binds.clone();
    count_binds.push(Bind::Int(COUNT_CAP));
    let count_sql = format!("SELECT COUNT(*) FROM (SELECT 1 FROM records{where_sql} LIMIT ?)");
    let count_row = bind_all(sqlx::query(&count_sql), &count_binds)
        .fetch_one(&*pool)
        .await
        .map_err(|e| {
            tracing::error!("records count query failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "query failed".to_string(),
            )
        })?;
    let total: i64 = count_row.try_get(0).unwrap_or(0);
    let total_exact = total < COUNT_CAP;

    let mut data_binds = base_binds;
    let mut data_where = where_sql;
    with_cursor(&mut data_where, &mut data_binds, params.cursor.as_ref());
    data_binds.push(Bind::Int(limit + 1));
    let data_sql = format!(
        "SELECT {LIST_COLS} FROM records{data_where} ORDER BY TimeMs DESC, id DESC LIMIT ?"
    );

    let rows = bind_all(sqlx::query(&data_sql), &data_binds)
        .fetch_all(&*pool)
        .await
        .map_err(|e| {
            tracing::error!("records list query failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "query failed".to_string(),
            )
        })?;

    let mut items: Vec<Value> = rows.iter().map(row_to_item).collect();
    let has_more = items.len() as i64 > limit;
    if has_more {
        items.pop();
    }
    let next_cursor = if has_more {
        items.last().and_then(|item| {
            let time_ms = item.get("timeMs")?.as_i64()?;
            let id = item.get("id")?.as_i64()?;
            Some(format!("{time_ms}:{id}"))
        })
    } else {
        None
    };

    Ok(Json(json!({
        "items": items,
        "nextCursor": next_cursor,
        "total": total,
        "totalExact": total_exact,
    })))
}

pub async fn record_detail(
    State(app_state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(params): Query<DetailParams>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let pool = app_state.db_pool.read().await;
    let sql = format!("SELECT {DETAIL_COLS} FROM records WHERE id = ?");
    let row = sqlx::query(&sql)
        .bind(id)
        .fetch_optional(&*pool)
        .await
        .map_err(|e| {
            tracing::error!("record detail query failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "query failed".to_string(),
            )
        })?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "record not found".to_string()))?;

    let has_payload =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM payloads WHERE record_id = ?")
            .bind(id)
            .fetch_one(&*pool)
            .await
            .unwrap_or(0)
            > 0;
    drop(pool);

    let mut body = row_to_item(&row);
    let object = body.as_object_mut().expect("row_to_item returns object");
    object.insert("prompt".to_string(), json!(text(&row, "Prompt")));
    object.insert("requestTail".to_string(), json!(text(&row, "RequestTail")));
    object.insert("answer".to_string(), json!(text(&row, "Answer")));
    object.insert("toolNames".to_string(), json!(text(&row, "ToolNames")));
    object.insert("apiKey".to_string(), json!(text(&row, "ApiKey")));
    object.insert("hasPayload".to_string(), json!(has_payload));

    let include_body = params
        .include
        .as_deref()
        .map(|v| v.eq_ignore_ascii_case("body"))
        .unwrap_or(false);
    if include_body {
        let payload = crate::db::records::load_payload(&app_state, id).await;
        let (request, response, headers) = payload
            .map(|p| (p.request, p.response, p.headers))
            .unwrap_or_default();
        object.insert("request".to_string(), json!(request));
        object.insert("response".to_string(), json!(response));
        object.insert("headers".to_string(), json!(headers));
    }

    Ok(Json(body))
}

pub async fn record_facets(
    State(app_state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let pool = app_state.db_pool.read().await;

    async fn distinct_text(pool: &SqlitePool, column: &str) -> Vec<String> {
        let sql = format!(
            "SELECT DISTINCT {column} FROM records WHERE {column} IS NOT NULL AND {column} != '' ORDER BY {column} LIMIT 200"
        );
        sqlx::query_scalar::<_, String>(&sql)
            .fetch_all(pool)
            .await
            .unwrap_or_default()
    }

    let models = distinct_text(&pool, "Model").await;
    let types = distinct_text(&pool, "Type").await;
    let backends = distinct_text(&pool, "Backend").await;

    let statuses: Vec<i64> = sqlx::query_scalar::<_, i64>(
        "SELECT DISTINCT Status FROM records WHERE Status IS NOT NULL ORDER BY Status LIMIT 200",
    )
    .fetch_all(&*pool)
    .await
    .unwrap_or_default();

    Ok(Json(json!({
        "models": models,
        "types": types,
        "statuses": statuses,
        "backends": backends,
    })))
}

#[cfg(test)]
mod tests {
    use super::prefix_upper;

    #[test]
    fn prefix_upper_sorts_after_every_matching_string() {
        let bound = prefix_upper("gpt");
        assert!("gpt".to_string() < bound);
        assert!("gpt-5.6-sol".to_string() < bound);
        assert!("gptzzz".to_string() < bound);
        assert!("gp".to_string() < bound);
        assert!("gpu".to_string() > bound);
    }
}
