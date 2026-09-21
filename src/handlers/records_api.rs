use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{Row, SqlitePool};

use crate::db::archive::ACTIVE_SHARD;
// 查询引擎（过滤 DSL / 跨分片查询 / 详情行 / 分面统计）已下沉至 db 层，本模块只做薄封装。
// `build_filters` 的 re-export 仅为 analysis_endpoint 的既有调用路径保留兼容。
pub(crate) use crate::db::records_query::build_filters;
use crate::db::records_query::{
    fetch_detail_row, has_payload, query_facets, query_records_active, query_records_multi,
    ShardInput,
};
use crate::state::app_state::AppState;
use std::sync::Arc;

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
    /// 指定归档分片 id；`active` 表示当前库。缺省时先查 active，再跨归档唯一匹配。
    pub shard: Option<String>,
}

/// 键集分页游标：`"<TimeMs>:<id>"`（单库）或 `"<TimeMs>:<id>:<shard>"`（跨月）。
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CursorKey {
    pub(crate) time_ms: i64,
    pub(crate) id: i64,
    pub(crate) shard: Option<String>,
}

/// 解析游标；两段式 shard 为 None，三段式 shard 为 Some。
pub(crate) fn parse_cursor(s: &str) -> Option<CursorKey> {
    let mut parts = s.split(':');
    let time_ms = parts.next()?.parse::<i64>().ok()?;
    let id = parts.next()?.parse::<i64>().ok()?;
    match parts.next() {
        None => Some(CursorKey {
            time_ms,
            id,
            shard: None,
        }),
        Some(shard) => {
            if shard.is_empty() || parts.next().is_some() {
                return None;
            }
            Some(CursorKey {
                time_ms,
                id,
                shard: Some(shard.to_string()),
            })
        }
    }
}

/// 生成游标：shard 为 None 时输出两段式以兼容单库分页。
pub(crate) fn format_cursor(time_ms: i64, id: i64, shard: Option<&str>) -> String {
    match shard {
        Some(s) => format!("{time_ms}:{id}:{s}"),
        None => format!("{time_ms}:{id}"),
    }
}

fn text(row: &sqlx::sqlite::SqliteRow, name: &str) -> Option<String> {
    row.try_get::<Option<String>, _>(name).ok().flatten()
}

pub(crate) fn int(row: &sqlx::sqlite::SqliteRow, name: &str) -> Option<i64> {
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

pub(crate) fn row_to_item(row: &sqlx::sqlite::SqliteRow) -> Value {
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
    // 仅在提供了 from/to 且命中归档分片时才走跨月检索；否则保持单库路径不变。
    let candidates = app_state
        .archive_registry
        .candidates(params.from, params.to)
        .await;
    if candidates.is_empty() {
        return list_records_active(&app_state, &params).await;
    }

    let active = app_state.query_pool.read().await.clone();
    let archives: Vec<ShardInput> = candidates
        .iter()
        .map(|shard| ShardInput {
            id: shard.id.clone(),
            pool: shard.pool.clone(),
        })
        .collect();

    let (items, next_cursor, total, total_exact) = query_records_multi(&active, &archives, &params)
        .await
        .map_err(|e| {
            tracing::error!("records multi-shard query failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "query failed".to_string(),
            )
        })?;

    Ok(Json(json!({
        "items": items,
        "nextCursor": next_cursor,
        "total": total,
        "totalExact": total_exact,
    })))
}

/// 默认单库列表路径：只查询 active 连接池，行为与历史版本完全一致。
async fn list_records_active(
    app_state: &Arc<AppState>,
    params: &ListParams,
) -> Result<Json<Value>, (StatusCode, String)> {
    let pool: tokio::sync::RwLockReadGuard<'_, SqlitePool> = app_state.query_pool.read().await;

    let (items, next_cursor, total, total_exact) =
        query_records_active(&*pool, params).await.map_err(|e| {
            tracing::error!("records list query failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "query failed".to_string(),
            )
        })?;

    Ok(Json(json!({
        "items": items,
        "nextCursor": next_cursor,
        "total": total,
        "totalExact": total_exact,
    })))
}

/// 组装详情 JSON 的列表字段与明细字段（不含按需正文）。
async fn build_detail_body(row: &sqlx::sqlite::SqliteRow, pool: &SqlitePool, id: i64) -> Value {
    let mut body = row_to_item(row);
    let payload = has_payload(pool, id).await;
    if let Some(object) = body.as_object_mut() {
        object.insert("prompt".to_string(), json!(text(row, "Prompt")));
        object.insert("requestTail".to_string(), json!(text(row, "RequestTail")));
        object.insert("answer".to_string(), json!(text(row, "Answer")));
        object.insert("toolNames".to_string(), json!(text(row, "ToolNames")));
        object.insert("apiKey".to_string(), json!(text(row, "ApiKey")));
        object.insert("hasPayload".to_string(), json!(payload));
    }
    body
}

/// 按需追加完整请求/响应/请求头（zstd 解压后明文）。
async fn append_body_payload(pool: &SqlitePool, id: i64, body: &mut Value) {
    let payload = crate::db::records::load_payload_from(pool, id).await;
    let (request, response, headers) = payload
        .map(|p| (p.request, p.response, p.headers))
        .unwrap_or_default();
    if let Some(object) = body.as_object_mut() {
        object.insert("request".to_string(), json!(request));
        object.insert("response".to_string(), json!(response));
        object.insert("headers".to_string(), json!(headers));
    }
}

pub async fn record_detail(
    State(app_state): State<Arc<AppState>>,
    Path(id): Path<i64>,
    Query(params): Query<DetailParams>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let include_body = params
        .include
        .as_deref()
        .map(|v| v.eq_ignore_ascii_case("body"))
        .unwrap_or(false);

    // 显式指定分片：active 或某个归档，找不到分片即 404。
    if let Some(shard) = params.shard.as_deref() {
        let (pool, shard_label) = if shard == ACTIVE_SHARD {
            (
                app_state.query_pool.read().await.clone(),
                ACTIVE_SHARD.to_string(),
            )
        } else if let Some(archive) = app_state.archive_registry.get(shard).await {
            (archive.pool.clone(), archive.id.clone())
        } else {
            return Err((StatusCode::NOT_FOUND, "shard not found".to_string()));
        };

        let Some(row) = fetch_detail_row(&pool, id).await? else {
            return Err((StatusCode::NOT_FOUND, "record not found".to_string()));
        };
        let mut body = build_detail_body(&row, &pool, id).await;
        if let Some(object) = body.as_object_mut() {
            object.insert("shard".to_string(), json!(shard_label));
        }
        if include_body {
            append_body_payload(&pool, id, &mut body).await;
        }
        return Ok(Json(body));
    }

    // 未指定分片：先查 active，命中则保持既有行为（不附加 shard 字段）。
    let active = app_state.query_pool.read().await.clone();
    if let Some(row) = fetch_detail_row(&active, id).await? {
        let mut body = build_detail_body(&row, &active, id).await;
        if include_body {
            append_body_payload(&active, id, &mut body).await;
        }
        return Ok(Json(body));
    }

    // active 未命中：在所有归档中按 id 唯一匹配。
    let shards = app_state.archive_registry.all_shards().await;
    let mut hits: Vec<(
        Arc<crate::db::archive::ArchiveShard>,
        sqlx::sqlite::SqliteRow,
    )> = Vec::new();
    for shard in &shards {
        if let Some(row) = fetch_detail_row(&shard.pool, id).await? {
            hits.push((shard.clone(), row));
        }
    }

    match hits.len() {
        0 => Err((StatusCode::NOT_FOUND, "record not found".to_string())),
        1 => {
            if let Some((shard, row)) = hits.into_iter().next() {
                let mut body = build_detail_body(&row, &shard.pool, id).await;
                if let Some(object) = body.as_object_mut() {
                    object.insert("shard".to_string(), json!(shard.id));
                }
                if include_body {
                    append_body_payload(&shard.pool, id, &mut body).await;
                }
                return Ok(Json(body));
            }
            Err((StatusCode::NOT_FOUND, "record not found".to_string()))
        }
        _ => {
            let mut ids: Vec<String> = vec!["ambiguous_record_id".to_string()];
            ids.extend(hits.iter().map(|(shard, _)| shard.id.clone()));
            Err((
                StatusCode::CONFLICT,
                serde_json::to_string(&ids).unwrap_or_else(|_| "ambiguous_record_id".to_string()),
            ))
        }
    }
}

pub async fn record_facets(
    State(app_state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let pool = app_state.query_pool.read().await;

    let (models, types, backends, clients) = query_facets(&pool).await;

    Ok(Json(json!({
        "models": models,
        "types": types,
        "backends": backends,
        "clients": clients,
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_roundtrip_two_and_three_fields() {
        let two = parse_cursor("1789639720742:273").unwrap();
        assert_eq!(
            two,
            CursorKey {
                time_ms: 1789639720742,
                id: 273,
                shard: None,
            }
        );
        assert_eq!(
            format_cursor(two.time_ms, two.id, None),
            "1789639720742:273"
        );

        let three = parse_cursor("1789639720742:273:record_202608").unwrap();
        assert_eq!(
            three,
            CursorKey {
                time_ms: 1789639720742,
                id: 273,
                shard: Some("record_202608".to_string()),
            }
        );
        assert_eq!(
            format_cursor(three.time_ms, three.id, three.shard.as_deref()),
            "1789639720742:273:record_202608"
        );

        assert!(parse_cursor("not-a-cursor").is_none());
        assert!(parse_cursor("1:2:3:4").is_none());
        assert!(parse_cursor("1:2:").is_none());
    }
}
