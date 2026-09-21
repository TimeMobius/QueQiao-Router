//! records 检索的 SQL/查询引擎：列清单、过滤 DSL、跨分片查询与详情/分面查询。
//!

use axum::http::StatusCode;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{Row, Sqlite, SqlitePool};

use crate::db::archive::ACTIVE_SHARD;

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

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CursorKey {
    pub(crate) time_ms: i64,
    pub(crate) id: i64,
    pub(crate) shard: Option<String>,
}

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

pub(crate) fn format_cursor(time_ms: i64, id: i64, shard: Option<&str>) -> String {
    match shard {
        Some(s) => format!("{time_ms}:{id}:{s}"),
        None => format!("{time_ms}:{id}"),
    }
}

pub(crate) fn text(row: &sqlx::sqlite::SqliteRow, name: &str) -> Option<String> {
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

#[derive(Debug, Clone)]
pub(crate) enum Bind {
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

pub(crate) fn build_filters(p: &ListParams) -> (String, Vec<Bind>) {
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
    let Some(c) = cursor.and_then(|c| parse_cursor(c)) else {
        return false;
    };
    sql.push_str(" AND (TimeMs, id) < (?, ?)");
    binds.push(Bind::Int(c.time_ms));
    binds.push(Bind::Int(c.id));
    true
}

pub(crate) fn bind_all<'q>(
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

/// 一次多分片查询的输入分片（只读归档或 active）。
#[derive(Clone)]
pub(crate) struct ShardInput {
    pub(crate) id: String,
    pub(crate) pool: SqlitePool,
}

/// 跨分片数据查询：在单个分片上按时间倒序取 `limit + 1` 行，供全局归并。
///
/// 当游标为三段式时，SQL 使用与全局全序一致的分片感知谓词
/// `(TimeMs, id) < 游标 OR ((TimeMs, id) = 游标 AND shard < 游标分片)`
/// 精确排除已返回的游标行，避免每个分片白占一个 `LIMIT` 名额导致的分页漏行。
async fn query_shard(
    pool: &SqlitePool,
    where_sql: &str,
    binds: &[Bind],
    cursor: Option<&CursorKey>,
    limit: i64,
    shard_id: &str,
) -> Result<Vec<(i64, i64, String, Value)>, sqlx::Error> {
    let mut data_where = where_sql.to_string();
    let mut data_binds: Vec<Bind> = binds.to_vec();
    if let Some(c) = cursor {
        match c.shard.as_deref() {
            Some(cursor_shard) => {
                let shard_less = i64::from(shard_id < cursor_shard);
                data_where
                    .push_str(" AND ((TimeMs, id) < (?, ?) OR ((TimeMs, id) = (?, ?) AND ? = 1))");
                data_binds.push(Bind::Int(c.time_ms));
                data_binds.push(Bind::Int(c.id));
                data_binds.push(Bind::Int(c.time_ms));
                data_binds.push(Bind::Int(c.id));
                data_binds.push(Bind::Int(shard_less));
            }
            None => {
                data_where.push_str(" AND (TimeMs, id) < (?, ?)");
                data_binds.push(Bind::Int(c.time_ms));
                data_binds.push(Bind::Int(c.id));
            }
        }
    }
    data_binds.push(Bind::Int(limit + 1));

    let data_sql = format!(
        "SELECT {LIST_COLS} FROM records{data_where} ORDER BY TimeMs DESC, id DESC LIMIT ?"
    );
    let rows = bind_all(sqlx::query(&data_sql), &data_binds)
        .fetch_all(pool)
        .await?;

    let mut out = Vec::with_capacity(rows.len());
    for row in &rows {
        let (Some(time_ms), Some(id)) = (int(row, "TimeMs"), int(row, "id")) else {
            continue;
        };
        out.push((time_ms, id, shard_id.to_string(), row_to_item(row)));
    }
    Ok(out)
}

/// 跨分片列表查询：并发查询 active 与命中候选归档，按全局
/// `(TimeMs DESC, id DESC, shard DESC)` 归并、分页，并汇总带上限的总数。
pub(crate) async fn query_records_multi(
    active: &SqlitePool,
    archives: &[ShardInput],
    params: &ListParams,
) -> Result<(Vec<Value>, Option<String>, i64, bool), sqlx::Error> {
    let (where_sql, base_binds) = build_filters(params);
    let limit = params.limit.unwrap_or(50).clamp(1, 500);
    let cursor = params.cursor.as_deref().and_then(parse_cursor);

    let mut all_shards: Vec<ShardInput> = Vec::with_capacity(archives.len() + 1);
    all_shards.push(ShardInput {
        id: ACTIVE_SHARD.to_string(),
        pool: active.clone(),
    });
    all_shards.extend(archives.iter().cloned());

    let count_sql = format!("SELECT COUNT(*) FROM (SELECT 1 FROM records{where_sql} LIMIT ?)");
    let mut total_sum: i64 = 0;
    for shard in &all_shards {
        let mut count_binds = base_binds.clone();
        count_binds.push(Bind::Int(COUNT_CAP));
        let row = bind_all(sqlx::query(&count_sql), &count_binds)
            .fetch_one(&shard.pool)
            .await?;
        total_sum = total_sum.saturating_add(row.try_get(0).unwrap_or(0));
    }
    let total = total_sum.min(COUNT_CAP);
    let total_exact = total_sum < COUNT_CAP;

    let shard_futures = all_shards.iter().map(|shard| {
        query_shard(
            &shard.pool,
            &where_sql,
            &base_binds,
            cursor.as_ref(),
            limit,
            &shard.id,
        )
    });
    let results = futures::future::join_all(shard_futures).await;

    let mut rows: Vec<(i64, i64, String, Value)> = Vec::new();
    for result in results {
        rows.extend(result?);
    }

    // 防御性后置过滤：保证严格小于游标的行才进入下一页。
    if let Some(c) = cursor.as_ref() {
        rows.retain(|(time_ms, id, shard, _)| match c.shard.as_deref() {
            Some(cursor_shard) => (*time_ms, *id, shard.as_str()) < (c.time_ms, c.id, cursor_shard),
            None => (*time_ms, *id) < (c.time_ms, c.id),
        });
    }

    rows.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then_with(|| b.1.cmp(&a.1))
            .then_with(|| b.2.cmp(&a.2))
    });

    let has_more = rows.len() as i64 > limit;
    if has_more {
        rows.truncate(limit as usize);
    }
    let next_cursor = if has_more {
        rows.last()
            .map(|(time_ms, id, shard, _)| format_cursor(*time_ms, *id, Some(shard.as_str())))
    } else {
        None
    };

    let items: Vec<Value> = rows
        .into_iter()
        .map(|(_, _, shard, mut value)| {
            if let Some(object) = value.as_object_mut() {
                object.insert("shard".to_string(), json!(shard));
            }
            value
        })
        .collect();

    Ok((items, next_cursor, total, total_exact))
}

/// 单库列表查询：只查 active 连接池，行为与历史单库路径完全一致。
pub(crate) async fn query_records_active(
    pool: &SqlitePool,
    params: &ListParams,
) -> Result<(Vec<Value>, Option<String>, i64, bool), sqlx::Error> {
    let (where_sql, base_binds) = build_filters(params);
    let limit = params.limit.unwrap_or(50).clamp(1, 500);

    let mut count_binds = base_binds.clone();
    count_binds.push(Bind::Int(COUNT_CAP));
    let count_sql = format!("SELECT COUNT(*) FROM (SELECT 1 FROM records{where_sql} LIMIT ?)");
    let count_row = bind_all(sqlx::query(&count_sql), &count_binds)
        .fetch_one(pool)
        .await?;
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
        .fetch_all(pool)
        .await?;

    let mut items: Vec<Value> = rows.iter().map(row_to_item).collect();
    let has_more = items.len() as i64 > limit;
    if has_more {
        items.pop();
    }
    let next_cursor = if has_more {
        items.last().and_then(|item| {
            let time_ms = item.get("timeMs")?.as_i64()?;
            let id = item.get("id")?.as_i64()?;
            Some(format_cursor(time_ms, id, None))
        })
    } else {
        None
    };

    Ok((items, next_cursor, total, total_exact))
}

/// 在指定连接池上按 id 取详情行。
pub(crate) async fn fetch_detail_row(
    pool: &SqlitePool,
    id: i64,
) -> Result<Option<sqlx::sqlite::SqliteRow>, (StatusCode, String)> {
    let sql = format!("SELECT {DETAIL_COLS} FROM records WHERE id = ?");
    sqlx::query(&sql)
        .bind(id)
        .fetch_optional(pool)
        .await
        .map_err(|e| {
            tracing::error!("record detail query failed: {e}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "query failed".to_string(),
            )
        })
}

/// 在指定连接池上判断记录是否含压缩正文。
pub(crate) async fn has_payload(pool: &SqlitePool, id: i64) -> bool {
    sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM payloads WHERE record_id = ?")
        .bind(id)
        .fetch_one(pool)
        .await
        .unwrap_or(0)
        > 0
}

/// 单列 DISTINCT 取值（白名单列），供分面查询复用。
async fn distinct_text(pool: &SqlitePool, column: &str) -> Vec<String> {
    let sql = format!(
        "SELECT DISTINCT {column} FROM records WHERE {column} IS NOT NULL AND {column} != '' ORDER BY {column} LIMIT 200"
    );
    sqlx::query_scalar::<_, String>(&sql)
        .fetch_all(pool)
        .await
        .unwrap_or_default()
}

/// 分面取值：models/types/backends/clients 四个 DISTINCT 列表。
pub(crate) async fn query_facets(
    pool: &SqlitePool,
) -> (Vec<String>, Vec<String>, Vec<String>, Vec<String>) {
    let models = distinct_text(pool, "Model").await;
    let types = distinct_text(pool, "Type").await;
    let backends = distinct_text(pool, "Backend").await;
    let clients = distinct_text(pool, "ClientName").await;

    (models, types, backends, clients)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

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

    #[test]
    fn prefix_upper_sorts_after_every_matching_string() {
        let bound = prefix_upper("gpt");
        assert!("gpt" < bound.as_str());
        assert!("gpt-5.6-sol" < bound.as_str());
        assert!("gptzzz" < bound.as_str());
        assert!("gp" < bound.as_str());
        assert!("gpu" > bound.as_str());
    }

    fn temp_db_path(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!(
            "qq_records_query_{}_{}_{}.db",
            std::process::id(),
            tag,
            nanos
        ))
    }

    async fn make_records_db(path: &std::path::Path) -> SqlitePool {
        let url = format!("sqlite:{}", path.display());
        let options = sqlx::sqlite::SqliteConnectOptions::from_str(&url)
            .unwrap()
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await.unwrap();
        sqlx::query(
            "CREATE TABLE records (\
                id INTEGER PRIMARY KEY, Time TEXT, TimeMs INTEGER, Type TEXT, Model TEXT, \
                Status INTEGER, Backend TEXT, IP TEXT, Method TEXT, Endpoint TEXT, \
                SessionId TEXT, ParentSessionId TEXT, RequestId TEXT, ClientName TEXT, \
                ClientVersion TEXT, UserAgent TEXT, LatencyMs REAL, TtftMs REAL, \
                UpstreamMs REAL, StreamMs REAL, PromptTokens INTEGER, CompletionTokens INTEGER, \
                TotalTokens INTEGER, MessageCount INTEGER, SystemCount INTEGER, ToolCount INTEGER, \
                AssistantCount INTEGER, ToolResultCount INTEGER, ImageCount INTEGER, Tool BOOLEAN, \
                Multimodal BOOLEAN, RequestBytes INTEGER, ResponseBytes INTEGER, FinishReason TEXT, \
                Error TEXT, RetryCount INTEGER, Prompt TEXT)",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    async fn insert_record(pool: &SqlitePool, id: i64, time_ms: i64) {
        sqlx::query("INSERT INTO records (id, Time, TimeMs, Model, Prompt) VALUES (?, ?, ?, ?, ?)")
            .bind(id)
            .bind("2026-08-01 00:00:00.000000")
            .bind(time_ms)
            .bind("test-model")
            .bind("hello")
            .execute(pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn cross_shard_merge_orders_and_paginates() {
        let active_path = temp_db_path("active");
        let archive_path = temp_db_path("archive");
        let active = make_records_db(&active_path).await;
        let archive_pool = make_records_db(&archive_path).await;

        // active 分片
        insert_record(&active, 5, 100).await;
        insert_record(&active, 3, 100).await;
        insert_record(&active, 9, 90).await;
        insert_record(&active, 1, 80).await;
        // 归档分片：与 active 存在 (TimeMs,id) 重叠，覆盖全序与并列场景
        insert_record(&archive_pool, 5, 100).await;
        insert_record(&archive_pool, 7, 95).await;
        insert_record(&archive_pool, 9, 90).await;
        insert_record(&archive_pool, 2, 70).await;

        let archives = vec![ShardInput {
            id: "record_202608".to_string(),
            pool: archive_pool.clone(),
        }];

        let params = ListParams {
            limit: Some(3),
            ..Default::default()
        };
        let (items, next, total, total_exact) = query_records_multi(&active, &archives, &params)
            .await
            .unwrap();
        assert_eq!(total, 8);
        assert!(total_exact);
        assert_eq!(items.len(), 3);
        assert!(next.is_some());

        let collect = |items: &[Value]| -> Vec<(i64, i64, String)> {
            items
                .iter()
                .map(|v| {
                    (
                        v["timeMs"].as_i64().unwrap(),
                        v["id"].as_i64().unwrap(),
                        v["shard"].as_str().unwrap().to_string(),
                    )
                })
                .collect()
        };

        let mut cursor = next;
        let mut collected = collect(&items);
        while let Some(c) = cursor {
            let params = ListParams {
                limit: Some(3),
                cursor: Some(c),
                ..Default::default()
            };
            let (items, next, _, _) = query_records_multi(&active, &archives, &params)
                .await
                .unwrap();
            collected.extend(collect(&items));
            cursor = next;
            assert!(collected.len() <= 8, "分页必须终止且不得产生重复/遗漏");
        }

        let expected = vec![
            (100, 5, "record_202608".to_string()),
            (100, 5, "active".to_string()),
            (100, 3, "active".to_string()),
            (95, 7, "record_202608".to_string()),
            (90, 9, "record_202608".to_string()),
            (90, 9, "active".to_string()),
            (80, 1, "active".to_string()),
            (70, 2, "record_202608".to_string()),
        ];
        assert_eq!(collected, expected);

        drop(active);
        drop(archive_pool);
        let _ = std::fs::remove_file(&active_path);
        let _ = std::fs::remove_file(&archive_path);
    }
}
