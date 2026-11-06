//! 会话时间线重建：把**同一会话**的记录按时间聚合，供 `/dashboard/records` 使用。
//!
//! ## 两类记录，两条路径
//!
//! 1. **有 `SessionId`**：直接 `WHERE SessionId = ?` 精确查询。这是主路径，
//!    与面板原有「会话时间线」按钮语义一致（原实现走 `?session_id=` 子串过滤 +
//!    客户端全等二次过滤，见 `web/assets/records.js` 的 `loadTimeline`）。
//! 2. **无 `SessionId`**：按 [`crate::db::session_fingerprint`] 的对话前缀指纹聚类。
//!    由于指纹是派生量、不在库里，只能先把候选窗口内的记录取出来，逐条解压请求体
//!    算指纹，再分组——因此这条路径有明确的扫描上限，且**只在用户显式要求时**才跑。
//!
//! ## 为什么不在数据库里加一列
//!
//! 指纹完全可从 `payloads` 表里已存的请求体推导，加列意味着 schema 迁移、
//! 双写维护、以及历史数据回填；而它的用途只有「面板上一个可选视图」。
//! 查询时现场计算 + 进程内缓存（见 `session_fingerprint::fingerprint_cached`）
//! 把代价限制在真正被查看的那批记录上，且零 schema 变更。

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::Deserialize;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;

use crate::db::engine::DbError;
use crate::db::records::load_payload_from;
use crate::db::records_query::{query_records_active, ListParams};
use crate::db::session_fingerprint::fingerprint_cached;
use crate::db::store::AuditStore;
use crate::state::app_state::AppState;

/// 指纹聚类模式下的候选扫描上限。
///
/// 每条候选都要解压一次请求体，所以必须封顶。2000 条足以覆盖「最近几小时」
/// 的常规排查；超出时响应里会带 `truncated: true`，前端提示收窄时间范围。
const FINGERPRINT_SCAN_CAP: i64 = 2_000;

/// 单次时间线返回的最大条数（与面板原实现的 `TIMELINE_MAX_ITEMS` 对齐）。
const TIMELINE_MAX_ITEMS: usize = 2_500;

#[derive(Debug, Default, Deserialize)]
pub struct TimelineParams {
    /// 已知会话 id。给了就直接精确查询，**不**跑指纹聚类。
    pub session_id: Option<String>,
    /// 对话前缀指纹（`prefix:xxxxxxxxxxxxxxxx` 形式或裸 16 位串）。
    /// 与 `session_id` 二选一。
    pub fingerprint: Option<String>,
    pub from: Option<i64>,
    pub to: Option<i64>,
    /// 复用列表接口的过滤参数，缩小指纹扫描范围。
    #[serde(rename = "type")]
    pub type_: Option<String>,
    pub model: Option<String>,
    pub client: Option<String>,
    pub backend: Option<String>,
}

/// 指纹分组结果。
struct FingerprintGroups {
    /// fingerprint -> 记录（已按时间升序）
    groups: HashMap<String, Vec<JsonValue>>,
    /// 无法归属的记录（请求体缺失 / 无 message 可哈希）
    unassigned: usize,
    /// 是否因扫描上限而截断
    truncated: bool,
    scanned: usize,
}

/// 在候选窗口内按指纹聚类。
///
/// 逐条解压请求体是这里的主要成本；命中缓存时只做一次哈希查表。
async fn group_by_fingerprint(
    active: &Arc<AuditStore>,
    params: &ListParams,
) -> Result<FingerprintGroups, DbError> {
    let scan = ListParams {
        from: params.from,
        to: params.to,
        type_: params.type_.clone(),
        model: params.model.clone(),
        client: params.client.clone(),
        backend: params.backend.clone(),
        limit: Some(FINGERPRINT_SCAN_CAP.min(500)),
        ..Default::default()
    };

    // 复用列表查询逐页拉取，直到凑满扫描上限或没有更多数据。
    let mut rows: Vec<JsonValue> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut truncated = false;
    loop {
        let page_params = ListParams {
            cursor: cursor.clone(),
            ..scan.clone()
        };
        let (items, next, _, _) = query_records_active(active, &page_params).await?;
        if items.is_empty() {
            break;
        }
        rows.extend(items);
        if rows.len() as i64 >= FINGERPRINT_SCAN_CAP {
            truncated = next.is_some();
            rows.truncate(FINGERPRINT_SCAN_CAP as usize);
            break;
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    let scanned = rows.len();
    let mut groups: HashMap<String, Vec<JsonValue>> = HashMap::new();
    let mut unassigned = 0usize;

    for mut item in rows {
        let Some(id) = item.get("id").and_then(JsonValue::as_i64) else {
            unassigned += 1;
            continue;
        };
        // 已有真实会话 id 的记录不参与指纹聚类：真实 id 永远优先，
        // 混进来只会让「同一会话」出现两种键。
        if item.get("sessionId").and_then(JsonValue::as_str).is_some() {
            continue;
        }
        let Some(payload) = load_payload_from(active, id).await else {
            unassigned += 1;
            continue;
        };
        let Ok(body) = serde_json::from_str::<JsonValue>(&payload.request) else {
            unassigned += 1;
            continue;
        };
        match fingerprint_cached(id, &body) {
            Some(fingerprint) => {
                item["fingerprint"] = json!(fingerprint);
                groups.entry(fingerprint).or_default().push(item);
            }
            None => unassigned += 1,
        }
    }

    for items in groups.values_mut() {
        sort_ascending(items);
    }

    Ok(FingerprintGroups {
        groups,
        unassigned,
        truncated,
        scanned,
    })
}

/// 按 (timeMs, id) 升序——时间线要按时间正序阅读。
fn sort_ascending(items: &mut [JsonValue]) {
    items.sort_by_key(|item| {
        (
            item.get("timeMs").and_then(JsonValue::as_i64).unwrap_or(0),
            item.get("id").and_then(JsonValue::as_i64).unwrap_or(0),
        )
    });
}

/// 归一化指纹参数：允许前端传 `prefix:xxxx` 或裸 `xxxx`。
fn normalize_fingerprint(raw: &str) -> String {
    raw.trim()
        .strip_prefix("prefix:")
        .unwrap_or_else(|| raw.trim())
        .to_string()
}

/// `GET /dashboard/api/session-timeline`
///
/// 两种模式（`session_id` / `fingerprint` 二选一）：
/// - 精确会话：返回该会话的记录，按时间升序；
/// - 指纹聚类：返回该指纹下的记录，并附带全部分组的概览（便于前端列出可选项）。
pub async fn session_timeline(
    State(app_state): State<Arc<AppState>>,
    Query(params): Query<TimelineParams>,
) -> Result<Json<JsonValue>, (StatusCode, String)> {
    let store = app_state.active_db().await;

    // 模式一：已知会话 id，精确查询。
    if let Some(session_id) = params
        .session_id
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        let list = ListParams {
            from: params.from,
            to: params.to,
            type_: params.type_.clone(),
            model: params.model.clone(),
            client: params.client.clone(),
            backend: params.backend.clone(),
            limit: Some(TIMELINE_MAX_ITEMS.min(500) as i64),
            ..Default::default()
        };
        let (items, _, total, total_exact) = query_records_active(&store, &list)
            .await
            .map_err(internal)?;

        // SessionId 在库里是 TEXT 精确列，这里再做一次全等过滤以防御调用方
        // 传入的前缀（历史面板用子串匹配，S1 会误命中 S10）。
        let mut exact: Vec<JsonValue> = items
            .into_iter()
            .filter(|item| item.get("sessionId").and_then(JsonValue::as_str) == Some(session_id))
            .collect();
        sort_ascending(&mut exact);

        return Ok(Json(json!({
            "mode": "session",
            "sessionId": session_id,
            "items": exact,
            "total": total,
            "totalExact": total_exact,
        })));
    }

    // 模式二：指纹聚类。没给指纹时返回全部分组概览，供前端选择。
    let list = ListParams {
        from: params.from,
        to: params.to,
        type_: params.type_.clone(),
        model: params.model.clone(),
        client: params.client.clone(),
        backend: params.backend.clone(),
        ..Default::default()
    };
    let grouped = group_by_fingerprint(&store, &list)
        .await
        .map_err(internal)?;

    let wanted = params
        .fingerprint
        .as_deref()
        .map(normalize_fingerprint)
        .filter(|s| !s.is_empty());

    let mut overview: Vec<JsonValue> = grouped
        .groups
        .iter()
        .map(|(fingerprint, items)| {
            let first = items.first();
            let last = items.last();
            json!({
                "fingerprint": fingerprint,
                "key": format!("prefix:{fingerprint}"),
                "count": items.len(),
                "firstTime": first.and_then(|i| i.get("time").cloned()),
                "lastTime": last.and_then(|i| i.get("time").cloned()),
                "firstTimeMs": first.and_then(|i| i.get("timeMs").cloned()),
                "lastTimeMs": last.and_then(|i| i.get("timeMs").cloned()),
                "model": first.and_then(|i| i.get("model").cloned()),
                "type": first.and_then(|i| i.get("type").cloned()),
                "clientName": first.and_then(|i| i.get("clientName").cloned()),
            })
        })
        .collect();
    overview.sort_by(|a, b| {
        b.get("count")
            .and_then(JsonValue::as_i64)
            .unwrap_or(0)
            .cmp(&a.get("count").and_then(JsonValue::as_i64).unwrap_or(0))
    });

    let items = wanted
        .as_deref()
        .and_then(|key| grouped.groups.get(key).cloned())
        .unwrap_or_default();

    Ok(Json(json!({
        "mode": "fingerprint",
        "fingerprint": wanted,
        "items": items,
        "sessions": overview,
        "scanned": grouped.scanned,
        "unassigned": grouped.unassigned,
        "truncated": grouped.truncated,
    })))
}

fn internal(error: DbError) -> (StatusCode, String) {
    tracing::error!("session timeline query failed: {error}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "query failed".to_string(),
    )
}

/// 供详情页单条记录算指纹（前端「这个会话还有哪些请求」入口用）。
pub async fn record_fingerprint(
    store: &AuditStore,
    record_id: i64,
) -> Result<Option<String>, DbError> {
    let Some(payload) = load_payload_from(store, record_id).await else {
        return Ok(None);
    };
    let Ok(body) = serde_json::from_str::<JsonValue>(&payload.request) else {
        return Ok(None);
    };
    Ok(fingerprint_cached(record_id, &body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_accepts_prefixed_and_bare() {
        assert_eq!(normalize_fingerprint("prefix:abc"), "abc");
        assert_eq!(normalize_fingerprint("abc"), "abc");
        assert_eq!(normalize_fingerprint("  prefix:abc  "), "abc");
    }

    #[test]
    fn sort_ascending_orders_by_time_then_id() {
        let mut items = vec![
            json!({"id": 3, "timeMs": 200}),
            json!({"id": 1, "timeMs": 100}),
            json!({"id": 2, "timeMs": 100}),
        ];
        sort_ascending(&mut items);
        let ids: Vec<i64> = items.iter().map(|i| i["id"].as_i64().unwrap()).collect();
        assert_eq!(ids, vec![1, 2, 3]);
    }
}
