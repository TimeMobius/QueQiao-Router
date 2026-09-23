//! 遗留归档（`user_version=0`、12 列明文 schema）的一次性就地迁移。
//!
//! 与 `records` 写入路径共用同一套提取管线（`extract_request` / `extract_response` /
//! `header_meta`）：按请求类型解析旧库正文，产出截断预览、统计字段与压缩 payloads，
//! 使迁移后的归档与 V2 原生写入的库在结构、检索与回放能力上完全一致。

use axum::http::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;
use tracing::{info, warn};

use crate::db::extract::{extract_request, extract_response, header_meta, HeaderMeta};
use crate::db::payload;
use crate::db::rotation;
use crate::models::requests::{MessageContent, RequestPayload};

/// 逐行处理批大小：SELECT 会带出整行请求/响应正文，
/// 取较小值限制单批驻留内存（500 行 × 数十 KB 量级）。
const MIGRATION_BATCH: i64 = 500;

/// 遗留库的明文正文列。正文写入 payloads、预览写入 Prompt/RequestTail/Answer 后
/// 这三列即成为死列（读路径不再引用），删除并 `VACUUM` 可回收空间。
const PLAINTEXT_COLUMNS: &[&str] = &["Request", "Response", "Headers"];

/// 就地迁移一份遗留归档。
///
/// 整个迁移在单个事务内完成：任何一步失败都会完整回滚，下次扫描可安全重试。
/// `TimeMs` 由本地墙钟文本 `Time` 结合当时的历史 UTC 偏移（含夏令时）回填，
/// 无法解析的行保持 NULL 隔离（不参与时间范围检索）。
pub(crate) async fn migrate_archive(pool: &SqlitePool) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;

    let existing: Vec<String> = sqlx::query("PRAGMA table_info(records)")
        .fetch_all(&mut *tx)
        .await?
        .iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();

    for (name, decl) in crate::db::schema::NEW_COLUMNS {
        if !existing.iter().any(|c| c == name) {
            sqlx::query(&format!("ALTER TABLE records ADD COLUMN {} {}", name, decl))
                .execute(&mut *tx)
                .await?;
        }
    }

    // 逐行处理需要写 payloads，其表必须先行存在。
    sqlx::query(crate::db::schema::PAYLOADS_DDL)
        .execute(&mut *tx)
        .await?;

    // 列存在性守卫：重复迁移（或已转换库）没有 Request 列时整体跳过逐行管线。
    let has_legacy_body = existing.iter().any(|c| c == "Request");
    let mut last_id: i64 = 0;
    let mut unparseable_time: u64 = 0;
    let mut unparseable_body: u64 = 0;
    if has_legacy_body {
        loop {
            let rows = sqlx::query(
                "SELECT id, Time, Type, Headers, Request, Response, Tool, Multimodal \
                 FROM records WHERE id > ? ORDER BY id LIMIT ?",
            )
            .bind(last_id)
            .bind(MIGRATION_BATCH)
            .fetch_all(&mut *tx)
            .await?;
            if rows.is_empty() {
                break;
            }
            for row in &rows {
                let id: i64 = row.get("id");
                let time: Option<String> = row.try_get("Time").ok().flatten();
                let type_label: Option<String> = row.try_get("Type").ok().flatten();
                let headers: Option<String> = row.try_get("Headers").ok().flatten();
                let request: String = row.try_get("Request").ok().flatten().unwrap_or_default();
                let response: String = row.try_get("Response").ok().flatten().unwrap_or_default();
                let legacy_tool: bool = row
                    .try_get::<Option<i64>, _>("Tool")
                    .ok()
                    .flatten()
                    .is_some_and(|v| v != 0);
                let legacy_multimodal: bool = row
                    .try_get::<Option<i64>, _>("Multimodal")
                    .ok()
                    .flatten()
                    .is_some_and(|v| v != 0);

                // TimeMs 回填：无法解析的行保持 NULL 隔离。
                let time_ms = match time.as_deref().and_then(rotation::parse_local_time_ms) {
                    Some(ms) => Some(ms),
                    None => {
                        unparseable_time += 1;
                        None
                    }
                };

                // 与 V2 写入路径一致：按类型解析请求 → extract_request 提取预览/统计，
                // 响应 JSON → extract_response 提取回答/工具名/finish_reason。
                let req_value: Option<Value> = if request.is_empty() {
                    None
                } else {
                    serde_json::from_str(&request).ok()
                };
                let parsed = req_value
                    .as_ref()
                    .and_then(|_| parse_legacy_request(type_label.as_deref(), &request));
                if !request.is_empty() && parsed.is_none() {
                    unparseable_body += 1;
                }
                let req_ext = parsed.as_ref().map(extract_request).unwrap_or_default();
                let resp_value: Option<Value> = if response.is_empty() {
                    None
                } else {
                    serde_json::from_str(&response).ok()
                };
                let resp_ext = resp_value
                    .as_ref()
                    .map(extract_response)
                    .unwrap_or_default();

                // 旧库 Headers 列是 JSON 对象，转成 HeaderMap 后复用 V2 的 header_meta。
                let header: HeaderMeta = headers
                    .as_deref()
                    .map(legacy_header_meta)
                    .unwrap_or_default();

                // Tool/Multimodal 与 V2 相同口径重算；解析失败时保留旧值。
                let tool = req_value
                    .as_ref()
                    .map(|v| v.get("tools").is_some())
                    .unwrap_or(legacy_tool);
                let multimodal = match parsed.as_ref() {
                    Some(RequestPayload::Chat(p)) => p.messages.iter().any(|m| {
                        matches!(
                            &m.content,
                            Some(MessageContent::Array(parts))
                                if parts.iter().any(|it| it.r#type == "image_url")
                        )
                    }),
                    Some(_) => false,
                    None => legacy_multimodal,
                };

                // 完整请求/响应/表头压缩进 payloads，供详情回放。
                let req_c = payload::compress(request.as_bytes());
                let resp_c = payload::compress(response.as_bytes());
                let hdr_c = payload::compress(headers.as_deref().unwrap_or("").as_bytes());
                let payload_id: i64 = sqlx::query(
                    "INSERT INTO payloads (record_id, codec, dict_id, request, response, headers, \
                     request_raw_len, response_raw_len, headers_raw_len) VALUES (?,?,?,?,?,?,?,?,?)",
                )
                .bind(id)
                .bind(&req_c.codec)
                .bind(req_c.dict_id.as_deref())
                .bind(&req_c.bytes)
                .bind(&resp_c.bytes)
                .bind(&hdr_c.bytes)
                .bind(req_c.raw_len)
                .bind(resp_c.raw_len)
                .bind(hdr_c.raw_len)
                .execute(&mut *tx)
                .await?
                .last_insert_rowid();

                sqlx::query(
                    "UPDATE records SET TimeMs = ?, Prompt = ?, RequestTail = ?, Answer = ?, \
                     ToolNames = ?, FinishReason = ?, Endpoint = ?, \
                     UserAgent = ?, ClientName = ?, ClientVersion = ?, ApiKey = ?, \
                     SessionId = ?, ParentSessionId = ?, RequestId = ?, SessionAffinity = ?, \
                     Tool = ?, Multimodal = ?, \
                     RequestBytes = ?, ResponseBytes = ?, PromptBytes = ?, RequestTailBytes = ?, \
                     AnswerBytes = ?, MessageCount = ?, SystemCount = ?, ToolCount = ?, \
                     AssistantCount = ?, ToolResultCount = ?, ImageCount = ?, payload_id = ? \
                     WHERE id = ?",
                )
                .bind(time_ms)
                .bind(&req_ext.prompt)
                .bind(&req_ext.request_tail)
                .bind(&resp_ext.answer)
                .bind(&resp_ext.tool_names)
                .bind(&resp_ext.finish_reason)
                .bind(
                    parsed
                        .as_ref()
                        .and_then(|p| p.get_endpoint().map(str::to_string)),
                )
                .bind(&header.user_agent)
                .bind(&header.client_name)
                .bind(&header.client_version)
                .bind(&header.api_key)
                .bind(&header.session_id)
                .bind(&header.parent_session_id)
                .bind(&header.request_id)
                .bind(&header.session_affinity)
                .bind(tool)
                .bind(multimodal)
                .bind(request.len() as i64)
                .bind(response.len() as i64)
                .bind(req_ext.prompt_bytes)
                .bind(req_ext.request_tail_bytes)
                .bind(resp_ext.answer_bytes)
                .bind(req_ext.message_count)
                .bind(req_ext.system_count)
                .bind(req_ext.tool_count)
                .bind(req_ext.assistant_count)
                .bind(req_ext.tool_result_count)
                .bind(req_ext.image_count)
                .bind(payload_id)
                .bind(id)
                .execute(&mut *tx)
                .await?;
                last_id = id;
            }
        }
    }

    let has_col = |c: &str| {
        existing.iter().any(|e| e == c)
            || crate::db::schema::NEW_COLUMNS.iter().any(|(n, _)| *n == c)
    };

    for (sql, required) in crate::db::schema::NEW_INDEXES {
        if required.iter().all(|c| has_col(c)) {
            sqlx::query(sql).execute(&mut *tx).await?;
        }
    }
    for sql in crate::db::schema::FTS_DDL {
        sqlx::query(sql).execute(&mut *tx).await?;
    }
    // 预览列在逐行管线中已写入，重建外部内容索引使 FTS 与之一致。
    sqlx::query("INSERT INTO records_fts(records_fts) VALUES('rebuild')")
        .execute(&mut *tx)
        .await?;

    // 遗留 schema 的索引集合未知（如 `idx_records_headers_app` 是基于 `headers` 的
    // 表达式索引），`DROP COLUMN` 会因索引仍引用被删列而失败。先按标识符（大小写
    // 不敏感）从索引 DDL 中找出引用明文列的索引并删除。
    for name in plaintext_indexes(&mut tx).await? {
        sqlx::query(&format!("DROP INDEX \"{}\"", name))
            .execute(&mut *tx)
            .await?;
    }

    // 正文已写入 payloads、预览已写入 Prompt/RequestTail/Answer，遗留明文列不再被任何
    // 查询引用，删除它们再由事务提交后的 VACUUM 回收空间。DDL 无法绑定标识符，列名为编译期常量。
    let mut slimmed = 0usize;
    for col in PLAINTEXT_COLUMNS {
        if existing.iter().any(|c| c == col) {
            sqlx::query(&format!("ALTER TABLE records DROP COLUMN {}", col))
                .execute(&mut *tx)
                .await?;
            slimmed += 1;
        }
    }

    let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM records")
        .fetch_one(&mut *tx)
        .await?;

    // PRAGMA 不支持绑定参数；此处为编译期常量，无注入风险。
    sqlx::query(&format!(
        "PRAGMA user_version = {}",
        crate::db::schema::SCHEMA_VERSION
    ))
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    // VACUUM 不能在事务内执行，且失败不应影响已完成并提交的迁移结果。
    if slimmed > 0 {
        if let Err(e) = sqlx::query("VACUUM").execute(pool).await {
            warn!(
                "Legacy archive VACUUM failed (data already migrated): {}",
                e
            );
        }
    }

    info!(
        "Legacy archive migrated to schema {}: {} rows, {} unparseable Time, {} unparseable request bodies, {} legacy plaintext columns dropped",
        crate::db::schema::SCHEMA_VERSION,
        total,
        unparseable_time,
        unparseable_body,
        slimmed
    );
    Ok(())
}

/// 找出 `records` 表上引用明文正文列（即将被删除）的索引名。
///
/// `PRAGMA index_info` 对表达式索引返回 NULL 列名，因此改读 `sqlite_master` 中的
/// 索引 DDL，按标识符（大小写不敏感）匹配被删列名。`sqlite_autoindex_*` 为约束
/// 自动索引（DDL 为 NULL），不含被删列，天然被排除。
async fn plaintext_indexes(
    tx: &mut sqlx::Transaction<'_, sqlx::sqlite::Sqlite>,
) -> Result<Vec<String>, sqlx::Error> {
    let rows = sqlx::query(
        "SELECT name, sql FROM sqlite_master \
         WHERE type = 'index' AND tbl_name = 'records' AND sql IS NOT NULL",
    )
    .fetch_all(&mut **tx)
    .await?;
    let mut names = Vec::new();
    for row in rows {
        let name: String = row.get("name");
        let sql: String = row.get("sql");
        let references = PLAINTEXT_COLUMNS.iter().any(|col| {
            sql.split(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
                .any(|tok| tok.eq_ignore_ascii_case(col))
        });
        if references {
            names.push(name);
        }
    }
    Ok(names)
}

/// 旧库 `Type` 标签到 V2 请求变体的映射：按标签选择对应结构体解析，失败返回 None。
/// 标签与 `records::request_type_label` 的取值一一对应，另含少量宽松别名。
fn parse_legacy_request(type_label: Option<&str>, body: &str) -> Option<RequestPayload> {
    use crate::models::requests as m;
    Some(match type_label {
        Some("chat.completions") => m::RequestPayload::Chat(serde_json::from_str(body).ok()?),
        Some("text_completion") | Some("completions") => {
            m::RequestPayload::Completion(serde_json::from_str(body).ok()?)
        }
        Some("embeddings") | Some("embedding") => {
            m::RequestPayload::Embedding(serde_json::from_str(body).ok()?)
        }
        Some("rerank") => m::RequestPayload::Rerank(serde_json::from_str(body).ok()?),
        Some("score") => m::RequestPayload::Score(serde_json::from_str(body).ok()?),
        Some("classify") => m::RequestPayload::Classify(serde_json::from_str(body).ok()?),
        Some("responses") => m::RequestPayload::Responses(serde_json::from_str(body).ok()?),
        Some("anthropic.messages") | Some("messages") => {
            m::RequestPayload::AnthropicMessages(serde_json::from_str(body).ok()?)
        }
        _ => return None,
    })
}

/// 旧库 `Headers` 列（JSON 对象字符串）转成 `HeaderMap`，使 `header_meta` 能复用
/// V2 的会话/客户端/凭据提取逻辑。
fn legacy_header_meta(headers_json: &str) -> HeaderMeta {
    let mut map = HeaderMap::new();
    if let Ok(obj) = serde_json::from_str::<serde_json::Map<String, Value>>(headers_json) {
        for (k, v) in obj {
            if let (Ok(name), Some(text)) = (k.parse::<HeaderName>(), v.as_str()) {
                if let Ok(value) = text.parse::<HeaderValue>() {
                    map.append(name, value);
                }
            }
        }
    }
    header_meta(&map)
}

#[cfg(test)]
mod tests {
    use super::migrate_archive;
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
    use sqlx::Row;
    use std::str::FromStr;

    #[tokio::test]
    async fn migrates_v0_db_with_expression_index_on_plaintext_column() {
        let dir = std::env::temp_dir().join(format!("qrouter_legacy_test_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("record.db");
        let _ = std::fs::remove_file(&path);
        let uri = format!("sqlite://{}", path.display());

        let setup = SqliteConnectOptions::from_str(&uri)
            .unwrap()
            .create_if_missing(true);
        let setup_pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(setup)
            .await
            .unwrap();
        sqlx::query(
            "CREATE TABLE records (
                id INTEGER PRIMARY KEY, Time TEXT, IP TEXT, Model TEXT, Type TEXT,
                CompletionTokens INTEGER, PromptTokens INTEGER, TotalTokens INTEGER,
                Tool BOOLEAN, Multimodal BOOLEAN, Headers TEXT, Request TEXT, Response TEXT
            )",
        )
        .execute(&setup_pool)
        .await
        .unwrap();
        sqlx::query("CREATE INDEX idx_records_headers_app ON records(lower(Headers))")
            .execute(&setup_pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO records (Time, IP, Model, Type, Headers, Request, Response) \
             VALUES ('2026-09-01 10:00:00', '1.2.3.4', 'gpt-4', 'chat.completions', \
                     '{\"User-Agent\":\"ua\"}', \
                     '{\"model\":\"gpt-4\",\"messages\":[{\"role\":\"user\",\"content\":\"hi\"}]}', \
                     '{\"choices\":[{\"message\":{\"content\":\"hello\"}}]}')",
        )
        .execute(&setup_pool)
        .await
        .unwrap();
        setup_pool.close().await;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(SqliteConnectOptions::from_str(&uri).unwrap())
            .await
            .unwrap();
        migrate_archive(&pool).await.unwrap();

        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(version, crate::db::schema::SCHEMA_VERSION);
        let cols: Vec<String> = sqlx::query("PRAGMA table_info(records)")
            .fetch_all(&pool)
            .await
            .unwrap()
            .iter()
            .map(|r| r.get::<String, _>("name"))
            .collect();
        assert!(!cols
            .iter()
            .any(|c| c == "Headers" || c == "Request" || c == "Response"));
        assert!(cols.iter().any(|c| c == "TimeMs"));
        let idx_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_records_headers_app'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(idx_count, 0);
        let (time_ms, prompt): (Option<i64>, Option<String>) =
            sqlx::query_as("SELECT TimeMs, Prompt FROM records WHERE id = 1")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(time_ms.is_some());
        assert_eq!(prompt.as_deref(), Some("hi"));

        pool.close().await;
        let _ = std::fs::remove_dir_all(&dir);
    }
}
