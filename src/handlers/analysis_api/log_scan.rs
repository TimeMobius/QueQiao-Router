//! 错误日志文件扫描（blocking 任务内执行）。

use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::Path;

use crate::handlers::{error_log_api, records_api};

use super::aggregation::{normalize_opt, LogErrorKey};

const MAX_LOG_LINES: i64 = 200_000;
const MAX_LOG_FILE_BYTES: u64 = 32 * 1024 * 1024;

pub(super) struct LogGroup {
    pub(super) kind: String,
    pub(super) status: Option<i64>,
    pub(super) model: Option<String>,
    pub(super) backend: Option<String>,
    pub(super) error: Option<String>,
    pub(super) count: i64,
}

pub(super) struct LogScan {
    pub(super) groups: Vec<LogGroup>,
    pub(super) unparsed: i64,
    /// 区间内解析出的全部可计入错误条目数（http_error + stream_interrupted），
    /// 即错误日志中实际的错误请求总量，用于并入汇总与趋势。
    pub(super) total_error_entries: i64,
    /// 按趋势粒度聚合的错误条目计数（桶标签 → 次数），与 SQL `bucket_expr` 对齐。
    pub(super) per_bucket: Vec<(String, i64)>,
    pub(super) files: Vec<String>,
    pub(super) warnings: Vec<String>,
    pub(super) total: i64,
}

/// 解析 `17/Sep/2026:07:28:00 +0000` 为 epoch 毫秒；失败返回 None。
fn parse_log_time(s: &str) -> Option<i64> {
    chrono::DateTime::parse_from_str(s, "%d/%b/%Y:%H:%M:%S %z")
        .ok()
        .map(|dt| dt.timestamp_millis())
}

/// 将 epoch 毫秒映射为与 SQL `bucket_expr` 完全一致的桶标签（进程本地时区），
/// 保证日志错误能并入同一条趋势桶。
fn bucket_label(ms: i64, interval: &str) -> Option<String> {
    use chrono::TimeZone;
    let dt = chrono::Local.timestamp_millis_opt(ms).single()?;
    Some(match interval {
        "hour" => dt.format("%Y-%m-%d %H").to_string(),
        "month" => dt.format("%Y-%m").to_string(),
        _ => dt.format("%Y-%m-%d").to_string(),
    })
}

/// 日志错误是否计入统计/错误率/趋势：仅计入带错误状态码（4xx/5xx）的条目。
/// 流中断（stream_interrupted，响应已提交 200，无状态码）不计入统计，但仍列于错误排行。
fn is_countable_log_error(status: Option<i64>) -> bool {
    status.is_some()
}

/// 日志请求路径 → 审计库 `Type` 列取值，必须与 `db::records::request_type_label` 保持一致。
fn log_type_label(path: &str) -> Option<&'static str> {
    match path {
        "/v1/chat/completions" => Some("chat.completions"),
        "/v1/completions" => Some("text_completion"),
        "/v1/embeddings" => Some("embeddings"),
        "/v1/rerank" | "/rerank" => Some("rerank"),
        "/score" => Some("score"),
        "/classify" => Some("classify"),
        "/v1/responses" => Some("responses"),
        "/v1/messages" => Some("anthropic.messages"),
        _ => None,
    }
}

/// 日志条目版通用筛选，语义与 `records_api::build_filters` 的 DB 侧保持一致：
/// model/ip 大小写不敏感前缀、apikey/status/backend 精确、client 对 UA 大小写不敏感子串
/// （ClientName 是 UA 解析片段，包含于 UA）、type 经路径映射后精确、errors=1 要求 Status>=400。
fn log_entry_matches(e: &error_log_api::LogEntry, p: &records_api::ListParams) -> bool {
    let ci_prefix = |field: &Option<String>, v: &str| match field {
        Some(s) => s.to_lowercase().starts_with(&v.to_lowercase()),
        None => false,
    };
    if let Some(v) = p.model.as_deref().filter(|s| !s.is_empty()) {
        if !ci_prefix(&e.model, v) {
            return false;
        }
    }
    if let Some(v) = p.ip.as_deref().filter(|s| !s.is_empty()) {
        if !ci_prefix(&e.ip, v) {
            return false;
        }
    }
    if let Some(v) = p.apikey.as_deref().filter(|s| !s.is_empty()) {
        if e.api_key.as_deref() != Some(v) {
            return false;
        }
    }
    if let Some(v) = p.status {
        if e.status != Some(v) {
            return false;
        }
    }
    if let Some(v) = p.backend.as_deref().filter(|s| !s.is_empty()) {
        if e.backend.as_deref() != Some(v) {
            return false;
        }
    }
    if let Some(v) = p.client.as_deref().filter(|s| !s.is_empty()) {
        let hay = e.user_agent.as_deref().unwrap_or("");
        if !hay.to_lowercase().contains(&v.to_lowercase()) {
            return false;
        }
    }
    if let Some(v) = p.type_.as_deref().filter(|s| !s.is_empty()) {
        if !matches!(e.path.as_deref(), Some(path) if log_type_label(path) == Some(v)) {
            return false;
        }
    }
    if p.errors
        .as_deref()
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
    {
        if !matches!(e.status, Some(s) if s >= 400) {
            return false;
        }
    }
    true
}

pub(super) fn scan_error_logs(
    dir: &Path,
    from: Option<i64>,
    to: Option<i64>,
    limit: usize,
    interval: &str,
    filters: &records_api::ListParams,
) -> LogScan {
    let mut warnings: Vec<String> = Vec::new();
    let mut files: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with("error.") && name.ends_with(".log") && entry.path().is_file() {
                files.push(entry.path());
            }
        }
    }
    files.sort();

    let mut counts: HashMap<LogErrorKey, i64> = HashMap::new();
    let mut bucket_counts: HashMap<String, i64> = HashMap::new();
    let mut unparsed: i64 = 0;
    let mut total_error_entries: i64 = 0;
    let mut total_lines: i64 = 0;
    let mut read_files: Vec<String> = Vec::new();
    let range_active = from.is_some() || to.is_some();
    let lo = from.unwrap_or(i64::MIN);
    let hi = to.unwrap_or(i64::MAX);

    'outer: for path in &files {
        let Ok(file) = std::fs::File::open(path) else {
            continue;
        };
        read_files.push(
            path.file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default(),
        );
        let mut file_bytes: u64 = 0;
        for line in BufReader::new(file).lines() {
            let Ok(line) = line else {
                break;
            };
            file_bytes += line.len() as u64 + 1;
            total_lines += 1;
            if total_lines > MAX_LOG_LINES {
                warnings.push(format!("log scan truncated at {MAX_LOG_LINES} lines"));
                break 'outer;
            }
            if file_bytes > MAX_LOG_FILE_BYTES {
                warnings.push("log scan truncated by per-file byte limit".to_string());
                break;
            }
            let entry = error_log_api::parse_line(&line);
            if entry.kind == "unparsed" {
                unparsed += 1;
            }
            let ts = entry.time.as_deref().and_then(parse_log_time);
            if range_active {
                match ts {
                    Some(t) if t < lo || t > hi => continue,
                    // 时间无法解析的条目被保留并计数。
                    None => unparsed += 1,
                    _ => {}
                }
            }
            let is_error = entry.kind == "http_error" || entry.kind == "stream_interrupted";
            if !is_error {
                continue;
            }
            if !log_entry_matches(&entry, filters) {
                continue;
            }
            // 错误排行：列出全部错误（含无状态码的流中断）。
            let countable = is_countable_log_error(entry.status);
            let key: LogErrorKey = (
                entry.kind,
                entry.status,
                normalize_opt(entry.model),
                normalize_opt(entry.backend),
                normalize_opt(entry.error),
            );
            *counts.entry(key).or_insert(0) += 1;
            // 统计/错误率/趋势：仅计入带状态码的错误，与汇总同口径。
            if countable {
                total_error_entries += 1;
                if let Some(t) = ts {
                    if let Some(label) = bucket_label(t, interval) {
                        *bucket_counts.entry(label).or_insert(0) += 1;
                    }
                }
            }
        }
    }

    let mut groups: Vec<LogGroup> = counts
        .into_iter()
        .map(|((kind, status, model, backend, error), count)| LogGroup {
            kind,
            status,
            model,
            backend,
            error,
            count,
        })
        .collect();
    groups.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.kind.cmp(&b.kind)));
    let total = groups.len() as i64;
    groups.truncate(limit);
    let per_bucket: Vec<(String, i64)> = bucket_counts.into_iter().collect();
    LogScan {
        groups,
        unparsed,
        total_error_entries,
        per_bucket,
        files: read_files,
        warnings,
        total,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_nginx_error_log_timestamp() {
        assert_eq!(
            parse_log_time("17/Sep/2026:07:28:00 +0000"),
            Some(1_789_630_080_000)
        );
        // 带偏移的输入应按偏移换算到 UTC。
        assert_eq!(
            parse_log_time("17/Sep/2026:15:28:00 +0800"),
            Some(1_789_630_080_000)
        );
        assert!(parse_log_time("not a time").is_none());
        assert!(parse_log_time("").is_none());
    }

    #[test]
    fn countable_log_error_requires_status_code() {
        assert!(is_countable_log_error(Some(500)));
        assert!(is_countable_log_error(Some(503)));
        assert!(is_countable_log_error(Some(422)));
        assert!(is_countable_log_error(Some(499)));
        assert!(is_countable_log_error(Some(400)));
        assert!(!is_countable_log_error(None));
    }

    #[test]
    fn scan_error_logs_lists_all_counts_status_only() {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let dir = std::env::temp_dir().join(format!("qq_logscan_{}_{}", pid, nanos));
        std::fs::create_dir_all(&dir).unwrap();
        let body = [
            "1.1.1.1 - - [20/Sep/2026:10:00:00 +0000] \"POST /v1/chat/completions HTTP/1.1\" 500 50 \"-\" \"ua\" 1.000s \"gpt-5.6\" \"-\" \"all failed\" \"-\"",
            "2.2.2.2 - - [20/Sep/2026:10:05:00 +0000] \"POST /v1/chat/completions HTTP/1.1\" 422 50 \"-\" \"ua\" 1.000s \"gpt-5.6\" \"-\" \"bad\" \"-\"",
            "3.3.3.3 - - [20/Sep/2026:10:06:00 +0000] \"POST /v1/chat/completions HTTP/1.1\" 404 50 \"-\" \"ua\" 1.000s \"\" \"-\" \"none\" \"-\"",
            "5.5.5.5 - - [20/Sep/2026:10:16:00 +0000] \"POST /v1/chat/completions HTTP/1.1\" 499 50 \"-\" \"ua\" 1.000s \"gpt-5.6\" \"-\" \"user cancel\" \"-\"",
            "[20/Sep/2026:10:20:00 +0000] STREAM_INTERRUPTED client=6.6.6.6 endpoint=/v1/chat/completions model=gpt-5.6 backend=b1 error=\"stream cut\"",
        ]
        .join("\n");
        std::fs::write(dir.join("error.2026-09-20.log"), body).unwrap();
        let scan = scan_error_logs(
            &dir,
            None,
            None,
            100,
            "day",
            &records_api::ListParams::default(),
        );
        // 错误排行列出全部 5 条（含无状态码的流中断）
        assert_eq!(scan.total, 5);
        // 统计/趋势只计入 4 条带状态码错误
        assert_eq!(scan.total_error_entries, 4);
        let bucket_sum: i64 = scan.per_bucket.iter().map(|(_, c)| c).sum();
        assert_eq!(bucket_sum, 4);
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn test_http_entry() -> error_log_api::LogEntry {
        error_log_api::LogEntry {
            kind: "http_error".to_string(),
            ip: Some("192.168.10.32".to_string()),
            path: Some("/v1/chat/completions".to_string()),
            status: Some(500),
            user_agent: Some("python-httpx/0.27.0".to_string()),
            model: Some("GPT-5.6-sol".to_string()),
            api_key: Some("sk-abc".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn log_entry_matches_mirrors_db_filter_semantics() {
        let e = test_http_entry();
        let lp = |kv: Vec<(&str, String)>| {
            let mut p = records_api::ListParams::default();
            for (k, v) in kv {
                match k {
                    "model" => p.model = Some(v),
                    "ip" => p.ip = Some(v),
                    "apikey" => p.apikey = Some(v),
                    "status" => p.status = v.parse().ok(),
                    "backend" => p.backend = Some(v),
                    "client" => p.client = Some(v),
                    "type" => p.type_ = Some(v),
                    "errors" => p.errors = Some(v),
                    _ => unreachable!(),
                }
            }
            p
        };

        assert!(log_entry_matches(&e, &records_api::ListParams::default()));

        // model：大小写不敏感前缀
        assert!(log_entry_matches(&e, &lp(vec![("model", "gpt".into())])));
        assert!(log_entry_matches(&e, &lp(vec![("model", "GPT-5".into())])));
        assert!(!log_entry_matches(
            &e,
            &lp(vec![("model", "deepseek".into())])
        ));
        // 条目缺 model 时任何 model 筛选都不命中
        let no_model = error_log_api::LogEntry {
            kind: "http_error".to_string(),
            ..Default::default()
        };
        assert!(!log_entry_matches(
            &no_model,
            &lp(vec![("model", "gpt".into())])
        ));

        // ip：大小写不敏感前缀
        assert!(log_entry_matches(&e, &lp(vec![("ip", "192.168".into())])));
        assert!(!log_entry_matches(&e, &lp(vec![("ip", "10.0.0".into())])));

        // apikey：精确
        assert!(log_entry_matches(
            &e,
            &lp(vec![("apikey", "sk-abc".into())])
        ));
        assert!(!log_entry_matches(&e, &lp(vec![("apikey", "sk-a".into())])));

        // status：精确
        assert!(log_entry_matches(&e, &lp(vec![("status", "500".into())])));
        assert!(!log_entry_matches(&e, &lp(vec![("status", "404".into())])));

        // client：对 UA 大小写不敏感子串
        assert!(log_entry_matches(
            &e,
            &lp(vec![("client", "PYTHON-HTTPX".into())])
        ));
        assert!(!log_entry_matches(&e, &lp(vec![("client", "curl".into())])));

        // type：经路径映射后精确
        assert!(log_entry_matches(
            &e,
            &lp(vec![("type", "chat.completions".into())])
        ));
        assert!(!log_entry_matches(
            &e,
            &lp(vec![("type", "responses".into())])
        ));

        // backend：http_error 无 backend，任何 backend 筛选都不命中
        assert!(!log_entry_matches(
            &e,
            &lp(vec![("backend", "alpha".into())])
        ));
        let stream = error_log_api::LogEntry {
            kind: "stream_interrupted".to_string(),
            backend: Some("alpha".to_string()),
            ..Default::default()
        };
        assert!(log_entry_matches(
            &stream,
            &lp(vec![("backend", "alpha".into())])
        ));

        // errors=1：要求 Status>=400，无状态码的流中断被排除
        assert!(log_entry_matches(&e, &lp(vec![("errors", "1".into())])));
        assert!(!log_entry_matches(
            &stream,
            &lp(vec![("errors", "1".into())])
        ));
    }

    #[test]
    fn scan_error_logs_applies_common_filters() {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let dir = std::env::temp_dir().join(format!("qq_logfilter_{}_{}", pid, nanos));
        std::fs::create_dir_all(&dir).unwrap();
        let body = [
            "1.1.1.1 - - [20/Sep/2026:10:00:00 +0000] \"POST /v1/chat/completions HTTP/1.1\" 500 50 \"-\" \"ua\" 1.000s \"gpt-5.6\" \"-\" \"all failed\" \"-\"",
            "2.2.2.2 - - [20/Sep/2026:10:05:00 +0000] \"POST /v1/responses HTTP/1.1\" 503 50 \"-\" \"ua\" 1.000s \"DeepSeek-V4\" \"-\" \"cooling\" \"-\"",
            "[20/Sep/2026:10:20:00 +0000] STREAM_INTERRUPTED client=6.6.6.6 endpoint=/v1/chat/completions model=gpt-5.6 backend=b1 error=\"stream cut\"",
        ]
        .join("\n");
        std::fs::write(dir.join("error.2026-09-20.log"), body).unwrap();

        // 无筛选：全部计入
        let scan = scan_error_logs(
            &dir,
            None,
            None,
            100,
            "day",
            &records_api::ListParams::default(),
        );
        assert_eq!(scan.total_error_entries, 2);

        // model 前缀筛选：只命中 gpt
        let p = records_api::ListParams {
            model: Some("gpt".to_string()),
            ..Default::default()
        };
        let scan = scan_error_logs(&dir, None, None, 100, "day", &p);
        assert_eq!(
            scan.total, 2,
            "gpt-5.6 的 http_error 与 stream_interrupted 都应命中"
        );
        assert_eq!(scan.total_error_entries, 1, "流中断不计入统计");
        assert_eq!(scan.groups.iter().map(|g| g.count).sum::<i64>(), 2);

        // ip 前缀筛选：全部不命中
        let p = records_api::ListParams {
            ip: Some("10.0.0".to_string()),
            ..Default::default()
        };
        let scan = scan_error_logs(&dir, None, None, 100, "day", &p);
        assert_eq!(scan.total, 0);
        assert_eq!(scan.total_error_entries, 0);

        // status 精确筛选
        let p = records_api::ListParams {
            status: Some(503),
            ..Default::default()
        };
        let scan = scan_error_logs(&dir, None, None, 100, "day", &p);
        assert_eq!(scan.total, 1);
        assert_eq!(scan.total_error_entries, 1);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
