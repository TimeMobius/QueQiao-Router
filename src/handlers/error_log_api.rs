use axum::{extract::Query, http::StatusCode, Json};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 500;
/// 单条错误信息的上限（字节，按字符边界安全截断）。
/// 上游（尤其是 pydantic 校验错误）可能把整个请求体重复塞进错误消息，实测一条
/// 400 的错误字段达 592 KB，不设上限时单页响应会被一条记录撑到数百 KB。
/// `raw` 与 `request_body` 不设上限，完整原文仍可从它们获取。
const MAX_ERROR_BYTES: usize = 16 * 1024;

#[derive(Debug, Default, Deserialize)]
pub struct ErrorLogParams {
    pub before: Option<u64>,
    pub limit: Option<i64>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct LogEntry {
    pub kind: String,
    pub raw: String,
    pub time: Option<String>,
    pub ip: Option<String>,
    pub method: Option<String>,
    pub path: Option<String>,
    pub status: Option<i64>,
    pub user_agent: Option<String>,
    pub latency: Option<String>,
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub backend: Option<String>,
    pub error: Option<String>,
    pub error_truncated: bool,
    pub error_bytes: Option<usize>,
    pub request_body: Option<String>,
}

impl LogEntry {
    fn unparsed(raw: &str) -> Self {
        LogEntry {
            kind: "unparsed".to_string(),
            raw: raw.to_string(),
            ..Default::default()
        }
    }

    fn cap_error(&mut self) {
        if let Some(msg) = self.error.take() {
            let (capped, truncated, total) = truncate_error(msg);
            self.error = Some(capped);
            self.error_truncated = truncated;
            self.error_bytes = truncated.then_some(total);
        }
    }
}

/// 按字符边界安全地把错误信息截断到 `MAX_ERROR_BYTES`。
/// 返回（截断后的文本, 是否发生截断, 原始字节数）。
fn truncate_error(msg: String) -> (String, bool, usize) {
    let total = msg.len();
    if total <= MAX_ERROR_BYTES {
        return (msg, false, total);
    }
    let mut end = MAX_ERROR_BYTES;
    while !msg.is_char_boundary(end) {
        end -= 1;
    }
    let mut capped = msg[..end].to_string();
    capped.push_str(&format!(
        "\n…[已截断：原始 {} 字节，完整内容见「原始行」]",
        total
    ));
    (capped, true, total)
}

/// Parses a Rust `{:?}`-style quoted string, returning the unescaped value and the
/// number of bytes consumed so the caller can resume scanning after it.
fn parse_debug_string(input: &str) -> Option<(String, usize)> {
    let s = input.trim_start();
    let bytes = s.as_bytes();
    if bytes.first() != Some(&b'"') {
        return None;
    }
    let mut out = String::new();
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => return Some((out, i + 1)),
            b'\\' => {
                i += 1;
                if i >= bytes.len() {
                    break;
                }
                match bytes[i] {
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'0' => out.push('\0'),
                    b'\\' => out.push('\\'),
                    b'"' => out.push('"'),
                    b'\'' => out.push('\''),
                    b'u' => {
                        if bytes.get(i + 1) == Some(&b'{') {
                            if let Some(close) = s[i + 2..].find('}') {
                                let hex = &s[i + 2..i + 2 + close];
                                if let Ok(cp) = u32::from_str_radix(hex, 16) {
                                    if let Some(ch) = char::from_u32(cp) {
                                        out.push(ch);
                                    }
                                }
                                i = i + 2 + close;
                            }
                        }
                    }
                    other => out.push(other as char),
                }
                i += 1;
            }
            _ => {
                let ch = s[i..].chars().next()?;
                out.push(ch);
                i += ch.len_utf8();
            }
        }
    }
    None
}

/// Consumes one quoted field and returns its value plus the remaining input.
fn take_quoted(input: &str) -> Option<(String, &str)> {
    let s = input.trim_start();
    let (value, consumed) = parse_debug_string(s)?;
    Some((value, &s[consumed..]))
}

fn next_token(input: &str) -> (&str, &str) {
    let s = input.trim_start();
    match s.find(char::is_whitespace) {
        Some(idx) => (&s[..idx], s[idx..].trim_start()),
        None => (s, ""),
    }
}

fn parse_kv_fields(input: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = input.trim();
    while !rest.is_empty() {
        let Some(eq) = rest.find('=') else { break };
        let key = rest[..eq].trim().to_string();
        let after = &rest[eq + 1..];
        let (value, consumed) = if after.starts_with('"') {
            match parse_debug_string(after) {
                Some(v) => v,
                None => break,
            }
        } else {
            let end = after.find(char::is_whitespace).unwrap_or(after.len());
            (after[..end].to_string(), end)
        };
        out.push((key, value));
        rest = after[consumed..].trim_start();
    }
    out
}

fn parse_stream_interrupted(line: &str, time: &str) -> LogEntry {
    let fields_input = line
        .trim_start()
        .strip_prefix("STREAM_INTERRUPTED")
        .unwrap_or(line);
    let mut entry = LogEntry {
        kind: "stream_interrupted".to_string(),
        raw: line.to_string(),
        time: Some(time.to_string()),
        ..Default::default()
    };
    for (key, value) in parse_kv_fields(fields_input) {
        match key.as_str() {
            "client" => entry.ip = Some(value),
            "endpoint" => entry.path = Some(value),
            "model" => entry.model = Some(value),
            "backend" => entry.backend = Some(value),
            "error" => entry.error = Some(value),
            _ => {}
        }
    }
    entry.cap_error();
    entry
}

fn parse_access_line(line: &str) -> Option<LogEntry> {
    let (ip, rest) = line.split_once(" - - [")?;
    let (time, rest) = rest.split_once("] ")?;

    let (request, rest) = take_quoted(rest)?;
    let (status_token, rest) = next_token(rest);
    let status = status_token.parse::<i64>().ok();
    let (_, rest) = next_token(rest);
    let (_, rest) = take_quoted(rest)?;
    let (user_agent, rest) = take_quoted(rest)?;
    let (latency, rest) = next_token(rest);
    let (model, rest) = take_quoted(rest)?;
    let (api_key, rest) = take_quoted(rest)?;
    let (error, tail_raw) = take_quoted(rest)?;
    let tail = tail_raw.trim();
    let request_body = if tail.is_empty() || tail == "\"-\"" {
        None
    } else {
        take_quoted(tail)
            .map(|(body, _)| body)
            .or_else(|| Some(tail.to_string()))
    };

    let mut parts = request.splitn(3, ' ');
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();

    let mut entry = LogEntry {
        kind: "http_error".to_string(),
        raw: line.to_string(),
        time: Some(time.to_string()),
        ip: Some(ip.to_string()),
        method: Some(method),
        path: Some(path),
        status,
        user_agent: Some(user_agent),
        latency: Some(latency.to_string()),
        model: Some(model),
        api_key: Some(api_key),
        backend: None,
        error: Some(error),
        request_body,
        ..Default::default()
    };
    entry.cap_error();
    Some(entry)
}

pub(crate) fn parse_line(raw: &str) -> LogEntry {
    let line = raw.trim_end();
    if line.is_empty() {
        return LogEntry::unparsed(raw);
    }
    if let Some(stripped) = line.strip_prefix('[') {
        if let Some((time, rest)) = stripped.split_once(']') {
            let rest = rest.trim_start();
            if rest.starts_with("STREAM_INTERRUPTED") {
                return parse_stream_interrupted(rest, time);
            }
        }
    }
    parse_access_line(line).unwrap_or_else(|| LogEntry::unparsed(line))
}

fn newest_error_log(log_dir: &Path) -> Option<PathBuf> {
    let entries = std::fs::read_dir(log_dir).ok()?;
    let mut best: Option<(String, PathBuf)> = None;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.starts_with("error.") || !name.ends_with(".log") {
            continue;
        }
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        if best.as_ref().map(|(k, _)| name > *k).unwrap_or(true) {
            best = Some((name, path));
        }
    }
    best.map(|(_, path)| path)
}

/// Reads up to `limit` complete lines ending at byte offset `before`, newest line first.
///
/// The file is walked backwards in chunks instead of being loaded whole, so cost scales
/// with the requested page rather than the log size. The returned offset is the start of
/// the oldest returned line, which makes it a line boundary safe to page from.
pub(crate) fn read_tail_lines(
    path: &Path,
    before: Option<u64>,
    limit: usize,
) -> std::io::Result<(Vec<String>, Option<u64>)> {
    let mut file = File::open(path)?;
    let size = file.metadata()?.len();
    let end = before.map(|b| b.min(size)).unwrap_or(size);
    let mut start = end;
    let mut buf: Vec<u8> = Vec::new();

    while start > 0 {
        let read_len = CHUNK_BYTES.min(start as usize);
        let from = start - read_len as u64;
        let mut chunk = vec![0u8; read_len];
        file.seek(SeekFrom::Start(from))?;
        file.read_exact(&mut chunk)?;
        chunk.extend_from_slice(&buf);
        buf = chunk;
        start = from;
        if buf.iter().filter(|b| **b == b'\n').count() > limit {
            break;
        }
    }

    let starts_at_boundary = if start == 0 {
        true
    } else {
        file.seek(SeekFrom::Start(start - 1))?;
        let mut one = [0u8; 1];
        file.read_exact(&mut one)?;
        one[0] == b'\n'
    };

    let mut segs: Vec<(usize, usize)> = Vec::new();
    let mut seg_start = 0usize;
    for (i, b) in buf.iter().enumerate() {
        if *b == b'\n' {
            segs.push((seg_start, i));
            seg_start = i + 1;
        }
    }
    let had_trailing = seg_start < buf.len();
    if had_trailing {
        segs.push((seg_start, buf.len()));
    }
    if !starts_at_boundary && !segs.is_empty() {
        segs.remove(0);
    }
    if had_trailing && end < size && !segs.is_empty() {
        segs.pop();
    }

    let take = limit.min(segs.len());
    let chosen = &segs[segs.len() - take..];
    let mut lines: Vec<String> = chosen
        .iter()
        .map(|(s, e)| String::from_utf8_lossy(&buf[*s..*e]).into_owned())
        .collect();
    let next_before = chosen
        .first()
        .map(|(s, _)| start + *s as u64)
        .filter(|offset| *offset > 0);
    lines.reverse();
    Ok((lines, next_before))
}

pub async fn error_log_tail(
    Query(params): Query<ErrorLogParams>,
) -> Result<Json<Value>, (StatusCode, String)> {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT) as usize;
    let log_dir = PathBuf::from(crate::logging::DEFAULT_LOG_DIR);
    let path = newest_error_log(&log_dir)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "no error log file found".to_string()))?;
    let file_name = path
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_default();
    let size = path.metadata().map(|m| m.len()).unwrap_or(0);
    let before = params.before;

    let (lines, next_before) =
        tokio::task::spawn_blocking(move || read_tail_lines(&path, before, limit))
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("error log task failed: {e}"),
                )
            })?
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("error log read failed: {e}"),
                )
            })?;

    let entries: Vec<LogEntry> = lines.iter().map(|l| parse_line(l)).collect();

    Ok(Json(json!({
        "file": file_name,
        "size": size,
        "entries": entries,
        "nextBefore": next_before,
        "hasMore": next_before.is_some(),
    })))
}

#[cfg(test)]
mod tests {
    use super::{newest_error_log, parse_line, read_tail_lines, truncate_error, MAX_ERROR_BYTES};
    use std::fs;
    use std::path::PathBuf;

    fn unique(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!(
            "qq_errlog_{}_{}_{}",
            std::process::id(),
            nanos,
            tag
        ));
        p
    }

    fn write(tag: &str, body: &[u8]) -> PathBuf {
        let p = unique(tag);
        fs::write(&p, body).unwrap();
        p
    }

    #[test]
    fn tail_returns_newest_first_and_flags_more() {
        let p = write("basic", b"A\nB\nC\n");
        let (lines, next) = read_tail_lines(&p, None, 2).unwrap();
        assert_eq!(lines, vec!["C".to_string(), "B".to_string()]);
        assert_eq!(next, Some(2));
        fs::remove_file(&p).ok();
    }

    #[test]
    fn tail_pages_backwards_without_gaps_or_duplicates() {
        let p = write("paging", b"L1\nL2\nL3\nL4\nL5\n");

        let (page1, c1) = read_tail_lines(&p, None, 2).unwrap();
        assert_eq!(page1, vec!["L5".to_string(), "L4".to_string()]);

        let (page2, c2) = read_tail_lines(&p, c1, 2).unwrap();
        assert_eq!(page2, vec!["L3".to_string(), "L2".to_string()]);

        let (page3, c3) = read_tail_lines(&p, c2, 2).unwrap();
        assert_eq!(page3, vec!["L1".to_string()]);
        assert_eq!(c3, None);

        let mut all: Vec<String> = Vec::new();
        all.extend(page1);
        all.extend(page2);
        all.extend(page3);
        assert_eq!(all, vec!["L5", "L4", "L3", "L2", "L1"]);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn tail_handles_missing_trailing_newline() {
        let p = write("noeol", b"A\nB");
        let (lines, next) = read_tail_lines(&p, None, 5).unwrap();
        assert_eq!(lines, vec!["B".to_string(), "A".to_string()]);
        assert_eq!(next, None);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn tail_of_empty_file_is_empty() {
        let p = write("empty", b"");
        let (lines, next) = read_tail_lines(&p, None, 5).unwrap();
        assert!(lines.is_empty());
        assert_eq!(next, None);
        fs::remove_file(&p).ok();
    }

    #[test]
    fn newest_error_log_picks_latest_date() {
        let dir = unique("dir");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("error.2026-09-16.log"), b"old\n").unwrap();
        fs::write(dir.join("error.2026-09-17.log"), b"new\n").unwrap();
        fs::write(dir.join("info.2026-09-17.log"), b"ignored\n").unwrap();

        let picked = newest_error_log(&dir).unwrap();
        assert_eq!(
            picked.file_name().unwrap().to_string_lossy(),
            "error.2026-09-17.log"
        );
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn parses_access_error_line_without_body() {
        let line = r#"127.0.0.1 - - [17/Sep/2026:07:27:02 +0000] "POST /v1/chat/completions HTTP/1.1" 422 - "-" "curl/8.9.1" 0.002s "-" "-" "The model `no-such-model` does not exist""#;
        let e = parse_line(line);
        assert_eq!(e.kind, "http_error");
        assert_eq!(e.time.as_deref(), Some("17/Sep/2026:07:27:02 +0000"));
        assert_eq!(e.ip.as_deref(), Some("127.0.0.1"));
        assert_eq!(e.method.as_deref(), Some("POST"));
        assert_eq!(e.path.as_deref(), Some("/v1/chat/completions"));
        assert_eq!(e.status, Some(422));
        assert_eq!(e.user_agent.as_deref(), Some("curl/8.9.1"));
        assert_eq!(e.latency.as_deref(), Some("0.002s"));
        assert_eq!(e.model.as_deref(), Some("-"));
        assert_eq!(
            e.error.as_deref(),
            Some("The model `no-such-model` does not exist")
        );
        assert_eq!(e.request_body, None);
    }

    #[test]
    fn caps_oversized_error_and_keeps_raw_intact() {
        let huge = "x".repeat(MAX_ERROR_BYTES + 5000);
        let line = format!(
            r#"10.0.0.5 - - [17/Sep/2026:07:28:00 +0000] "POST /v1/responses HTTP/1.1" 400 - "-" "curl/8.9.1" 0.064s "-" "1" "{}" "{{}}""#,
            huge
        );
        let e = parse_line(&line);
        let err = e.error.expect("error should be present");
        assert!(e.error_truncated);
        assert_eq!(e.error_bytes, Some(huge.len()));
        assert!(err.len() < huge.len());
        assert!(err.contains("已截断"));
        assert!(e.raw.len() > huge.len(), "raw keeps the untruncated line");
        assert_eq!(e.request_body.as_deref(), Some("{}"));
    }

    #[test]
    fn leaves_short_error_untouched() {
        let e = parse_line(
            r#"127.0.0.1 - - [17/Sep/2026:07:27:02 +0000] "POST /v1/responses HTTP/1.1" 400 - "-" "curl/8.9.1" 0.002s "-" "-" "boom""#,
        );
        assert_eq!(e.error.as_deref(), Some("boom"));
        assert!(!e.error_truncated);
        assert_eq!(e.error_bytes, None);
    }

    #[test]
    fn truncation_respects_char_boundaries() {
        let ascii_bytes_ending_one_byte_before_limit = MAX_ERROR_BYTES - 1;
        let msg = "a".repeat(ascii_bytes_ending_one_byte_before_limit) + &"中".repeat(10);
        let (capped, truncated, total) = truncate_error(msg);
        assert!(truncated);
        assert_eq!(total, ascii_bytes_ending_one_byte_before_limit + 10 * 3);
        assert!(capped.starts_with(&"a".repeat(ascii_bytes_ending_one_byte_before_limit)));
        assert!(capped.contains("已截断"));
    }

    #[test]
    fn parses_access_error_line_with_escaped_body() {
        let line = r#"10.0.0.5 - - [17/Sep/2026:07:28:00 +0000] "POST /v1/messages HTTP/1.1" 500 - "-" "python-httpx/0.27.0" 1.250s "claude-x" "sk-abc12345..." "upstream boom" "{\"model\":\"claude-x\",\"nested\":\"a\nb\"}""#;
        let e = parse_line(line);
        assert_eq!(e.kind, "http_error");
        assert_eq!(e.status, Some(500));
        assert_eq!(e.path.as_deref(), Some("/v1/messages"));
        assert_eq!(e.model.as_deref(), Some("claude-x"));
        assert_eq!(e.error.as_deref(), Some("upstream boom"));
        let body = e.request_body.expect("body should be extracted");
        assert!(body.contains("\"model\":\"claude-x\""));
        assert!(body.contains('\n'), "escape sequences should be unescaped");
    }

    #[test]
    fn parses_stream_interrupted_line() {
        let line = r#"[17/Sep/2026:08:00:00 +0000] STREAM_INTERRUPTED client=10.0.0.9 endpoint=/v1/chat/completions model=gpt-5 backend=alpha error="upstream stream interrupted: connection reset""#;
        let e = parse_line(line);
        assert_eq!(e.kind, "stream_interrupted");
        assert_eq!(e.time.as_deref(), Some("17/Sep/2026:08:00:00 +0000"));
        assert_eq!(e.ip.as_deref(), Some("10.0.0.9"));
        assert_eq!(e.path.as_deref(), Some("/v1/chat/completions"));
        assert_eq!(e.model.as_deref(), Some("gpt-5"));
        assert_eq!(e.backend.as_deref(), Some("alpha"));
        assert_eq!(
            e.error.as_deref(),
            Some("upstream stream interrupted: connection reset")
        );
        assert_eq!(e.status, None);
    }

    #[test]
    fn keeps_unparsed_lines_as_raw() {
        let e = parse_line("some totally unexpected system message");
        assert_eq!(e.kind, "unparsed");
        assert_eq!(e.raw, "some totally unexpected system message");
    }
}
