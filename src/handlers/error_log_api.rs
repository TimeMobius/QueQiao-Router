use axum::{extract::Query, http::StatusCode, Json};
use serde::Deserialize;
use serde_json::{json, Value};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

const CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_LIMIT: i64 = 100;
const MAX_LIMIT: i64 = 500;

#[derive(Debug, Default, Deserialize)]
pub struct ErrorLogParams {
    pub before: Option<u64>,
    pub limit: Option<i64>,
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

    Ok(Json(json!({
        "file": file_name,
        "size": size,
        "lines": lines,
        "nextBefore": next_before,
        "hasMore": next_before.is_some(),
    })))
}

#[cfg(test)]
mod tests {
    use super::{newest_error_log, read_tail_lines};
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
}
