use crate::db::engine::{DbError, Value};
use crate::db::store::AuditStore;
use futures::TryStreamExt;
use sqlx::sqlite::SqlitePool;
use sqlx::Row;

pub(crate) async fn verify(source: &SqlitePool, target: &AuditStore) -> Result<(), DbError> {
    let src_records: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM records")
        .fetch_one(source)
        .await
        .map_err(crate::db::map_sqlx_error)?;
    let src_payloads: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM payloads")
        .fetch_one(source)
        .await
        .map_err(crate::db::map_sqlx_error)?;
    let (src_min, src_max): (Option<i64>, Option<i64>) =
        sqlx::query_as("SELECT MIN(TimeMs), MAX(TimeMs) FROM records")
            .fetch_one(source)
            .await
            .map_err(crate::db::map_sqlx_error)?;
    let (src_req, src_resp, src_hdr): (i64, i64, i64) = sqlx::query_as(
        "SELECT COALESCE(SUM(length(request)),0), COALESCE(SUM(length(response)),0), COALESCE(SUM(length(headers)),0) FROM payloads",
    )
    .fetch_one(source)
    .await
    .map_err(crate::db::map_sqlx_error)?;

    let dst_records = target
        .query_one("SELECT COUNT(*) FROM records", Vec::new())
        .await?
        .map(|row| row.get_i64(0))
        .transpose()?
        .unwrap_or(0);
    let dst_payloads = target
        .query_one("SELECT COUNT(*) FROM payloads", Vec::new())
        .await?
        .map(|row| row.get_i64(0))
        .transpose()?
        .unwrap_or(0);
    let (dst_min, dst_max) = match target
        .query_one("SELECT MIN(TimeMs), MAX(TimeMs) FROM records", Vec::new())
        .await?
    {
        Some(row) => (row.get_opt_i64(0)?, row.get_opt_i64(1)?),
        None => (None, None),
    };
    let (dst_req, dst_resp, dst_hdr) = match target
        .query_one(
            "SELECT COALESCE(SUM(length(request)),0), COALESCE(SUM(length(response)),0), COALESCE(SUM(length(headers)),0) FROM payloads",
            Vec::new(),
        )
        .await?
    {
        Some(row) => (row.get_i64(0)?, row.get_i64(1)?, row.get_i64(2)?),
        None => (0, 0, 0),
    };

    println!("\n== verify ==");
    println!("records:  src={src_records} dst={dst_records}");
    println!("payloads: src={src_payloads} dst={dst_payloads}");
    println!("timeMs:   src={src_min:?}..{src_max:?} dst={dst_min:?}..{dst_max:?}");
    println!(
        "bytes:    req {src_req}/{dst_req}  resp {src_resp}/{dst_resp}  hdr {src_hdr}/{dst_hdr}"
    );

    if src_records != dst_records
        || src_payloads != dst_payloads
        || (src_min, src_max) != (dst_min, dst_max)
        || (src_req, src_resp, src_hdr) != (dst_req, dst_resp, dst_hdr)
    {
        return Err(DbError::Shape(
            "verification failed: totals differ".to_string(),
        ));
    }

    let payload_sql = "SELECT record_id, codec, dict_id, request, response, headers, \
                       request_raw_len, response_raw_len, headers_raw_len FROM payloads";
    let mut payloads = sqlx::query(payload_sql).fetch(source);
    while let Some(src_row) = payloads
        .try_next()
        .await
        .map_err(crate::db::map_sqlx_error)?
    {
        let id: i64 = src_row
            .try_get("record_id")
            .map_err(crate::db::map_sqlx_error)?;
        let dst_row = target
            .query_one(
                format!("{payload_sql} WHERE record_id = ?"),
                vec![Value::Integer(id)],
            )
            .await?
            .ok_or_else(|| DbError::Shape(format!("payload {id} missing in target")))?;
        for (idx, name) in [(1, "codec"), (2, "dict_id")] {
            let src: Option<String> = src_row.try_get(name).map_err(crate::db::map_sqlx_error)?;
            if src.as_deref() != dst_row.get_opt_str(idx)? {
                return Err(DbError::Shape(format!("payload {id} {name} mismatch")));
            }
        }
        for (idx, name) in [(3, "request"), (4, "response"), (5, "headers")] {
            let src_blob: Option<Vec<u8>> =
                src_row.try_get(name).map_err(crate::db::map_sqlx_error)?;
            if src_blob.as_deref() != dst_row.get_opt_blob(idx)? {
                return Err(DbError::Shape(format!("payload {id} {name} mismatch")));
            }
        }
        for (idx, name) in [
            (6, "request_raw_len"),
            (7, "response_raw_len"),
            (8, "headers_raw_len"),
        ] {
            let src: Option<i64> = src_row.try_get(name).map_err(crate::db::map_sqlx_error)?;
            if src != dst_row.get_opt_i64(idx)? {
                return Err(DbError::Shape(format!("payload {id} {name} mismatch")));
            }
        }
    }
    drop(payloads);

    let invalid_search_text = target
        .query_one(
            "SELECT COUNT(*) FROM records WHERE SearchText IS NULL OR SearchText <> \
             COALESCE(Prompt, '') || ? || COALESCE(RequestTail, '') || ? || COALESCE(Answer, '')",
            vec![Value::Text("\n".to_string()), Value::Text("\n".to_string())],
        )
        .await?
        .ok_or_else(|| DbError::Shape("search text verification returned no count".to_string()))?
        .get_i64(0)?;
    if invalid_search_text != 0 {
        return Err(DbError::Shape(format!(
            "verification failed: {invalid_search_text} records have invalid SearchText"
        )));
    }

    let source_has_fts: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'records_fts'",
    )
    .fetch_one(source)
    .await
    .map_err(crate::db::map_sqlx_error)?;
    for term in ["hello", "error", "model", "request", "function"] {
        let scanned = target
            .query_one(
                "SELECT COUNT(*) FROM records WHERE instr(lower(SearchText), ?) > 0",
                vec![Value::Text(term.to_string())],
            )
            .await?
            .ok_or_else(|| DbError::Shape("FTS scan returned no count".to_string()))?
            .get_i64(0)?;
        let indexed = target
            .query_one(
                "SELECT COUNT(*) FROM records WHERE fts_match(SearchText, ?) AND instr(lower(SearchText), ?) > 0",
                vec![Value::Text(term.to_string()), Value::Text(term.to_string())],
            )
            .await?
            .ok_or_else(|| DbError::Shape("FTS query returned no count".to_string()))?
            .get_i64(0)?;
        if scanned != indexed {
            return Err(DbError::Shape(format!(
                "verification failed: FTS '{term}' scan={scanned} index={indexed}"
            )));
        }
        if source_has_fts > 0 {
            let src_count: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM records WHERE id IN (SELECT rowid FROM records_fts WHERE records_fts MATCH ?)",
            )
            .bind(term)
            .fetch_one(source)
            .await
            .map_err(crate::db::map_sqlx_error)?;
            println!("fts '{term}': legacy tokens={src_count}, target substring={indexed}");
        } else {
            println!("fts '{term}': scan={scanned} index={indexed}");
        }
    }

    println!("verification passed");
    Ok(())
}

#[cfg(test)]
#[path = "migrate_verify_tests.rs"]
mod tests;
