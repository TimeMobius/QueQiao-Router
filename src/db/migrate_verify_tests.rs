use super::verify;
use crate::db::engine::Value;
use crate::db::store::AuditStore;
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

async fn fixture(with_fts: bool) -> (SqlitePool, Arc<AuditStore>, PathBuf) {
    let source = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE records (id INTEGER PRIMARY KEY, TimeMs INTEGER, \
         Prompt TEXT, RequestTail TEXT, Answer TEXT)",
    )
    .execute(&source)
    .await
    .unwrap();
    sqlx::query(crate::db::schema::PAYLOADS_DDL)
        .execute(&source)
        .await
        .unwrap();
    let directory = std::env::temp_dir().join(format!(
        "qq-verify-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&directory).unwrap();
    let target_path = directory.join("target.db");
    let target = if with_fts {
        AuditStore::open(&target_path, false, Duration::from_secs(1)).await
    } else {
        AuditStore::open_deferred_fts(&target_path, Duration::from_secs(1)).await
    }
    .unwrap();
    for id in 1..=8_i64 {
        sqlx::query("INSERT INTO records (id, TimeMs, Prompt) VALUES (?, ?, 'myhello')")
            .bind(id)
            .bind(id)
            .execute(&source)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO payloads (record_id, codec, request, headers, request_raw_len, headers_raw_len) \
             VALUES (?, 'none', ?, ?, 2, 0)",
        )
        .bind(id)
        .bind(vec![b'a', u8::try_from(id).unwrap()])
        .bind(Vec::<u8>::new())
        .execute(&source)
        .await
        .unwrap();
        target
            .execute(
                "INSERT INTO records (id, TimeMs, Prompt, SearchText) VALUES (?, ?, 'myhello', ?)",
                vec![
                    Value::Integer(id),
                    Value::Integer(id),
                    Value::Text("myhello\n\n".to_string()),
                ],
            )
            .await
            .unwrap();
        target
            .execute(
                "INSERT INTO payloads (record_id, codec, request, headers, request_raw_len, headers_raw_len) \
                 VALUES (?, 'none', ?, ?, 2, 0)",
                vec![
                    Value::Integer(id),
                    Value::Blob(vec![b'a', u8::try_from(id).unwrap()]),
                    Value::Blob(Vec::new()),
                ],
            )
            .await
            .unwrap();
    }
    (source, target, directory)
}

async fn cleanup(source: SqlitePool, target: Arc<AuditStore>, directory: PathBuf) {
    source.close().await;
    drop(target);
    std::fs::remove_dir_all(directory).unwrap();
}

#[tokio::test]
async fn rejects_same_length_payload_corruption_after_first_six_rows() {
    let (source, target, directory) = fixture(true).await;
    verify(&source, &target).await.unwrap();

    target
        .execute(
            "UPDATE payloads SET request = ? WHERE record_id = 8",
            vec![Value::Blob(vec![b'z', 8])],
        )
        .await
        .unwrap();

    let error = verify(&source, &target).await.unwrap_err();
    assert!(error.to_string().contains("payload 8 request mismatch"));
    cleanup(source, target, directory).await;
}

#[tokio::test]
async fn rejects_payload_metadata_corruption() {
    let (source, target, directory) = fixture(true).await;
    for (column, changed, original) in [
        ("codec", "'zstd'", "'none'"),
        ("dict_id", "'wrong-dictionary'", "NULL"),
        ("request_raw_len", "99", "2"),
        ("response_raw_len", "99", "NULL"),
        ("headers_raw_len", "99", "0"),
    ] {
        target
            .execute(
                format!("UPDATE payloads SET {column} = {changed} WHERE record_id = 8"),
                Vec::new(),
            )
            .await
            .unwrap();

        let error = verify(&source, &target).await.unwrap_err();
        assert!(error
            .to_string()
            .contains(&format!("payload 8 {column} mismatch")));

        target
            .execute(
                format!("UPDATE payloads SET {column} = {original} WHERE record_id = 8"),
                Vec::new(),
            )
            .await
            .unwrap();
    }
    cleanup(source, target, directory).await;
}

#[tokio::test]
async fn distinguishes_null_payload_from_empty_blob() {
    let (source, target, directory) = fixture(true).await;
    target
        .execute(
            "UPDATE payloads SET response = ? WHERE record_id = 8",
            vec![Value::Blob(Vec::new())],
        )
        .await
        .unwrap();

    let error = verify(&source, &target).await.unwrap_err();

    assert!(error.to_string().contains("payload 8 response mismatch"));
    cleanup(source, target, directory).await;
}

#[tokio::test]
async fn permits_legacy_token_counts_to_differ_from_verified_substring_counts() {
    let (source, target, directory) = fixture(true).await;
    sqlx::query("CREATE VIRTUAL TABLE records_fts USING fts5(Prompt, content='records')")
        .execute(&source)
        .await
        .unwrap();
    sqlx::query("INSERT INTO records_fts(records_fts) VALUES('rebuild')")
        .execute(&source)
        .await
        .unwrap();
    let tokens: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM records_fts WHERE records_fts MATCH 'hello'")
            .fetch_one(&source)
            .await
            .unwrap();
    assert_eq!(tokens, 0);

    verify(&source, &target).await.unwrap();

    cleanup(source, target, directory).await;
}

#[tokio::test]
async fn rejects_search_text_that_does_not_preserve_previews() {
    let (source, target, directory) = fixture(true).await;
    target
        .execute(
            "UPDATE records SET SearchText = 'other text' WHERE id = 8",
            Vec::new(),
        )
        .await
        .unwrap();

    let error = verify(&source, &target).await.unwrap_err();

    assert!(error.to_string().contains("invalid SearchText"));
    cleanup(source, target, directory).await;
}

#[tokio::test]
async fn rejects_missing_target_fts_index() {
    let (source, target, directory) = fixture(false).await;

    let result = verify(&source, &target).await;

    assert!(result.is_err());
    cleanup(source, target, directory).await;
}
