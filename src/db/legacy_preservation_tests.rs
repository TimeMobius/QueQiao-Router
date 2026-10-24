use sqlx::sqlite::SqlitePoolOptions;

#[tokio::test]
async fn preserves_converted_records_when_plaintext_compatibility_columns_are_empty() {
    // Given an intermediate schema whose content already lives in previews/payloads.
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE records (
            id INTEGER PRIMARY KEY, Time TEXT, IP TEXT, Model TEXT, Type TEXT,
            CompletionTokens INTEGER, PromptTokens INTEGER, TotalTokens INTEGER,
            Tool BOOLEAN, Multimodal BOOLEAN, Headers TEXT, Request TEXT, Response TEXT,
            TimeMs INTEGER, Prompt TEXT, RequestTail TEXT, Answer TEXT
        )",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(crate::db::schema::PAYLOADS_DDL)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO records (id, Time, TimeMs, Model, Type, Prompt, RequestTail, Answer)
         VALUES (7, '2026-10-24 00:00:00', 1792800000123, 'existing-model',
                 'chat.completions', 'keep-preview', 'keep-tail', 'keep-answer')",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO payloads (record_id, codec, request, response, headers)
         VALUES (7, 'none', ?, ?, ?)",
    )
    .bind(b"keep-request".as_slice())
    .bind(b"keep-response".as_slice())
    .bind(b"keep-headers".as_slice())
    .execute(&pool)
    .await
    .unwrap();

    // When the shared legacy adapter upgrades its schema.
    super::migrate_archive(&pool).await.unwrap();

    // Then existing converted content is preserved instead of rebuilt from NULL.
    let preview: (i64, String, String, String) =
        sqlx::query_as("SELECT TimeMs, Prompt, RequestTail, Answer FROM records WHERE id = 7")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        preview,
        (
            1_792_800_000_123,
            "keep-preview".into(),
            "keep-tail".into(),
            "keep-answer".into()
        )
    );
    let payload: (Vec<u8>, Vec<u8>, Vec<u8>) =
        sqlx::query_as("SELECT request, response, headers FROM payloads WHERE record_id = 7")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(
        payload,
        (
            b"keep-request".to_vec(),
            b"keep-response".to_vec(),
            b"keep-headers".to_vec()
        )
    );
    pool.close().await;
}
