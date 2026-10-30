use super::*;
use crate::db::store::AuditStore;
use std::sync::Arc;
use std::time::Duration;

fn temp_dir(tag: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!("qq-aggregate-{tag}-{nanos}"));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn today_minus(days: i64) -> i64 {
    local_day_ms(now_ms() - days * 86_400_000).unwrap()
}

#[allow(clippy::too_many_arguments)]
async fn insert(
    store: &AuditStore,
    time_ms: i64,
    model: &str,
    status: Value,
    prompt: i64,
    latency: Value,
    ttft: Value,
) {
    store
        .execute(
            "INSERT INTO records (Time, TimeMs, Model, ApiKey, IP, Type, Backend, ClientName, \
             Status, PromptTokens, CompletionTokens, TotalTokens, LatencyMs, TtftMs, SearchText) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            vec![
                Value::Text("2026-10-01 00:00:00.000000".to_string()),
                Value::Integer(time_ms),
                Value::Text(model.to_string()),
                Value::Text("sk-a".to_string()),
                Value::Text("10.0.0.1".to_string()),
                Value::Text("chat.completions".to_string()),
                Value::Text("b1".to_string()),
                Value::Text("cli-a".to_string()),
                status,
                Value::Integer(prompt),
                Value::Integer(prompt * 2),
                Value::Integer(prompt * 3),
                latency,
                ttft,
                Value::Text("text".to_string()),
            ],
        )
        .await
        .unwrap();
}

async fn open_store(dir: &Path, name: &str) -> Arc<AuditStore> {
    AuditStore::open(&dir.join(name), false, Duration::from_secs(5))
        .await
        .unwrap()
}

#[tokio::test]
async fn active_rollup_accumulates_only_new_rows() {
    let dir = temp_dir("incremental");
    let audit_path = dir.join("record.db");
    let store = open_store(&dir, "record.db").await;
    let day1 = today_minus(3);
    let day2 = next_local_day(day1).unwrap();

    insert(
        &store,
        day1 + 1000,
        "gpt-4",
        Value::Integer(200),
        10,
        Value::Real(100.0),
        Value::Null,
    )
    .await;
    insert(
        &store,
        day1 + 2000,
        "gpt-4",
        Value::Integer(500),
        20,
        Value::Real(300.0),
        Value::Null,
    )
    .await;
    insert(
        &store,
        day1 + 3000,
        "",
        Value::Null,
        30,
        Value::Null,
        Value::Null,
    )
    .await;

    ensure_active(&audit_path, "active", &store).await.unwrap();
    assert!(
        active_caught_up(
            &audit_path,
            "active",
            &store,
            next_local_day(day2).unwrap() - 1
        )
        .await
    );

    let slice = read_active_slice(&audit_path, "active", day1, day2)
        .await
        .unwrap();
    assert_eq!(slice.days.len(), 1);
    let acc = slice.days[0].acc;
    assert_eq!(acc.rows_all, 3);
    assert_eq!(acc.counted, 2);
    assert_eq!(acc.success, 1);
    assert_eq!(acc.errors, 1);
    assert_eq!(acc.prompt_tokens, 60);
    assert_eq!(acc.total_tokens, 180);
    assert_eq!(acc.latency_sum, 400.0);
    assert_eq!(acc.latency_count, 2);
    assert_eq!(acc.latency_max, 300.0);
    assert_eq!(slice.dims["model"]["gpt-4"].rows_all, 2);
    assert_eq!(slice.dims["model"]["Unknown"].rows_all, 1);
    let mut distinct = slice.distinct["model"].clone();
    distinct.sort();
    assert_eq!(distinct, vec!["".to_string(), "gpt-4".to_string()]);

    insert(
        &store,
        day1 + 4000,
        "claude",
        Value::Integer(201),
        5,
        Value::Real(50.0),
        Value::Real(7.0),
    )
    .await;
    insert(
        &store,
        day2 + 1000,
        "claude",
        Value::Integer(404),
        1,
        Value::Real(10.0),
        Value::Null,
    )
    .await;

    ensure_active(&audit_path, "active", &store).await.unwrap();
    let slice = read_active_slice(&audit_path, "active", day1, next_local_day(day2).unwrap())
        .await
        .unwrap();
    assert_eq!(slice.days.len(), 2);
    let day1_acc = slice
        .days
        .iter()
        .find(|day| day.day_ms == day1)
        .unwrap()
        .acc;
    assert_eq!(day1_acc.rows_all, 4);
    assert_eq!(day1_acc.counted, 3);
    assert_eq!(day1_acc.success, 2);
    assert_eq!(day1_acc.errors, 1);
    assert_eq!(day1_acc.prompt_tokens, 65);
    assert_eq!(day1_acc.latency_sum, 450.0);
    assert_eq!(day1_acc.ttft_count, 1);
    let day2_acc = slice
        .days
        .iter()
        .find(|day| day.day_ms == day2)
        .unwrap()
        .acc;
    assert_eq!(day2_acc.rows_all, 1);
    assert_eq!(day2_acc.errors, 1);
    let mut models = slice.dims["model"].keys().cloned().collect::<Vec<_>>();
    models.sort();
    assert_eq!(
        models,
        vec![
            "Unknown".to_string(),
            "claude".to_string(),
            "gpt-4".to_string()
        ]
    );
    assert_eq!(slice.latency_hist.values().sum::<i64>(), 4);
}

#[tokio::test]
async fn active_rollup_resets_when_database_rotates() {
    let dir = temp_dir("rotate");
    let audit_path = dir.join("record.db");
    let day = today_minus(2);
    let old_store = open_store(&dir, "record.db").await;
    insert(
        &old_store,
        day + 1000,
        "old",
        Value::Integer(200),
        10,
        Value::Null,
        Value::Null,
    )
    .await;
    ensure_active(&audit_path, "active", &old_store)
        .await
        .unwrap();
    drop(old_store);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", audit_path.display()));
    }

    let new_store = open_store(&dir, "record.db").await;
    insert(
        &new_store,
        day + 5000,
        "new",
        Value::Integer(200),
        1,
        Value::Null,
        Value::Null,
    )
    .await;
    ensure_active(&audit_path, "active", &new_store)
        .await
        .unwrap();

    let slice = read_active_slice(&audit_path, "active", day, next_local_day(day).unwrap())
        .await
        .unwrap();
    let acc = slice.days[0].acc;
    assert_eq!(acc.rows_all, 1);
    assert_eq!(acc.prompt_tokens, 1);
    assert_eq!(
        slice.dims["model"].keys().cloned().collect::<Vec<_>>(),
        vec!["new".to_string()]
    );
}

#[tokio::test]
async fn active_rollup_is_not_ready_before_build() {
    let dir = temp_dir("not-ready");
    let audit_path = dir.join("record.db");
    let store = open_store(&dir, "record.db").await;
    let day = today_minus(1);
    insert(
        &store,
        day + 1000,
        "m",
        Value::Integer(200),
        1,
        Value::Null,
        Value::Null,
    )
    .await;
    assert!(
        !active_caught_up(
            &audit_path,
            "active",
            &store,
            next_local_day(day).unwrap() - 1
        )
        .await
    );
    assert!(
        read_active_slice(&audit_path, "active", day, next_local_day(day).unwrap())
            .await
            .is_none()
    );
}

#[tokio::test]
async fn active_rollup_is_ready_before_today_is_processed() {
    let dir = temp_dir("ready-boundary");
    let audit_path = dir.join("record.db");
    let store = open_store(&dir, "record.db").await;
    let yesterday = today_minus(1);
    insert(
        &store,
        yesterday + 1000,
        "m",
        Value::Integer(200),
        1,
        Value::Null,
        Value::Null,
    )
    .await;
    ensure_active(&audit_path, "active", &store).await.unwrap();
    insert(
        &store,
        today_minus(0) + 1000,
        "m",
        Value::Integer(200),
        1,
        Value::Null,
        Value::Null,
    )
    .await;
    assert!(
        active_caught_up(
            &audit_path,
            "active",
            &store,
            next_local_day(yesterday).unwrap() - 1
        )
        .await
    );
    assert!(
        !active_caught_up(
            &audit_path,
            "active",
            &store,
            next_local_day(today_minus(0)).unwrap() - 1
        )
        .await
    );
}

#[allow(clippy::too_many_arguments)]
async fn insert_dims(
    store: &AuditStore,
    time_ms: i64,
    model: &str,
    r#type: &str,
    backend: &str,
    client: &str,
) {
    store
        .execute(
            "INSERT INTO records (Time, TimeMs, Model, ApiKey, IP, Type, Backend, ClientName, \
             Status, PromptTokens, CompletionTokens, TotalTokens, LatencyMs, TtftMs, SearchText) \
             VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            vec![
                Value::Text("2026-10-01 00:00:00.000000".to_string()),
                Value::Integer(time_ms),
                Value::Text(model.to_string()),
                Value::Text("sk-a".to_string()),
                Value::Text("10.0.0.1".to_string()),
                Value::Text(r#type.to_string()),
                Value::Text(backend.to_string()),
                Value::Text(client.to_string()),
                Value::Integer(200),
                Value::Integer(1),
                Value::Integer(2),
                Value::Integer(3),
                Value::Null,
                Value::Null,
                Value::Text("text".to_string()),
            ],
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn active_facets_match_scan_values() {
    let dir = temp_dir("facets");
    let audit_path = dir.join("record.db");
    let store = open_store(&dir, "record.db").await;
    let day = today_minus(2);
    insert_dims(
        &store,
        day + 1000,
        "gpt-4",
        "chat.completions",
        "b1",
        "cli-a",
    )
    .await;
    insert_dims(&store, day + 2000, "", "", "", "").await;
    insert_dims(&store, day + 3000, "claude", "embeddings", "b2", "cli-b").await;
    ensure_active(&audit_path, "active", &store).await.unwrap();

    let facets = active_facets(&audit_path, "active", &store).await.unwrap();
    assert_eq!(
        facets.models,
        vec!["claude".to_string(), "gpt-4".to_string()]
    );
    assert_eq!(
        facets.types,
        vec!["chat.completions".to_string(), "embeddings".to_string()]
    );
    assert_eq!(facets.backends, vec!["b1".to_string(), "b2".to_string()]);
    assert_eq!(
        facets.clients,
        vec!["cli-a".to_string(), "cli-b".to_string()]
    );

    let (models, types, backends, clients) = crate::db::records_query::query_facets(&store).await;
    assert_eq!(facets.models, models);
    assert_eq!(facets.types, types);
    assert_eq!(facets.backends, backends);
    assert_eq!(facets.clients, clients);
}
