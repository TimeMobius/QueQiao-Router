use std::fs::{self, OpenOptions};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Mutex;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};

use super::archive::ArchiveRegistry;
use super::engine::Value;
use super::migrate::{detect_schema, migrate_path_to_v8, SchemaKind};
use super::store::AuditStore;

static ENV_LOCK: Mutex<()> = Mutex::new(());

fn temp_dir(tag: &str) -> PathBuf {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qq-migration-{tag}-{}-{stamp}", std::process::id()))
}

async fn create_pool(path: &Path) -> SqlitePool {
    let url = format!("sqlite:{}", path.display());
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::from_str(&url)
                .unwrap()
                .create_if_missing(true),
        )
        .await
        .unwrap()
}

async fn create_v7(path: &Path, rows: usize, headers: bool) {
    let pool = create_pool(path).await;
    let mut columns = vec![
        "id INTEGER PRIMARY KEY".to_string(),
        "Time TEXT".to_string(),
        "IP TEXT".to_string(),
        "Model TEXT".to_string(),
        "Type TEXT".to_string(),
        "CompletionTokens INTEGER".to_string(),
        "PromptTokens INTEGER".to_string(),
        "TotalTokens INTEGER".to_string(),
        "Tool INTEGER".to_string(),
        "Multimodal INTEGER".to_string(),
    ];
    if headers {
        columns.push("Headers TEXT".to_string());
    }
    columns.extend(
        super::schema::NEW_COLUMNS
            .iter()
            .map(|(name, declaration)| format!("{name} {declaration}")),
    );
    sqlx::query(&format!("CREATE TABLE records ({})", columns.join(",")))
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(super::schema::PAYLOADS_DDL)
        .execute(&pool)
        .await
        .unwrap();
    for ddl in super::schema::FTS_DDL {
        sqlx::query(ddl).execute(&pool).await.unwrap();
    }
    for id in 1..=rows {
        let id = i64::try_from(id).unwrap();
        sqlx::query(
            "INSERT INTO records (id, Time, TimeMs, Model, Type, Prompt, RequestTail, Answer) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind("2026-10-24 00:00:00.000000")
        .bind(id)
        .bind("v7-model")
        .bind("chat.completions")
        .bind("hello migration")
        .bind("request tail")
        .bind("answer migration")
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO payloads (record_id, codec, request, response, headers, \
             request_raw_len, response_raw_len, headers_raw_len) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(id)
        .bind("test")
        .bind(vec![1_u8, 2, 3, u8::try_from(id).unwrap()])
        .bind(vec![4_u8, 5, 6, u8::try_from(id).unwrap()])
        .bind(if headers { Some(vec![7_u8, 8]) } else { None })
        .bind(4_i64)
        .bind(4_i64)
        .bind(if headers { Some(2_i64) } else { None })
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query("PRAGMA user_version = 7")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}

async fn create_raw_v1(path: &Path, include_time: bool) {
    let pool = create_pool(path).await;
    let time = if include_time { "Time TEXT," } else { "" };
    sqlx::query(&format!(
        "CREATE TABLE records (id INTEGER PRIMARY KEY, {time} IP TEXT, Model TEXT, Type TEXT, \
         CompletionTokens INTEGER, PromptTokens INTEGER, TotalTokens INTEGER, Tool INTEGER, \
         Multimodal INTEGER, Headers TEXT, Request TEXT, Response TEXT)"
    ))
    .execute(&pool)
    .await
    .unwrap();
    let request = r#"{"model":"raw-v1","messages":[{"role":"user","content":"hello raw"}]}"#;
    let response = r#"{"choices":[{"message":{"content":"hello answer"},"finish_reason":"stop"}]}"#;
    if include_time {
        sqlx::query(
            "INSERT INTO records (id, Time, IP, Model, Type, Headers, Request, Response) \
             VALUES (1, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind("2026-10-24 00:00:00.000000")
        .bind("127.0.0.1")
        .bind("raw-v1")
        .bind("chat.completions")
        .bind(r#"{"User-Agent":"migration-test"}"#)
        .bind(request)
        .bind(response)
        .execute(&pool)
        .await
        .unwrap();
    } else {
        sqlx::query(
            "INSERT INTO records (id, IP, Model, Type, Headers, Request, Response) \
             VALUES (1, ?, ?, ?, ?, ?, ?)",
        )
        .bind("127.0.0.1")
        .bind("raw-v1")
        .bind("chat.completions")
        .bind(r#"{"User-Agent":"migration-test"}"#)
        .bind(request)
        .bind(response)
        .execute(&pool)
        .await
        .unwrap();
    }
    sqlx::query("PRAGMA user_version = 1")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
}

async fn record_count(path: &Path) -> i64 {
    let store = AuditStore::open(path, true, Duration::from_secs(5))
        .await
        .unwrap();
    let row = store
        .query_one("SELECT COUNT(*) FROM records", Vec::new())
        .await
        .unwrap()
        .unwrap();
    row.get_i64(0).unwrap()
}

#[tokio::test]
async fn populated_v7_cutover_preserves_records_and_payloads() {
    let dir = temp_dir("v7-populated");
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("record.db");
    create_v7(&path, 2, true).await;

    migrate_path_to_v8(&path).await.unwrap();

    assert!(matches!(
        detect_schema(&path).await.unwrap(),
        SchemaKind::V8
    ));
    assert_eq!(record_count(&path).await, 2);
    assert!(dir.join("record.db.bak").exists());
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn empty_v7_cutover_verifies_empty_bounds() {
    let dir = temp_dir("v7-empty");
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("record.db");
    create_v7(&path, 0, true).await;

    migrate_path_to_v8(&path).await.unwrap();

    assert!(matches!(
        detect_schema(&path).await.unwrap(),
        SchemaKind::V8
    ));
    assert_eq!(record_count(&path).await, 0);
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn slim_v7_without_headers_is_detected_and_migrated() {
    let dir = temp_dir("v7-slim");
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("record.db");
    create_v7(&path, 1, false).await;

    assert!(matches!(
        detect_schema(&path).await.unwrap(),
        SchemaKind::V7
    ));
    migrate_path_to_v8(&path).await.unwrap();

    assert_eq!(record_count(&path).await, 1);
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn raw_v1_startup_migration_uses_staged_legacy_extraction() {
    let _guard = ENV_LOCK.lock().unwrap();
    let dir = temp_dir("raw-v1");
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("record.db");
    create_raw_v1(&path, true).await;
    let previous_path = std::env::var_os("RECD_PATH");
    std::env::set_var("RECD_PATH", format!("sqlite:{}", path.display()));

    let store = super::init_db().await.unwrap();
    assert_eq!(record_count(&path).await, 1);
    assert!(
        store
            .query_one(
                "SELECT COUNT(*) FROM records WHERE SearchText LIKE ?",
                vec![Value::Text("%hello raw%".to_string())],
            )
            .await
            .unwrap()
            .unwrap()
            .get_i64(0)
            .unwrap()
            > 0
    );
    drop(store);

    let second = super::init_db().await.unwrap();
    drop(second);
    match previous_path {
        Some(value) => std::env::set_var("RECD_PATH", value),
        None => std::env::remove_var("RECD_PATH"),
    }
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn backup_collision_uses_next_original_backup_name() {
    let dir = temp_dir("backup-collision");
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("record.db");
    create_v7(&path, 1, true).await;
    fs::write(dir.join("record.db.bak"), b"existing backup").unwrap();

    migrate_path_to_v8(&path).await.unwrap();

    assert_eq!(
        fs::read(dir.join("record.db.bak")).unwrap(),
        b"existing backup"
    );
    assert!(dir.join("record.db.bak.1").exists());
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn failed_legacy_conversion_leaves_original_bytes_untouched() {
    let dir = temp_dir("failure-bytes");
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("record.db");
    create_raw_v1(&path, false).await;
    let before = fs::read(&path).unwrap();

    assert!(migrate_path_to_v8(&path).await.is_err());

    assert_eq!(fs::read(&path).unwrap(), before);
    assert!(!dir.join("record.db.bak").exists());
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn migration_waits_for_process_lock_and_retries_stale_work() {
    let dir = temp_dir("lock");
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("record.db");
    create_v7(&path, 0, true).await;
    let lock_path = PathBuf::from(format!("{}{}", path.display(), ".v8-migrate.lock"));
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .unwrap();
    lock.try_lock().unwrap();

    let mut task = tokio::spawn({
        let path = path.clone();
        async move { migrate_path_to_v8(&path).await }
    });
    assert!(tokio::time::timeout(Duration::from_millis(100), &mut task)
        .await
        .is_err());
    lock.unlock().unwrap();
    task.await.unwrap().unwrap();

    assert!(matches!(
        detect_schema(&path).await.unwrap(),
        SchemaKind::V8
    ));
    fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn legacy_archive_is_lazily_upgraded_for_search() {
    let dir = temp_dir("archive-search");
    fs::create_dir_all(&dir).unwrap();
    let archive = dir.join("record_202610.db");
    create_raw_v1(&archive, true).await;

    let registry = ArchiveRegistry::new(dir.join("record.db"));
    registry.ensure_scanned().await;
    let shard = registry.get("record_202610").await.unwrap();
    let row = shard
        .store
        .query_one(
            "SELECT COUNT(*) FROM records WHERE fts_match(SearchText, ?)",
            vec![Value::Text("hello".to_string())],
        )
        .await
        .unwrap()
        .unwrap();

    assert_eq!(row.get_i64(0).unwrap(), 1);
    assert!(matches!(
        detect_schema(&archive).await.unwrap(),
        SchemaKind::V8
    ));
    fs::remove_dir_all(dir).unwrap();
}
