//! Regression guard for the audit writer's id lookup.
//!
//! Startup used to block for minutes because `next_id` ran
//! `SELECT COALESCE(MAX(id), 0) + 1 FROM records`. Wrapping `MAX(id)` in an
//! expression defeats SQLite/Turso's min/max rowid optimization, so the query
//! compiled to a full table scan (`Rewind`/`Next`) of a 41 GB audit database
//! over NFS instead of the O(1) `Last`/`Prev` rowid probe.
//!
//! Asserting on the compiled bytecode — not on query latency — keeps this test
//! fast and deterministic on any machine size. Re-wrapping the id lookup in an
//! expression fails here rather than costing minutes of production downtime.

use super::store::AuditStore;
use crate::db::writer;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Opcodes that walk a table instead of seeking in it. Their presence in the
/// plan for the id lookup means the query scales with the audit history.
const SCAN_OPCODES: [&str; 2] = ["Rewind", "Next"];

fn temp_path(tag: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "qq-next-id-{tag}-{}-{nanos}.db",
        std::process::id()
    ))
}

async fn open_store(tag: &str) -> (std::path::PathBuf, std::sync::Arc<AuditStore>) {
    let path = temp_path(tag);
    let _ = std::fs::remove_file(&path);
    let store = AuditStore::open(&path, false, Duration::from_secs(5))
        .await
        .unwrap();
    (path, store)
}

/// Read the opcode column (index 1) of `EXPLAIN <sql>`.
async fn explain_opcodes(store: &AuditStore, sql: &str) -> Vec<String> {
    let rows = store
        .query(&format!("EXPLAIN {sql}"), Vec::new())
        .await
        .unwrap();
    rows.iter()
        .map(|row| row.get_str(1).unwrap_or("?").to_string())
        .collect()
}

#[tokio::test]
async fn non_empty_table_id_lookup_does_not_scan() {
    let (path, store) = open_store("nonempty").await;
    let connection = store.write().await;
    connection
        .execute_batch(
            "INSERT INTO records (id, TimeMs, Model) VALUES
                 (1, 1000, 'a'), (2, 2000, 'b'), (7, 7000, 'c');",
        )
        .await
        .unwrap();
    drop(connection);

    let next = writer::next_id_for_test(&store).await.unwrap();
    assert_eq!(next, 8, "id lookup must continue after the highest id");

    let opcodes = explain_opcodes(&store, writer::NEXT_ID_SQL).await;
    for opcode in SCAN_OPCODES {
        assert!(
            !opcodes.iter().any(|seen| seen == opcode),
            "id lookup regressed into a table scan: plan contains `{opcode}` -> {opcodes:?}"
        );
    }
    assert!(
        opcodes.iter().any(|seen| seen == "Last"),
        "id lookup should seek the end of the rowid index via `Last`, got {opcodes:?}"
    );
    // `RowId` reads the rowid B-tree column directly. Together with `Last` this
    // proves `records.id INTEGER PRIMARY KEY` is still a plain rowid alias (not
    // a separate primary-key index), which is what keeps the seek O(1).
    assert!(
        opcodes.iter().any(|seen| seen == "RowId"),
        "expected a rowid read, so `records.id` must remain a rowid alias: {opcodes:?}"
    );

    drop(store);
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn empty_table_id_lookup_starts_at_one_without_scanning() {
    let (path, store) = open_store("empty").await;

    let next = writer::next_id_for_test(&store).await.unwrap();
    assert_eq!(next, 1, "a fresh shard must start its ids at 1");

    // An empty table cannot prove anything about scanning cost, but the plan
    // shape must still be the O(1) probe rather than an aggregate scan.
    let opcodes = explain_opcodes(&store, writer::NEXT_ID_SQL).await;
    for opcode in SCAN_OPCODES {
        assert!(
            !opcodes.iter().any(|seen| seen == opcode),
            "id lookup regressed into a table scan: plan contains `{opcode}` -> {opcodes:?}"
        );
    }

    drop(store);
    let _ = std::fs::remove_file(&path);
}

/// The expression-wrapped forms are the shape that broke startup; keep them
/// documented as regressions so the reason for the current form stays visible.
#[tokio::test]
async fn expression_wrapped_max_id_is_the_regression_we_guard_against() {
    let (path, store) = open_store("regression").await;
    let connection = store.write().await;
    connection
        .execute_batch("INSERT INTO records (id, TimeMs, Model) VALUES (1, 1000, 'a');")
        .await
        .unwrap();
    drop(connection);

    let old_form = "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM records";
    let opcodes = explain_opcodes(&store, old_form).await;

    // Documented, not asserted-and-forgotten: if a future Turso release makes
    // the wrapped form indexable, this assertion is what tells us the guard in
    // the other tests is still meaningful.
    assert!(
        opcodes
            .iter()
            .any(|seen| seen == "Rewind" || seen == "Next"),
        "the wrapped-MAX form is expected to scan; plan was {opcodes:?}"
    );

    drop(store);
    let _ = std::fs::remove_file(&path);
}
