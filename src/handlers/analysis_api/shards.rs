//! 分片查询（并发受限）：组装分片列表、按时间切片并发扫描。

use once_cell::sync::Lazy;
use sqlx::sqlite::SqliteRow;
use sqlx::SqlitePool;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::db::archive::ACTIVE_SHARD;
use crate::db::records_query;
use crate::state::app_state::AppState;

use super::aggregation::col_text;
use super::params::{to_list_params, AnalysisParams, DAY_MS, DISTINCT_CAP};

const SHARD_CONCURRENCY: usize = 8;

/// 限制同时打到 SQLite 的分片查询数，避免跨月/多归档时打开过多连接。
static SHARD_SEM: Lazy<Semaphore> = Lazy::new(|| Semaphore::new(SHARD_CONCURRENCY));

/// 一次多分片查询的输入分片（只读归档或 active）。
#[derive(Clone)]
pub(super) struct ShardInput {
    pub(super) id: String,
    pub(super) pool: SqlitePool,
}

async fn fetch(
    pool: &SqlitePool,
    sql: &str,
    binds: &[records_query::Bind],
) -> Result<Vec<SqliteRow>, sqlx::Error> {
    records_query::bind_all(sqlx::query(sql), binds)
        .fetch_all(pool)
        .await
}

/// 等待一组已 spawn 的查询任务并收集结果。
///
/// 必须用 `tokio::spawn`：`join_all` 下 sqlx 的 SQLite 查询会在同一任务内串行执行
/// （实测单请求 CPU ≈100%），spawn 后各查询才会落到独立的连接执行线程上。
async fn join_queries(
    tasks: Vec<tokio::task::JoinHandle<Result<Vec<SqliteRow>, sqlx::Error>>>,
) -> Result<Vec<Vec<SqliteRow>>, sqlx::Error> {
    let mut out = Vec::with_capacity(tasks.len());
    for task in tasks {
        let rows = task
            .await
            .map_err(|e| sqlx::Error::Protocol(format!("query task join failed: {e}")))??;
        out.push(rows);
    }
    Ok(out)
}

/// 不切片的整段查询：用于单行汇总或需要跨分片并集的查询。
///
/// 这类查询切片不减少总扫描量，反而成倍放大查询开销，因此只在趋势/维度这类
/// 大结果集上切片。
pub(super) async fn fetch_all_shards(
    shards: &[ShardInput],
    sql: &str,
    binds: &[records_query::Bind],
) -> Result<Vec<Vec<SqliteRow>>, sqlx::Error> {
    let tasks = shards
        .iter()
        .map(|s| {
            let pool = s.pool.clone();
            let sql = sql.to_string();
            let binds = binds.to_vec();
            tokio::spawn(async move {
                let _permit = SHARD_SEM.acquire().await;
                fetch(&pool, &sql, &binds).await
            })
        })
        .collect();
    join_queries(tasks).await
}

/// 将 `[from, to]` 切成至多 4 段连续、无缝、互不重叠的闭区间。
///
/// 闭区间保证每行恰好落入一段（`start_{i+1} == end_i + 1`），因此分段查询的
/// 结果与整段查询完全等价，可直接交给既有的可加合并逻辑。
fn range_slices(from: i64, to: i64) -> Vec<(i64, i64)> {
    let span = to.saturating_sub(from);
    if to <= from || span < DAY_MS {
        return vec![(from, to)];
    }
    let splits = (span / DAY_MS + i64::from(span % DAY_MS != 0)).clamp(1, 4);
    let step = span / splits;
    if splits <= 1 || step <= 0 {
        return vec![(from, to)];
    }
    let mut out = Vec::with_capacity(splits as usize);
    for i in 0..splits {
        let start = if i == 0 {
            from
        } else {
            from.saturating_add(i.saturating_mul(step))
        };
        let end = if i == splits - 1 {
            to
        } else {
            from.saturating_add((i + 1).saturating_mul(step)) - 1
        };
        out.push((start, end));
    }
    out
}

/// 对每个 (分片, 时间片) 组合并发执行一次聚合查询。
///
/// 分片内按时间切片并行扫描，每个组合产出一个 `Vec<SqliteRow>`；既有的
/// `merge_summaries`/`merge_group_map`/`merge_error_maps` 对全部内层向量求和，
/// 因此分片与切片可统一处理，无需改动合并逻辑。
pub(super) async fn fetch_sliced<F>(
    shards: &[ShardInput],
    p: &AnalysisParams,
    from: i64,
    to: i64,
    make_sql: F,
) -> Result<Vec<Vec<SqliteRow>>, sqlx::Error>
where
    F: Fn(&str) -> String,
{
    let slices = range_slices(from, to);
    let mut tasks = Vec::with_capacity(shards.len().saturating_mul(slices.len()));
    for shard in shards {
        for &(slice_from, slice_to) in &slices {
            let pool = shard.pool.clone();
            let (where_sql, binds) =
                records_query::build_filters(&to_list_params(p, slice_from, slice_to));
            let sql = make_sql(&where_sql);
            tasks.push(tokio::spawn(async move {
                let _permit = SHARD_SEM.acquire().await;
                fetch(&pool, &sql, &binds).await
            }));
        }
    }
    join_queries(tasks).await
}

/// 跨分片精确基数：各分片取 DISTINCT 值后在 Rust 侧并集去重，避免
/// 「各分片 COUNT(DISTINCT) 相加」重复计数。`column` 仅接受白名单字面量。
/// 不切片：切片的 LIMIT 语义会破坏去重并放大开销。
pub(super) async fn distinct_count_across_shards(
    shards: &[ShardInput],
    where_sql: &str,
    binds: &[records_query::Bind],
    column: &str,
) -> Result<(usize, bool), sqlx::Error> {
    let rows =
        fetch_all_shards(shards, &super::sql::distinct_sql(where_sql, column), binds).await?;
    let mut set: HashSet<String> = HashSet::new();
    for shard_rows in &rows {
        for row in shard_rows {
            if let Some(value) = col_text(row, "v") {
                set.insert(value);
            }
        }
    }
    Ok((set.len(), set.len() >= DISTINCT_CAP as usize))
}

/// 组装分片列表：active 恒在首位，命中候选归档追加在后。
pub(super) async fn collect_shards(
    app_state: &Arc<AppState>,
    from: i64,
    to: i64,
) -> Vec<ShardInput> {
    let candidates = app_state
        .archive_registry
        .candidates(Some(from), Some(to))
        .await;
    let active = app_state.query_pool.read().await.clone();
    let mut shards = Vec::with_capacity(candidates.len() + 1);
    shards.push(ShardInput {
        id: ACTIVE_SHARD.to_string(),
        pool: active,
    });
    for c in &candidates {
        shards.push(ShardInput {
            id: c.id.clone(),
            pool: c.pool.clone(),
        });
    }
    shards
}

/// 检测归档目录中未被注册的 `record_YYYYMM*` 文件（遗留/不支持分片）。
pub(super) fn skipped_archive_warning(skipped: usize) -> Option<String> {
    (skipped > 0).then(|| format!("{skipped} legacy/unsupported archive shard(s) skipped"))
}

#[cfg(test)]
mod tests {
    use super::super::params::HOUR_MS;
    use super::*;

    #[test]
    fn range_slices_cover_endpoints_contiguously_without_overlap() {
        let from = 1_000_000_i64;
        let to = from + 30 * DAY_MS;
        let slices = range_slices(from, to);
        assert_eq!(slices.len(), 4);
        assert_eq!(slices.first().unwrap().0, from);
        assert_eq!(slices.last().unwrap().1, to);
        for pair in slices.windows(2) {
            assert_eq!(pair[1].0, pair[0].1 + 1, "slices must be gap-free");
        }
        for &(start, end) in &slices {
            assert!(start <= end, "slice must be non-empty");
        }
        let covered: i64 = slices.iter().map(|(s, e)| e - s + 1).sum();
        assert_eq!(covered, to - from + 1);
    }

    #[test]
    fn range_slices_collapse_short_and_reversed_ranges() {
        assert_eq!(range_slices(0, HOUR_MS), vec![(0, HOUR_MS)]);
        assert_eq!(range_slices(0, DAY_MS - 1), vec![(0, DAY_MS - 1)]);
        assert_eq!(range_slices(5, 5), vec![(5, 5)]);
        assert_eq!(range_slices(10, 5), vec![(10, 5)]);
        assert_eq!(range_slices(0, 2 * DAY_MS).len(), 2);
    }
}
