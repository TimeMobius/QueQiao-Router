//! 归档汇总快路径：无筛选的分析请求直接读派生汇总库，避免重复全表扫描。
//!
//! 只有同时满足以下条件才启用：无任何筛选参数、`interval` 不是 hour、
//! `dimension` 不是 hour、区间内每个归档都有指纹有效的汇总。
//! 任一条不满足即返回 `None`，调用方走原有扫描路径，语义完全一致。

use std::collections::{HashMap, HashSet};

use crate::db::aggregate::{self, Acc, Slice};
use crate::db::engine::DbError;

use super::aggregation::{
    col_i64, col_text, merge_summaries, metrics_from_row, summary_from_row, Metrics, SummaryAgg,
};
use super::optimized::fetch_optimized;
use super::params::{AnalysisParams, DISTINCT_CAP, GROUP_CAP, LATENCY_COL, TTFT_COL};
use super::shards::{fetch_sliced, ShardInput};
use super::sql::{dims_sql, distinct_sql, latency_hist_sql, summary_sql, trend_sql};

#[derive(Default)]
pub(super) struct Data {
    pub(super) summaries: Vec<SummaryAgg>,
    pub(super) trend: Vec<Vec<(String, Metrics)>>,
    pub(super) dims: Vec<Vec<(String, Metrics)>>,
    pub(super) latency_hist: Vec<HashMap<i64, i64>>,
    pub(super) ttft_hist: Vec<HashMap<i64, i64>>,
    pub(super) distinct_models: i64,
    pub(super) distinct_ips: i64,
    pub(super) distinct_capped: bool,
    pub(super) warnings: Vec<String>,
}

pub(super) fn enabled() -> bool {
    !std::env::var("ANALYSIS_ROLLUP")
        .map(|value| value.eq_ignore_ascii_case("off"))
        .unwrap_or(false)
}

pub(super) fn supports(params: &AnalysisParams) -> bool {
    params.type_.as_deref().is_none_or(str::is_empty)
        && params.model.as_deref().is_none_or(str::is_empty)
        && params.status.is_none()
        && params.backend.as_deref().is_none_or(str::is_empty)
        && params.ip.as_deref().is_none_or(str::is_empty)
        && params.client.as_deref().is_none_or(str::is_empty)
        && params.apikey.as_deref().is_none_or(str::is_empty)
        && params.errors.as_deref().is_none_or(str::is_empty)
}

struct ShardScan {
    summary: SummaryAgg,
    trend: Vec<(String, Metrics)>,
    dims: Vec<(String, Metrics)>,
    latency_hist: HashMap<i64, i64>,
    ttft_hist: HashMap<i64, i64>,
}

pub(super) async fn analyse(
    shards: &[ShardInput],
    params: &AnalysisParams,
    from: i64,
    to: i64,
    interval: &str,
    dim: &str,
    dim_expr: &str,
) -> Result<Option<Data>, DbError> {
    if !enabled() || !supports(params) || interval == "hour" || dim == "hour" {
        return Ok(None);
    }
    if !shards.iter().any(|shard| !shard.is_active) {
        return Ok(None);
    }

    let audit_path = crate::db::resolve_db_path();
    let mut data = Data::default();
    let mut models: HashSet<String> = HashSet::new();
    let mut ips: HashSet<String> = HashSet::new();

    for shard in shards {
        if shard.is_active {
            let scan = scan_shard(shard, params, from, to, interval, dim_expr).await?;
            models.extend(distinct_values(shard, from, to, "Model").await?);
            ips.extend(distinct_values(shard, from, to, "IP").await?);
            push_scan(&mut data, scan);
            continue;
        }

        let Some((first_full, full_end)) = full_day_bounds(from, to) else {
            return Ok(None);
        };
        let Some(archive_path) = shard.path.clone() else {
            return Ok(None);
        };
        if full_end <= first_full {
            let scan = scan_shard(shard, params, from, to, interval, dim_expr).await?;
            models.extend(distinct_values(shard, from, to, "Model").await?);
            ips.extend(distinct_values(shard, from, to, "IP").await?);
            push_scan(&mut data, scan);
            continue;
        }

        let slice = aggregate::read_slice(
            &audit_path,
            &shard.id,
            &archive_path,
            shard.min_ms,
            shard.max_ms,
            first_full,
            full_end,
        )
        .await;
        let Some(slice) = slice else {
            spawn_build(audit_path.clone(), shard);
            return Ok(None);
        };

        let mut per_shard = slice_to_scan(&slice, interval, dim);
        models.extend(
            slice
                .distinct
                .get("model")
                .into_iter()
                .flatten()
                .filter(|value| !value.is_empty())
                .cloned(),
        );
        ips.extend(
            slice
                .distinct
                .get("ip")
                .into_iter()
                .flatten()
                .filter(|value| !value.is_empty())
                .cloned(),
        );

        for (correction_from, correction_to) in correction_windows(from, to, first_full, full_end) {
            let scan = scan_shard(
                shard,
                params,
                correction_from,
                correction_to,
                interval,
                dim_expr,
            )
            .await?;
            models.extend(distinct_values(shard, correction_from, correction_to, "Model").await?);
            ips.extend(distinct_values(shard, correction_from, correction_to, "IP").await?);
            merge_scan(&mut per_shard, scan);
        }

        data.summaries.push(per_shard.summary);
        data.trend.push(per_shard.trend);
        data.dims.push(per_shard.dims);
        data.latency_hist.push(per_shard.latency_hist);
        data.ttft_hist.push(per_shard.ttft_hist);
    }

    data.distinct_models = models.len() as i64;
    data.distinct_ips = ips.len() as i64;
    data.distinct_capped =
        models.len() >= DISTINCT_CAP as usize || ips.len() >= DISTINCT_CAP as usize;
    Ok(Some(data))
}

fn spawn_build(audit_path: std::path::PathBuf, shard: &ShardInput) {
    let Some(path) = shard.path.clone() else {
        return;
    };
    let shard_id = shard.id.clone();
    let store = shard.store.clone();
    let min_ms = shard.min_ms;
    let max_ms = shard.max_ms;
    tokio::spawn(async move {
        if let Err(error) =
            aggregate::ensure_for_shard(audit_path, shard_id, path, store, min_ms, max_ms).await
        {
            tracing::warn!(%error, "archive rollup unavailable; analysis uses scan path");
        }
    });
}

fn push_scan(data: &mut Data, scan: ShardScan) {
    data.summaries.push(scan.summary);
    data.trend.push(scan.trend);
    data.dims.push(scan.dims);
    data.latency_hist.push(scan.latency_hist);
    data.ttft_hist.push(scan.ttft_hist);
}

fn merge_scan(target: &mut ShardScan, extra: ShardScan) {
    target.summary.metrics.add(extra.summary.metrics);
    target.trend = merge_groups(std::mem::take(&mut target.trend), extra.trend);
    target.dims = merge_groups(std::mem::take(&mut target.dims), extra.dims);
    merge_map(&mut target.latency_hist, extra.latency_hist);
    merge_map(&mut target.ttft_hist, extra.ttft_hist);
}

fn merge_groups(
    base: Vec<(String, Metrics)>,
    extra: Vec<(String, Metrics)>,
) -> Vec<(String, Metrics)> {
    if extra.is_empty() {
        return base;
    }
    let mut map: HashMap<String, Metrics> = base.into_iter().collect();
    for (key, value) in extra {
        map.entry(key).or_default().add(value);
    }
    map.into_iter().collect()
}

fn merge_map(target: &mut HashMap<i64, i64>, extra: HashMap<i64, i64>) {
    for (bucket, count) in extra {
        *target.entry(bucket).or_insert(0) += count;
    }
}

async fn scan_shard(
    shard: &ShardInput,
    params: &AnalysisParams,
    from: i64,
    to: i64,
    interval: &str,
    dim_expr: &str,
) -> Result<ShardScan, DbError> {
    let single = std::slice::from_ref(shard);
    let summary_rows = fetch_optimized(single, params, from, to, summary_sql).await?;
    let summaries: Vec<SummaryAgg> = summary_rows
        .iter()
        .flat_map(|rows| rows.iter().map(summary_from_row))
        .collect();

    let trend_rows = fetch_sliced(single, params, from, to, |where_sql| {
        trend_sql(interval, where_sql)
    })
    .await?;
    let mut trend_map: HashMap<String, Metrics> = HashMap::new();
    for row in trend_rows.iter().flatten() {
        let label = col_text(row, "label").unwrap_or_default();
        trend_map
            .entry(label)
            .or_default()
            .add(metrics_from_row(row));
    }

    let dim_rows = fetch_sliced(single, params, from, to, |where_sql| {
        dims_sql(where_sql, dim_expr, GROUP_CAP + 1)
    })
    .await?;
    let mut dim_map: HashMap<String, Metrics> = HashMap::new();
    for row in dim_rows.iter().flatten() {
        let name = col_text(row, "name").unwrap_or_else(|| "Unknown".to_string());
        dim_map.entry(name).or_default().add(metrics_from_row(row));
    }

    let mut latency_hist = HashMap::new();
    for row in fetch_optimized(single, params, from, to, |where_sql| {
        latency_hist_sql(where_sql, LATENCY_COL)
    })
    .await?
    .iter()
    .flatten()
    {
        *latency_hist.entry(col_i64(row, "bucket")).or_insert(0) += col_i64(row, "c");
    }
    let mut ttft_hist = HashMap::new();
    for row in fetch_optimized(single, params, from, to, |where_sql| {
        latency_hist_sql(where_sql, TTFT_COL)
    })
    .await?
    .iter()
    .flatten()
    {
        *ttft_hist.entry(col_i64(row, "bucket")).or_insert(0) += col_i64(row, "c");
    }

    Ok(ShardScan {
        summary: merge_summaries(&summaries),
        trend: trend_map.into_iter().collect(),
        dims: dim_map.into_iter().collect(),
        latency_hist,
        ttft_hist,
    })
}

async fn distinct_values(
    shard: &ShardInput,
    from: i64,
    to: i64,
    column: &str,
) -> Result<HashSet<String>, DbError> {
    let single = std::slice::from_ref(shard);
    let rows = fetch_optimized(single, &AnalysisParams::default(), from, to, |where_sql| {
        distinct_sql(where_sql, column)
    })
    .await?;
    let mut values = HashSet::new();
    for row in rows.iter().flatten() {
        if let Some(value) = col_text(row, "v") {
            values.insert(value);
        }
    }
    Ok(values)
}

fn metrics_from_acc(acc: &Acc) -> Metrics {
    Metrics {
        requests: acc.counted,
        success: acc.success,
        errors: acc.errors,
        prompt_tokens: acc.prompt_tokens,
        completion_tokens: acc.completion_tokens,
        total_tokens: acc.total_tokens,
        latency_sum: acc.latency_sum,
        latency_count: acc.latency_count,
        latency_max: acc.latency_max,
        ttft_sum: acc.ttft_sum,
        ttft_count: acc.ttft_count,
    }
}

fn dim_metrics_from_acc(acc: &Acc) -> Metrics {
    Metrics {
        requests: acc.rows_all,
        latency_max: acc.latency_max,
        ..metrics_from_acc(acc)
    }
}

fn trend_metrics_from_acc(acc: &Acc) -> Metrics {
    Metrics {
        requests: 0,
        latency_max: 0.0,
        ..metrics_from_acc(acc)
    }
}

fn slice_to_scan(slice: &Slice, interval: &str, dim: &str) -> ShardScan {
    let mut summary_acc = Acc::default();
    let mut trend_map: HashMap<String, Metrics> = HashMap::new();
    for day in &slice.days {
        summary_acc.add(&day.acc);
        let Some(label) = aggregate::local_day_label(day.day_ms, interval == "month") else {
            continue;
        };
        trend_map
            .entry(label)
            .or_default()
            .add(trend_metrics_from_acc(&day.acc));
    }
    let dim_map = slice
        .dims
        .get(dim)
        .map(|values| {
            values
                .iter()
                .map(|(name, acc)| (name.clone(), dim_metrics_from_acc(acc)))
                .collect::<HashMap<_, _>>()
        })
        .unwrap_or_default();

    ShardScan {
        summary: SummaryAgg {
            metrics: metrics_from_acc(&summary_acc),
            models: slice
                .distinct
                .get("model")
                .map(|set| set.len() as i64)
                .unwrap_or(0),
            ips: slice
                .distinct
                .get("ip")
                .map(|set| set.len() as i64)
                .unwrap_or(0),
        },
        trend: trend_map.into_iter().collect(),
        dims: dim_map.into_iter().collect(),
        latency_hist: slice.latency_hist.clone(),
        ttft_hist: slice.ttft_hist.clone(),
    }
}

/// 完整自然日区间 `[first_full, full_end)`；无完整自然日时返回退化区间。
fn full_day_bounds(from: i64, to: i64) -> Option<(i64, i64)> {
    let from_day = aggregate::local_day_ms(from)?;
    let first_full = if from == from_day {
        from_day
    } else {
        aggregate::next_local_day(from_day)?
    };
    let to_day = aggregate::local_day_ms(to)?;
    let to_day_end = aggregate::next_local_day(to_day)?;
    let full_end = if to + 1 >= to_day_end {
        to_day_end
    } else {
        to_day
    };
    Some((first_full, full_end))
}

fn correction_windows(from: i64, to: i64, first_full: i64, full_end: i64) -> Vec<(i64, i64)> {
    let mut windows = Vec::new();
    if from < first_full {
        windows.push((from, first_full - 1));
    }
    if full_end <= to {
        windows.push((full_end, to));
    }
    windows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::engine::Value;
    use crate::db::store::AuditStore;
    use crate::handlers::analysis_api::params::DEFAULT_DIM_EXPR;
    use std::sync::Arc;
    use std::time::Duration;

    fn temp_dir(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("qq-rollup-{tag}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    struct Fixture {
        dir: std::path::PathBuf,
        path: std::path::PathBuf,
        store: Arc<AuditStore>,
        shard: ShardInput,
        days: Vec<i64>,
        from: i64,
        to: i64,
    }

    async fn fixture(tag: &str, records: usize) -> Fixture {
        let dir = temp_dir(tag);
        let path = dir.join("record_202608.db");
        let store = AuditStore::open(&path, false, Duration::from_secs(5))
            .await
            .unwrap();
        let base = aggregate::local_day_ms(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
                - 40 * 86_400_000,
        )
        .unwrap();
        let days = vec![base, aggregate::next_local_day(base).unwrap()];
        let models = ["gpt-4", "", "claude", "gpt-4"];
        let ips = ["10.0.0.1", "10.0.0.2", ""];
        let mut min_ms = i64::MAX;
        let mut max_ms = i64::MIN;
        for index in 0..records {
            let day = days[index % days.len()];
            let time_ms = day + 3_600_000 + (index as i64) * 1_000;
            min_ms = min_ms.min(time_ms);
            max_ms = max_ms.max(time_ms);
            let status = match index % 5 {
                0 => Value::Integer(200),
                1 => Value::Integer(500),
                2 => Value::Null,
                3 => Value::Integer(404),
                _ => Value::Integer(201),
            };
            let latency = if index % 3 == 0 {
                Value::Real(120.0 + index as f64)
            } else {
                Value::Null
            };
            let ttft = if index % 4 == 0 {
                Value::Real(30.5 + index as f64)
            } else {
                Value::Null
            };
            store
                .execute(
                    "INSERT INTO records (Time, TimeMs, Model, ApiKey, IP, Type, Backend, ClientName, \
                     Status, PromptTokens, CompletionTokens, TotalTokens, LatencyMs, TtftMs, SearchText) \
                     VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    vec![
                        Value::Text("2026-08-10 00:00:00.000000".to_string()),
                        Value::Integer(time_ms),
                        Value::Text(models[index % models.len()].to_string()),
                        Value::Text(if index % 2 == 0 { "sk-a" } else { "" }.to_string()),
                        Value::Text(ips[index % ips.len()].to_string()),
                        Value::Text(if index % 3 == 0 { "chat.completions" } else { "embeddings" }.to_string()),
                        Value::Text(if index % 2 == 0 { "b1" } else { "b2" }.to_string()),
                        Value::Text(if index % 2 == 0 { "cli-a" } else { "" }.to_string()),
                        status,
                        Value::Integer(10 + index as i64),
                        Value::Integer(20 + index as i64),
                        Value::Integer(30 + index as i64),
                        latency,
                        ttft,
                        Value::Text("text".to_string()),
                    ],
                )
                .await
                .unwrap();
        }
        let shard = ShardInput {
            id: "record_202608".to_string(),
            store: store.clone(),
            path: Some(path.clone()),
            min_ms: Some(min_ms),
            max_ms: Some(max_ms),
            is_active: false,
            active_month: None,
        };
        Fixture {
            dir,
            path,
            store,
            shard,
            days,
            from: min_ms,
            to: max_ms,
        }
    }

    impl Fixture {
        async fn build(&self) {
            aggregate::ensure_for_shard(
                self.dir.join("record.db"),
                self.shard.id.clone(),
                self.path.clone(),
                self.store.clone(),
                self.shard.min_ms,
                self.shard.max_ms,
            )
            .await
            .unwrap();
        }
    }

    fn assert_metrics(label: &str, left: &Metrics, right: &Metrics) {
        assert_eq!(left.requests, right.requests, "{label} requests");
        assert_eq!(left.success, right.success, "{label} success");
        assert_eq!(left.errors, right.errors, "{label} errors");
        assert_eq!(left.prompt_tokens, right.prompt_tokens, "{label} prompt");
        assert_eq!(
            left.completion_tokens, right.completion_tokens,
            "{label} completion"
        );
        assert_eq!(left.total_tokens, right.total_tokens, "{label} total");
        assert!(
            (left.latency_sum - right.latency_sum).abs() < 1e-9,
            "{label} latency_sum {} vs {}",
            left.latency_sum,
            right.latency_sum
        );
        assert_eq!(
            left.latency_count, right.latency_count,
            "{label} latency_count"
        );
        assert!(
            (left.latency_max - right.latency_max).abs() < 1e-9,
            "{label} latency_max"
        );
        assert!(
            (left.ttft_sum - right.ttft_sum).abs() < 1e-9,
            "{label} ttft_sum"
        );
        assert_eq!(left.ttft_count, right.ttft_count, "{label} ttft_count");
    }

    fn assert_group_eq(label: &str, left: &[(String, Metrics)], right: &[(String, Metrics)]) {
        let left: HashMap<_, _> = left.iter().cloned().collect();
        let right: HashMap<_, _> = right.iter().cloned().collect();
        assert_eq!(
            left.keys().collect::<std::collections::BTreeSet<_>>(),
            right.keys().collect::<std::collections::BTreeSet<_>>(),
            "{label} keys"
        );
        for (key, value) in &left {
            assert_metrics(&format!("{label}[{key}]"), value, &right[key]);
        }
    }

    async fn distinct_values_raw(
        shard: &ShardInput,
        from: i64,
        to: i64,
        column: &str,
    ) -> HashSet<String> {
        let sql = |where_sql: &str| {
            format!(
                "SELECT DISTINCT {column} AS v FROM records{where_sql} AND {column} IS NOT NULL LIMIT 10000"
            )
        };
        let rows = fetch_optimized(
            std::slice::from_ref(shard),
            &AnalysisParams::default(),
            from,
            to,
            sql,
        )
        .await
        .unwrap();
        let mut values = HashSet::new();
        for row in rows.iter().flatten() {
            if let Some(value) = col_text(row, "v") {
                values.insert(value);
            }
        }
        values
    }

    async fn compare(fixture: &Fixture, from: i64, to: i64, interval: &str, dim: &str) {
        let params = AnalysisParams::default();
        let dim_expr = super::super::params::dimension_expr(dim).unwrap_or(DEFAULT_DIM_EXPR);
        let scan = scan_shard(&fixture.shard, &params, from, to, interval, dim_expr)
            .await
            .unwrap();
        let (first_full, full_end) = full_day_bounds(from, to).unwrap();
        let slice = aggregate::read_slice(
            &fixture.dir.join("record.db"),
            &fixture.shard.id,
            &fixture.path,
            fixture.shard.min_ms,
            fixture.shard.max_ms,
            first_full,
            full_end,
        )
        .await
        .unwrap();
        let mut rolled = slice_to_scan(&slice, interval, dim);
        let mut models: HashSet<String> = slice
            .distinct
            .get("model")
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let mut ips: HashSet<String> = slice
            .distinct
            .get("ip")
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let full_days_done = models.len();
        for (correction_from, correction_to) in correction_windows(from, to, first_full, full_end) {
            let extra = scan_shard(
                &fixture.shard,
                &params,
                correction_from,
                correction_to,
                interval,
                dim_expr,
            )
            .await
            .unwrap();
            models.extend(
                distinct_values_raw(&fixture.shard, correction_from, correction_to, "Model").await,
            );
            ips.extend(
                distinct_values_raw(&fixture.shard, correction_from, correction_to, "IP").await,
            );
            merge_scan(&mut rolled, extra);
        }
        let _ = full_days_done;
        rolled.summary.models = models.len() as i64;
        rolled.summary.ips = ips.len() as i64;
        assert_metrics("summary", &scan.summary.metrics, &rolled.summary.metrics);
        assert_eq!(scan.summary.models, rolled.summary.models, "summary models");
        assert_eq!(scan.summary.ips, rolled.summary.ips, "summary ips");
        assert_group_eq("trend", &scan.trend, &rolled.trend);
        assert_group_eq("dims", &scan.dims, &rolled.dims);
        assert_eq!(scan.latency_hist, rolled.latency_hist, "latency hist");
        assert_eq!(scan.ttft_hist, rolled.ttft_hist, "ttft hist");
    }

    #[tokio::test]
    async fn matches_scan_for_aligned_and_partial_ranges() {
        let fixture = fixture("parity", 60).await;
        fixture.build().await;
        let params = AnalysisParams::default();

        compare(&fixture, fixture.from, fixture.to, "day", "model").await;

        let scan_all = scan_shard(
            &fixture.shard,
            &params,
            fixture.from,
            fixture.to,
            "day",
            DEFAULT_DIM_EXPR,
        )
        .await
        .unwrap();

        // 每个维度都必须与扫描路径一致。
        for dim in aggregate::DIMS {
            compare(&fixture, fixture.from, fixture.to, "day", dim).await;
        }

        // 首尾都不是本地零点，触发两侧补扫。
        let from = fixture.days[0] + 3_600_000 + 2_000;
        let to = fixture.days[1] + 3_600_000 + 20_000;
        compare(&fixture, from, to, "day", "model").await;

        // 月粒度合并日桶。
        compare(&fixture, fixture.days[0], fixture.to, "month", "status").await;

        assert!(scan_all.summary.metrics.requests > 0);
        std::fs::remove_dir_all(&fixture.dir).unwrap();
    }

    #[tokio::test]
    async fn rebuilds_when_archive_fingerprint_changes() {
        let fixture = fixture("rebuild", 20).await;
        fixture.build().await;
        let before = aggregate::read_slice(
            &fixture.dir.join("record.db"),
            &fixture.shard.id,
            &fixture.path,
            fixture.shard.min_ms,
            fixture.shard.max_ms,
            fixture.days[0],
            aggregate::next_local_day(fixture.days[1]).unwrap(),
        )
        .await
        .unwrap();
        assert!(!before.days.is_empty());

        // 修改文件内容（模拟重建）后指纹失效。
        std::thread::sleep(std::time::Duration::from_millis(10));
        fixture
            .store
            .execute(
                "INSERT INTO records (Time, TimeMs, Model, Status) VALUES (?,?,?,?)",
                vec![
                    Value::Text("2026-08-10 00:00:00.000000".to_string()),
                    Value::Integer(fixture.days[0] + 7_200_000),
                    Value::Text("new-model".to_string()),
                    Value::Integer(200),
                ],
            )
            .await
            .unwrap();
        fixture.store.checkpoint_truncate().await.unwrap();

        let stale = aggregate::read_slice(
            &fixture.dir.join("record.db"),
            &fixture.shard.id,
            &fixture.path,
            fixture.shard.min_ms,
            fixture.shard.max_ms,
            fixture.days[0],
            aggregate::next_local_day(fixture.days[1]).unwrap(),
        )
        .await;
        assert!(stale.is_none(), "stale rollup must not validate");

        fixture.build().await;
        let rebuilt = aggregate::read_slice(
            &fixture.dir.join("record.db"),
            &fixture.shard.id,
            &fixture.path,
            fixture.shard.min_ms,
            fixture.shard.max_ms,
            fixture.days[0],
            aggregate::next_local_day(fixture.days[1]).unwrap(),
        )
        .await;
        assert!(rebuilt.is_some());
        std::fs::remove_dir_all(&fixture.dir).unwrap();
    }

    #[tokio::test]
    async fn missing_rollup_returns_none() {
        let fixture = fixture("missing", 5).await;
        let slice = aggregate::read_slice(
            &fixture.dir.join("record.db"),
            &fixture.shard.id,
            &fixture.path,
            fixture.shard.min_ms,
            fixture.shard.max_ms,
            fixture.days[0],
            aggregate::next_local_day(fixture.days[1]).unwrap(),
        )
        .await;
        assert!(slice.is_none());
        std::fs::remove_dir_all(&fixture.dir).unwrap();
    }

    #[test]
    fn rejects_filtered_requests() {
        assert!(supports(&AnalysisParams::default()));
        let mut params = AnalysisParams {
            model: Some("gpt".to_string()),
            ..Default::default()
        };
        assert!(!supports(&params));
        params.model = Some(String::new());
        assert!(supports(&params));
        params.errors = Some("1".to_string());
        assert!(!supports(&params));
    }
}
