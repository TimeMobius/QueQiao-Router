//! 行解析与合并：SQL 行 → 内部聚合结构，跨分片/切片可加合并与分位近似。

use sqlx::sqlite::SqliteRow;
use sqlx::Row;
use std::collections::HashMap;

use super::params::{LATENCY_BUCKET_CAP_MS, LATENCY_BUCKET_MS};

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct Metrics {
    pub(super) requests: i64,
    pub(super) success: i64,
    pub(super) errors: i64,
    pub(super) prompt_tokens: i64,
    pub(super) completion_tokens: i64,
    pub(super) total_tokens: i64,
    pub(super) latency_sum: f64,
    pub(super) latency_count: i64,
    pub(super) latency_max: f64,
    pub(super) ttft_sum: f64,
    pub(super) ttft_count: i64,
}

impl Metrics {
    pub(super) fn add(&mut self, other: Metrics) {
        self.requests += other.requests;
        self.success += other.success;
        self.errors += other.errors;
        self.prompt_tokens += other.prompt_tokens;
        self.completion_tokens += other.completion_tokens;
        self.total_tokens += other.total_tokens;
        self.latency_sum += other.latency_sum;
        self.latency_count += other.latency_count;
        self.latency_max = self.latency_max.max(other.latency_max);
        self.ttft_sum += other.ttft_sum;
        self.ttft_count += other.ttft_count;
    }

    pub(super) fn avg_latency(&self) -> Option<f64> {
        (self.latency_count > 0).then(|| self.latency_sum / self.latency_count as f64)
    }

    pub(super) fn avg_ttft(&self) -> Option<f64> {
        (self.ttft_count > 0).then(|| self.ttft_sum / self.ttft_count as f64)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub(super) struct SummaryAgg {
    pub(super) metrics: Metrics,
    /// 各分片 DISTINCT 计数之和，仅在单分片时精确。
    pub(super) models: i64,
    pub(super) ips: i64,
}

pub(super) type DbErrorKey = (i64, String, String, String);
pub(super) type LogErrorKey = (
    String,
    Option<i64>,
    Option<String>,
    Option<String>,
    Option<String>,
);

pub(super) fn col_i64(row: &SqliteRow, name: &str) -> i64 {
    row.try_get::<Option<i64>, _>(name)
        .ok()
        .flatten()
        .unwrap_or(0)
}

pub(super) fn col_text(row: &SqliteRow, name: &str) -> Option<String> {
    row.try_get::<Option<String>, _>(name).ok().flatten()
}

pub(super) fn col_f64(row: &SqliteRow, name: &str) -> f64 {
    row.try_get::<Option<f64>, _>(name)
        .ok()
        .flatten()
        .unwrap_or(0.0)
}

pub(super) fn metrics_from_row(row: &SqliteRow) -> Metrics {
    Metrics {
        requests: col_i64(row, "requests"),
        success: col_i64(row, "success"),
        errors: col_i64(row, "errors"),
        prompt_tokens: col_i64(row, "promptTokens"),
        completion_tokens: col_i64(row, "completionTokens"),
        total_tokens: col_i64(row, "totalTokens"),
        latency_sum: col_f64(row, "latencySum"),
        latency_count: col_i64(row, "latencyCount"),
        latency_max: col_f64(row, "latencyMax"),
        ttft_sum: col_f64(row, "ttftSum"),
        ttft_count: col_i64(row, "ttftCount"),
    }
}

pub(super) fn summary_from_row(row: &SqliteRow) -> SummaryAgg {
    SummaryAgg {
        metrics: metrics_from_row(row),
        models: col_i64(row, "models"),
        ips: col_i64(row, "ips"),
    }
}

pub(super) fn merge_summaries(aggs: &[SummaryAgg]) -> SummaryAgg {
    let mut out = SummaryAgg::default();
    for a in aggs {
        out.metrics.add(a.metrics);
        out.models += a.models;
        out.ips += a.ips;
    }
    out
}

pub(super) fn merge_group_map(per_shard: Vec<Vec<(String, Metrics)>>) -> HashMap<String, Metrics> {
    let mut out: HashMap<String, Metrics> = HashMap::new();
    for shard in per_shard {
        for (name, m) in shard {
            out.entry(name).or_default().add(m);
        }
    }
    out
}

pub(super) fn merge_error_maps(maps: Vec<HashMap<DbErrorKey, i64>>) -> HashMap<DbErrorKey, i64> {
    let mut out: HashMap<DbErrorKey, i64> = HashMap::new();
    for m in maps {
        for (k, v) in m {
            *out.entry(k).or_insert(0) += v;
        }
    }
    out
}

/// 合并各分片的时延直方图（桶 → 计数求和），按桶升序返回。
pub(super) fn merge_histograms(rows_per_shard: &[Vec<SqliteRow>]) -> Vec<(i64, i64)> {
    let mut map: HashMap<i64, i64> = HashMap::new();
    for rows in rows_per_shard {
        for row in rows {
            *map.entry(col_i64(row, "bucket")).or_insert(0) += col_i64(row, "c");
        }
    }
    let mut buckets: Vec<(i64, i64)> = map.into_iter().collect();
    buckets.sort_by_key(|entry| entry.0);
    buckets
}

/// 由累积直方图求分位值，返回该分位所在桶的上界（毫秒）。
pub(super) fn percentile_ms(buckets: &[(i64, i64)], total: i64, p: f64) -> Option<f64> {
    if total <= 0 || buckets.is_empty() {
        return None;
    }
    let rank = ((p * total as f64).ceil() as i64).max(1);
    let mut acc = 0i64;
    for (bucket, count) in buckets {
        acc += count;
        if acc >= rank {
            let upper = (bucket + 1).saturating_mul(LATENCY_BUCKET_MS);
            return Some(upper.min(LATENCY_BUCKET_CAP_MS + LATENCY_BUCKET_MS) as f64);
        }
    }
    buckets
        .last()
        .map(|(bucket, _)| ((bucket + 1).saturating_mul(LATENCY_BUCKET_MS)) as f64)
}

pub(super) fn normalize_opt(value: Option<String>) -> Option<String> {
    value.and_then(|s| {
        let t = s.trim();
        if t.is_empty() || t == "-" {
            None
        } else {
            Some(t.to_string())
        }
    })
}

#[cfg(test)]
mod tests {
    use super::super::params::{AnalysisParams, DEFAULT_DIM_EXPR, GROUP_CAP, LATENCY_COL};
    use super::super::shards::{fetch_all_shards, fetch_sliced, ShardInput};
    use super::super::sql::{dims_sql, errors_db_sql, latency_hist_sql, summary_sql, trend_sql};
    use super::*;
    use crate::db::archive::ACTIVE_SHARD;
    use sqlx::SqlitePool;
    use std::time::UNIX_EPOCH;

    #[test]
    fn summary_merge_sums_counts_and_tokens_not_averages() {
        let a = SummaryAgg {
            metrics: Metrics {
                requests: 10,
                success: 5,
                errors: 2,
                prompt_tokens: 100,
                completion_tokens: 50,
                total_tokens: 150,
                latency_sum: 100.0,
                latency_count: 2,
                latency_max: 500.0,
                ..Default::default()
            },
            models: 2,
            ips: 3,
        };
        let b = SummaryAgg {
            metrics: Metrics {
                requests: 30,
                success: 27,
                errors: 1,
                prompt_tokens: 400,
                completion_tokens: 100,
                total_tokens: 500,
                latency_sum: 300.0,
                latency_count: 4,
                latency_max: 200.0,
                ..Default::default()
            },
            models: 4,
            ips: 5,
        };
        let merged = merge_summaries(&[a, b]);
        assert_eq!(merged.metrics.requests, 40);
        assert_eq!(merged.metrics.success, 32);
        assert_eq!(merged.metrics.errors, 3);
        assert_eq!(merged.metrics.prompt_tokens, 500);
        assert_eq!(merged.metrics.completion_tokens, 150);
        assert_eq!(merged.metrics.total_tokens, 650);
        // 时延：和与计数相加，最大值取 max 而非相加。
        assert_eq!(merged.metrics.latency_sum, 400.0);
        assert_eq!(merged.metrics.latency_count, 6);
        assert_eq!(merged.metrics.latency_max, 500.0);
        assert_eq!(merged.metrics.avg_latency(), Some(400.0 / 6.0));
        // DISTINCT 计数是各分片之和（上界语义）。
        assert_eq!(merged.models, 6);
        assert_eq!(merged.ips, 8);
        // 若误用平均，success 会是 16；确认不是平均值。
        assert_ne!(merged.metrics.success, (5 + 27) / 2);
    }

    #[test]
    fn latency_percentile_uses_bucket_upper_bound() {
        let buckets = vec![(0i64, 99i64), (10i64, 1i64)];
        assert_eq!(percentile_ms(&buckets, 100, 0.50), Some(50.0));
        assert_eq!(percentile_ms(&buckets, 100, 0.95), Some(50.0));
        assert_eq!(percentile_ms(&buckets, 100, 0.99), Some(50.0));
        assert_eq!(percentile_ms(&buckets, 100, 1.0), Some(550.0));
        assert_eq!(percentile_ms(&buckets, 0, 0.95), None);
        assert_eq!(percentile_ms(&[], 10, 0.95), None);
    }

    #[test]
    fn error_group_merge_accumulates_same_key_across_shards() {
        let mut first: HashMap<DbErrorKey, i64> = HashMap::new();
        first.insert(
            (422, "m1".to_string(), "b1".to_string(), "bad".to_string()),
            3,
        );
        first.insert(
            (500, "m2".to_string(), "b2".to_string(), "boom".to_string()),
            1,
        );
        let mut second: HashMap<DbErrorKey, i64> = HashMap::new();
        second.insert(
            (422, "m1".to_string(), "b1".to_string(), "bad".to_string()),
            7,
        );
        let merged = merge_error_maps(vec![first, second]);
        assert_eq!(
            merged.get(&(422, "m1".to_string(), "b1".to_string(), "bad".to_string())),
            Some(&10)
        );
        assert_eq!(
            merged.get(&(500, "m2".to_string(), "b2".to_string(), "boom".to_string())),
            Some(&1)
        );
        assert_eq!(merged.len(), 2);
    }

    async fn temp_db(tag: &str) -> (std::path::PathBuf, SqlitePool) {
        let nanos = std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let path = std::env::temp_dir().join(format!(
            "qq_analysis_{}_{}_{}.db",
            std::process::id(),
            tag,
            nanos
        ));
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(&path)
            .create_if_missing(true);
        let pool = SqlitePool::connect_with(options).await.unwrap();
        sqlx::query(
            "CREATE TABLE records (\
                id INTEGER PRIMARY KEY, TimeMs INTEGER, Type TEXT, Model TEXT, Status INTEGER, \
                Backend TEXT, IP TEXT, ClientName TEXT, UserAgent TEXT, ApiKey TEXT, \
                PromptTokens INTEGER, CompletionTokens INTEGER, TotalTokens INTEGER, Error TEXT, \
                LatencyMs REAL, TtftMs REAL)",
        )
        .execute(&pool)
        .await
        .unwrap();
        (path, pool)
    }

    #[tokio::test]
    async fn sql_queries_execute_and_aggregate_on_temp_db() {
        let (path, pool) = temp_db("agg").await;
        sqlx::query(
            "INSERT INTO records (id, TimeMs, Type, Model, Status, Backend, IP, ClientName, \
             UserAgent, ApiKey, PromptTokens, CompletionTokens, TotalTokens, Error, LatencyMs, TtftMs) \
             VALUES (1, 3600000, 'chat', 'm1', 200, 'b1', '1.1.1.1', 'c1', 'ua', 'k1', 10, 5, 15, NULL, 120.0, 40.0), \
                    (2, 7200000, 'chat', 'm1', 500, 'b1', '1.1.1.1', 'c1', 'ua', 'k1', 20, 8, 28, 'boom', 800.0, 30.0)",
        )
        .execute(&pool)
        .await
        .unwrap();

        let shards = [ShardInput {
            id: ACTIVE_SHARD.to_string(),
            pool: pool.clone(),
        }];
        let params = AnalysisParams::default();
        let (from, to) = (0_i64, 10_000_000_i64);

        let summary_rows = fetch_sliced(&shards, &params, from, to, summary_sql)
            .await
            .unwrap();
        let merged = merge_summaries(
            &summary_rows
                .iter()
                .flat_map(|rows| rows.iter().map(summary_from_row))
                .collect::<Vec<_>>(),
        );
        assert_eq!(merged.metrics.requests, 2);
        assert_eq!(merged.metrics.success, 1);
        assert_eq!(merged.metrics.errors, 1);
        assert_eq!(merged.metrics.total_tokens, 43);
        assert_eq!(merged.metrics.latency_count, 2);
        assert_eq!(merged.metrics.latency_sum, 920.0);
        assert_eq!(merged.metrics.latency_max, 800.0);
        assert_eq!(merged.metrics.avg_latency(), Some(460.0));
        assert_eq!(merged.models, 1);
        assert_eq!(merged.ips, 1);

        let hist_rows =
            fetch_all_shards(&shards, &latency_hist_sql(" WHERE 1=1", LATENCY_COL), &[])
                .await
                .unwrap();
        let buckets = merge_histograms(&hist_rows);
        assert_eq!(buckets.iter().map(|(_, count)| count).sum::<i64>(), 2);
        assert_eq!(percentile_ms(&buckets, 2, 0.99), Some(850.0));

        let dim_rows = fetch_sliced(&shards, &params, from, to, |w| {
            dims_sql(w, DEFAULT_DIM_EXPR, GROUP_CAP + 1)
        })
        .await
        .unwrap();
        let dim = metrics_from_row(&dim_rows[0][0]);
        assert_eq!(dim.requests, 2);
        assert_eq!(dim.total_tokens, 43);

        let err_rows = fetch_sliced(&shards, &params, from, to, errors_db_sql)
            .await
            .unwrap();
        let row = &err_rows[0][0];
        assert_eq!(col_i64(row, "status"), 500);
        assert_eq!(col_text(row, "model").as_deref(), Some("m1"));
        assert_eq!(col_text(row, "error").as_deref(), Some("boom"));
        assert_eq!(col_i64(row, "c"), 1);

        let trend_rows = fetch_sliced(&shards, &params, from, to, |w| trend_sql("hour", w))
            .await
            .unwrap();
        let trend_total: i64 = trend_rows[0]
            .iter()
            .map(|r| col_i64(r, "totalTokens"))
            .sum();
        assert_eq!(trend_total, 43);
        assert!(!trend_rows[0].is_empty());

        drop(pool);
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn requests_equal_success_plus_all_errors() {
        let (path, pool) = temp_db("counted").await;
        sqlx::query(
            "INSERT INTO records (id, TimeMs, Model, Status, Error, LatencyMs) VALUES \
             (1, 1000, 'm1', 200, NULL, 10.0), \
             (2, 2000, 'm1', 500, 'boom', 20.0), \
             (3, 3000, 'm1', 499, 'user cancel', 30.0), \
             (4, 4000, 'm1', 422, 'bad', 40.0), \
             (5, 5000, '',    500, 'nomodel', 50.0)",
        )
        .execute(&pool)
        .await
        .unwrap();
        let shards = [ShardInput {
            id: ACTIVE_SHARD.to_string(),
            pool: pool.clone(),
        }];
        let rows = fetch_all_shards(&shards, &summary_sql(" WHERE 1=1"), &[])
            .await
            .unwrap();
        let merged = merge_summaries(
            &rows
                .iter()
                .flat_map(|r| r.iter().map(summary_from_row))
                .collect::<Vec<_>>(),
        );
        // 请求总量 = 成功(1) + 所有带状态码错误(4)
        assert_eq!(merged.metrics.requests, 5);
        assert_eq!(merged.metrics.success, 1);
        assert_eq!(merged.metrics.errors, 4);
        drop(pool);
        let _ = std::fs::remove_file(&path);
    }
}
