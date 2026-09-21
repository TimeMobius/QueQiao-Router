//! SQL（标识符全部来自白名单/常量）。

use super::params::{
    bucket_expr, COUNT_CAP, DISTINCT_CAP, LATENCY_BUCKET_CAP_MS, LATENCY_BUCKET_MS,
};

pub(super) const SQL_SUCCESS: &str =
    "COALESCE(SUM(CASE WHEN Status >= 200 AND Status < 400 THEN 1 ELSE 0 END),0)";
// 所有带状态码的错误（含 499/422/无模型名）都计入错误数。
pub(super) const SQL_ERRORS: &str = "COALESCE(SUM(CASE WHEN Status >= 400 THEN 1 ELSE 0 END),0)";
// 请求总量 = 成功 + 所有带状态码错误（Status>=200），保证 成功率+错误率=100%。
pub(super) const SQL_REQUESTS_COUNTED: &str = "COUNT(CASE WHEN Status >= 200 THEN 1 END)";
pub(super) const SQL_PROMPT: &str = "COALESCE(SUM(COALESCE(PromptTokens,0)),0)";
pub(super) const SQL_COMPLETION: &str = "COALESCE(SUM(COALESCE(CompletionTokens,0)),0)";
pub(super) const SQL_TOTAL: &str = "COALESCE(SUM(COALESCE(TotalTokens,0)),0)";
pub(super) const SQL_LATENCY_SUM: &str = "COALESCE(SUM(LatencyMs),0) AS latencySum";
pub(super) const SQL_LATENCY_COUNT: &str = "COUNT(LatencyMs) AS latencyCount";
pub(super) const SQL_LATENCY_MAX: &str = "COALESCE(MAX(LatencyMs),0) AS latencyMax";
pub(super) const SQL_TTFT_SUM: &str = "COALESCE(SUM(TtftMs),0) AS ttftSum";
pub(super) const SQL_TTFT_COUNT: &str = "COUNT(TtftMs) AS ttftCount";

pub(super) fn summary_sql(where_sql: &str) -> String {
    format!(
        "SELECT {SQL_REQUESTS_COUNTED} AS requests, {SQL_SUCCESS} AS success, {SQL_ERRORS} AS errors, \
         {SQL_PROMPT} AS promptTokens, {SQL_COMPLETION} AS completionTokens, \
         {SQL_TOTAL} AS totalTokens, COUNT(DISTINCT Model) AS models, \
         COUNT(DISTINCT IP) AS ips, {SQL_LATENCY_SUM}, {SQL_LATENCY_COUNT}, \
         {SQL_LATENCY_MAX}, {SQL_TTFT_SUM}, {SQL_TTFT_COUNT} FROM records{where_sql}"
    )
}

pub(super) fn trend_sql(interval: &str, where_sql: &str) -> String {
    format!(
        "SELECT {} AS label, {SQL_SUCCESS} AS success, {SQL_ERRORS} AS errors, \
         {SQL_PROMPT} AS promptTokens, {SQL_COMPLETION} AS completionTokens, \
         {SQL_TOTAL} AS totalTokens, {SQL_LATENCY_SUM}, {SQL_LATENCY_COUNT}, \
         {SQL_TTFT_SUM}, {SQL_TTFT_COUNT} FROM records{where_sql} GROUP BY 1 ORDER BY 1 ASC",
        bucket_expr(interval)
    )
}

pub(super) fn dims_sql(where_sql: &str, expr: &str, cap: usize) -> String {
    format!(
        "SELECT {expr} AS name, COUNT(*) AS requests, {SQL_SUCCESS} AS success, \
         {SQL_ERRORS} AS errors, {SQL_PROMPT} AS promptTokens, \
         {SQL_COMPLETION} AS completionTokens, {SQL_TOTAL} AS totalTokens, \
         {SQL_LATENCY_SUM}, {SQL_LATENCY_COUNT}, {SQL_LATENCY_MAX}, \
         {SQL_TTFT_SUM}, {SQL_TTFT_COUNT} \
         FROM records{where_sql} GROUP BY 1 LIMIT {cap}"
    )
}

/// 时延直方图（固定宽度分桶）用于分位近似；`column` 仅取白名单常量。
pub(super) fn latency_hist_sql(where_sql: &str, column: &str) -> String {
    format!(
        "SELECT CAST(MIN(COALESCE({column},0), {cap}) / {width} AS INTEGER) AS bucket, \
         COUNT(*) AS c FROM records{where_sql} AND {column} IS NOT NULL \
         GROUP BY bucket ORDER BY bucket",
        cap = LATENCY_BUCKET_CAP_MS,
        width = LATENCY_BUCKET_MS
    )
}

pub(super) fn errors_db_sql(where_sql: &str) -> String {
    format!(
        "SELECT Status AS status, COALESCE(NULLIF(Model,''),'Unknown') AS model, \
         COALESCE(Backend,'') AS backend, COALESCE(NULLIF(Error,''),'-') AS error, \
         COUNT(*) AS c FROM records{where_sql} AND Status >= 400 GROUP BY 1,2,3,4 \
         ORDER BY c DESC LIMIT {}",
        COUNT_CAP + 1
    )
}

pub(super) fn distinct_sql(where_sql: &str, column: &str) -> String {
    format!(
        "SELECT DISTINCT {column} AS v FROM records{where_sql} \
         AND {column} IS NOT NULL AND {column} <> '' LIMIT {DISTINCT_CAP}"
    )
}
