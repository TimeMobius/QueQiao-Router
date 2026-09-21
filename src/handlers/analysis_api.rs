//! 只读分析 API：聚合历史审计数据（active 库 + 月度归档）与 `logs/error.*.log`。
//!
//! - `GET /dashboard/api/analysis`：汇总 / 趋势 / 维度分页 / Top。
//! - `GET /dashboard/api/analysis/errors`：错误分组（数据库或日志文件）。
//!
//! 设计约束：零 schema 变更；只读查询；分片查询并发受 `Semaphore` 限制；结果带
//! 15s 进程内缓存；所有标识符来自白名单，所有值均参数绑定。绝不返回请求体、
//! 响应体、原始 API Key 或原始日志行。
//!
//! 实现拆分为下列子模块，本文件仅为薄门面：
//! - `params`：查询参数结构与解析/白名单
//! - `sql`：聚合 SQL 模板
//! - `shards`：分片查询（并发受限）与时间切片
//! - `aggregation`：行解析与跨分片可加合并
//! - `cache`：进程内结果缓存与统一错误响应
//! - `log_scan`：错误日志文件扫描
//! - `analysis_endpoint` / `errors_endpoint`：两个 HTTP 端点

mod aggregation;
mod analysis_endpoint;
mod cache;
mod errors_endpoint;
mod log_scan;
mod params;
mod shards;
mod sql;

pub use analysis_endpoint::analysis;
pub use errors_endpoint::analysis_errors;
pub use params::AnalysisParams;
