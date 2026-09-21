//! 数据库月份轮转模块。
//!
//! 包含纯函数时间辅助（`yyyymm` / `next_month_boundary_ms` / `yyyymm_from_ms` /
//! `parse_local_time_ms` / `should_rotate`）与异步轮转编排
//! （`current_data_yyyymm` / `init_rotation_state` / `rotate_locked` /
//! `check_and_rotate` / `rotate_if_needed`）。
//!
//! 轮转由「进程时钟 + 数据月份」驱动，不依赖文件系统元数据（mtime/ctime），
//! 因此在 NFS 等元数据不可靠的文件系统上也能正确工作；
//! 同时写路径只做一次原子加载 + 比较，稳态零文件系统系统调用。

use chrono::{DateTime, Datelike, Local, NaiveDate, TimeZone};
use std::fs;
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{error, info, warn};

use super::{init_db_pool, init_query_pool, resolve_db_path};
use crate::state::app_state::AppState;

/// 编码本地时间为 YYYYMM，例如 2026-09 -> 202609。
pub fn yyyymm(dt: DateTime<Local>) -> i32 {
    dt.year() * 100 + dt.month() as i32
}

/// active 月份的下一个月 1 号 00:00 本地时间的 epoch 毫秒。
///
/// 正确处理 12 月 -> 次年 1 月的跨年；使用 `earliest()` 处理 DST，
/// 保证边界落在本地时间月初零点。
pub fn next_month_boundary_ms(active_yyyymm: i32) -> i64 {
    let year = active_yyyymm / 100;
    let month = active_yyyymm % 100;
    let (next_year, next_month) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };

    NaiveDate::from_ymd_opt(next_year, next_month as u32, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .and_then(|naive| Local.from_local_datetime(&naive).earliest())
        .map(|dt| dt.timestamp_millis())
        // 合法 YYYYMM 永远能构造出边界；此处兜底避免 panic。
        .unwrap_or(i64::MAX)
}

/// 由 epoch 毫秒得到本地年月。
pub fn yyyymm_from_ms(ms: i64) -> Option<i32> {
    Local.timestamp_millis_opt(ms).single().map(yyyymm)
}

/// 解析本地时间文本（写入时 `%Y-%m-%d %H:%M:%S%.6f`）为真实 epoch 毫秒，
/// 应用当时的历史 UTC 偏移（含夏令时）；无法解析返回 None。
pub fn parse_local_time_ms(s: &str) -> Option<i64> {
    let s = s.trim();
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
        .ok()
        .and_then(|naive| Local.from_local_datetime(&naive).earliest())
        .map(|dt| dt.timestamp_millis())
}

/// 数据月落后于当前月时需要封存（严格大于，容忍时钟回拨）。
pub fn should_rotate(active_yyyymm: i32, now: DateTime<Local>) -> bool {
    yyyymm(now) > active_yyyymm
}

/// 当前 record.db 中 MAX(TimeMs) 所属的本地月份；空表/无值返回 None。
async fn current_data_yyyymm(app_state: &Arc<AppState>) -> Option<i32> {
    let pool = app_state.db_pool.read().await;
    let max: Option<i64> = sqlx::query_scalar("SELECT MAX(TimeMs) FROM records")
        .fetch_one(&*pool)
        .await
        .ok()
        .flatten();
    max.and_then(yyyymm_from_ms)
}

/// 启动时确定内存中的 active 月份与边界（数据驱动，不 stat 文件）。
pub async fn init_rotation_state(app_state: &Arc<AppState>) {
    let now_month = yyyymm(Local::now());
    let active = current_data_yyyymm(app_state).await.unwrap_or(now_month);
    app_state.active_yyyymm.store(active, Ordering::Release);
    app_state
        .next_month_boundary_ms
        .store(next_month_boundary_ms(active), Ordering::Release);
}

/// 轮转动作；调用方必须已持有 db_rotation_lock。
async fn rotate_locked(app_state: &Arc<AppState>, seal_yyyymm: i32) {
    let db_path = resolve_db_path();
    if !db_path.exists() {
        return;
    }

    let archive_dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    let mut archive_path = archive_dir.join(format!("record_{:06}.db", seal_yyyymm));

    info!(
        "Database rotation needed. Archiving {} to {}",
        db_path.display(),
        archive_path.display()
    );

    // 同名归档已存在时追加 unix 秒后缀，避免覆盖历史归档。
    if archive_path.exists() {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_default();
        archive_path = archive_dir.join(format!("record_{:06}_{}.db", seal_yyyymm, timestamp));
        warn!(
            "Archive file already exists. Renaming to {}",
            archive_path.display()
        );
    }

    // 获取写锁以阻塞新的请求写入，安全完成轮转。
    let mut pool_guard = app_state.db_pool.write().await;
    // 与写池同序获取查询池写锁；关闭会等待在途查询归还连接后优雅排空。
    let mut query_guard = app_state.query_pool.write().await;

    // 关闭当前连接池以释放文件锁。
    pool_guard.close().await;
    query_guard.close().await;

    match fs::rename(&db_path, &archive_path) {
        Ok(_) => info!("Database archived successfully."),
        Err(e) => {
            error!("Failed to archive database: {}", e);
        }
    }

    info!("Re-initializing database pool after rotation.");
    let config = app_state.config_manager.get_config().await;
    match init_db_pool(&config).await {
        Ok(new_pool) => {
            *pool_guard = new_pool;
            let now_month = yyyymm(Local::now());
            app_state.active_yyyymm.store(now_month, Ordering::Release);
            app_state
                .next_month_boundary_ms
                .store(next_month_boundary_ms(now_month), Ordering::Release);
            info!("Database pool re-initialized successfully.");
        }
        Err(e) => {
            error!(
                "Failed to re-initialize database pool after rotation: {}",
                e
            );
        }
    }

    match init_query_pool().await {
        Ok(new_query_pool) => {
            *query_guard = new_query_pool;
            info!("Query pool re-initialized successfully.");
        }
        Err(e) => {
            error!("Failed to re-initialize query pool after rotation: {}", e);
        }
    }
}

/// 后台数据驱动检查。
pub async fn check_and_rotate(app_state: &Arc<AppState>) {
    let _lock = app_state.db_rotation_lock.lock().await;
    let now_month = yyyymm(Local::now());
    if let Some(data_month) = current_data_yyyymm(app_state).await {
        if data_month < now_month {
            rotate_locked(app_state, data_month).await;
            return;
        }
        if data_month != app_state.active_yyyymm.load(Ordering::Acquire) {
            app_state.active_yyyymm.store(data_month, Ordering::Release);
            app_state
                .next_month_boundary_ms
                .store(next_month_boundary_ms(data_month), Ordering::Release);
        }
    }
}

/// 写路径零成本边界检查；仅在跨月那一刻才做一次 DB 查询与轮转。
pub async fn rotate_if_needed(app_state: &Arc<AppState>, time_ms: i64) {
    if time_ms < app_state.next_month_boundary_ms.load(Ordering::Acquire) {
        return; // 快路径：一次原子加载 + 比较
    }
    let _lock = app_state.db_rotation_lock.lock().await;
    if time_ms < app_state.next_month_boundary_ms.load(Ordering::Acquire) {
        return; // 获取锁后二次检查，避免重复轮转
    }
    let now_month = yyyymm(Local::now());
    let seal = match current_data_yyyymm(app_state).await {
        Some(d) if d < now_month => d,
        _ => app_state.active_yyyymm.load(Ordering::Acquire),
    };
    rotate_locked(app_state, seal).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    fn local(y: i32, m: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Local> {
        Local.with_ymd_and_hms(y, m, d, h, mi, s).single().unwrap()
    }

    #[test]
    fn test_yyyymm_encoding() {
        assert_eq!(yyyymm(local(2026, 9, 18, 12, 0, 0)), 202609);
        assert_eq!(yyyymm(local(2026, 1, 1, 0, 0, 0)), 202601);
        assert_eq!(yyyymm(local(2026, 12, 31, 23, 59, 59)), 202612);
    }

    #[test]
    fn test_next_month_boundary_is_first_day_midnight() {
        let ms = next_month_boundary_ms(202609);
        let dt = Local.timestamp_millis_opt(ms).single().unwrap();
        assert_eq!(dt.year(), 2026);
        assert_eq!(dt.month(), 10);
        assert_eq!(dt.day(), 1);
        assert_eq!(dt.hour(), 0);
        assert_eq!(dt.minute(), 0);
        assert_eq!(dt.second(), 0);
        assert_eq!(dt.timestamp_subsec_millis(), 0);
    }

    #[test]
    fn test_dec_to_jan_rollover() {
        let ms = next_month_boundary_ms(202612);
        let dt = Local.timestamp_millis_opt(ms).single().unwrap();
        assert_eq!(dt.year(), 2027);
        assert_eq!(dt.month(), 1);
        assert_eq!(dt.day(), 1);
        assert_eq!(dt.hour(), 0);
        assert_eq!(dt.minute(), 0);
        assert_eq!(dt.second(), 0);
    }

    #[test]
    fn test_should_rotate_true_when_month_advances() {
        assert!(should_rotate(202608, local(2026, 9, 1, 0, 0, 0)));
    }

    #[test]
    fn test_should_rotate_false_same_month() {
        assert!(!should_rotate(202609, local(2026, 9, 30, 23, 59, 59)));
    }

    #[test]
    fn test_should_rotate_false_on_clock_rewind() {
        assert!(!should_rotate(202609, local(2026, 8, 1, 0, 0, 0)));
    }

    #[test]
    fn test_yyyymm_from_ms_roundtrip() {
        let dt = local(2026, 9, 18, 12, 0, 0);
        assert_eq!(yyyymm_from_ms(dt.timestamp_millis()), Some(202609));
    }

    #[test]
    fn test_parse_local_time_ms_matches_local_construction() {
        let expected = local(2026, 3, 15, 10, 0, 0).timestamp_millis();
        assert_eq!(
            parse_local_time_ms("2026-03-15 10:00:00.000000"),
            Some(expected)
        );
        assert_eq!(parse_local_time_ms("2026-03-15 10:00:00"), Some(expected));
        assert_eq!(
            parse_local_time_ms("2026-03-15 10:00:00.123456"),
            Some(expected + 123)
        );
    }

    #[test]
    fn test_parse_local_time_ms_invalid_is_none() {
        assert_eq!(parse_local_time_ms(""), None);
        assert_eq!(parse_local_time_ms("not-a-time"), None);
        assert_eq!(parse_local_time_ms("2026-13-45 99:99:99"), None);
    }
}
