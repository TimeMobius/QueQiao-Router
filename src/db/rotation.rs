//! 数据库月份轮转的纯函数辅助模块。
//!
//! 轮转由「进程时钟 + 数据月份」驱动，不依赖文件系统元数据（mtime/ctime），
//! 因此在 NFS 等元数据不可靠的文件系统上也能正确工作；
//! 同时写路径只做一次原子加载 + 比较，稳态零文件系统系统调用。

use chrono::{DateTime, Datelike, Local, NaiveDate, TimeZone};

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

/// 数据月落后于当前月时需要封存（严格大于，容忍时钟回拨）。
pub fn should_rotate(active_yyyymm: i32, now: DateTime<Local>) -> bool {
    yyyymm(now) > active_yyyymm
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
}
