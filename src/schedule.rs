//! 定时任务的时间计算（跟随进程本地时区）。

use chrono::{Days, Local, TimeZone};

/// 解析 `HH:MM`（本地时间）；`off`、空串或非法格式返回 `None`。
pub fn parse_hhmm(value: &str) -> Option<(u32, u32)> {
    let value = value.trim();
    if value.is_empty() || value.eq_ignore_ascii_case("off") {
        return None;
    }
    let (hour, minute) = value.split_once(':')?;
    let hour: u32 = hour.trim().parse().ok()?;
    let minute: u32 = minute.trim().parse().ok()?;
    (hour < 24 && minute < 60).then_some((hour, minute))
}

/// 严格晚于 `now_ms` 的下一个本地 `hour:minute`。
///
/// 跨夏令时的时间缺口会被跳过，保证返回值始终是一个真实存在的本地时刻。
pub fn next_daily_after(now_ms: i64, hour: u32, minute: u32) -> Option<i64> {
    let now = Local.timestamp_millis_opt(now_ms).single()?;
    for offset in 0..3 {
        let day = now.date_naive().checked_add_days(Days::new(offset))?;
        let candidate = day.and_hms_opt(hour, minute, 0)?;
        let Some(local) = Local.from_local_datetime(&candidate).earliest() else {
            continue;
        };
        if local.timestamp_millis() > now_ms {
            return Some(local.timestamp_millis());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_hhmm_and_off() {
        assert_eq!(parse_hhmm("04:00"), Some((4, 0)));
        assert_eq!(parse_hhmm(" 23:59 "), Some((23, 59)));
        assert_eq!(parse_hhmm("off"), None);
        assert_eq!(parse_hhmm("OFF"), None);
        assert_eq!(parse_hhmm(""), None);
        assert_eq!(parse_hhmm("24:00"), None);
        assert_eq!(parse_hhmm("4"), None);
        assert_eq!(parse_hhmm("4:60"), None);
    }

    #[test]
    fn next_daily_is_in_the_future_and_matches_local_clock() {
        let now = Local
            .with_ymd_and_hms(2026, 10, 31, 12, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        let next = next_daily_after(now, 4, 0).unwrap();
        assert!(next > now);
        assert!(next - now <= 24 * 3_600_000);
        let local = Local.timestamp_millis_opt(next).single().unwrap();
        use chrono::Timelike;
        assert_eq!((local.hour(), local.minute()), (4, 0));
    }

    #[test]
    fn next_daily_rolls_to_tomorrow_when_now_is_exact() {
        let now = Local
            .with_ymd_and_hms(2026, 10, 31, 4, 0, 0)
            .single()
            .unwrap()
            .timestamp_millis();
        let next = next_daily_after(now, 4, 0).unwrap();
        assert_eq!(next - now, 24 * 3_600_000);
    }
}
