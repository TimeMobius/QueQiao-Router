//! 按【本地时区日期】轮转的日志文件写入器。
//!
//! `tracing_appender::rolling::RollingFileAppender` 在非 test 构建中硬编码使用
//! `OffsetDateTime::now_utc()` 生成文件名，导致容器（`TZ=Asia/Shanghai`）中
//! 本地日期 `2026-10-10 00:00–07:59 +0800` 的日志被写入 `error.2026-10-09.log`。
//! 本模块用 `chrono::Local` 计算当前本地日期，命名与轮转全部跟随进程本地时区。
//!
//! 文件名格式与 tracing-appender 的每日轮转保持一致：`<prefix>.<YYYY-MM-DD>.<suffix>`，
//! 保留策略也与之对齐（最多保留 `max_files` 个匹配的日期文件，含最新文件）。

use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use chrono::{DateTime, FixedOffset, NaiveDate, Offset};

/// 按本地日期每日轮转的文件写入器。
///
/// 该类型实现了 [`std::io::Write`]，会被 [`tracing_appender::non_blocking`] 的
/// 后台工作线程独占持有，因此无需内部可变性：`write` 使用 `&mut self`，
/// 只有工作线程会调用它。类型满足 `Send + 'static`。
pub struct LocalRollingFileAppender {
    dir: PathBuf,
    prefix: String,
    suffix: String,
    max_files: usize,
    /// 注入的时钟，返回带固定偏移的本地时间；测试可注入确定性时钟。
    clock: Box<dyn Fn() -> DateTime<FixedOffset> + Send + Sync>,
    /// 当前打开文件对应的本地日期。
    current_date: NaiveDate,
    /// 当前打开的日志文件（无缓冲，写入即落盘）。
    file: File,
}

impl LocalRollingFileAppender {
    /// 使用默认时钟（进程本地时区）创建 appender。
    ///
    /// 会创建 `dir` 目录，按今天的本地日期打开 `dir/<prefix>.<YYYY-MM-DD>.<suffix>`
    /// （create + append），并立即执行一次保留策略清理。
    pub fn new(
        dir: impl AsRef<Path>,
        prefix: &str,
        suffix: &str,
        max_files: usize,
    ) -> io::Result<Self> {
        Self::with_clock(dir, prefix, suffix, max_files, || {
            let now = chrono::Local::now();
            DateTime::from_naive_utc_and_offset(now.naive_utc(), now.offset().fix())
        })
    }

    /// 使用注入时钟创建 appender。
    ///
    /// 该构造函数主要用于测试，以便确定性地控制"当前本地日期"；生产代码应使用
    /// [`LocalRollingFileAppender::new`]。
    pub fn with_clock<F>(
        dir: impl AsRef<Path>,
        prefix: &str,
        suffix: &str,
        max_files: usize,
        clock: F,
    ) -> io::Result<Self>
    where
        F: Fn() -> DateTime<FixedOffset> + Send + Sync + 'static,
    {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let date = clock().date_naive();
        let file = Self::open_for(&dir, prefix, suffix, date)?;

        let appender = Self {
            dir,
            prefix: prefix.to_string(),
            suffix: suffix.to_string(),
            max_files,
            clock: Box::new(clock),
            current_date: date,
            file,
        };
        appender.prune();
        Ok(appender)
    }

    /// 按 `<prefix>.<YYYY-MM-DD>.<suffix>` 拼装文件名。
    fn file_name(prefix: &str, date: NaiveDate, suffix: &str) -> String {
        format!("{}.{}.{}", prefix, date.format("%Y-%m-%d"), suffix)
    }

    /// 以 create + append 模式打开指定日期的日志文件。
    fn open_for(dir: &Path, prefix: &str, suffix: &str, date: NaiveDate) -> io::Result<File> {
        let path = dir.join(Self::file_name(prefix, date, suffix));
        OpenOptions::new().create(true).append(true).open(path)
    }

    /// 从文件名解析本地日期；仅接受与 `prefix + "." + <date> + "." + suffix`
    /// 完全匹配的名称，其余一律返回 `None`。
    fn parse_date(name: &str, prefix: &str, suffix: &str) -> Option<NaiveDate> {
        let prefix_dot = format!("{}.", prefix);
        let suffix_dot = format!(".{}", suffix);
        let stem = name.strip_prefix(&prefix_dot)?.strip_suffix(&suffix_dot)?;
        NaiveDate::parse_from_str(stem, "%Y-%m-%d").ok()
    }

    /// 清理旧日志，与 tracing-appender 0.2.5 的每日保留语义对齐：
    /// 只统计名称完全匹配 `prefix.<date>.suffix` 的【普通文件】，若数量超过
    /// `max_files`，按日期升序删除最旧的 `count - max_files` 个。
    /// 目录、符号链接、以及其他任何不匹配的文件都不会被考虑或删除。
    fn prune(&self) {
        let entries = match fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(_) => return,
        };

        let mut dates: Vec<NaiveDate> = Vec::new();
        for entry in entries.flatten() {
            let metadata = match entry.metadata() {
                Ok(metadata) => metadata,
                Err(_) => continue,
            };
            // appender 只创建文件，绝不删除目录或符号链接。
            if !metadata.is_file() {
                continue;
            }
            let file_name = entry.file_name();
            let file_name = match file_name.to_str() {
                Some(name) => name,
                None => continue,
            };
            if let Some(date) = Self::parse_date(file_name, &self.prefix, &self.suffix) {
                dates.push(date);
            }
        }

        if dates.len() <= self.max_files {
            return;
        }

        dates.sort();
        let remove_count = dates.len() - self.max_files;
        for date in dates.iter().take(remove_count) {
            let path = self
                .dir
                .join(Self::file_name(&self.prefix, *date, &self.suffix));
            // 单个删除失败不影响其余清理。
            let _ = fs::remove_file(path);
        }
    }
}

impl Write for LocalRollingFileAppender {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let date = (self.clock)().date_naive();
        if date != self.current_date {
            // 跨本地日期：打开新文件并替换当前文件（旧 File 在此被 drop 关闭），
            // 更新日期后再次执行保留策略。
            self.file = Self::open_for(&self.dir, &self.prefix, &self.suffix, date)?;
            self.current_date = date;
            self.prune();
        }
        // 无缓冲：写入即落盘，无需 flush。
        self.file.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static COUNTER: AtomicUsize = AtomicUsize::new(0);

    /// 为每个测试创建唯一的临时目录。
    fn unique_temp_dir(tag: &str) -> PathBuf {
        let seq = COUNTER.fetch_add(1, Ordering::SeqCst);
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!(
            "lrsa_test_{}_{}_{}_{}",
            tag,
            std::process::id(),
            seq,
            nanos
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn cleanup(dir: &Path) {
        let _ = fs::remove_dir_all(dir);
    }

    /// 构造带固定偏移（东八区）的本地时间点。
    fn at_plus8(date: NaiveDate, hour: u32, min: u32, sec: u32) -> DateTime<FixedOffset> {
        let offset = FixedOffset::east_opt(8 * 3600).expect("offset");
        let naive = date.and_hms_opt(hour, min, sec).expect("valid time");
        offset
            .from_local_datetime(&naive)
            .single()
            .expect("unambiguous local time")
    }

    fn read_to_string(path: &Path) -> String {
        fs::read_to_string(path).expect("read log file")
    }

    #[test]
    fn construction_creates_file_named_by_injected_local_date() {
        let dir = unique_temp_dir("construct");
        let date = NaiveDate::from_ymd_opt(2026, 10, 9).unwrap();
        let clock_date = date;
        let mut appender =
            LocalRollingFileAppender::with_clock(&dir, "info", "log", 10, move || {
                at_plus8(clock_date, 12, 0, 0)
            })
            .expect("create appender");

        appender.write_all(b"hello-local").expect("write");
        drop(appender);

        let path = dir.join("info.2026-10-09.log");
        assert!(
            path.exists(),
            "file named by injected local date must exist"
        );
        assert_eq!(read_to_string(&path), "hello-local");

        cleanup(&dir);
    }

    #[test]
    fn multiple_writes_same_date_keep_single_file_in_order() {
        let dir = unique_temp_dir("samedate");
        let date = NaiveDate::from_ymd_opt(2026, 10, 9).unwrap();
        let mut appender =
            LocalRollingFileAppender::with_clock(&dir, "info", "log", 10, move || {
                at_plus8(date, 12, 0, 0)
            })
            .expect("create appender");

        appender.write_all(b"one\n").expect("write one");
        appender.write_all(b"two\n").expect("write two");
        appender.write_all(b"three\n").expect("write three");
        drop(appender);

        let entries: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(entries.len(), 1, "only one file for one local date");
        assert_eq!(entries[0], "info.2026-10-09.log");
        assert_eq!(
            read_to_string(&dir.join("info.2026-10-09.log")),
            "one\ntwo\nthree\n"
        );

        cleanup(&dir);
    }

    #[test]
    fn rollover_at_local_midnight_creates_new_file() {
        let dir = unique_temp_dir("rollover");
        let day1 = NaiveDate::from_ymd_opt(2026, 10, 9).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2026, 10, 10).unwrap();

        // 用共享原子量在两次写入之间切换时钟日期。
        let advanced = std::sync::Arc::new(AtomicUsize::new(0));
        let clock_flag = advanced.clone();
        let mut appender =
            LocalRollingFileAppender::with_clock(&dir, "info", "log", 10, move || {
                if clock_flag.load(Ordering::SeqCst) == 0 {
                    at_plus8(day1, 23, 59, 59)
                } else {
                    at_plus8(day2, 0, 0, 0)
                }
            })
            .expect("create appender");

        appender
            .write_all(b"before-midnight\n")
            .expect("write before");
        advanced.store(1, Ordering::SeqCst);
        appender
            .write_all(b"after-midnight\n")
            .expect("write after");
        drop(appender);

        let old_path = dir.join("info.2026-10-09.log");
        let new_path = dir.join("info.2026-10-10.log");
        assert!(old_path.exists(), "old local-date file must exist");
        assert!(new_path.exists(), "new local-date file must exist");
        assert_eq!(read_to_string(&old_path), "before-midnight\n");
        assert_eq!(read_to_string(&new_path), "after-midnight\n");

        cleanup(&dir);
    }

    #[test]
    fn retention_keeps_max_files_and_drops_oldest() {
        let dir = unique_temp_dir("retention");
        let dates = [
            NaiveDate::from_ymd_opt(2026, 10, 1).unwrap(),
            NaiveDate::from_ymd_opt(2026, 10, 2).unwrap(),
            NaiveDate::from_ymd_opt(2026, 10, 3).unwrap(),
            NaiveDate::from_ymd_opt(2026, 10, 4).unwrap(),
        ];

        let index = std::sync::Arc::new(AtomicUsize::new(0));
        let index_flag = index.clone();
        let mut appender =
            LocalRollingFileAppender::with_clock(&dir, "info", "log", 3, move || {
                let i = index_flag.load(Ordering::SeqCst);
                at_plus8(dates[i], 12, 0, 0)
            })
            .expect("create appender");

        appender.write_all(b"d1\n").expect("write d1");
        for (i, _) in dates.iter().enumerate().skip(1) {
            index.store(i, Ordering::SeqCst);
            appender
                .write_all(format!("d{}\n", i + 1).as_bytes())
                .expect("write day");
        }
        drop(appender);

        let mut names: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "info.2026-10-02.log".to_string(),
                "info.2026-10-03.log".to_string(),
                "info.2026-10-04.log".to_string(),
            ],
            "oldest date file must be pruned, newest three retained"
        );
        assert!(!dir.join("info.2026-10-01.log").exists());

        cleanup(&dir);
    }

    #[test]
    fn prune_ignores_unrelated_files() {
        let dir = unique_temp_dir("ignore");
        let day1 = NaiveDate::from_ymd_opt(2026, 10, 9).unwrap();
        let day2 = NaiveDate::from_ymd_opt(2026, 10, 10).unwrap();

        // 预置不相关文件与不同前缀/非法日期的文件。
        fs::write(dir.join("notes.txt"), b"x").unwrap();
        fs::write(dir.join("info.bak"), b"x").unwrap();
        fs::write(dir.join("error.2026-10-05.log"), b"x").unwrap();
        fs::write(dir.join("info.notadate.log"), b"x").unwrap();

        let advanced = std::sync::Arc::new(AtomicUsize::new(0));
        let clock_flag = advanced.clone();
        let mut appender =
            LocalRollingFileAppender::with_clock(&dir, "info", "log", 1, move || {
                if clock_flag.load(Ordering::SeqCst) == 0 {
                    at_plus8(day1, 12, 0, 0)
                } else {
                    at_plus8(day2, 12, 0, 0)
                }
            })
            .expect("create appender");

        appender.write_all(b"a\n").expect("write day1");
        advanced.store(1, Ordering::SeqCst);
        appender.write_all(b"b\n").expect("write day2");
        drop(appender);

        // 不相关文件必须全部幸存。
        assert!(dir.join("notes.txt").exists());
        assert!(dir.join("info.bak").exists());
        assert!(dir.join("error.2026-10-05.log").exists());
        assert!(dir.join("info.notadate.log").exists());
        // max_files=1：只有最新的匹配文件保留。
        assert!(dir.join("info.2026-10-10.log").exists());
        assert!(!dir.join("info.2026-10-09.log").exists());

        cleanup(&dir);
    }

    #[test]
    fn default_clock_uses_process_local_date() {
        let dir = unique_temp_dir("defaultclock");
        let appender =
            LocalRollingFileAppender::new(&dir, "info", "log", 10).expect("create appender");
        drop(appender);

        let expected = format!("info.{}.log", chrono::Local::now().format("%Y-%m-%d"));
        assert!(
            dir.join(&expected).exists(),
            "default clock must name file by local date ({})",
            expected
        );

        cleanup(&dir);
    }

    #[test]
    fn local_format_time_renders_local_date_and_offset() {
        use tracing_subscriber::fmt::time::FormatTime;

        let mut output = String::new();
        {
            let mut writer = tracing_subscriber::fmt::format::Writer::new(&mut output);
            super::super::LocalFormatTime
                .format_time(&mut writer)
                .expect("format_time");
        }

        let now = chrono::Local::now();
        let expected_date = now.format("%Y-%m-%d").to_string();
        let expected_offset = now.format("%:z").to_string();
        assert!(
            output.starts_with(&expected_date),
            "timestamp must start with local date, got {output}"
        );
        assert!(
            output.ends_with(&expected_offset),
            "timestamp must end with local offset {expected_offset}, got {output}"
        );
    }
}
