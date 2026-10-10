use std::fs;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{filter, fmt, EnvFilter, Layer};

mod rolling_local;
use rolling_local::LocalRollingFileAppender;

/// 本地时区时间格式化器，用于 system / console 层。
///
/// 以 `%Y-%m-%d %H:%M:%S%.6f%:z` 渲染进程本地时间（含固定偏移，如 `+08:00`），
/// 替代 tracing-subscriber 默认的 UTC RFC3339（`Z`）格式。
/// 生产环境通过 `TZ` 环境变量提供宿主机时区。
struct LocalFormatTime;

impl tracing_subscriber::fmt::time::FormatTime for LocalFormatTime {
    fn format_time(&self, w: &mut tracing_subscriber::fmt::format::Writer<'_>) -> std::fmt::Result {
        write!(
            w,
            "{}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.6f%:z")
        )
    }
}

/// 日志系统配置
pub struct LogConfig {
    pub log_dir: String,
    pub general_log_retention_days: usize,
    pub error_log_retention_days: usize,
}

pub const DEFAULT_LOG_DIR: &str = "logs";

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            log_dir: DEFAULT_LOG_DIR.to_string(),
            general_log_retention_days: 10,
            error_log_retention_days: 30,
        }
    }
}

/// 初始化日志系统
///
/// 返回一个WorkerGuard向量，必须在程序运行期间保持活跃以确保日志正确刷新
pub fn init_logging(config: LogConfig) -> Vec<WorkerGuard> {
    // 确保日志目录存在
    fs::create_dir_all(&config.log_dir).expect("Failed to create log directory");

    let mut guards = Vec::new();

    // 创建通用日志的非阻塞写入器（INFO, WARNING等）
    // 使用每日轮转（按进程本地时区日期命名）
    let general_file_appender = LocalRollingFileAppender::new(
        &config.log_dir,
        "info",
        "log",
        config.general_log_retention_days,
    )
    .expect("Failed to create general log appender");

    let (general_non_blocking, general_guard) =
        tracing_appender::non_blocking(general_file_appender);
    guards.push(general_guard);

    // 创建错误日志的非阻塞写入器（每日轮转，按本地时区日期命名）
    let error_file_appender = LocalRollingFileAppender::new(
        &config.log_dir,
        "error",
        "log",
        config.error_log_retention_days,
    )
    .expect("Failed to create error log appender");

    let (error_non_blocking, error_guard) = tracing_appender::non_blocking(error_file_appender);
    guards.push(error_guard);

    // 创建 system 日志的非阻塞写入器（每日轮转，按本地时区日期命名）
    let system_file_appender = LocalRollingFileAppender::new(
        &config.log_dir,
        "system",
        "log",
        config.general_log_retention_days,
    )
    .expect("Failed to create system log appender");

    let (system_non_blocking, system_guard) = tracing_appender::non_blocking(system_file_appender);
    guards.push(system_guard);

    // 创建控制台输出的非阻塞写入器
    let (console_non_blocking, console_guard) = tracing_appender::non_blocking(std::io::stdout());
    guards.push(console_guard);

    // 1. Access Info Layer: 仅 target="access_log" && level!=ERROR
    // 使用自定义格式：只输出 message，不带任何 tracing 默认的修饰
    let access_info_layer = fmt::layer()
        .with_writer(general_non_blocking)
        .with_ansi(false)
        .event_format(
            fmt::format()
                .with_level(false)
                .with_target(false)
                .without_time()
                .with_file(false)
                .with_line_number(false)
                .compact(),
        )
        .with_filter(filter::filter_fn(|meta| {
            meta.target() == "access_log" && !matches!(meta.level().as_str(), "ERROR")
        }));

    // 2. Access Error Layer: 仅 target="access_log" && level==ERROR
    let access_error_layer = fmt::layer()
        .with_writer(error_non_blocking)
        .with_ansi(false)
        .event_format(
            fmt::format()
                .with_level(false)
                .with_target(false)
                .without_time()
                .with_file(false)
                .with_line_number(false)
                .compact(),
        )
        .with_filter(filter::filter_fn(|meta| {
            meta.target() == "access_log" && meta.level().as_str() == "ERROR"
        }));

    // 3. System Layer: 记录日志 (受 RUST_LOG 控制，默认 info)
    let system_env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    let system_layer = fmt::layer()
        .with_writer(system_non_blocking)
        .with_ansi(false)
        .with_timer(LocalFormatTime)
        .with_filter(system_env_filter);

    // 4. Console Layer: 全部显示 (保持默认行为，方便调试)
    let console_layer = fmt::layer()
        .with_writer(console_non_blocking)
        .with_timer(LocalFormatTime)
        .with_filter(EnvFilter::from_default_env());

    // 构建订阅者
    tracing_subscriber::registry()
        .with(access_info_layer)
        .with(access_error_layer)
        .with(system_layer)
        .with(console_layer)
        .init();

    guards
}
