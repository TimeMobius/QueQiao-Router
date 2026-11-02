//! 网关请求体大小上限。
//!
//! Axum 的 `DefaultBodyLimit` 默认为 2 MiB。多轮 Agent 会话每一轮都会重发全量
//! 历史（文本 + 内联 base64 图片），几轮之后请求体就会越过 2 MiB；而客户端自身
//! 的请求预算（例如 DSH 的 `maxRequestImageBytes` 默认 20 MiB，且只统计图片
//! base64 载荷）尚未触发。结果是客户端拿到一个无法自愈的 413：网关成了最先撞上
//! 的墙，历史又只增不减，此后的每一轮都必然 413。
//!
//! 因此网关默认上限提升到 64 MiB，并允许通过 `MAX_REQUEST_BODY_BYTES` 覆盖，
//! 让网关的请求体上限明确高于客户端侧预算之和。

/// 默认请求体上限：64 MiB。
///
/// 需要明显高于客户端侧预算之和（DSH 默认 20 MiB 图片载荷 + 数 MiB 文本/工具/
/// JSON 结构），否则客户端的多轮自动恢复逻辑还没启动就会先收到网关 413。
pub const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 64 * 1024 * 1024;

/// 当前进程生效的请求体上限。
///
/// 由路由层以请求扩展的形式注入，`CustomJson` 在构造 413 错误体时读取它，
/// 从而让错误消息携带当前生效的阈值。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RequestBodyLimit(pub usize);

impl RequestBodyLimit {
    pub const fn bytes(self) -> usize {
        self.0
    }
}

/// 解析 `MAX_REQUEST_BODY_BYTES` 的原始值。
///
/// 未设置、空白、非数字或 `0` 都回退到 [`DEFAULT_MAX_REQUEST_BODY_BYTES`]。
pub fn parse_max_request_body_bytes(raw: Option<&str>) -> usize {
    raw.and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|bytes| *bytes > 0)
        .unwrap_or(DEFAULT_MAX_REQUEST_BODY_BYTES)
}

/// 读取并解析 `MAX_REQUEST_BODY_BYTES`；非法值记录一次告警。
pub fn resolve_request_body_limit() -> usize {
    let raw = std::env::var("MAX_REQUEST_BODY_BYTES").ok();
    let limit = parse_max_request_body_bytes(raw.as_deref());

    if let Some(value) = raw {
        let trimmed = value.trim();
        let valid = trimmed
            .parse::<usize>()
            .map(|bytes| bytes > 0)
            .unwrap_or(false);
        if !valid {
            tracing::warn!(
                value = %trimmed,
                default_bytes = limit,
                "invalid MAX_REQUEST_BODY_BYTES; falling back to the default request body limit"
            );
        }
    }

    limit
}

/// 将字节数格式化为便于阅读的形式（用于启动日志与 413 错误体）。
pub fn format_bytes(bytes: usize) -> String {
    const KIB: usize = 1024;
    const MIB: usize = KIB * 1024;
    const GIB: usize = MIB * 1024;

    if bytes >= GIB && bytes.is_multiple_of(GIB) {
        format!("{} GiB", bytes / GIB)
    } else if bytes >= MIB && bytes.is_multiple_of(MIB) {
        format!("{} MiB", bytes / MIB)
    } else if bytes >= KIB && bytes.is_multiple_of(KIB) {
        format!("{} KiB", bytes / KIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes as f64 / MIB as f64)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes as f64 / KIB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_positive_byte_count() {
        assert_eq!(parse_max_request_body_bytes(Some("33554432")), 33_554_432);
        assert_eq!(parse_max_request_body_bytes(Some(" 1048576 ")), 1_048_576);
    }

    #[test]
    fn falls_back_to_default_for_missing_or_invalid_values() {
        assert_eq!(
            parse_max_request_body_bytes(None),
            DEFAULT_MAX_REQUEST_BODY_BYTES
        );
        assert_eq!(
            parse_max_request_body_bytes(Some("")),
            DEFAULT_MAX_REQUEST_BODY_BYTES
        );
        assert_eq!(
            parse_max_request_body_bytes(Some("   ")),
            DEFAULT_MAX_REQUEST_BODY_BYTES
        );
        assert_eq!(
            parse_max_request_body_bytes(Some("64MiB")),
            DEFAULT_MAX_REQUEST_BODY_BYTES
        );
        assert_eq!(
            parse_max_request_body_bytes(Some("-1")),
            DEFAULT_MAX_REQUEST_BODY_BYTES
        );
        assert_eq!(
            parse_max_request_body_bytes(Some("0")),
            DEFAULT_MAX_REQUEST_BODY_BYTES
        );
    }

    #[test]
    fn formats_common_byte_counts() {
        assert_eq!(format_bytes(0), "0 bytes");
        assert_eq!(format_bytes(1536), "1.5 KiB");
        assert_eq!(format_bytes(2 * 1024 * 1024), "2 MiB");
        assert_eq!(format_bytes(DEFAULT_MAX_REQUEST_BODY_BYTES), "64 MiB");
        assert_eq!(format_bytes(1024 * 1024 * 1024), "1 GiB");
        assert_eq!(
            format_bytes(DEFAULT_MAX_REQUEST_BODY_BYTES * 3 / 2),
            "96 MiB"
        );
    }
}
