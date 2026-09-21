use std::net::SocketAddr;

use axum::http::HeaderMap;

const HEADER_X_FORWARDED_FOR: &str = "x-forwarded-for";
const HEADER_X_REAL_IP: &str = "x-real-ip";

/// 从请求头中提取客户端真实 IP
///
/// 尝试顺序：
/// 1. `X-Forwarded-For`: 标准代理头，取第一个 IP
/// 2. `X-Real-IP`: Nginx 等常用头
/// 3. `SocketAddr`: TCP 连接的远端地址
pub fn get_client_ip(headers: &HeaderMap, addr: Option<SocketAddr>) -> String {
    if let Some(xff) = headers.get(HEADER_X_FORWARDED_FOR) {
        if let Ok(xff_str) = xff.to_str() {
            let raw_ip = xff_str.split(',').next().unwrap_or(xff_str).trim();
            return clean_ip(raw_ip);
        }
    }

    if let Some(xri) = headers.get(HEADER_X_REAL_IP) {
        if let Ok(xri_str) = xri.to_str() {
            return clean_ip(xri_str.trim());
        }
    }

    if let Some(addr) = addr {
        return clean_ip(&addr.ip().to_string());
    }

    "unknown".to_string()
}

/// 辅助函数：清洗 IP 地址（移除 IPv4-mapped IPv6 前缀）
fn clean_ip(ip: &str) -> String {
    if let Some(ipv4) = ip.strip_prefix("::ffff:") {
        ipv4.to_string()
    } else {
        ip.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_client_ip_from_x_forwarded_for() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "203.0.113.195, 70.41.3.18".parse().unwrap(),
        );

        let result = get_client_ip(&headers, None);
        assert_eq!(result, "203.0.113.195");
    }

    #[test]
    fn test_get_client_ip_from_x_real_ip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", "192.168.1.100".parse().unwrap());

        let result = get_client_ip(&headers, None);
        assert_eq!(result, "192.168.1.100");
    }

    #[test]
    fn test_get_client_ip_ipv4_mapped() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "::ffff:192.168.1.1".parse().unwrap());

        let result = get_client_ip(&headers, None);
        assert_eq!(result, "192.168.1.1");
    }

    #[test]
    fn test_get_client_ip_unknown() {
        let headers = HeaderMap::new();

        let result = get_client_ip(&headers, None);
        assert_eq!(result, "unknown");
    }
}
