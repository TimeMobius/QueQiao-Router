use crate::config::{
    connection_pool::ConnectionPoolConfig,
    types::{ClientConfig, Config},
};
use reqwest::Client;
use tokio::sync::RwLockReadGuard;

pub struct ClientManager {
    client: Client,
}

impl Default for ClientManager {
    fn default() -> Self {
        Self::new(ConnectionPoolConfig::default())
    }
}

impl ClientManager {
    pub fn new(pool: ConnectionPoolConfig) -> Self {
        let client = Client::builder()
            // TCP 连接建立超时：10秒 (快速失败)
            .connect_timeout(std::time::Duration::from_secs(10))
            // 全局总超时：30分钟 (避免截断长流，但防止永久挂起)
            .timeout(std::time::Duration::from_secs(1800))
            // 每 host 最大空闲连接；enabled=false 时映射为 0，禁用连接池 keep-alive
            .pool_max_idle_per_host(pool.effective_max_idle_per_host())
            // 空闲连接淘汰：0 时映射为 None（reqwest 不淘汰空闲连接）
            .pool_idle_timeout(pool.effective_idle_timeout())
            // TCP keepalive 探测：0 时映射为 None（禁用 TCP keepalive）
            .tcp_keepalive(pool.effective_tcp_keepalive())
            // 禁用 Nagle 算法，降低 SSE 流式小包延迟
            .tcp_nodelay(true)
            .build()
            .expect("Failed to build reqwest client");
        ClientManager { client }
    }

    pub fn get_client(&self) -> Client {
        self.client.clone()
    }

    // 实现find_matching_clients函数
    pub async fn find_matching_clients<'a>(
        &self,
        config: &RwLockReadGuard<'a, Config>,
        model: &str,
    ) -> Vec<ClientConfig> {
        let matching_clients: Vec<ClientConfig> = config
            .openai_clients
            .iter()
            .filter(|client| match client.model_match.match_type.as_str() {
                "keyword" => client
                    .model_match
                    .value
                    .iter()
                    .any(|keyword| model.contains(keyword)),
                "exact" => client.model_match.value.contains(&model.to_string()),
                _ => false,
            })
            .cloned()
            .collect();

        matching_clients
    }
}
