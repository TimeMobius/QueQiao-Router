use crate::app_error::AppError;
use crate::client::client_manager::ClientManager;
use crate::client::routing::select_clients;
use crate::config::config_manager::ConfigManager;
use crate::config::types::{ClientConfig, HealthCheckConfig, LoadBalancingStrategy};
use crate::metrics::active_requests::get_active_counts_for_clients;
use crate::metrics::prometheus::FAILOVER_TOTAL;
use crate::models::AccessLogMeta;
use crate::services::circuit_breaker::{AttemptPermission, CircuitBreakerRegistry};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

#[derive(Clone)]
pub struct DispatcherService {
    config_manager: Arc<ConfigManager>,
    client_manager: Arc<ClientManager>,
    circuit_breakers: CircuitBreakerRegistry,
}

fn select_fallback_model(clients: &[ClientConfig]) -> Option<String> {
    clients.iter().find_map(|client| client.fallback.clone())
}

impl DispatcherService {
    pub fn new(config_manager: Arc<ConfigManager>, client_manager: Arc<ClientManager>) -> Self {
        Self {
            config_manager,
            client_manager,
            circuit_breakers: CircuitBreakerRegistry::new(HealthCheckConfig::default()),
        }
    }

    fn should_retry_on_client_error(resp: &Response) -> bool {
        let status = resp.status();
        matches!(status.as_u16(), 403 | 404 | 422 | 429)
    }

    /// 解析给定模型名称对应的客户端列表，并应用负载均衡
    async fn resolve_clients(
        &self,
        model_name: &str,
        routing_keys: &Option<Vec<(String, usize)>>,
        endpoint: Option<&str>,
    ) -> Result<Vec<ClientConfig>, AppError> {
        let config_guard = self.config_manager.get_config_guard().await;
        let matching_clients = self
            .client_manager
            .find_matching_clients(&config_guard, model_name)
            .await;

        let strategy = &config_guard.load_balancing.strategy;

        // 最少连接策略需要读取 active counts
        let active_counts: Option<HashMap<String, i64>> =
            if matches!(strategy, LoadBalancingStrategy::LeastConnections) {
                Some(get_active_counts_for_clients(
                    &matching_clients,
                    model_name,
                    endpoint,
                ))
            } else {
                None
            };

        // 应用负载均衡策略（支持确定路由、加权随机、最少连接）
        let matching_clients = select_clients(
            matching_clients,
            routing_keys.clone(),
            strategy,
            active_counts.as_ref(),
        );

        if matching_clients.is_empty() {
            Err(AppError::ClientNotFound(model_name.to_string()))
        } else {
            Ok(matching_clients)
        }
    }

    pub async fn execute<F, Fut>(
        &self,
        initial_model: &str,
        routing_keys: Option<Vec<(String, usize)>>,
        endpoint: Option<&str>,
        request_callback: F,
    ) -> Response
    where
        F: FnMut(&ClientConfig, &str) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Response, AppError>> + Send + 'static,
    {
        let config = self.config_manager.get_config().await;
        self.circuit_breakers.configure(config.health_check);
        let cb = Arc::new(Mutex::new(request_callback));
        let mut current_model = initial_model.to_string();
        let mut all_tried_clients = Vec::new();

        loop {
            let clients = match self
                .resolve_clients(&current_model, &routing_keys, endpoint)
                .await
            {
                Ok(c) => c,
                Err(e) => return e.into_response(),
            };

            let execution_result = self
                .execute_client_chain_with_breaker(
                    &clients,
                    &current_model,
                    cb.clone(),
                    &mut all_tried_clients,
                )
                .await;
            // ... (后面保持不变)
            match execution_result {
                // 成功获得响应（包括 4xx 客户端错误，这些被视为业务成功处理）
                Ok(mut response) => {
                    let mut error_msg_opt = None;
                    // 如果响应中包含 AccessLogMeta 且有错误信息，将所有尝试过的客户端追加上去
                    if let Some(meta) = response.extensions_mut().get_mut::<AccessLogMeta>() {
                        if let Some(err_msg) = &mut meta.error {
                            *err_msg = format!("{} (Tried: {:?})", err_msg, all_tried_clients);
                            error_msg_opt = Some(err_msg.clone());
                        }
                    }

                    // 如果是服务端错误 (5xx)，且我们有更新后的错误信息，重新构建响应体以包含 Tried 列表
                    // 这样可以确保客户端收到的错误信息与服务端日志一致
                    if response.status().is_server_error() {
                        if let Some(msg) = error_msg_opt {
                            let new_body =
                                crate::app_error::build_error_body(&msg, "internal_error");
                            let mut new_response =
                                (response.status(), axum::Json(new_body)).into_response();
                            // 必须保留原来的 extension (Meta)，否则日志就丢了
                            *new_response.extensions_mut() = response.extensions().clone();
                            return new_response;
                        }
                    }
                    return response;
                }

                // 触发了 Fallback
                Err(Some(fallback_model)) => {
                    info!(
                        "All clients for model '{}' failed or triggered fallback. Switching to fallback model: '{}'",
                        current_model, fallback_model
                    );
                    // Record failover event
                    FAILOVER_TOTAL.with_label_values(&[&current_model]).inc();
                    current_model = fallback_model;
                    continue;
                }

                // 所有尝试都失败，且没有 Fallback
                Err(None) => {
                    let error_message = format!(
                        "All upstream providers failed for model '{}'. Tried clients: {:?}",
                        current_model, all_tried_clients
                    );
                    warn!("{}", error_message);

                    let mut response =
                        AppError::InternalServerError(error_message.clone()).into_response();

                    // 尝试注入错误日志元数据
                    response.extensions_mut().insert(AccessLogMeta {
                        model: current_model.clone(),
                        backend: all_tried_clients
                            .first()
                            .cloned()
                            .unwrap_or_else(|| "unknown".to_string()),
                        error: Some(error_message),
                        request_body: None,
                    });
                    return response;
                }
            }
        }
    }

    async fn execute_client_chain_with_breaker<F, Fut>(
        &self,
        clients: &[ClientConfig],
        model_name: &str,
        request_callback: Arc<Mutex<F>>,
        tried_clients_accumulator: &mut Vec<String>,
    ) -> Result<Response, Option<String>>
    where
        F: FnMut(&ClientConfig, &str) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Response, AppError>> + Send + 'static,
    {
        Self::execute_client_chain_with_registry(
            &self.circuit_breakers,
            clients,
            model_name,
            request_callback,
            tried_clients_accumulator,
        )
        .await
    }

    #[cfg(test)]
    async fn execute_client_chain<F, Fut>(
        clients: &[ClientConfig],
        model_name: &str,
        request_callback: Arc<Mutex<F>>,
        tried_clients_accumulator: &mut Vec<String>,
    ) -> Result<Response, Option<String>>
    where
        F: FnMut(&ClientConfig, &str) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Response, AppError>> + Send + 'static,
    {
        let registry = CircuitBreakerRegistry::new(HealthCheckConfig {
            enabled: false,
            ..HealthCheckConfig::default()
        });
        Self::execute_client_chain_with_registry(
            &registry,
            clients,
            model_name,
            request_callback,
            tried_clients_accumulator,
        )
        .await
    }

    async fn execute_client_chain_with_registry<F, Fut>(
        registry: &CircuitBreakerRegistry,
        clients: &[ClientConfig],
        model_name: &str,
        request_callback: Arc<Mutex<F>>,
        tried_clients_accumulator: &mut Vec<String>,
    ) -> Result<Response, Option<String>>
    where
        F: FnMut(&ClientConfig, &str) -> Fut + Send + 'static,
        Fut: Future<Output = Result<Response, AppError>> + Send + 'static,
    {
        let mut attempted_client = false;

        for client in clients {
            match registry.allow(client) {
                AttemptPermission::Blocked { .. } => {
                    debug!("Skipping cooling backend {}", client.name);
                    continue;
                }
                AttemptPermission::Allowed | AttemptPermission::HalfOpen => {}
            }

            attempted_client = true;
            tried_clients_accumulator.push(client.name.clone());
            debug!("Dispatching request to client: {}", client.name);

            let result = {
                let mut cb = request_callback.lock().await;
                cb(client, model_name).await
            };

            match result {
                Ok(mut resp) => {
                    let status = resp.status();
                    let should_fallback =
                        status.is_client_error() && Self::should_retry_on_client_error(&resp);

                    if status.is_success() || (status.is_client_error() && !should_fallback) {
                        registry.record_success(client);
                        if resp.extensions().get::<AccessLogMeta>().is_none() {
                            resp.extensions_mut().insert(AccessLogMeta {
                                model: model_name.to_string(),
                                backend: client.name.clone(),
                                error: if status.is_client_error() {
                                    Some(format!("Upstream client error: {}", status))
                                } else {
                                    None
                                },
                                request_body: None,
                            });
                        }
                        return Ok(resp);
                    }

                    registry.record_failure(client);
                    warn!(
                        "Client {} returned retryable error {}. Continuing ordered fallback...",
                        client.name, status
                    );
                }
                Err(e) => {
                    registry.record_failure(client);
                    warn!(
                        "Client {} failed with error: {}. Continuing ordered fallback...",
                        client.name, e
                    );
                }
            }
        }

        if !attempted_client {
            let error_message = format!(
                "All upstream providers for model '{}' are cooling down",
                model_name
            );
            let mut response = (
                StatusCode::SERVICE_UNAVAILABLE,
                axum::Json(crate::app_error::build_error_body(
                    &error_message,
                    "upstream_unavailable",
                )),
            )
                .into_response();
            response.extensions_mut().insert(AccessLogMeta {
                model: model_name.to_string(),
                backend: "unknown".to_string(),
                error: Some(error_message),
                request_body: None,
            });
            return Ok(response);
        }

        if let Some(fallback_model) = select_fallback_model(clients) {
            return Err(Some(fallback_model));
        }

        Err(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn client_with_fallback(fallback: Option<&str>) -> ClientConfig {
        ClientConfig {
            fallback: fallback.map(str::to_owned),
            ..ClientConfig::default()
        }
    }

    #[test]
    fn selects_first_configured_fallback_in_client_order() {
        let clients = vec![
            client_with_fallback(None),
            client_with_fallback(Some("backup-model-a")),
            client_with_fallback(Some("backup-model-b")),
        ];

        assert_eq!(
            select_fallback_model(&clients),
            Some("backup-model-a".to_string())
        );
    }

    #[test]
    fn returns_none_when_no_client_configures_fallback() {
        let clients = vec![client_with_fallback(None), client_with_fallback(None)];

        assert_eq!(select_fallback_model(&clients), None);
    }

    #[test]
    fn retries_forbidden_and_too_many_requests_responses() {
        let forbidden = (StatusCode::FORBIDDEN, ()).into_response();
        let too_many_requests = (StatusCode::TOO_MANY_REQUESTS, ()).into_response();

        assert!(DispatcherService::should_retry_on_client_error(&forbidden));
        assert!(DispatcherService::should_retry_on_client_error(
            &too_many_requests
        ));
    }

    fn fallback_clients() -> Vec<ClientConfig> {
        vec![
            ClientConfig {
                name: "minimax3_remote2".to_string(),
                fallback: Some("DeepSeek-V4-Pro".to_string()),
                ..ClientConfig::default()
            },
            ClientConfig {
                name: "minimax3_remote1".to_string(),
                fallback: Some("DeepSeek-V4-Pro".to_string()),
                ..ClientConfig::default()
            },
        ]
    }

    #[tokio::test]
    async fn continues_to_model_fallback_after_forbidden_from_all_clients() {
        let clients = fallback_clients();
        let request_callback = |_: &ClientConfig, _: &str| async {
            Ok::<Response, AppError>((StatusCode::FORBIDDEN, ()).into_response())
        };
        let mut tried_clients = Vec::new();

        let result = DispatcherService::execute_client_chain(
            &clients,
            "MiniMax-M3",
            Arc::new(Mutex::new(request_callback)),
            &mut tried_clients,
        )
        .await;

        assert!(matches!(
            result,
            Err(Some(fallback_model)) if fallback_model == "DeepSeek-V4-Pro"
        ));
        assert_eq!(
            tried_clients,
            clients
                .iter()
                .map(|client| client.name.clone())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn continues_to_model_fallback_after_too_many_requests_from_all_clients() {
        let clients = fallback_clients();
        let request_callback = |_: &ClientConfig, _: &str| async {
            Ok::<Response, AppError>((StatusCode::TOO_MANY_REQUESTS, ()).into_response())
        };
        let mut tried_clients = Vec::new();

        let result = DispatcherService::execute_client_chain(
            &clients,
            "MiniMax-M3",
            Arc::new(Mutex::new(request_callback)),
            &mut tried_clients,
        )
        .await;

        assert!(matches!(
            result,
            Err(Some(fallback_model)) if fallback_model == "DeepSeek-V4-Pro"
        ));
        assert_eq!(
            tried_clients,
            clients
                .iter()
                .map(|client| client.name.clone())
                .collect::<Vec<_>>()
        );
    }

    #[tokio::test]
    async fn returns_service_unavailable_without_calling_cooling_backends() {
        let clients = fallback_clients();
        let registry = CircuitBreakerRegistry::new(HealthCheckConfig {
            failure_threshold: 1,
            cooldown_seconds: 30,
            ..HealthCheckConfig::default()
        });
        for client in &clients {
            registry.record_failure(client);
        }

        let callback_calls = Arc::new(AtomicUsize::new(0));
        let calls = Arc::clone(&callback_calls);
        let request_callback = move |_: &ClientConfig, _: &str| {
            let calls = Arc::clone(&calls);
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok::<Response, AppError>((StatusCode::OK, ()).into_response())
            }
        };
        let mut tried_clients = Vec::new();

        let result = DispatcherService::execute_client_chain_with_registry(
            &registry,
            &clients,
            "MiniMax-M3",
            Arc::new(Mutex::new(request_callback)),
            &mut tried_clients,
        )
        .await;

        match result {
            Ok(response) => assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE),
            Err(fallback) => panic!("expected 503 response, received fallback: {fallback:?}"),
        }
        assert_eq!(callback_calls.load(Ordering::SeqCst), 0);
        assert!(tried_clients.is_empty());
    }
}
