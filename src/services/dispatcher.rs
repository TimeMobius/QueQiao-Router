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

/// 上游 4xx 的语义分类，决定“是否重试下一个后端”与“如何记账健康度”。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum UpstreamClientErrorKind {
    /// 401：凭证缺失或无效。本后端配置的 api_key（或透传的客户端 key）被拒。
    CredentialInvalid,
    /// 403：凭证有效但无权使用（分组被收回、额度耗尽、地区/模型白名单）。
    PermissionDenied,
    /// 404：上游不认识该模型或路径——能力缺失，而非服务宕机。
    ModelNotSupported,
    /// 429：限流，稍后可用。
    RateLimited,
    /// 其他 4xx：请求本身不合法（参数、格式、体积），换后端无意义。
    RequestRejected,
}

impl UpstreamClientErrorKind {
    /// 稳定的短标识，用于日志与审计。
    fn as_str(self) -> &'static str {
        match self {
            Self::CredentialInvalid => "credential_invalid",
            Self::PermissionDenied => "permission_denied",
            Self::ModelNotSupported => "model_not_supported",
            Self::RateLimited => "rate_limited",
            Self::RequestRejected => "request_rejected",
        }
    }

    /// 给调用方/运维的处置建议，写进日志便于直接照做。
    fn caller_advice(self) -> &'static str {
        match self {
            Self::CredentialInvalid => {
                "upstream rejected the credential: fix the client api_key or the caller's key"
            }
            Self::PermissionDenied => {
                "credential is valid but not permitted (group/quota/allowlist): ask the provider or switch backend"
            }
            Self::ModelNotSupported => {
                "upstream does not serve this model: use another model name or backend"
            }
            Self::RateLimited => "upstream is rate limiting: back off and retry later",
            Self::RequestRejected => "request itself is invalid: fix the request body",
        }
    }

    /// 熔断记账：这次交互是否算作“后端故障”。
    ///
    /// **只有 429 算。** 其余 4xx 都证明后端活着并且正确应答了，把这次请求拒掉是
    /// 它的正常业务行为，不是可用性故障：
    ///
    /// - 401/403：凭证问题（无效 / 无权限），换 key 或找管理员即可，后端本身没坏。
    /// - 404：这个后端没有该模型（能力缺口）。熔断器是**按 client 记账**的，
    ///   为一个模型把整个 client 冷却，会连带屏蔽它在其它模型上的正常服务，
    ///   代价远大于收益。
    /// - 400/422/413：请求本身不合法，是调用方的问题。绝不能因此冷却后端——
    ///   否则任何人发垃圾请求就能把后端打出服务。
    ///
    /// 429 不同：它是**明确的容量/负载信号**，上游在要求我们退避。计入故障、
    /// 让熔断器按退避曲线冷却，正是对上游限流的正确响应；反之若记成功，
    /// 我们会在上游明确说“别来了”的时候继续猛打。
    fn counts_as_backend_failure(self) -> bool {
        matches!(self, Self::RateLimited)
    }
}

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

    /// 上游 4xx 的语义分类。
    ///
    /// 这四类错误的“责任方”完全不同，混为一谈会同时造成误判和误熔断：
    ///
    /// | 状态码 | 语义 | 换后端重试 | 熔断记账 |
    /// | :--- | :--- | :--- | :--- |
    /// | 401 | 凭证缺失/无效 | 有（别的后端用不同 key） | 成功（后端活着） |
    /// | 403 | 凭证有效但无权限（分组/额度/地区） | 有（别的后端可能有权） | 成功（后端活着） |
    /// | 404 | 上游不认识这个模型/路径（能力缺失） | 有（别的后端可能支持） | 成功（后端活着） |
    /// | 429 | 限流，稍后可用 | 有（换后端能立刻绕过） | **失败（容量信号）** |
    /// | 其他 4xx（400/422 等） | 请求本身不合法 | 没有（换谁都一样） | 成功（调用方的问题） |
    fn classify_upstream_client_error(status: StatusCode) -> UpstreamClientErrorKind {
        match status.as_u16() {
            // 凭证问题：本后端不可用，但**不是**服务宕机。
            401 => UpstreamClientErrorKind::CredentialInvalid,
            // 权限问题：key 有效但被拒（分组被收回、额度耗尽、地区限制）。
            403 => UpstreamClientErrorKind::PermissionDenied,
            // 能力缺失：上游没有这个模型。注意 404 **不是**“服务 down”——
            // 服务 down 是连接失败（连不上/超时），在 AppError::ReqwestError
            // 里由 is_connect()/is_timeout() 处理，会走到 Err 分支记 failure。
            404 => UpstreamClientErrorKind::ModelNotSupported,
            // 限流：明确的“稍后再试”，最容易通过换后端绕过。
            429 => UpstreamClientErrorKind::RateLimited,
            // 400/405/409/413/415/422... 请求本身有问题，换后端无意义。
            _ => UpstreamClientErrorKind::RequestRejected,
        }
    }

    /// 是否值得换下一个后端再试。
    ///
    /// 除“请求本身不合法”外都值得重试：不同后端有不同的 key、权限、模型清单和配额。
    /// 请求级错误换谁都一样，直接透传，避免把同一份坏请求打到所有上游。
    fn should_retry_on_client_error(resp: &Response) -> bool {
        !matches!(
            Self::classify_upstream_client_error(resp.status()),
            UpstreamClientErrorKind::RequestRejected
        )
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
        // 记录最后一个值得透传给调用方的上游 4xx 响应。
        // 各错误码语义不同（见 classify_upstream_client_error），因此透传时
        // 保留上游原始状态码与响应体，调用方才能区分“key 无效 / 分组无权 /
        // 模型不存在 / 被限流”，而不是一律看到 500。
        let mut last_client_error: Option<(UpstreamClientErrorKind, Response)> = None;

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

                    // 成功响应，或“不值得换后端”的请求级错误（400/422/413）：
                    // 后者是调用方请求本身不合法，不能因此冷却后端。
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

                    let kind = Self::classify_upstream_client_error(status);

                    // 记账口径见 counts_as_backend_failure：只有 429 这种容量信号
                    // 计入后端故障，其余 4xx 是“后端活着但拒绝本次请求”，记成功。
                    // 注意：服务真正宕机走的是 Err 分支（连接失败/超时），那条路
                    // 记 failure，与此处无关。
                    if kind.counts_as_backend_failure() {
                        registry.record_failure(client);
                    } else {
                        registry.record_success(client);
                    }
                    let hint = match kind {
                        UpstreamClientErrorKind::CredentialInvalid => {
                            "凭证无效：检查该 client 的 api_key（或透传的客户端 key）"
                        }
                        UpstreamClientErrorKind::PermissionDenied => {
                            "无权限：key 有效但分组/额度/白名单不允许，换后端可能绕过"
                        }
                        UpstreamClientErrorKind::ModelNotSupported => {
                            "模型不被该上游支持：换后端或改用该后端支持的模型名"
                        }
                        UpstreamClientErrorKind::RateLimited => {
                            "被限流：换后端可立即绕过，本后端稍后自动恢复"
                        }
                        UpstreamClientErrorKind::RequestRejected => "请求本身不合法：换后端无意义",
                    };
                    warn!(
                        "Client {} returned {} ({}; counted as {}); {}. Continuing ordered fallback...",
                        client.name,
                        status,
                        kind.as_str(),
                        if kind.counts_as_backend_failure() {
                            "backend failure"
                        } else {
                            "healthy"
                        },
                        hint
                    );
                    if resp.extensions().get::<AccessLogMeta>().is_none() {
                        resp.extensions_mut().insert(AccessLogMeta {
                            model: model_name.to_string(),
                            backend: client.name.clone(),
                            error: Some(format!(
                                "Upstream client error: {status} ({}) - {hint}",
                                kind.as_str()
                            )),
                            request_body: None,
                        });
                    }
                    last_client_error = Some((kind, resp));
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

        // 没有 fallback 可用：把上游最后的 4xx 原样透传（状态码 + 响应体）。
        //
        // 为什么不统一改成 500：
        // - 401/403 是**鉴权/权限**问题，调用方必须看到“换 key / 找管理员”，
        //   网关报 500 会让人去查后端宕机，方向完全错。
        // - 404 是**模型能力缺失**（上游没这个模型），不是服务 down；报 500 会
        //   让人误以为后端挂了，实际换模型名就好。
        // - 429 是**限流**，调用方应退避重试，报 500 会触发错误的告警与重试策略。
        // - 400/422 是**请求本身不合法**，原样返回上游的校验信息最有用。
        if let Some((kind, mut resp)) = last_client_error {
            let status = resp.status();
            if let Some(meta) = resp.extensions_mut().get_mut::<AccessLogMeta>() {
                meta.error = Some(format!(
                    "All clients failed; passing through upstream {status} ({}): {}",
                    kind.as_str(),
                    kind.caller_advice()
                ));
            }
            return Ok(resp);
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
    fn classifies_upstream_client_errors_by_meaning() {
        // 每个码的“责任方”不同，分类必须能区分开。
        use UpstreamClientErrorKind::*;
        assert_eq!(
            DispatcherService::classify_upstream_client_error(StatusCode::UNAUTHORIZED),
            CredentialInvalid
        );
        assert_eq!(
            DispatcherService::classify_upstream_client_error(StatusCode::FORBIDDEN),
            PermissionDenied
        );
        assert_eq!(
            DispatcherService::classify_upstream_client_error(StatusCode::NOT_FOUND),
            ModelNotSupported
        );
        assert_eq!(
            DispatcherService::classify_upstream_client_error(StatusCode::TOO_MANY_REQUESTS),
            RateLimited
        );
        assert_eq!(
            DispatcherService::classify_upstream_client_error(StatusCode::BAD_REQUEST),
            RequestRejected
        );
        assert_eq!(
            DispatcherService::classify_upstream_client_error(StatusCode::UNPROCESSABLE_ENTITY),
            RequestRejected
        );
    }

    #[test]
    fn only_rate_limit_counts_as_backend_failure() {
        // 只有 429 是容量信号，计入后端故障；其余 4xx 都证明后端活着且应答正常。
        use UpstreamClientErrorKind::*;
        assert!(RateLimited.counts_as_backend_failure());

        assert!(!CredentialInvalid.counts_as_backend_failure());
        assert!(!PermissionDenied.counts_as_backend_failure());
        assert!(!ModelNotSupported.counts_as_backend_failure());
        assert!(!RequestRejected.counts_as_backend_failure());
    }

    #[test]
    fn retries_credential_permission_model_and_rate_limit_errors() {
        // 401/403/404/429 都值得换下一个后端：不同后端有不同 key、权限、模型清单、配额。
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            assert!(
                DispatcherService::should_retry_on_client_error(&(status, ()).into_response()),
                "{status} should be retried on the next client"
            );
        }
    }

    #[test]
    fn does_not_retry_request_level_errors() {
        // 400/422 是请求本身不合法，换后端结果一样，只会把坏请求打满所有上游。
        for status in [
            StatusCode::BAD_REQUEST,
            StatusCode::UNPROCESSABLE_ENTITY,
            StatusCode::PAYLOAD_TOO_LARGE,
        ] {
            assert!(
                !DispatcherService::should_retry_on_client_error(&(status, ()).into_response()),
                "{status} must not be retried on other clients"
            );
        }
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
    async fn passes_through_upstream_auth_and_model_errors_verbatim() {
        // 单客户端、无 fallback：401/403/404/429 必须原样透传，不能被改写成 500。
        // 分别对应：key 无效、分组无权、模型不存在、被限流——四种完全不同的处置动作。
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            let clients = vec![ClientConfig {
                name: "hapi-OpenAI".to_string(),
                ..ClientConfig::default()
            }];
            let request_callback = move |_: &ClientConfig, _: &str| async move {
                Ok::<Response, AppError>((status, ()).into_response())
            };
            let mut tried_clients = Vec::new();

            let result = DispatcherService::execute_client_chain(
                &clients,
                "gpt-5",
                Arc::new(Mutex::new(request_callback)),
                &mut tried_clients,
            )
            .await;

            match result {
                Ok(response) => assert_eq!(
                    response.status(),
                    status,
                    "{status} should be passed through, not rewritten to 500"
                ),
                Err(fallback) => {
                    panic!("expected pass-through {status}, received fallback: {fallback:?}")
                }
            }
            assert_eq!(tried_clients, vec!["hapi-OpenAI".to_string()]);
        }
    }

    #[tokio::test]
    async fn passes_through_request_level_error_without_trying_other_clients() {
        // 400 是请求本身的问题：不该换后端重试，且原样透传。
        let clients = vec![
            ClientConfig {
                name: "a".to_string(),
                ..ClientConfig::default()
            },
            ClientConfig {
                name: "b".to_string(),
                ..ClientConfig::default()
            },
        ];
        let request_callback = |_: &ClientConfig, _: &str| async {
            Ok::<Response, AppError>((StatusCode::BAD_REQUEST, ()).into_response())
        };
        let mut tried_clients = Vec::new();

        let result = DispatcherService::execute_client_chain(
            &clients,
            "gpt-5",
            Arc::new(Mutex::new(request_callback)),
            &mut tried_clients,
        )
        .await;

        match result {
            Ok(response) => assert_eq!(response.status(), StatusCode::BAD_REQUEST),
            Err(fallback) => panic!("expected pass-through 400, received: {fallback:?}"),
        }
        // 只打了第一个后端就返回，没有把坏请求刷到 b。
        assert_eq!(tried_clients, vec!["a".to_string()]);
    }

    /// 连续 N 次返回同一状态码，统计真正打到上游的次数。
    async fn calls_until_cooling(status: StatusCode, rounds: usize) -> (usize, Vec<u16>) {
        let clients = vec![ClientConfig {
            name: "hapi-OpenAI".to_string(),
            ..ClientConfig::default()
        }];
        let registry = CircuitBreakerRegistry::new(HealthCheckConfig {
            failure_threshold: 1,
            cooldown_seconds: 30,
            ..HealthCheckConfig::default()
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let mut seen_statuses = Vec::new();

        for _ in 0..rounds {
            let hits = Arc::clone(&calls);
            let request_callback = move |_: &ClientConfig, _: &str| {
                let hits = Arc::clone(&hits);
                async move {
                    hits.fetch_add(1, Ordering::SeqCst);
                    Ok::<Response, AppError>((status, ()).into_response())
                }
            };
            let mut tried_clients = Vec::new();

            let result = DispatcherService::execute_client_chain_with_registry(
                &registry,
                &clients,
                "gpt-5",
                Arc::new(Mutex::new(request_callback)),
                &mut tried_clients,
            )
            .await;

            match result {
                Ok(response) => seen_statuses.push(response.status().as_u16()),
                Err(fallback) => panic!("expected response, received: {fallback:?}"),
            }
        }

        (calls.load(Ordering::SeqCst), seen_statuses)
    }

    #[tokio::test]
    async fn auth_and_model_errors_do_not_trip_circuit_breaker() {
        // 401/403/404/400 都说明后端活着且正确应答，连续发生也不该冷却：
        // 每次请求都要真实打到上游，把上游状态码原样带回。
        for status in [
            StatusCode::UNAUTHORIZED,
            StatusCode::FORBIDDEN,
            StatusCode::NOT_FOUND,
            StatusCode::BAD_REQUEST,
        ] {
            let (calls, seen) = calls_until_cooling(status, 3).await;
            assert_eq!(
                calls, 3,
                "{status} should keep hitting upstream, not cool down"
            );
            assert_eq!(seen, vec![status.as_u16(); 3]);
        }
    }

    #[tokio::test]
    async fn rate_limit_trips_circuit_breaker() {
        // 429 是容量信号：failure_threshold=1 时，第 2 次请求应被熔断器拦下
        // （不再打到上游），最终由"全部冷却"分支返回 503。
        let (calls, seen) = calls_until_cooling(StatusCode::TOO_MANY_REQUESTS, 2).await;

        assert_eq!(calls, 1, "after a 429 the backend should be cooling down");
        assert_eq!(seen[0], StatusCode::TOO_MANY_REQUESTS.as_u16());
        assert_eq!(seen[1], StatusCode::SERVICE_UNAVAILABLE.as_u16());
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
