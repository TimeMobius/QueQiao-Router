use axum::middleware as axum_middleware;
use mimalloc::MiMalloc;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;

use queqiao_router::{client, config, db, middleware, routes, state};

use client::client_manager::ClientManager;
use db::writer::{AuditWriter, WriterConfig};
use db::{check_and_rotate, init_db, init_rotation_state};
use state::app_state::AppState;

#[cfg(feature = "check-api-auth")]
mod check_api_auth {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../Check_API/auth-lib/src/lib.rs"
    ));
}

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

fn main() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(128)
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async {
        if std::env::args().any(|arg| arg == "--migrate-db" || arg == "--verify-db") {
            if let Err(error) = db::migrate::run_cli().await {
                eprintln!("database migration failed: {error}");
                std::process::exit(1);
            }
            return;
        }

        // Initialize tracing
        // 配置自定义的日志系统 (File + Console)
        let log_config = queqiao_router::logging::LogConfig::default();
        let _guards = queqiao_router::logging::init_logging(log_config);
        println!("Logging system initialized.");

        // Load configuration
        let config_manager = config::config_manager::ConfigManager::new("config/config.yaml")
            .await
            .expect("Failed to load configuration");
        println!("Configuration loaded successfully.");

        let config_manager = Arc::new(config_manager);

        // Initialize client manager (built once at startup; connection-pool
        // settings are read from the loaded config and require a restart to change)
        let config = config_manager.get_config().await;
        let client_manager = Arc::new(ClientManager::new(config.connection_pool));

        let db = init_db()
            .await
            .expect("Failed to initialize audit database");
        let audit_writer = AuditWriter::start(db.clone(), WriterConfig::default())
            .await
            .expect("Failed to start audit writer");
        println!("Audit database and writer initialized.");

        let app_state = Arc::new(AppState::new(
            config_manager,
            client_manager,
            db,
            audit_writer,
        ));
        println!("Application state constructed.");

        init_rotation_state(&app_state).await;

        // ============================================================================
        // 启动独立 Metrics Worker 线程
        // ============================================================================
        use queqiao_router::metrics::{middleware as metrics_middleware, worker};

        let (metrics_sender, metrics_receiver) = worker::create_metrics_channel();
        metrics_middleware::set_metrics_sender(metrics_sender.clone());
        println!("Metrics channel created.");

        // 启动独立的 metrics worker 任务
        tokio::spawn(async move {
            worker::start_metrics_worker(metrics_receiver).await;
        });
        println!("Metrics worker started.");

        // Add a small delay to ensure the database is fully initialized on disk
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;

        // Background task for database rotation
        let rotation_state = app_state.clone();
        let rotation_interval: u64 = std::env::var("DB_ROTATION_CHECK_INTERVAL_SEC")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60);

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(tokio::time::Duration::from_secs(rotation_interval));
            loop {
                interval.tick().await;
                check_and_rotate(&rotation_state).await;
            }
        });

        let optimize_interval: u64 = std::env::var("DB_FTS_OPTIMIZE_INTERVAL_SEC")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(21_600);
        if optimize_interval > 0 {
            let optimize_state = app_state.clone();
            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(optimize_interval));
                interval.tick().await;
                loop {
                    interval.tick().await;
                    let _lock = optimize_state.db_rotation_lock.lock().await;
                    let store = optimize_state.active_db().await;
                    let started = std::time::Instant::now();
                    match store.optimize_fts().await {
                        Ok(()) => tracing::info!(
                            elapsed_ms =
                                u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
                            "FTS index optimized"
                        ),
                        Err(error) => {
                            tracing::warn!(%error, "FTS index optimization failed")
                        }
                    }
                }
            });
        }

        let app = routes::create_router(app_state.clone()).layer(axum_middleware::from_fn(
            middleware::access_log::access_log_middleware,
        )); // Access Log 最外层

        // ============================================================================
        // 可选：Check_API 认证 + Token 追踪插件
        // 编译时通过 --features check-api-auth 启用
        // 无 feature 时以下代码被编译器移除，零性能影响
        // ============================================================================
        #[cfg(feature = "check-api-auth")]
        let app = check_api_auth::add_auth_to_router(app);

        // Run our app with hyper, listening globally on port 8000 or SERVER_PORT env var
        let port = std::env::var("SERVER_PORT")
            .unwrap_or_else(|_| "8000".to_string())
            .parse()
            .expect("SERVER_PORT must be a number");
        // Listen on IPv6 "any" address (::), which generally also supports IPv4 (dual-stack)
        let addr = SocketAddr::from(([0, 0, 0, 0, 0, 0, 0, 0], port));
        let listener = TcpListener::bind(addr).await.unwrap();
        println!("listening on {}", addr);
        tracing::info!("listening on {}", addr);
        let shutdown_state = app_state.clone();
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(shutdown_signal())
        .await
        .unwrap();
        shutdown_state.audit_writer.shutdown().await;
    });
}
