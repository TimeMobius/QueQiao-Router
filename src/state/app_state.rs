use crate::client::client_manager::ClientManager;
use crate::config::config_manager::ConfigManager;
use crate::services::dispatcher::DispatcherService;
use crate::services::models_cache::ModelsCache;
use chrono::Local;
use sqlx::SqlitePool;
use std::sync::atomic::{AtomicI32, AtomicI64};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct AppState {
    pub config_manager: Arc<ConfigManager>,
    pub client_manager: Arc<ClientManager>,
    pub dispatcher_service: DispatcherService,
    pub db_pool: RwLock<SqlitePool>,
    pub db_rotation_lock: Mutex<()>,
    pub models_cache: ModelsCache,
    /// 内存中跟踪的 active 数据月份（YYYYMM），写路径只读取该原子量。
    pub active_yyyymm: AtomicI32,
    /// active 月份的下一个月 1 号 00:00 本地时间的 epoch 毫秒。
    pub next_month_boundary_ms: AtomicI64,
}

impl AppState {
    pub fn new(
        config_manager: Arc<ConfigManager>,
        client_manager: Arc<ClientManager>,
        db_pool: SqlitePool,
    ) -> Self {
        let dispatcher_service =
            DispatcherService::new(config_manager.clone(), client_manager.clone());
        let current = crate::db::rotation::yyyymm(Local::now());
        AppState {
            config_manager,
            client_manager,
            dispatcher_service,
            db_pool: RwLock::new(db_pool),
            db_rotation_lock: Mutex::new(()),
            models_cache: ModelsCache::new(),
            active_yyyymm: AtomicI32::new(current),
            next_month_boundary_ms: AtomicI64::new(crate::db::rotation::next_month_boundary_ms(
                current,
            )),
        }
    }
}
