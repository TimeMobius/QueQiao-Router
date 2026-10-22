use crate::client::client_manager::ClientManager;
use crate::config::config_manager::ConfigManager;
use crate::db::store::AuditStore;
use crate::services::dispatcher::DispatcherService;
use crate::services::models_cache::ModelsCache;
use chrono::Local;
use std::sync::atomic::{AtomicI32, AtomicI64};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};

pub struct AppState {
    pub config_manager: Arc<ConfigManager>,
    pub client_manager: Arc<ClientManager>,
    pub dispatcher_service: DispatcherService,
    /// Active audit database. Rotation swaps the `Arc`; writers hold the read
    /// guard for the duration of a write so rotation cannot rename mid-write.
    pub db: RwLock<Arc<AuditStore>>,
    pub db_rotation_lock: Mutex<()>,
    pub models_cache: ModelsCache,
    pub archive_registry: Arc<crate::db::archive::ArchiveRegistry>,
    pub active_yyyymm: AtomicI32,
    pub next_month_boundary_ms: AtomicI64,
}

impl AppState {
    pub fn new(
        config_manager: Arc<ConfigManager>,
        client_manager: Arc<ClientManager>,
        db: Arc<AuditStore>,
    ) -> Self {
        let dispatcher_service =
            DispatcherService::new(config_manager.clone(), client_manager.clone());
        let current = crate::db::rotation::yyyymm(Local::now());
        let archive_registry = Arc::new(crate::db::archive::ArchiveRegistry::new(
            crate::db::resolve_db_path(),
        ));
        AppState {
            config_manager,
            client_manager,
            dispatcher_service,
            db: RwLock::new(db),
            db_rotation_lock: Mutex::new(()),
            models_cache: ModelsCache::new(),
            archive_registry,
            active_yyyymm: AtomicI32::new(current),
            next_month_boundary_ms: AtomicI64::new(crate::db::rotation::next_month_boundary_ms(
                current,
            )),
        }
    }

    /// Clone the active store for read-only use. Safe across rotation: the
    /// returned `Arc` keeps the old inode alive until the query finishes.
    pub async fn active_db(&self) -> Arc<AuditStore> {
        self.db.read().await.clone()
    }
}
