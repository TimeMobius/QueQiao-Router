use super::types::{Config, ExtraBodyCached};
use notify::{
    recommended_watcher, Event, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher,
};
use serde_yaml;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{RwLock, RwLockReadGuard};
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info};

/// Interval for the (mtime, size) polling fallback.
///
/// inotify only fires for changes that go through this kernel's own mount.
/// On NFS, writes made by *other* clients (or by other mount instances of
/// the same export) never generate inotify events here, no matter how the
/// file is written. Periodically polling the file's (mtime, size) guarantees
/// hot reload works regardless of which client wrote the config.
const CONFIG_POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Fingerprint of the config file on disk: (modification time, size).
type FileFingerprint = (SystemTime, u64);

pub struct ConfigManager {
    config: Arc<RwLock<Config>>,
    _config_path: String,         // 保留路径以备后用，添加 _ 前缀消除未使用警告
    _watcher: RecommendedWatcher, // Keep watcher alive
}

impl ConfigManager {
    /// 创建新的 ConfigManager 并开始监听配置文件变化
    pub async fn new(config_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = Self::load_config(config_path)?;
        config.config_generation = 1;
        let config_arc = Arc::new(RwLock::new(config));

        // Fingerprint of the file we just loaded, so the polling fallback does
        // not treat the initial state as "changed" on its first tick.
        let initial = Self::stat_fingerprint(config_path).unwrap_or((SystemTime::UNIX_EPOCH, 0));
        let last_applied = Arc::new(RwLock::new(initial));

        // Initialize watching immediately to get the watcher instance
        let watcher =
            Self::setup_watcher(config_path, config_arc.clone(), last_applied.clone()).await?;

        // inotify misses changes made by other NFS clients / other mount
        // instances of the same export, so run a polling fallback in parallel.
        // Both paths funnel into reload_if_changed, which dedupes by fingerprint.
        Self::start_polling_task(
            config_arc.clone(),
            last_applied.clone(),
            config_path.to_string(),
        );

        let manager = ConfigManager {
            config: config_arc,
            _config_path: config_path.to_string(),
            _watcher: watcher,
        };

        Ok(manager)
    }

    pub fn load_config(config_path: &str) -> Result<Config, Box<dyn std::error::Error>> {
        let contents = fs::read_to_string(config_path)?;
        let mut config: Config = serde_yaml::from_str(&contents)?;
        Self::populate_extra_body_cache(&mut config);
        Ok(config)
    }

    /// Parse each client's `extra_body` JSON string into the cached Value tree
    /// so that runtime hot path avoids per-request serde_json::from_str.
    fn populate_extra_body_cache(config: &mut Config) {
        for client in &mut config.openai_clients {
            let parsed = client.extra_body.as_deref().and_then(|raw| {
                if raw.trim().is_empty() {
                    return None;
                }
                match serde_json::from_str(raw) {
                    Ok(serde_json::Value::Object(map)) => Some(map),
                    _ => {
                        tracing::warn!(
                            "Client '{}' extra_body is not a valid JSON object, skipping",
                            client.name
                        );
                        None
                    }
                }
            });
            client.extra_body_cached = ExtraBodyCached(parsed);
        }
    }

    pub async fn get_config(&self) -> Config {
        self.config.read().await.clone()
    }

    pub async fn get_config_guard(&self) -> RwLockReadGuard<'_, Config> {
        self.config.read().await
    }

    /// Return a consistent snapshot of (Config, config_generation) for use by the cache.
    /// The generation is read atomically with the config contents so they always match.
    pub async fn get_config_with_generation(&self) -> (Config, u64) {
        let guard = self.config.read().await;
        let gen = guard.config_generation;
        (guard.clone(), gen)
    }

    /// (mtime, size) of `path`, used as a cheap change fingerprint.
    fn stat_fingerprint(path: &str) -> std::io::Result<FileFingerprint> {
        let meta = fs::metadata(path)?;
        Ok((meta.modified()?, meta.len()))
    }

    /// Reload the config file if its on-disk fingerprint (mtime, size) differs
    /// from the last successfully applied one.
    ///
    /// Shared by the inotify callback and the polling fallback. The final
    /// fingerprint check happens under the config write lock, so a racing pair
    /// (inotify event + poll tick) applies a given version exactly once. A
    /// parse failure keeps the previous config and leaves the fingerprint
    /// untouched, so the next event/tick retries the reload.
    async fn reload_if_changed(
        config: &Arc<RwLock<Config>>,
        config_path: &str,
        last_applied: &Arc<RwLock<FileFingerprint>>,
    ) {
        let fingerprint = match Self::stat_fingerprint(config_path) {
            Ok(fp) => fp,
            Err(e) => {
                // File momentarily unreadable (atomic rename in flight, NFS hiccup).
                // The next event/tick will retry.
                debug!("Config watch: cannot stat {config_path}: {e}");
                return;
            }
        };

        // Fast path: this version is already live.
        if *last_applied.read().await == fingerprint {
            return;
        }

        // Parse outside the config write lock so a slow read (e.g. over NFS)
        // never stalls in-flight request handling. The error is stringified
        // up front because Box<dyn StdError> is not Send and must not be held
        // across the write-lock await below.
        match Self::load_config(config_path).map_err(|e| e.to_string()) {
            Ok(mut new_config) => {
                let mut guard = config.write().await;
                let mut applied = last_applied.write().await;
                if *applied == fingerprint {
                    // A concurrent reload already applied this exact version.
                    return;
                }
                // Increment generation atomically with the config swap
                let next_gen = guard.config_generation.wrapping_add(1);
                new_config.config_generation = next_gen;
                *guard = new_config;
                *applied = fingerprint;
                info!("Config reloaded successfully (generation {next_gen}).");
            }
            Err(e) => {
                error!("Failed to reload config: {e}");
            }
        }
    }

    async fn setup_watcher(
        config_path_str: &str,
        config: Arc<RwLock<Config>>,
        last_applied: Arc<RwLock<FileFingerprint>>,
    ) -> NotifyResult<RecommendedWatcher> {
        let config_path = config_path_str.to_string();
        let config_path_for_check = config_path.clone();

        // Capture the runtime handle to submit tasks from the non-async watcher thread
        let runtime_handle = tokio::runtime::Handle::current();

        // Create a watcher object
        let mut watcher = recommended_watcher(move |res: NotifyResult<Event>| {
            match res {
                Ok(event) => {
                    // Check if the event is for our config file
                    // Using loose matching because editors often save to temp files and rename
                    if event
                        .paths
                        .iter()
                        .any(|p| p.to_string_lossy().contains(&config_path_for_check))
                    {
                        info!("Config file changed, reloading...");
                        let config_task = config.clone();
                        let last_applied_task = last_applied.clone();
                        let path = config_path_for_check.clone();
                        // Run the reload on the runtime so the watcher thread
                        // never blocks on NFS I/O; small delay lets the write settle.
                        runtime_handle.spawn(async move {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            Self::reload_if_changed(&config_task, &path, &last_applied_task).await;
                        });
                    }
                }
                Err(e) => error!("watch error: {e:?}"),
            }
        })?;

        // Add a path to be watched.
        // Watch the parent directory to handle atomic saves (rename/move) better
        let path_to_watch = Path::new(config_path_str)
            .parent()
            .unwrap_or(Path::new("."));

        watcher.watch(path_to_watch, RecursiveMode::NonRecursive)?;

        info!(
            "Started watching config file directory: {:?}",
            path_to_watch
        );
        Ok(watcher)
    }

    /// Polling fallback for filesystems where inotify cannot see remote
    /// changes — NFS in particular: other clients, or other mount instances of
    /// the same export, never generate inotify events in our kernel.
    ///
    /// Note: on NFS the client's attribute cache (acregmax, default up to 60s)
    /// can delay how quickly a freshly written mtime is visible here. Mounting
    /// with a small `actimeo` (e.g. `actimeo=1`) keeps this fallback prompt.
    fn start_polling_task(
        config: Arc<RwLock<Config>>,
        last_applied: Arc<RwLock<FileFingerprint>>,
        config_path: String,
    ) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(CONFIG_POLL_INTERVAL);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                // First tick fires immediately; the fingerprint fast path makes
                // it a no-op for the just-loaded config.
                ticker.tick().await;
                Self::reload_if_changed(&config, &config_path, &last_applied).await;
            }
        });
    }
}
