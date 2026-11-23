use super::types::{Config, ExtraBodyCached};
use notify::{
    recommended_watcher, Event, RecommendedWatcher, RecursiveMode, Result as NotifyResult, Watcher,
};
use serde_yaml;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{RwLock, RwLockReadGuard};
use tokio::time::MissedTickBehavior;
use tracing::{debug, error, info, warn};

/// Interval for the (mtime, size) polling fallback.
///
/// inotify only fires for changes that go through this kernel's own mount.
/// On NFS, writes made by *other* clients (or by other mount instances of
/// the same export) never generate inotify events here, no matter how the
/// file is written. Periodically polling the file's (mtime, size) guarantees
/// hot reload works regardless of which client wrote the config.
const CONFIG_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Fingerprint of the config file on disk: (modification time, size).
type FileFingerprint = (SystemTime, u64);

/// Why loading the config file failed.
///
/// The distinction matters at runtime: a missing/unreadable file leaves the
/// last good config serving while `/health` reports the failure, whereas a
/// parse/validation error is a *content* problem the operator has to fix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigErrorKind {
    /// `read_to_string` failed (ENOENT, EACCES, EISDIR, NFS hiccup...).
    Read,
    /// YAML syntax error.
    Parse,
    /// YAML parsed but failed semantic validation (or is unusable, e.g. no clients).
    Validate,
}

impl ConfigErrorKind {
    /// Stable label for logs and Prometheus.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Parse => "parse",
            Self::Validate => "validate",
        }
    }
}

/// A config load failure with its kind and a human-readable message.
#[derive(Debug, Clone)]
pub struct ConfigError {
    pub kind: ConfigErrorKind,
    pub message: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind.as_str(), self.message)
    }
}

impl std::error::Error for ConfigError {}

/// Snapshot of the config file's health, exposed through `/health`.
#[derive(Debug, Clone)]
pub struct ConfigHealth {
    /// `false` when the on-disk config is currently unusable: missing,
    /// unreadable, unparsable, invalid, or it declares no backend at all.
    pub ok: bool,
    /// Last observed failure (cleared on the next successful load).
    pub last_error: Option<ConfigError>,
    /// Number of failed load attempts since process start.
    pub failures: u64,
    /// `(mtime, size)` last applied successfully; `None` before the first load.
    pub last_applied: Option<FileFingerprint>,
    /// Config generation currently serving requests.
    pub generation: u64,
}

/// State shared between the manager handle, the inotify callback and the
/// polling watchdog. Kept behind one `Arc` so every reload path observes and
/// publishes the same view of "what is live" and "what is broken".
struct Shared {
    /// The config currently serving requests. Never cleared on a failed reload.
    config: RwLock<Config>,
    /// Why the on-disk file is currently unusable (`None` = healthy).
    health: RwLock<Option<ConfigError>>,
    /// Monotonic count of failed load attempts (read/parse/validate).
    failures: AtomicU64,
    /// `(mtime, size)` last applied successfully; the reload de-dupe key.
    last_applied: RwLock<FileFingerprint>,
    /// Watched path, for diagnostics and health reporting.
    path: String,
}

pub struct ConfigManager {
    shared: Arc<Shared>,
    _watcher: RecommendedWatcher, // Keep watcher alive
}

impl ConfigManager {
    /// 创建新的 ConfigManager 并开始监听配置文件变化
    ///
    /// 启动期配置不可用直接返回 `Err`（进程退出），不做降级——带着空配置
    /// 对外提供服务只会把错误推迟到第一个请求。
    pub async fn new(config_path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut config = Self::load_config(config_path)?;
        config.config_generation = 1;

        // Fingerprint of the file we just loaded, so the polling fallback does
        // not treat the initial state as "changed" on its first tick.
        let initial = Self::stat_fingerprint(config_path).unwrap_or((SystemTime::UNIX_EPOCH, 0));

        let shared = Arc::new(Shared {
            config: RwLock::new(config),
            health: RwLock::new(None),
            failures: AtomicU64::new(0),
            last_applied: RwLock::new(initial),
            path: config_path.to_string(),
        });

        // Initialize watching immediately to get the watcher instance
        let watcher = Self::setup_watcher(shared.clone()).await?;

        // inotify misses changes made by other NFS clients / other mount
        // instances of the same export, so run a polling fallback in parallel.
        // Both paths funnel into reload_if_changed, which dedupes by fingerprint.
        Self::start_polling_task(shared.clone());

        Ok(ConfigManager {
            shared,
            _watcher: watcher,
        })
    }

    /// Read + parse + validate the config file, preserving the failure *kind*.
    fn load_config_typed(config_path: &str) -> Result<Config, ConfigError> {
        let contents = fs::read_to_string(config_path).map_err(|e| ConfigError {
            kind: ConfigErrorKind::Read,
            message: format!("cannot read {config_path}: {e}"),
        })?;
        let mut config: Config = serde_yaml::from_str(&contents).map_err(|e| ConfigError {
            kind: ConfigErrorKind::Parse,
            message: format!("invalid YAML in {config_path}: {e}"),
        })?;
        Self::populate_extra_body_cache(&mut config);
        Self::validate_config(&config).map_err(|e| ConfigError {
            kind: ConfigErrorKind::Validate,
            message: e,
        })?;
        Self::log_model_rewrites(&config);
        Ok(config)
    }

    pub fn load_config(config_path: &str) -> Result<Config, Box<dyn std::error::Error>> {
        Self::load_config_typed(config_path).map_err(Box::<dyn std::error::Error>::from)
    }

    /// 配置语义校验。
    ///
    /// 返回 `Err` 时，热重载会保留旧配置并等待下次重试（见 `reload_if_changed`），
    /// 因此校验失败不会把运行中的网关打挂；启动时失败则会直接退出，
    /// 让写错的配置尽早暴露。
    ///
    /// 空 `openai_clients` 视为不可用：这样的配置能解析、能启动，但任何
    /// 请求都路由不到后端（`/v1/models` 502、chat 422）。静默接受它只会让
    /// `/health` 报 200 而业务全挂，因此这里直接拒绝。
    fn validate_config(config: &Config) -> Result<(), String> {
        if config.openai_clients.is_empty() {
            return Err(
                "openai_clients: no backend configured; at least one client is required"
                    .to_string(),
            );
        }

        let mut seen_names = std::collections::HashSet::new();

        for client in &config.openai_clients {
            if client.name.trim().is_empty() {
                return Err("openai_clients: client name must not be empty".to_string());
            }
            if !seen_names.insert(client.name.as_str()) {
                return Err(format!(
                    "openai_clients: duplicate client name '{}'",
                    client.name
                ));
            }
            if !matches!(client.model_match.match_type.as_str(), "exact" | "keyword") {
                return Err(format!(
                    "client '{}': model_match.type must be 'exact' or 'keyword', got '{}'",
                    client.name, client.model_match.match_type
                ));
            }

            let Some(rewrite) = &client.model_rewrite else {
                continue;
            };
            for (from, to) in rewrite {
                if from.trim().is_empty() {
                    return Err(format!(
                        "client '{}': model_rewrite contains an empty model name",
                        client.name
                    ));
                }
                if to.trim().is_empty() {
                    return Err(format!(
                        "client '{}': model_rewrite['{}'] has an empty target",
                        client.name, from
                    ));
                }
                if from == to {
                    tracing::warn!(
                        "client '{}': model_rewrite['{}'] maps to itself, no-op",
                        client.name,
                        from
                    );
                }
            }
        }

        Ok(())
    }

    /// 启动/热重载时打印生效的模型别名，便于运维核对实际转发目标。
    fn log_model_rewrites(config: &Config) {
        for client in &config.openai_clients {
            let Some(rewrite) = &client.model_rewrite else {
                continue;
            };
            for (from, to) in rewrite {
                info!(
                    "client '{}': model alias '{}' -> '{}'",
                    client.name, from, to
                );
            }
        }
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
        self.shared.config.read().await.clone()
    }

    pub async fn get_config_guard(&self) -> RwLockReadGuard<'_, Config> {
        self.shared.config.read().await
    }

    /// Return a consistent snapshot of (Config, config_generation) for use by the cache.
    /// The generation is read atomically with the config contents so they always match.
    pub async fn get_config_with_generation(&self) -> (Config, u64) {
        let guard = self.shared.config.read().await;
        let gen = guard.config_generation;
        (guard.clone(), gen)
    }

    /// Whether the config on disk is currently usable, and why not if it is not.
    ///
    /// `/health` turns this into a 503 so that a deleted, unreadable, invalid
    /// or backend-less config file is visible to Docker/K8s probes instead of
    /// silently serving 200 while every model request fails.
    pub async fn health(&self) -> ConfigHealth {
        let last_error = self.shared.health.read().await.clone();
        let generation = self.shared.config.read().await.config_generation;
        ConfigHealth {
            ok: last_error.is_none(),
            last_error,
            failures: self.shared.failures.load(Ordering::Relaxed),
            last_applied: Some(*self.shared.last_applied.read().await),
            generation,
        }
    }

    /// Path of the watched config file (diagnostics only).
    pub fn config_path(&self) -> &str {
        &self.shared.path
    }

    /// Publish a failed load attempt: remember why, count it, and warn once per
    /// *distinct* reason so a 30s watchdog does not flood the log.
    async fn record_failure(shared: &Shared, error: ConfigError) {
        let mut guard = shared.health.write().await;
        let changed = match guard.as_ref() {
            Some(previous) => previous.kind != error.kind || previous.message != error.message,
            None => true,
        };
        *guard = Some(error.clone());
        drop(guard);

        let failures = shared.failures.fetch_add(1, Ordering::Relaxed) + 1;
        if changed {
            warn!(
                kind = error.kind.as_str(),
                failures,
                path = %shared.path,
                "config file is unusable ({}); serving the last good config and re-checking every {:?}: {}",
                error.kind.as_str(),
                CONFIG_POLL_INTERVAL,
                error.message
            );
        } else {
            debug!(
                kind = error.kind.as_str(),
                failures, "config file is still unusable: {}", error.message
            );
        }
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
    async fn reload_if_changed(shared: &Arc<Shared>) {
        let config_path = shared.path.as_str();
        let fingerprint = match Self::stat_fingerprint(config_path) {
            Ok(fp) => fp,
            Err(e) => {
                // The file is gone or unreadable (atomic rename in flight, NFS
                // hiccup, bad mount, permission change). The last good config
                // keeps serving, but this is an operator-visible failure:
                // record it so /health reports 503 instead of hiding it.
                Self::record_failure(
                    shared,
                    ConfigError {
                        kind: ConfigErrorKind::Read,
                        message: format!("cannot stat {config_path}: {e}"),
                    },
                )
                .await;
                return;
            }
        };

        // Fast path: this version is already live. Only taken while healthy —
        // if a failure is recorded we must re-validate even when the
        // fingerprint is unchanged. Otherwise restoring an identical file
        // (`mv` keeps mtime and size) would leave /health stuck at 503 forever.
        let healthy = shared.health.read().await.is_none();
        if healthy && *shared.last_applied.read().await == fingerprint {
            return;
        }

        // Parse outside the config write lock so a slow read (e.g. over NFS)
        // never stalls in-flight request handling. The error is stringified
        // up front because Box<dyn StdError> is not Send and must not be held
        // across the write-lock await below.
        match Self::load_config_typed(config_path) {
            Ok(mut new_config) => {
                let mut guard = shared.config.write().await;
                let mut applied = shared.last_applied.write().await;
                if *applied == fingerprint {
                    // This exact version is already live. We only get here while
                    // a failure is recorded (see the fast path above), so the
                    // content is fine again — clear the marker without swapping
                    // the config or bumping the generation.
                    drop(applied);
                    drop(guard);
                    if shared.health.write().await.take().is_some() {
                        info!("config file recovered; /health back to normal status");
                    }
                    return;
                }
                // Increment generation atomically with the config swap
                let next_gen = guard.config_generation.wrapping_add(1);
                new_config.config_generation = next_gen;
                *guard = new_config;
                *applied = fingerprint;
                drop(applied);
                drop(guard);

                // Clear the failure marker: the file is usable again.
                let recovered = shared.health.write().await.take().is_some();
                if recovered {
                    info!("config file recovered; /health back to normal status");
                }
                info!("Config reloaded successfully (generation {next_gen}).");
            }
            Err(error) => {
                Self::record_failure(shared, error).await;
            }
        }
    }

    async fn setup_watcher(shared: Arc<Shared>) -> NotifyResult<RecommendedWatcher> {
        let config_path_str = shared.path.clone();
        let config_path_for_check = config_path_str.clone();

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
                        let shared_task = shared.clone();
                        // Run the reload on the runtime so the watcher thread
                        // never blocks on NFS I/O; small delay lets the write settle.
                        runtime_handle.spawn(async move {
                            tokio::time::sleep(Duration::from_millis(100)).await;
                            Self::reload_if_changed(&shared_task).await;
                        });
                    }
                }
                Err(e) => error!("watch error: {e:?}"),
            }
        })?;

        // Add a path to be watched.
        // Watch the parent directory to handle atomic saves (rename/move) better
        let path_to_watch = Path::new(config_path_str.as_str())
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
    fn start_polling_task(shared: Arc<Shared>) {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(CONFIG_POLL_INTERVAL);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                // First tick fires immediately; the fingerprint fast path makes
                // it a no-op for the just-loaded config.
                ticker.tick().await;
                Self::reload_if_changed(&shared).await;
            }
        });
    }
}
#[cfg(test)]
mod tests_support {
    //! Fixtures shared by the config-manager test modules.
    use super::*;
    use std::path::{Path, PathBuf};

    pub(super) const VALID: &str = "openai_clients:\n  - name: \"a\"\n    base_url: \"http://127.0.0.1:1/v1\"\n    model_match: { type: \"exact\", value: [\"m\"] }\n";

    pub(super) fn temp_dir(tag: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("qq-cfgmgr-{tag}-{nanos}"));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    pub(super) fn write_config(dir: &Path, contents: &str) -> PathBuf {
        let path = dir.join("config.yaml");
        std::fs::write(&path, contents).unwrap();
        path
    }

    pub(super) fn as_str(path: &Path) -> String {
        path.to_string_lossy().into_owned()
    }

    /// Build the shared state the way `ConfigManager::new` does, without the
    /// watcher (tests drive `reload_if_changed` directly).
    ///
    /// The fingerprint is taken from a file that may already be gone, so it
    /// falls back to the "never applied" sentinel exactly like `new` does for
    /// an unreadable path.
    pub(super) fn shared_for(path: &Path, config: Config) -> Arc<Shared> {
        let fingerprint =
            ConfigManager::stat_fingerprint(&as_str(path)).unwrap_or((SystemTime::UNIX_EPOCH, 0));
        Arc::new(Shared {
            config: RwLock::new(config),
            health: RwLock::new(None),
            failures: AtomicU64::new(0),
            last_applied: RwLock::new(fingerprint),
            path: as_str(path),
        })
    }

    pub(super) async fn health_of(shared: &Arc<Shared>) -> ConfigHealth {
        let last_error = shared.health.read().await.clone();
        let generation = shared.config.read().await.config_generation;
        ConfigHealth {
            ok: last_error.is_none(),
            last_error,
            failures: shared.failures.load(Ordering::Relaxed),
            last_applied: Some(*shared.last_applied.read().await),
            generation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::tests_support::*;
    use super::*;

    #[test]
    fn empty_client_list_is_rejected() {
        let dir = temp_dir("empty");
        let path = write_config(&dir, "openai_clients: []\n");

        let error = ConfigManager::load_config_typed(&as_str(&path))
            .expect_err("a backend-less config must be refused");
        assert_eq!(error.kind, ConfigErrorKind::Validate);
        assert!(
            error.message.contains("no backend configured"),
            "{}",
            error.message
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn missing_file_reports_read_kind() {
        let dir = temp_dir("missing");
        let path = dir.join("absent.yaml");

        let error = ConfigManager::load_config_typed(&as_str(&path)).unwrap_err();
        assert_eq!(error.kind, ConfigErrorKind::Read);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn malformed_yaml_reports_parse_kind() {
        let dir = temp_dir("parse");
        let path = write_config(&dir, "openai_clients: \"not-a-list\"\n");

        let error = ConfigManager::load_config_typed(&as_str(&path)).unwrap_err();
        assert_eq!(error.kind, ConfigErrorKind::Parse);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn valid_config_loads_with_its_clients() {
        let dir = temp_dir("valid");
        let path = write_config(&dir, VALID);

        let config = ConfigManager::load_config_typed(&as_str(&path)).expect("valid config");
        assert_eq!(config.openai_clients.len(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn deleting_the_config_makes_health_unhealthy_then_recover() {
        let dir = temp_dir("health");
        let path = write_config(&dir, VALID);
        let config = ConfigManager::load_config_typed(&as_str(&path)).unwrap();
        let shared = shared_for(&path, config);

        assert!(health_of(&shared).await.ok, "baseline must be healthy");

        // Delete the file: the watchdog/reload must mark the config unusable
        // while the in-memory config keeps serving.
        std::fs::remove_file(&path).unwrap();
        ConfigManager::reload_if_changed(&shared).await;

        let health = health_of(&shared).await;
        assert!(!health.ok, "a deleted config must fail /health");
        assert_eq!(
            health.last_error.as_ref().unwrap().kind,
            ConfigErrorKind::Read
        );
        assert_eq!(health.failures, 1, "the failure counter must advance");
        assert_eq!(
            shared.config.read().await.openai_clients.len(),
            1,
            "the live config must survive a failed reload"
        );

        // Restore a valid file: the next reload clears the failure.
        write_config(&dir, &VALID.replace("\"a\"", "\"b\""));
        ConfigManager::reload_if_changed(&shared).await;
        let recovered = health_of(&shared).await;
        assert!(recovered.ok, "a successful reload must restore health");
        assert!(recovered.last_error.is_none());
        assert_eq!(recovered.failures, 1, "past failures stay counted");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn invalid_reload_keeps_previous_generation_and_reports_failure() {
        let dir = temp_dir("invalid");
        let path = write_config(&dir, VALID);
        let mut config = ConfigManager::load_config_typed(&as_str(&path)).unwrap();
        config.config_generation = 7;
        let shared = shared_for(&path, config);

        write_config(&dir, "openai_clients: [ oops\n");
        ConfigManager::reload_if_changed(&shared).await;

        let live = shared.config.read().await;
        assert_eq!(live.config_generation, 7, "failed reload must not swap");
        assert_eq!(live.openai_clients.len(), 1);
        drop(live);

        let health = health_of(&shared).await;
        assert!(!health.ok);
        assert_eq!(
            health.last_error.as_ref().unwrap().kind,
            ConfigErrorKind::Parse
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn empty_client_reload_is_reported_as_validation_failure() {
        let dir = temp_dir("emptyreload");
        let path = write_config(&dir, VALID);
        let config = ConfigManager::load_config_typed(&as_str(&path)).unwrap();
        let shared = shared_for(&path, config);

        write_config(&dir, "openai_clients: []\n");
        ConfigManager::reload_if_changed(&shared).await;

        let health = health_of(&shared).await;
        assert!(!health.ok, "a backend-less reload must fail /health");
        assert_eq!(
            health.last_error.as_ref().unwrap().kind,
            ConfigErrorKind::Validate
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn repeated_identical_failures_are_counted_but_not_relogged() {
        let dir = temp_dir("repeat");
        let path = dir.join("absent.yaml");
        let config_dir = temp_dir("repeat-src");
        let source = write_config(&config_dir, VALID);
        let config = ConfigManager::load_config_typed(&as_str(&source)).unwrap();
        let shared = shared_for(&path, config);

        for _ in 0..3 {
            ConfigManager::reload_if_changed(&shared).await;
        }
        let health = health_of(&shared).await;
        assert_eq!(health.failures, 3, "every attempt is counted");
        assert!(!health.ok);

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&config_dir);
    }

    #[test]
    fn config_error_display_includes_kind() {
        let error = ConfigError {
            kind: ConfigErrorKind::Validate,
            message: "boom".to_string(),
        };
        assert_eq!(error.to_string(), "validate: boom");
        assert_eq!(ConfigErrorKind::Read.as_str(), "read");
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::tests_support::*;
    use super::*;

    /// Restoring the *same* file content (same mtime, same size — what `mv`
    /// back does) must still clear the failure marker. Regression guard for
    /// the fingerprint fast path, which used to leave /health stuck at 503.
    #[tokio::test]
    async fn restoring_identical_content_clears_the_failure() {
        let dir = temp_dir("identical");
        let path = write_config(&dir, VALID);
        let mut config = ConfigManager::load_config_typed(&as_str(&path)).unwrap();
        // Mirror ConfigManager::new, which sets generation 1 for the first load.
        config.config_generation = 1;
        let shared = shared_for(&path, config);

        let backup = dir.join("config.yaml.bak");
        std::fs::rename(&path, &backup).unwrap();
        ConfigManager::reload_if_changed(&shared).await;
        assert!(
            !health_of(&shared).await.ok,
            "deleted config must be unhealthy"
        );

        // Restore with the original mtime/size preserved: fingerprint unchanged.
        std::fs::rename(&backup, &path).unwrap();
        ConfigManager::reload_if_changed(&shared).await;

        let health = health_of(&shared).await;
        assert!(health.ok, "identical restore must clear the failure");
        assert_eq!(
            shared.config.read().await.config_generation,
            1,
            "no swap/generation bump is needed when content is unchanged"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
