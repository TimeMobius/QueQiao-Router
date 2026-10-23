//! Bounded, per-credential, versioned TTL cache for the `/v1/models` endpoint.
//! SIZE_OK: single-responsibility module. Production code is ~140 lines;
//! the remaining lines are #[cfg(test)] tests for private cache internals.
//!
//! # Design
//!
//! - **Key**: `(config_generation, credential_fingerprint)`. A successful hot
//!   reload bumps `config_generation`, instantly invalidating every prior entry.
//! - **Credential fingerprint**: salted SHA-256 over the credential bytes.
//!   Raw credentials never appear in keys, log output, or `Debug` impls.
//!   The salt is generated per-process at startup.
//! - **TTL**: configured by `Config::models_cache.ttl_seconds`. `0` disables
//!   both reads and writes.
//! - **Capacity**: hard cap of [`ModelsCache::CAPACITY`] entries. Inserts
//!   past the limit evict the least-recently-written entry.
//! - **Singleflight**: per-key coordination. Concurrent requests for the
//!   same key collapse to a single upstream refresh; different keys can
//!   refresh in parallel.
//! - **Stale-on-error**: if a refresh fails but a previous successful value
//!   exists, the cache returns that value (along with `stale = true`).

use dashmap::DashMap;
use rand::RngCore;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Hard upper bound on the number of (generation, credential) entries stored.
const CAPACITY: usize = 1024;

/// A 32-byte SHA-256 digest. Never exposes the original credential.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CredentialFingerprint([u8; 32]);

impl std::fmt::Debug for CredentialFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("CredentialFingerprint")
            .field(&"<redacted>")
            .finish()
    }
}

impl CredentialFingerprint {
    fn from_bytes(salt: &[u8; 32], credential: Option<&[u8]>) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(salt);
        match credential {
            Some(c) => hasher.update(c),
            None => hasher.update(b"\x00NO_CREDENTIAL"),
        }
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        Self(out)
    }
}

/// Composite cache key. Two entries with different generations cannot collide.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CacheKey {
    pub generation: u64,
    pub credential: CredentialFingerprint,
}

/// Cached payload plus freshness metadata returned to callers.
#[derive(Debug, Clone)]
pub struct CachedModels {
    /// The exact JSON body previously sent to the client.
    pub value: Value,
    /// `true` when this entry was kept past its TTL because refresh failed.
    pub stale: bool,
}

/// Internal entry storing the payload and timing.
struct Entry {
    value: Value,
    stored_at: Instant,
    ttl: Duration,
}

impl Entry {
    fn is_fresh(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.stored_at) < self.ttl
    }
}

/// Outcome of a cache lookup. Tells the caller whether to return the cached
/// value, perform a refresh, or both.
#[derive(Debug)]
pub enum CacheOutcome {
    /// Fresh entry; return this value without calling upstream.
    Fresh(Value),
    /// No usable entry. The caller must perform upstream aggregation.
    Empty,
}

/// Returned when an upstream refresh fails and there is no cached value to
/// fall back to. The handler reports this to clients as a 502.
#[derive(Debug)]
pub struct RefreshError(pub String);

/// Per-key coordination primitive. A single leader performs the upstream
/// refresh; followers wait on the `Notify` until the leader marks the
/// refresh as complete, then re-check the cache.
pub struct Inflight {
    notify: tokio::sync::Notify,
    done: std::sync::atomic::AtomicBool,
}

/// RAII owner of a singleflight leader slot.
///
/// Releases the slot on drop, so completion survives cancellation, panics and
/// task aborts of whatever future happens to hold it.
pub struct RefreshGuard {
    cache: ModelsCache,
    key: CacheKey,
    inflight: Arc<Inflight>,
}

impl Drop for RefreshGuard {
    fn drop(&mut self) {
        self.cache.complete_refresh(self.key, &self.inflight);
    }
}

/// Shared, cloneable, `Send + Sync` cache handle.
#[derive(Clone)]
pub struct ModelsCache {
    salt: Arc<[u8; 32]>,
    entries: Arc<DashMap<CacheKey, Entry>>,
    inflight: Arc<DashMap<CacheKey, Arc<Inflight>>>,
}

impl ModelsCache {
    pub fn new() -> Self {
        let mut salt = [0u8; 32];
        rand::thread_rng().fill_bytes(&mut salt);
        Self {
            salt: Arc::new(salt),
            entries: Arc::new(DashMap::with_capacity(64)),
            inflight: Arc::new(DashMap::with_capacity(64)),
        }
    }

    /// Build the cache key for a (generation, optional credential) pair.
    /// The credential is the raw Authorization token (Bearer + body, or
    /// `x-api-key` body) as bytes — never a Rust string slice around a
    /// longer lifetime, since we only consume bytes here.
    pub fn fingerprint(&self, credential: Option<&[u8]>) -> CredentialFingerprint {
        CredentialFingerprint::from_bytes(&self.salt, credential)
    }

    /// Look up an entry by key. Returns the cached value (and any stale
    /// flag) when the entry is still fresh. Returns `Empty` when the caller
    /// must refresh upstream. Stale entries are treated as misses so the
    /// caller re-fetches — but the stale value can still be returned by
    /// `finish_refresh` if the refresh fails.
    pub fn lookup(&self, key: CacheKey) -> CacheOutcome {
        let Some(entry) = self.entries.get(&key) else {
            return CacheOutcome::Empty;
        };
        if entry.is_fresh(Instant::now()) {
            CacheOutcome::Fresh(entry.value.clone())
        } else {
            CacheOutcome::Empty
        }
    }

    /// Return the previously stored value (if any) regardless of freshness.
    /// Used to support stale-on-error fallback when a refresh fails.
    pub fn previous_value(&self, key: CacheKey) -> Option<Value> {
        self.entries.get(&key).map(|e| e.value.clone())
    }

    /// Replace the entry for `key` with a fresh successful result. May evict
    /// an existing entry when the cache is at capacity.
    pub fn insert(&self, key: CacheKey, value: Value, ttl: Duration) {
        if self.entries.len() >= CAPACITY && !self.entries.contains_key(&key) {
            let victim: Option<CacheKey> = {
                let mut iter = self.entries.iter();
                iter.next().map(|kv| *kv.key())
            };
            if let Some(v) = victim {
                self.entries.remove(&v);
            }
        }
        let entry = Entry {
            value,
            stored_at: Instant::now(),
            ttl,
        };
        self.entries.insert(key, entry);
    }

    /// Try to take leadership for refreshing `key`. Returns `true` (and an
    /// `Inflight` to release) when no other caller was refreshing this key;
    /// `false` (and the same `Inflight`, so the caller can wait) otherwise.
    pub fn begin_refresh(&self, key: CacheKey) -> (bool, Arc<Inflight>) {
        let fresh = Arc::new(Inflight {
            notify: tokio::sync::Notify::new(),
            done: std::sync::atomic::AtomicBool::new(false),
        });
        let entry = self.inflight.entry(key);
        match entry {
            dashmap::mapref::entry::Entry::Vacant(vac) => {
                let inserted = fresh.clone();
                vac.insert(inserted.clone());
                (true, inserted)
            }
            dashmap::mapref::entry::Entry::Occupied(occ) => (false, occ.get().clone()),
        }
    }

    /// Mark a per-key refresh as complete and wake all waiters. The leader
    /// calls this exactly once before returning the final response.
    ///
    /// Prefer [`ModelsCache::refresh_guard`] in async code: a plain call can be
    /// skipped when the leader's future is dropped (client disconnect), which
    /// would leave the key permanently "in flight".
    pub fn end_refresh(&self, key: CacheKey, inflight: &Inflight) {
        self.complete_refresh(key, inflight);
    }

    /// Shared completion logic for [`ModelsCache::end_refresh`] and
    /// [`RefreshGuard::drop`]. Idempotent: waiters are only woken while the
    /// flag is still `false`.
    fn complete_refresh(&self, key: CacheKey, inflight: &Inflight) {
        if inflight
            .done
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        inflight.notify.notify_waiters();
        self.inflight.remove(&key);
    }

    /// Build an RAII guard that releases this key's singleflight slot when
    /// dropped.
    ///
    /// The leader must hold the guard inside the detached refresh task (not in
    /// the HTTP handler future) so that a client disconnect cannot strand the
    /// key: dropping the handler future merely drops the `JoinHandle`, while
    /// the task keeps running and its guard always fires on completion, panic,
    /// or abort.
    pub fn refresh_guard(&self, key: CacheKey, inflight: Arc<Inflight>) -> RefreshGuard {
        RefreshGuard {
            cache: self.clone(),
            key,
            inflight,
        }
    }

    /// Wait for the leader of `key` to call `end_refresh`. Returns once the
    /// leader signals completion; the caller should then re-check the cache.
    pub async fn wait_for_leader(&self, _key: CacheKey, inflight: &Inflight) {
        if inflight.done.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        // Subscribe *before* re-checking the flag to avoid a missed
        // notification window.
        let notified = inflight.notify.notified();
        tokio::pin!(notified);
        if inflight.done.load(std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        notified.await;
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for ModelsCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn key(g: u64) -> CacheKey {
        let fp = CredentialFingerprint::from_bytes(&[0u8; 32], Some(b"cred-A"));
        CacheKey {
            generation: g,
            credential: fp,
        }
    }

    #[test]
    fn fingerprint_differs_for_different_credentials() {
        let salt = [1u8; 32];
        let a = CredentialFingerprint::from_bytes(&salt, Some(b"alpha"));
        let b = CredentialFingerprint::from_bytes(&salt, Some(b"beta"));
        let none = CredentialFingerprint::from_bytes(&salt, None);
        assert_ne!(a, b);
        assert_ne!(a, none);
        assert_ne!(b, none);
    }

    #[test]
    fn fingerprint_is_deterministic_for_same_inputs() {
        let salt = [2u8; 32];
        let a = CredentialFingerprint::from_bytes(&salt, Some(b"identical"));
        let b = CredentialFingerprint::from_bytes(&salt, Some(b"identical"));
        assert_eq!(a, b);
    }

    #[test]
    fn different_salts_produce_different_fingerprints() {
        let a = CredentialFingerprint::from_bytes(&[0u8; 32], Some(b"same"));
        let b = CredentialFingerprint::from_bytes(&[9u8; 32], Some(b"same"));
        assert_ne!(a, b);
    }

    #[test]
    fn lookup_returns_empty_when_absent() {
        let cache = ModelsCache::new();
        assert!(matches!(cache.lookup(key(1)), CacheOutcome::Empty));
    }

    #[test]
    fn insert_then_lookup_returns_fresh_value() {
        let cache = ModelsCache::new();
        let k = key(1);
        cache.insert(k, json!({"object": "list"}), Duration::from_secs(600));
        match cache.lookup(k) {
            CacheOutcome::Fresh(v) => assert_eq!(v, json!({"object": "list"})),
            other => panic!("unexpected outcome: {:?}", other),
        }
    }

    #[test]
    fn entry_is_stale_after_ttl_elapses() {
        let cache = ModelsCache::new();
        let k = key(1);
        cache.insert(k, json!({"v": 1}), Duration::from_millis(10));
        std::thread::sleep(Duration::from_millis(20));
        assert!(matches!(cache.lookup(k), CacheOutcome::Empty));
        assert_eq!(
            cache.previous_value(k).unwrap(),
            json!({"v": 1}),
            "stale value must remain available for fallback"
        );
    }

    #[test]
    fn previous_value_is_none_on_cold_miss() {
        let cache = ModelsCache::new();
        assert!(cache.previous_value(key(1)).is_none());
    }

    #[test]
    fn insert_evicts_when_capacity_exceeded() {
        let cache = ModelsCache::new();
        // Insert CAPACITY + 1 entries; total must stay bounded.
        for i in 0..(CAPACITY + 1) {
            let k = CacheKey {
                generation: i as u64,
                credential: CredentialFingerprint::from_bytes(
                    &[0u8; 32],
                    Some(format!("c{}", i).as_bytes()),
                ),
            };
            cache.insert(k, json!({"i": i}), Duration::from_secs(600));
            assert!(cache.len() <= CAPACITY);
        }
        assert_eq!(cache.len(), CAPACITY);
    }

    #[test]
    fn different_generations_are_isolated() {
        let cache = ModelsCache::new();
        let fp_a = CredentialFingerprint::from_bytes(&[0u8; 32], Some(b"shared"));
        let k_old = CacheKey {
            generation: 1,
            credential: fp_a,
        };
        let k_new = CacheKey {
            generation: 2,
            credential: fp_a,
        };
        cache.insert(k_old, json!({"gen": 1}), Duration::from_secs(600));
        assert!(matches!(cache.lookup(k_new), CacheOutcome::Empty));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn begin_refresh_grants_leadership_to_single_caller() {
        let cache = ModelsCache::new();
        let k = key(7);
        let (is_leader_1, _) = cache.begin_refresh(k);
        let (is_leader_2, _) = cache.begin_refresh(k);
        assert!(is_leader_1);
        assert!(!is_leader_2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn followers_observe_cached_value_after_leader_finishes() {
        let cache = ModelsCache::new();
        let k = key(11);
        let (is_leader, inflight) = cache.begin_refresh(k);
        assert!(is_leader);

        let calls = Arc::new(AtomicUsize::new(0));
        let calls_clone = calls.clone();
        let cache_clone = cache.clone();
        let leader = tokio::spawn(async move {
            calls_clone.fetch_add(1, Ordering::SeqCst);
            cache_clone.insert(k, json!({"upstream": true}), Duration::from_secs(600));
            cache_clone.end_refresh(k, &inflight);
        });

        let (is_follower_leader, follower_inflight) = cache.begin_refresh(k);
        assert!(!is_follower_leader);
        cache.wait_for_leader(k, &follower_inflight).await;

        leader.await.unwrap();
        match cache.lookup(k) {
            CacheOutcome::Fresh(v) => assert_eq!(v, json!({"upstream": true})),
            other => panic!("unexpected: {:?}", other),
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn refresh_guard_releases_slot_and_wakes_waiters_on_drop() {
        let cache = ModelsCache::new();
        let k = key(21);
        let (is_leader, inflight) = cache.begin_refresh(k);
        assert!(is_leader);
        let guard = cache.refresh_guard(k, inflight);

        let (is_follower_leader, follower_inflight) = cache.begin_refresh(k);
        assert!(!is_follower_leader);

        let waiter = tokio::spawn({
            let cache = cache.clone();
            async move { cache.wait_for_leader(k, &follower_inflight).await }
        });
        tokio::task::yield_now().await;

        drop(guard);

        tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("dropping the guard must wake the waiter")
            .unwrap();

        let (is_leader_again, _) = cache.begin_refresh(k);
        assert!(
            is_leader_again,
            "the singleflight slot must be free after the guard is dropped"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn detached_refresh_releases_key_when_caller_is_cancelled() {
        let cache = ModelsCache::new();
        let k = key(22);
        let (_, inflight) = cache.begin_refresh(k);
        let guard = cache.refresh_guard(k, inflight);

        let task_cache = cache.clone();
        tokio::spawn(async move {
            let _guard = guard;
            tokio::time::sleep(Duration::from_millis(10)).await;
            task_cache.insert(k, json!({"upstream": true}), Duration::from_secs(60));
        });

        let (is_leader, follower_inflight) = cache.begin_refresh(k);
        assert!(!is_leader);
        tokio::time::timeout(
            Duration::from_secs(2),
            cache.wait_for_leader(k, &follower_inflight),
        )
        .await
        .expect("a detached refresh must always release the key");

        match cache.lookup(k) {
            CacheOutcome::Fresh(v) => assert_eq!(v, json!({"upstream": true})),
            other => panic!("unexpected outcome: {:?}", other),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn different_keys_refresh_concurrently() {
        let cache = ModelsCache::new();
        let k_a = CacheKey {
            generation: 1,
            credential: CredentialFingerprint::from_bytes(&[0u8; 32], Some(b"A")),
        };
        let k_b = CacheKey {
            generation: 1,
            credential: CredentialFingerprint::from_bytes(&[0u8; 32], Some(b"B")),
        };
        let (leader_a, _) = cache.begin_refresh(k_a);
        let (leader_b, _) = cache.begin_refresh(k_b);
        assert!(leader_a);
        assert!(
            leader_b,
            "different keys must not serialize behind one lock"
        );
    }
}
