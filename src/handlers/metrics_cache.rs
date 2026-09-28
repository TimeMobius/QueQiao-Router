use super::MetricName;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum MetricValue {
    Integer(i64),
    Real(f64),
}

impl MetricValue {
    pub(super) fn number(self) -> f64 {
        match self {
            Self::Integer(value) => value as f64,
            Self::Real(value) => value,
        }
    }

    pub(super) fn combine(&mut self, other: Self, maximum: bool) {
        match (self, other) {
            (Self::Integer(left), Self::Integer(right)) => {
                *left = if maximum {
                    (*left).max(right)
                } else {
                    left.saturating_add(right)
                };
            }
            (Self::Real(left), Self::Real(right)) => {
                *left = if maximum {
                    left.max(right)
                } else {
                    *left + right
                };
            }
            _ => unreachable!("SQL metric type is fixed by the metric whitelist"),
        }
    }
}

pub(super) type GroupRows = HashMap<String, HashMap<MetricName, MetricValue>>;

const ARCHIVE_TTL: Duration = Duration::from_secs(3600);
const ACTIVE_TTL: Duration = Duration::from_secs(30);
const CAPACITY: usize = 64;
static CACHE: Lazy<Mutex<HashMap<String, (Instant, bool, GroupRows)>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

pub(super) fn get(key: &str) -> Option<GroupRows> {
    let cache = CACHE.lock().ok()?;
    let (created, active, rows) = cache.get(key)?;
    (created.elapsed() < ttl(*active)).then(|| rows.clone())
}

const fn ttl(active: bool) -> Duration {
    if active {
        ACTIVE_TTL
    } else {
        ARCHIVE_TTL
    }
}

pub(super) fn insert(key: String, active: bool, rows: &GroupRows) {
    if let Ok(mut cache) = CACHE.lock() {
        cache.retain(|_, (created, active, _)| created.elapsed() < ttl(*active));
        if cache.len() >= CAPACITY && !cache.contains_key(&key) {
            cache.clear();
        }
        cache.insert(key, (Instant::now(), active, rows.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn active_cache_expires_before_archive_cache() {
        let key = format!("metrics_ttl_test_{}", std::process::id());
        let rows = GroupRows::from([(
            "m".to_string(),
            HashMap::from([(MetricName::Requests, MetricValue::Integer(1))]),
        )]);
        insert(key.clone(), true, &rows);
        assert_eq!(get(&key), Some(rows.clone()));
        if let Ok(mut cache) = CACHE.lock() {
            cache.get_mut(&key).unwrap().0 = Instant::now() - Duration::from_secs(31);
        }
        assert!(get(&key).is_none());
        insert(key.clone(), false, &rows);
        if let Ok(mut cache) = CACHE.lock() {
            cache.get_mut(&key).unwrap().0 = Instant::now() - Duration::from_secs(31);
        }
        assert_eq!(get(&key), Some(rows));
    }
}
