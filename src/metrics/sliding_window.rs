use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{Duration, Instant};

struct SlidingWindow {
    window_duration: Duration,
    data: Mutex<VecDeque<(f64, Instant)>>,
}

impl SlidingWindow {
    fn new(window_duration: Duration) -> Self {
        Self {
            window_duration,
            data: Mutex::new(VecDeque::new()),
        }
    }

    fn push(&self, value: f64) {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();

        // 移除过期数据（按时间窗口）
        while let Some((_, timestamp)) = data.front() {
            if now.duration_since(*timestamp) >= self.window_duration {
                data.pop_front();
            } else {
                break;
            }
        }

        data.push_back((value, now));
    }

    fn max(&self) -> f64 {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();

        // 先清理过期数据
        while let Some((_, timestamp)) = data.front() {
            if now.duration_since(*timestamp) >= self.window_duration {
                data.pop_front();
            } else {
                break;
            }
        }

        data.iter().map(|(v, _)| *v).fold(f64::NAN, f64::max)
    }

    fn avg(&self) -> f64 {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();

        // 先清理过期数据
        while let Some((_, timestamp)) = data.front() {
            if now.duration_since(*timestamp) >= self.window_duration {
                data.pop_front();
            } else {
                break;
            }
        }

        if data.is_empty() {
            return 0.0;
        }
        data.iter().map(|(v, _)| *v).sum::<f64>() / data.len() as f64
    }

    fn count(&self) -> usize {
        let mut data = self.data.lock().unwrap();
        let now = Instant::now();

        while let Some((_, timestamp)) = data.front() {
            if now.duration_since(*timestamp) >= self.window_duration {
                data.pop_front();
            } else {
                break;
            }
        }

        data.len()
    }
}

type WindowMap = DashMap<String, SlidingWindow>;

static TTFT_WINDOWS_1M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static TTFT_WINDOWS_10M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static TTFT_WINDOWS_1H: Lazy<WindowMap> = Lazy::new(|| DashMap::new());

static LATENCY_WINDOWS_1M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static LATENCY_WINDOWS_10M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static LATENCY_WINDOWS_1H: Lazy<WindowMap> = Lazy::new(|| DashMap::new());

static TPS_WINDOWS_1M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static TPS_WINDOWS_10M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static TPS_WINDOWS_1H: Lazy<WindowMap> = Lazy::new(|| DashMap::new());

static RPS_WINDOWS_1M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static RPS_WINDOWS_10M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static RPS_WINDOWS_1H: Lazy<WindowMap> = Lazy::new(|| DashMap::new());

static ACTIVE_WINDOWS_1M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static ACTIVE_WINDOWS_10M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static ACTIVE_WINDOWS_1H: Lazy<WindowMap> = Lazy::new(|| DashMap::new());

static SUCCESS_WINDOWS_1M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static SUCCESS_WINDOWS_10M: Lazy<WindowMap> = Lazy::new(|| DashMap::new());
static SUCCESS_WINDOWS_1H: Lazy<WindowMap> = Lazy::new(|| DashMap::new());

static SUCCESS_WINDOW_OVERALL: Lazy<SlidingWindow> =
    Lazy::new(|| SlidingWindow::new(Duration::from_secs(3600)));

fn make_key(model: &str, backend: &str) -> String {
    format!("{}:{}", model, backend)
}

fn get_or_create_window(map: &WindowMap, key: &str, window_duration: Duration) {
    if !map.contains_key(key) {
        map.entry(key.to_string())
            .or_insert_with(|| SlidingWindow::new(window_duration));
    }
}

fn get_window_ref<'a>(
    map: &'a WindowMap,
    key: &str,
) -> dashmap::mapref::one::Ref<'a, String, SlidingWindow> {
    map.get(key).unwrap()
}

pub fn update_ttft_windows(value: f64, model: &str, backend: &str) {
    let key = make_key(model, backend);
    get_or_create_window(&TTFT_WINDOWS_1M, &key, Duration::from_secs(60));
    get_window_ref(&TTFT_WINDOWS_1M, &key).push(value);
    get_or_create_window(&TTFT_WINDOWS_10M, &key, Duration::from_secs(600));
    get_window_ref(&TTFT_WINDOWS_10M, &key).push(value);
    get_or_create_window(&TTFT_WINDOWS_1H, &key, Duration::from_secs(3600));
    get_window_ref(&TTFT_WINDOWS_1H, &key).push(value);
}

pub fn update_latency_windows(value: f64, model: &str, backend: &str) {
    let key = make_key(model, backend);
    get_or_create_window(&LATENCY_WINDOWS_1M, &key, Duration::from_secs(60));
    get_window_ref(&LATENCY_WINDOWS_1M, &key).push(value);
    get_or_create_window(&LATENCY_WINDOWS_10M, &key, Duration::from_secs(600));
    get_window_ref(&LATENCY_WINDOWS_10M, &key).push(value);
    get_or_create_window(&LATENCY_WINDOWS_1H, &key, Duration::from_secs(3600));
    get_window_ref(&LATENCY_WINDOWS_1H, &key).push(value);
}

pub fn update_tps_windows(value: f64, model: &str, backend: &str) {
    let key = make_key(model, backend);
    get_or_create_window(&TPS_WINDOWS_1M, &key, Duration::from_secs(60));
    get_window_ref(&TPS_WINDOWS_1M, &key).push(value);
    get_or_create_window(&TPS_WINDOWS_10M, &key, Duration::from_secs(600));
    get_window_ref(&TPS_WINDOWS_10M, &key).push(value);
    get_or_create_window(&TPS_WINDOWS_1H, &key, Duration::from_secs(3600));
    get_window_ref(&TPS_WINDOWS_1H, &key).push(value);
}

pub fn update_active_windows(value: f64, model: &str, backend: &str) {
    let key = make_key(model, backend);
    get_or_create_window(&ACTIVE_WINDOWS_1M, &key, Duration::from_secs(60));
    get_window_ref(&ACTIVE_WINDOWS_1M, &key).push(value);
    get_or_create_window(&ACTIVE_WINDOWS_10M, &key, Duration::from_secs(600));
    get_window_ref(&ACTIVE_WINDOWS_10M, &key).push(value);
    get_or_create_window(&ACTIVE_WINDOWS_1H, &key, Duration::from_secs(3600));
    get_window_ref(&ACTIVE_WINDOWS_1H, &key).push(value);
}

pub fn update_success_windows(success: bool, model: &str, backend: &str) {
    let key = make_key(model, backend);
    let value = if success { 1.0 } else { 0.0 };
    get_or_create_window(&SUCCESS_WINDOWS_1M, &key, Duration::from_secs(60));
    get_window_ref(&SUCCESS_WINDOWS_1M, &key).push(value);
    get_or_create_window(&SUCCESS_WINDOWS_10M, &key, Duration::from_secs(600));
    get_window_ref(&SUCCESS_WINDOWS_10M, &key).push(value);
    get_or_create_window(&SUCCESS_WINDOWS_1H, &key, Duration::from_secs(3600));
    get_window_ref(&SUCCESS_WINDOWS_1H, &key).push(value);
}

pub fn get_ttft_1m_avg(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    TTFT_WINDOWS_1M.get(&key).map(|w| w.avg()).unwrap_or(0.0)
}

pub fn get_ttft_1m_max(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    TTFT_WINDOWS_1M.get(&key).map(|w| w.max()).unwrap_or(0.0)
}
pub fn get_ttft_10m_max(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    TTFT_WINDOWS_10M.get(&key).map(|w| w.max()).unwrap_or(0.0)
}
pub fn get_ttft_1h_max(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    TTFT_WINDOWS_1H.get(&key).map(|w| w.max()).unwrap_or(0.0)
}

pub fn get_latency_1m_avg(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    LATENCY_WINDOWS_1M.get(&key).map(|w| w.avg()).unwrap_or(0.0)
}

pub fn get_latency_1m_max(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    LATENCY_WINDOWS_1M.get(&key).map(|w| w.max()).unwrap_or(0.0)
}
pub fn get_latency_10m_max(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    LATENCY_WINDOWS_10M
        .get(&key)
        .map(|w| w.max())
        .unwrap_or(0.0)
}
pub fn get_latency_1h_max(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    LATENCY_WINDOWS_1H.get(&key).map(|w| w.max()).unwrap_or(0.0)
}

pub fn get_tps_1m_avg(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    TPS_WINDOWS_1M.get(&key).map(|w| w.avg()).unwrap_or(0.0)
}
pub fn get_tps_10m_avg(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    TPS_WINDOWS_10M.get(&key).map(|w| w.avg()).unwrap_or(0.0)
}
pub fn get_tps_1h_avg(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    TPS_WINDOWS_1H.get(&key).map(|w| w.avg()).unwrap_or(0.0)
}

pub fn get_active_1m_max(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    ACTIVE_WINDOWS_1M.get(&key).map(|w| w.max()).unwrap_or(0.0)
}
pub fn get_active_10m_max(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    ACTIVE_WINDOWS_10M.get(&key).map(|w| w.max()).unwrap_or(0.0)
}
pub fn get_active_1h_max(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    ACTIVE_WINDOWS_1H.get(&key).map(|w| w.max()).unwrap_or(0.0)
}

pub fn get_success_1m(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    SUCCESS_WINDOWS_1M.get(&key).map(|w| w.avg()).unwrap_or(0.0)
}
pub fn get_success_10m(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    SUCCESS_WINDOWS_10M
        .get(&key)
        .map(|w| w.avg())
        .unwrap_or(0.0)
}
pub fn get_success_1h(model: &str, backend: &str) -> f64 {
    let key = make_key(model, backend);
    SUCCESS_WINDOWS_1H.get(&key).map(|w| w.avg()).unwrap_or(0.0)
}

pub fn update_success_overall(success: bool) {
    SUCCESS_WINDOW_OVERALL.push(if success { 1.0 } else { 0.0 });
}

pub fn get_success_overall() -> f64 {
    SUCCESS_WINDOW_OVERALL.avg()
}

pub fn update_rps_windows(endpoint: &str) {
    get_or_create_window(&RPS_WINDOWS_1M, endpoint, Duration::from_secs(60));
    RPS_WINDOWS_1M.get(endpoint).unwrap().push(1.0);
    get_or_create_window(&RPS_WINDOWS_10M, endpoint, Duration::from_secs(600));
    RPS_WINDOWS_10M.get(endpoint).unwrap().push(1.0);
    get_or_create_window(&RPS_WINDOWS_1H, endpoint, Duration::from_secs(3600));
    RPS_WINDOWS_1H.get(endpoint).unwrap().push(1.0);
}

fn get_rps(window: &WindowMap, endpoint: &str, window_secs: f64) -> f64 {
    window
        .get(endpoint)
        .map(|w| w.count() as f64 / window_secs)
        .unwrap_or(0.0)
}

pub fn get_rps_1m(endpoint: &str) -> f64 {
    get_rps(&RPS_WINDOWS_1M, endpoint, 60.0)
}

pub fn get_rps_10m(endpoint: &str) -> f64 {
    get_rps(&RPS_WINDOWS_10M, endpoint, 600.0)
}

pub fn get_rps_1h(endpoint: &str) -> f64 {
    get_rps(&RPS_WINDOWS_1H, endpoint, 3600.0)
}
