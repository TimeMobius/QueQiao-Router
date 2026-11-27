//! 实时监控数据（供 /dashboard 3D 面板使用）。
//!
//! 设计约束：**不能影响网关主链路性能**。
//! - 没有面板订阅、且最近 10s 内没人拉过快照时，所有 emit 都是「一次原子读 + 返回」；
//! - 采样在独立 tokio 任务里每 2s 跑一次，只读 `prometheus::gather()` 的 proto 数据；
//! - 环形缓冲有固定上限，事件只保留最近 200 条。

use once_cell::sync::Lazy;
use serde::Serialize;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

const RECENT_CAP: usize = 200;
const BUS_CAP: usize = 256;
const IDLE_AFTER_MS: i64 = 10_000;
const SAMPLE_MS: i64 = 2_000;
const RATE_WINDOW_MS: i64 = 60_000;
const PCT_WINDOW_MS: i64 = 300_000;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

/// 单条请求的标签级元信息（来自审计 Record，不需要额外解析）。
#[derive(Clone, Debug, Default, Serialize)]
pub struct RequestMeta {
    pub ts_ms: i64,
    pub model: String,
    pub backend: String,
    pub endpoint: String,
    pub format: String,
    pub status: i64,
    pub latency_ms: f64,
    pub ttft_ms: Option<f64>,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub ctx_bytes: i64,
    pub multimodal: bool,
    pub tools: bool,
    pub tool_count: i64,
    pub images: i64,
    pub messages: i64,
    pub stream: bool,
    pub error: Option<String>,
    pub request_id: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(tag = "kind")]
pub enum LiveEvent {
    #[serde(rename = "start")]
    Start {
        ts_ms: i64,
        model: String,
        backend: String,
    },
    #[serde(rename = "end")]
    End { meta: RequestMeta },
}

static BUS: Lazy<broadcast::Sender<Arc<LiveEvent>>> = Lazy::new(|| broadcast::channel(BUS_CAP).0);
static RECENT: Lazy<Mutex<VecDeque<Arc<RequestMeta>>>> =
    Lazy::new(|| Mutex::new(VecDeque::with_capacity(RECENT_CAP)));
static LAST_READ: AtomicI64 = AtomicI64::new(0);
/// 由采样器每 2s 更新的近似当前时间，热路径上避免取系统时钟。
static NOW_MS: AtomicI64 = AtomicI64::new(0);

/// 有人在看面板吗？（订阅者存在，或最近 10s 拉过快照）
#[inline]
pub fn active() -> bool {
    BUS.receiver_count() > 0
        || NOW_MS.load(Ordering::Relaxed) - LAST_READ.load(Ordering::Relaxed) < IDLE_AFTER_MS
}

pub fn subscribe() -> broadcast::Receiver<Arc<LiveEvent>> {
    BUS.subscribe()
}

/// 请求开始（调度器选定后端时调用）。
#[inline]
pub fn emit_start(model: &str, backend: &str) {
    if !active() {
        return;
    }
    let _ = BUS.send(Arc::new(LiveEvent::Start {
        ts_ms: now_ms(),
        model: model.to_string(),
        backend: backend.to_string(),
    }));
}

/// 请求结束（审计记录构造完成后调用）。
pub fn emit_record(meta: RequestMeta) {
    if !active() {
        return;
    }
    let arc = Arc::new(meta);
    if let Ok(mut buf) = RECENT.lock() {
        if buf.len() >= RECENT_CAP {
            buf.pop_front();
        }
        buf.push_back(arc.clone());
    }
    let _ = BUS.send(Arc::new(LiveEvent::End {
        meta: (*arc).clone(),
    }));
}

fn recent_since(since_ms: i64) -> Vec<RequestMeta> {
    let Ok(buf) = RECENT.lock() else {
        return Vec::new();
    };
    buf.iter()
        .filter(|m| m.ts_ms >= since_ms)
        .map(|m| (**m).clone())
        .collect()
}

// ---------------------------------------------------------------------------
// 采样器
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq)]
enum Fam {
    Requests,
    Active,
    Errors,
    Tokens,
    Tps,
    TtftBucket,
    LatBucket,
}

struct Sample {
    ts: i64,
    values: HashMap<String, f64>,
    caps: HashMap<String, f64>,
}

fn key(fam: Fam, model: &str, backend: &str, extra: &str, le: f64) -> String {
    format!("{:?}\u{1}{model}\u{1}{backend}\u{1}{extra}\u{1}{le}", fam)
}

fn split_key(k: &str) -> (String, String, String, String, f64) {
    let mut it = k.split('\u{1}');
    let _fam = it.next().unwrap_or_default();
    let model = it.next().unwrap_or_default().to_string();
    let backend = it.next().unwrap_or_default().to_string();
    let extra = it.next().unwrap_or_default().to_string();
    let le = it.next().unwrap_or("0").parse::<f64>().unwrap_or(0.0);
    (model, backend, extra, String::new(), le)
}

fn flatten() -> (HashMap<String, f64>, HashMap<String, f64>) {
    use prometheus::proto::MetricType;
    let mut values: HashMap<String, f64> = HashMap::new();
    let mut caps: HashMap<String, f64> = HashMap::new();
    for mf in prometheus::gather() {
        let name = mf.get_name();
        for m in mf.get_metric() {
            let labels: HashMap<&str, &str> = m
                .get_label()
                .iter()
                .map(|l| (l.get_name(), l.get_value()))
                .collect();
            let model = labels.get("model").copied().unwrap_or("");
            let backend = labels.get("backend").copied().unwrap_or("");
            match name {
                "gpt_api_requests_total" => {
                    let status = labels.get("status").copied().unwrap_or("");
                    let v = m.get_counter().get_value();
                    *values
                        .entry(key(Fam::Requests, model, backend, status, 0.0))
                        .or_default() += v;
                }
                "gpt_api_active_requests" => {
                    let v = m.get_gauge().get_value();
                    *values
                        .entry(key(Fam::Active, model, backend, "", 0.0))
                        .or_default() += v;
                }
                "gpt_api_active_requests_1h_max" => {
                    let v = m.get_gauge().get_value();
                    *caps
                        .entry(key(Fam::Active, model, backend, "", 0.0))
                        .or_default() += v;
                }
                "gpt_api_errors_total" => {
                    let ty = labels.get("error_type").copied().unwrap_or("");
                    let v = m.get_counter().get_value();
                    *values
                        .entry(key(Fam::Errors, model, backend, ty, 0.0))
                        .or_default() += v;
                }
                "gpt_api_tokens_total" => {
                    let ty = labels.get("type").copied().unwrap_or("");
                    let v = m.get_counter().get_value();
                    *values
                        .entry(key(Fam::Tokens, model, backend, ty, 0.0))
                        .or_default() += v;
                }
                "gpt_api_tps_1m_avg" => {
                    let v = m.get_gauge().get_value();
                    values
                        .entry(key(Fam::Tps, model, backend, "", 0.0))
                        .or_insert(v);
                }
                "gpt_api_ttft_seconds_bucket" => {
                    let le = labels
                        .get("le")
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(f64::INFINITY);
                    let v = m.get_histogram().get_bucket().iter().fold(0.0, |acc, b| {
                        if (b.get_upper_bound() - le).abs() < 1e-9 {
                            b.get_cumulative_count() as f64
                        } else {
                            acc
                        }
                    });
                    values.insert(key(Fam::TtftBucket, model, backend, "", le), v);
                }
                "gpt_api_latency_seconds_bucket" => {
                    let le = labels
                        .get("le")
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(f64::INFINITY);
                    let v = m.get_histogram().get_bucket().iter().fold(0.0, |acc, b| {
                        if (b.get_upper_bound() - le).abs() < 1e-9 {
                            b.get_cumulative_count() as f64
                        } else {
                            acc
                        }
                    });
                    values.insert(key(Fam::LatBucket, model, backend, "", le), v);
                }
                _ => {
                    let _ = MetricType::COUNTER;
                }
            }
        }
    }
    (values, caps)
}

fn quantile(buckets: &[(f64, f64)], p: f64) -> Option<f64> {
    let total = buckets.last().map(|b| b.1).unwrap_or(0.0);
    if total <= 0.0 {
        return None;
    }
    let target = total * p;
    let mut prev_le = 0.0;
    let mut prev_c = 0.0;
    for &(le, c) in buckets {
        if c >= target {
            if !le.is_finite() {
                return Some(prev_le);
            }
            let span = c - prev_c;
            let frac = if span > 0.0 {
                (target - prev_c) / span
            } else {
                0.0
            };
            return Some(prev_le + frac * (le - prev_le));
        }
        if le.is_finite() {
            prev_le = le;
        }
        prev_c = c;
    }
    Some(prev_le)
}

fn pct_buckets(
    oldest: &HashMap<String, f64>,
    newest: &HashMap<String, f64>,
    fam: Fam,
    model: &str,
) -> Vec<(f64, f64)> {
    let mut acc: HashMap<u64, (f64, f64)> = HashMap::new();
    for (k, v) in newest {
        if !k.starts_with(&format!("{:?}\u{1}", fam)) {
            continue;
        }
        let (m, _backend, _extra, _s, le) = split_key(k);
        if m != model {
            continue;
        }
        let base = oldest.get(k).copied().unwrap_or(0.0);
        let delta = (v - base).max(0.0);
        let sort = le.to_bits();
        let e = acc.entry(sort).or_insert((le, 0.0));
        e.1 += delta;
    }
    let mut out: Vec<(f64, f64)> = acc.values().copied().collect();
    out.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    out
}

fn level_of(busy: f64) -> (&'static str, &'static str) {
    if busy >= 75.0 {
        ("over", "过载")
    } else if busy >= 50.0 {
        ("busy", "繁忙")
    } else if busy >= 25.0 {
        ("normal", "正常")
    } else {
        ("smooth", "流畅")
    }
}

#[derive(Default)]
struct Acc {
    req: f64,
    http_err: f64,
    int_err: f64,
    active: f64,
    cap: f64,
    tps: f64,
    tokens_out: f64,
}

fn acc_mut<'a>(per: &'a mut HashMap<String, Acc>, model: &str, backend: &str) -> &'a mut Acc {
    per.entry(format!("{model}\u{1}{backend}")).or_default()
}

fn build_snapshot(samples: &VecDeque<Sample>) -> serde_json::Value {
    let Some(newest) = samples.back() else {
        return serde_json::json!({"ready": false});
    };
    let now = newest.ts;
    let rate_old = samples
        .iter()
        .find(|s| s.ts >= now - RATE_WINDOW_MS)
        .unwrap_or(newest);
    let pct_old = samples.front().unwrap_or(newest);
    let dt = ((now - rate_old.ts) as f64 / 1000.0).max(1.0);

    // (model, backend) -> counters
    let mut per: HashMap<String, Acc> = HashMap::new();
    let mut models: Vec<String> = Vec::new();

    for (k, v) in &newest.values {
        let (model, backend, extra, _s, _le) = split_key(k);
        if model.is_empty() || model == "-" || model == "unknown" {
            continue;
        }
        if !models.contains(&model) {
            models.push(model.clone());
        }
        let base = rate_old.values.get(k).copied().unwrap_or(0.0);
        let delta = (v - base).max(0.0);
        if k.starts_with("Requests") {
            let e = acc_mut(&mut per, &model, &backend);
            e.req += delta;
            if extra.starts_with('4') || extra.starts_with('5') {
                e.http_err += delta;
            }
        } else if k.starts_with("Errors") {
            if extra != "client_error" {
                acc_mut(&mut per, &model, &backend).int_err += delta;
            }
        } else if k.starts_with("Tokens") && extra == "completion" {
            acc_mut(&mut per, &model, &backend).tokens_out += delta;
        }
    }
    for (k, v) in &newest.values {
        let (model, backend, _e, _s, _le) = split_key(k);
        if k.starts_with("Active") {
            acc_mut(&mut per, &model, &backend).active = *v;
        } else if k.starts_with("Tps") {
            let e = acc_mut(&mut per, &model, &backend);
            if *v > e.tps {
                e.tps = *v;
            }
        }
    }
    for (k, v) in &newest.caps {
        let (model, backend, _e, _s, _le) = split_key(k);
        acc_mut(&mut per, &model, &backend).cap = *v;
    }

    // 按模型聚合
    let mut model_rows = Vec::new();
    for model in &models {
        let mut total_req = 0.0;
        let mut active = 0.0;
        let mut http_err = 0.0;
        let mut int_err = 0.0;
        let mut out_tok = 0.0;
        let mut max_tps: f64 = 0.0;
        let mut backends: Vec<serde_json::Value> = Vec::new();
        let mut shares: HashMap<String, f64> = HashMap::new();
        for (k, acc) in &per {
            // per 的 key 是 "model\u{1}backend"（没有 family 前缀）
            let mut it = k.split('\u{1}');
            let m = it.next().unwrap_or("");
            let backend = it.next().unwrap_or("");
            if m != model || backend.is_empty() || backend == "-" || backend == "unknown" {
                continue;
            }
            total_req += acc.req;
            active += acc.active;
            http_err += acc.http_err;
            int_err += acc.int_err;
            out_tok += acc.tokens_out;
            max_tps = max_tps.max(acc.tps);
            shares.insert(backend.to_string(), acc.req);
            backends.push(serde_json::json!({
                "name": backend,
                "req": acc.req.round(),
                "share": 0.0,
                "active": acc.active.round() as i64,
                "decode": (acc.tps * 10.0).round() / 10.0,
            }));
        }
        if total_req <= 0.0 && active <= 0.0 {
            continue;
        }
        for b in backends.iter_mut() {
            let name = b.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let share = if total_req > 0.0 {
                shares.get(name).copied().unwrap_or(0.0) / total_req
            } else {
                0.0
            };
            b["share"] = serde_json::json!((share * 1000.0).round() / 1000.0);
        }
        backends.sort_by(|a, b| {
            let sa = a.get("share").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let sb = b.get("share").and_then(|v| v.as_f64()).unwrap_or(0.0);
            sb.partial_cmp(&sa).unwrap_or(std::cmp::Ordering::Equal)
        });
        let ttft_b = pct_buckets(&pct_old.values, &newest.values, Fam::TtftBucket, model);
        let p50 = quantile(&ttft_b, 0.5);
        let p95 = quantile(&ttft_b, 0.95);
        let http_rate = if total_req > 0.0 {
            http_err / total_req
        } else {
            0.0
        };
        let err_rate = if total_req > 0.0 {
            (http_err + int_err) / total_req
        } else {
            0.0
        };
        let success = 1.0 - http_rate;

        let nb = backends.len().max(1) as f64;
        let cap_ref = (active).max(nb * 4.0).max(1.0);
        let load = (active / cap_ref).min(1.0);
        let ttft_score = p95
            .map(|t| ((1.0_f64 + t / 3.0).ln() / (1.0_f64 + 60.0 / 3.0).ln()).min(1.0))
            .unwrap_or(0.0);
        let speed = (1.0 - (max_tps / 120.0)).clamp(0.0, 1.0);
        let busy = 100.0 * (0.35 * load + 0.25 * ttft_score + 0.20 * speed + 0.20 * 0.0);
        let (level, level_cn) = level_of(busy);
        let reason = if http_rate > 0.10 || err_rate > 0.10 {
            format!(
                "错误率 {:.1}%（用户可见 {:.1}%）",
                err_rate * 100.0,
                http_rate * 100.0
            )
        } else if load > 0.0 {
            format!(
                "并发压力 {:.0}% · TTFT {:.0}% · TPS效率 {:.0}%",
                load * 100.0,
                ttft_score * 100.0,
                speed * 100.0
            )
        } else {
            String::new()
        };
        let status = if http_rate > 0.10 || err_rate > 0.10 {
            "jam"
        } else if busy >= 75.0 {
            "jam"
        } else if busy >= 50.0 {
            "warn"
        } else {
            "ok"
        };

        model_rows.push(serde_json::json!({
            "id": model,
            "name": model,
            "family": "all",
            "rpm": (total_req * 60.0 / dt * 100.0).round() / 100.0,
            "active": active.round() as i64,
            "ttftP50": p50.map(|v| (v * 100.0).round() / 100.0),
            "ttftP95": p95.map(|v| (v * 100.0).round() / 100.0),
            "decode": (max_tps * 10.0).round() / 10.0,
            "outTok": out_tok.round() * 60.0 / dt,
            "successRate": ((success * 10000.0).round() / 10000.0),
            "errRate": ((err_rate * 10000.0).round() / 10000.0),
            "status": status,
            "level": level,
            "levelCn": level_cn,
            "reason": reason,
            "busy": (busy * 10.0).round() / 10.0,
            "parts": {
                "traffic": 0.0,
                "load": (load * 1000.0).round() / 1000.0,
                "ttft": (ttft_score * 1000.0).round() / 1000.0,
                "speed": (speed * 1000.0).round() / 1000.0,
            },
            "backends": backends,
        }));
    }
    model_rows.sort_by(|a, b| {
        let ra = a.get("rpm").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let rb = b.get("rpm").and_then(|v| v.as_f64()).unwrap_or(0.0);
        rb.partial_cmp(&ra).unwrap_or(std::cmp::Ordering::Equal)
    });

    let total_rpm: f64 = model_rows
        .iter()
        .map(|m| m.get("rpm").and_then(|v| v.as_f64()).unwrap_or(0.0))
        .sum();
    let total_active: i64 = model_rows
        .iter()
        .map(|m| m.get("active").and_then(|v| v.as_i64()).unwrap_or(0))
        .sum();
    let idle = model_rows
        .iter()
        .filter(|m| {
            m.get("level").and_then(|v| v.as_str()) == Some("smooth")
                && m.get("active").and_then(|v| v.as_i64()).unwrap_or(0) == 0
        })
        .count();
    let events = recent_since(now - 60_000);

    serde_json::json!({
        "ready": true,
        "ts": now,
        "gateway": "",
        "instance": "",
        "window": 60,
        "pctWindow": 300,
        "totals": {
            "rpm": (total_rpm * 10.0).round() / 10.0,
            "active": total_active,
            "models": model_rows.len(),
            "idle": idle,
            "jam": model_rows.iter().filter(|m| m.get("status").and_then(|v| v.as_str()) == Some("jam")).count(),
            "ttftP50": serde_json::Value::Null,
            "ttftP95": serde_json::Value::Null,
            "decode": model_rows.iter().filter_map(|m| m.get("decode").and_then(|v| v.as_f64())).fold(0.0, f64::max),
            "outTok": model_rows.iter().filter_map(|m| m.get("outTok").and_then(|v| v.as_f64())).sum::<f64>().round(),
        },
        "models": model_rows,
        "events": events,
    })
}

static SNAPSHOT: Lazy<Mutex<serde_json::Value>> =
    Lazy::new(|| Mutex::new(serde_json::json!({"ready": false})));

/// 启动采样任务（在 tokio runtime 中调用一次）。
pub fn start_sampler() {
    tokio::spawn(async move {
        let mut samples: VecDeque<Sample> = VecDeque::new();
        loop {
            let ts = now_ms();
            NOW_MS.store(ts, Ordering::Relaxed);
            let (values, caps) = flatten();
            samples.push_back(Sample { ts, values, caps });
            while let Some(front) = samples.front() {
                if ts - front.ts > PCT_WINDOW_MS + 10_000 {
                    samples.pop_front();
                } else {
                    break;
                }
            }
            let snap = build_snapshot(&samples);
            if let Ok(mut guard) = SNAPSHOT.lock() {
                *guard = snap;
            }
            tokio::time::sleep(std::time::Duration::from_millis(SAMPLE_MS as u64)).await;
        }
    });
}

/// 读取当前快照（面板轮询）。
pub fn snapshot() -> serde_json::Value {
    LAST_READ.store(now_ms(), Ordering::Relaxed);
    SNAPSHOT
        .lock()
        .map(|g| g.clone())
        .unwrap_or_else(|_| serde_json::json!({"ready": false}))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_interpolates_between_buckets() {
        let b = [(0.1, 10.0), (1.0, 20.0), (10.0, 40.0)];
        let p50 = quantile(&b, 0.5).unwrap();
        assert!(p50 >= 1.0 && p50 <= 10.0);
    }

    #[test]
    fn level_thresholds() {
        assert_eq!(level_of(10.0).0, "smooth");
        assert_eq!(level_of(30.0).0, "normal");
        assert_eq!(level_of(60.0).0, "busy");
        assert_eq!(level_of(90.0).0, "over");
    }

    #[test]
    fn inactive_emit_is_cheap_and_safe() {
        emit_start("m", "b");
        emit_record(RequestMeta::default());
    }
}
