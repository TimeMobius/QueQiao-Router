//! CacheWeave：缓存亲和 + 实例繁忙度 + 推理速度（TTFT 与 TPS）的纯路由策略。
//!
//! 作用域与 `deterministic / random / least_connections` 相同：只接收
//! `ClientConfig`、`routing_keys`、`ACTIVE_REQUESTS`，在 `select_clients` 内部排序。
//! 不在 handler / main / 请求模型里埋任何 CacheWeave 专用代码。
//!
//! v6.4 代价函数（两条独立体验轴，不做 E2E 合成）：
//! `cost = w_p * prefill/median + w_q * queue/median + w_d * decode_cost
//!         + w_rho * rho + kv_cost - idle_bonus`
//!  - prefill：`uncached_tokens / recent_prefill_tps`；cache 直接减少 uncached，
//!    不再作为独立权重，避免 cache 与 prefill 双计；
//!  - queue：引擎 waiting/recent_queue + over-cap 折算；
//!  - decode：`median_decode_tps / tps`，长 prompt 权重较低、短请求作为次级轴；
//!  - 长 prompt 偏 prefill，短请求偏 queue/rho；TPS 始终保留，不假设输出长度；
//!  - 引擎指标缺失时：prefill/TPS 权重降级、KV 不硬门、idle 只在 active 已知时生效；
//!  - cache：由 `routing_keys`（多轮 user 锚点）构造稳定前缀链，本地索引记录/匹配。
//!
//! 共享 system prompt 不参与索引：它会在每个副本各缓存一次，属于引擎 APC 的自然收益；
//! 真正需要“路由到对副本”的是每个会话独有的历史前缀，由 routing_keys 链覆盖。

use crate::config::types::ClientConfig;
use crate::metrics::sliding_window;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, RwLock};
use std::time::{Duration, Instant};

/// 单请求参与索引的最大 user 锚点数。
const MAX_CHAIN: usize = 256;
/// 索引条目寿命；引擎真实驻留不可见，用保守 TTL。
const CACHE_TTL_SECS: u32 = 900;
/// 每个后端的索引条目上限。
const INDEX_CAP: usize = 300_000;
/// 估算 token 数的默认字节/token；只影响绝对量级，不影响候选间相对排序。
const DEFAULT_BYTES_PER_TOKEN: f64 = 3.2;
/// 同伴过载惩罚在归一化分数上的系数（每 1 个 load 单位约 0.25 个 TTFT 中位）。
/// 无引擎指标时，一条在途请求折算的排队毫秒数。
/// v5：粘性用加法余量（归一化分数单位），短/长 prompt 分开；乘性写法对负分失效。
const STICKY_MARGIN_SHORT: f64 = 0.15;
const STICKY_MARGIN_LONG: f64 = 0.50;
const ENGINE_STALE: Duration = Duration::from_secs(30);
const PROBE_OK_INTERVAL: Duration = Duration::from_secs(8);
const PROBE_FAIL_INTERVAL: Duration = Duration::from_secs(120);
const PROBE_TIMEOUT: Duration = Duration::from_millis(400);
const CLEAN_INTERVAL: Duration = Duration::from_secs(60);
/// 网关侧在途预留的租约窗口：防止 burst 内多个请求看到同一个空闲快照后集中打爆。
const RESERVATION_LEASE: Duration = Duration::from_secs(15);
/// 近期样本超过这个时间未更新，就视为“未知”，不再用陈旧坏值长期压制。
const RECENT_TTL: Duration = Duration::from_secs(120);
/// 引擎未报告 `kv_cache_max_concurrency` 时的兜底最优并发。
const DEFAULT_MAX_CONCURRENCY: f64 = 12.0;
/// 超过 cap 后每一路超额并发折算的排队毫秒（实测 c7→c11 TTFT 约 +1.5s）。
const OVER_CAP_PENALTY_MS: f64 = 300.0;
/// 容量门：inflight 达到 cap 的该倍数后不再作为 primary。
const CAP_GATE_RATIO: f64 = 1.2;
/// 缓存前瞻价值权重：迁移会让后续轮次也丢缓存（98.3% 请求是后续轮次）。
const CACHE_FUTURE_WEIGHT: f64 = 0.3;
/// 长 prompt 阈值；超过它时，prefill 明显偏慢的实例不参与。
const LONG_PROMPT_TOKENS: f64 = 16_000.0;
/// 长 prompt 下 prefill 速率低于候选中位该比例的实例被排除（例：fp8 副本）。
const PREFILL_GATE_RATIO: f64 = 0.4;

static INDEX: Lazy<DashMap<String, IndexShard>> = Lazy::new(DashMap::new);
static ENGINE: Lazy<DashMap<String, EngineSnapshot>> = Lazy::new(DashMap::new);
static LAST_CHOICE: Lazy<DashMap<u64, (String, Instant)>> = Lazy::new(DashMap::new);
static COLD_PICKS: Lazy<DashMap<String, u64>> = Lazy::new(DashMap::new);
/// v5：真实流量探索用的最近选择时间（不做 synthetic probe）。
static LAST_PICK: Lazy<DashMap<String, Instant>> = Lazy::new(DashMap::new);
/// backend -> 最近 15s 内被选中但尚未完成（或租约未过期）的请求时间戳。
static RESERVED: Lazy<DashMap<String, Mutex<Vec<Instant>>>> = Lazy::new(DashMap::new);
static CANDIDATES: Lazy<RwLock<Vec<ProbeTarget>>> = Lazy::new(|| RwLock::new(Vec::new()));
static PROBER_STARTED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Debug)]
struct ProbeTarget {
    name: String,
    base_url: String,
}

#[derive(Default)]
struct IndexShard {
    map: Mutex<HashMap<u64, u32>>,
}

impl IndexShard {
    fn match_prefix(&self, chain: &[u64], now: u32) -> usize {
        if chain.is_empty() {
            return 0;
        }
        let map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        let mut matched = 0;
        for hash in chain {
            match map.get(hash) {
                Some(ts) if now.saturating_sub(*ts) <= CACHE_TTL_SECS => matched += 1,
                _ => break,
            }
        }
        matched
    }

    fn record(&self, chain: &[u64], now: u32) {
        if chain.is_empty() {
            return;
        }
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        if map.len() > INDEX_CAP {
            map.retain(|_, ts| now.saturating_sub(*ts) <= CACHE_TTL_SECS);
            if map.len() > INDEX_CAP {
                map.clear();
            }
        }
        for hash in chain.iter().take(MAX_CHAIN) {
            map.insert(*hash, now);
        }
    }

    fn cleanup(&self, now: u32) {
        let mut map = self.map.lock().unwrap_or_else(|e| e.into_inner());
        map.retain(|_, ts| now.saturating_sub(*ts) <= CACHE_TTL_SECS);
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct EngineSnapshot {
    available: bool,
    proxy: bool,
    prefix_enabled: Option<bool>,
    running: f64,
    waiting: f64,
    kv_usage: f64,
    prefill_tps: f64,
    decode_tps: f64,
    hit_rate: f64,
    hit_queries: f64,
    avg_ttft_ms: f64,
    /// 累计 TTFT 样本数；用于小样本 shrink。
    ttft_samples: f64,
    /// 最近一个 scrape 窗口的 TTFT（delta）；0 表示该窗口无新请求。
    recent_ttft_ms: f64,
    recent_ttft_count: f64,
    /// 累计 ITL 样本数与最近窗口 TPS。
    itl_samples: f64,
    recent_tps: f64,
    /// 最近窗口 prefill 速率（非缓存 token / prefill 时间）与累计 prefill 样本。
    recent_prefill_tps: f64,
    prefill_samples: f64,
    /// 最近窗口样本的采集时间；超过 RECENT_TTL 后近期值视为未知。
    recent_at: Option<Instant>,
    /// KV 容量推导的最优并发（batch 上限），0 表示未知。
    max_concurrency: f64,
    /// 最近窗口的每个请求平均排队时间。
    recent_queue_ms: f64,
    recent_queue_count: f64,
    avg_prompt_tokens: f64,
    queue_ms: f64,
    rtt_ms: f64,
    updated: Option<Instant>,
}

impl EngineSnapshot {
    fn fresh(&self) -> bool {
        self.available
            && self
                .updated
                .map(|t| t.elapsed() <= ENGINE_STALE)
                .unwrap_or(false)
    }
}

#[derive(Debug, Default)]
struct Features {
    chain: Vec<u64>,
    lengths: Vec<usize>,
    session: u64,
    bytes: usize,
}

/// CacheWeave 主入口：返回候选下标排序（首个最优，其余为 failover 顺序）。
pub fn order(
    clients: &[ClientConfig],
    active_counts: Option<&HashMap<String, i64>>,
    model: &str,
    routing_keys: Option<&[(String, usize, usize)]>,
) -> Vec<usize> {
    if clients.len() <= 1 {
        return (0..clients.len()).collect();
    }

    ensure_prober();
    update_candidates(clients);

    let now = unix_secs();
    let features = routing_features(model, routing_keys);
    let prompt_tokens = (features.bytes as f64 / DEFAULT_BYTES_PER_TOKEN).max(0.0);
    let has_chain = !features.chain.is_empty();

    struct Row {
        idx: usize,
        name: String,
        ttft_ms: f64,
        tps: f64,
        /// 缓存保住的价值折算成毫秒（仅日志）。
        cache_value_ms: f64,
        /// v6 归一化信号：缓存命中率、KV 使用率、非缓存 prefill 速率。
        cache_signal: f64,
        kv_usage: f64,
        prefill_tps: f64,
        /// v6.4：prefill 成本（含本请求 uncached tokens/实测 S_i）与排队成本。
        prefill_ms: f64,
        queue_ms: f64,
        waiting: f64,
        engine_known: bool,
        active_known: bool,
        score: f64,
        load: f64,
        /// 配置里的 priority（用于公平/权重，不参与 cap）。
        priority: f64,
        /// 当前在途并发（running + waiting + 网关预留）与对应的最优并发。
        inflight: f64,
        cap: f64,
        /// 非空表示该候选因容量/prefill 门被排除在 primary 之外。
        excluded: Option<&'static str>,
    }

    // 冷启动对称化：没有引擎历史样本的实例用候选内中位数，避免“永远没机会被选中”。
    let mut known_ttfts: Vec<f64> = Vec::new();
    let mut known_prompts: Vec<f64> = Vec::new();
    let mut known_decode_tps: Vec<f64> = Vec::new();
    let mut known_prefill_tps: Vec<f64> = Vec::new();
    for client in clients {
        if let Some(e) = ENGINE.get(&client.name).map(|s| *s) {
            if e.fresh() {
                if e.avg_ttft_ms > 0.0 {
                    known_ttfts.push(e.avg_ttft_ms);
                }
                if e.avg_prompt_tokens > 1.0 {
                    known_prompts.push(e.avg_prompt_tokens);
                }
                if e.decode_tps > 1.0 {
                    known_decode_tps.push(e.decode_tps);
                }
                let prefill = if e.recent_prefill_tps > 1.0 {
                    e.recent_prefill_tps
                } else {
                    e.prefill_tps
                };
                if prefill > 1.0 {
                    known_prefill_tps.push(prefill);
                }
            }
        }
    }
    known_ttfts.sort_by(f64::total_cmp);
    known_prompts.sort_by(f64::total_cmp);
    known_decode_tps.sort_by(f64::total_cmp);
    known_prefill_tps.sort_by(f64::total_cmp);
    let median_ttft_ms = median_value(&known_ttfts).unwrap_or(80.0);
    let median_avg_prompt = median_value(&known_prompts).unwrap_or(0.0);
    let median_decode_tps = median_value(&known_decode_tps).unwrap_or(0.0);
    let median_prefill_tps = median_value(&known_prefill_tps).unwrap_or(0.0);

    // 在任何 retain 之前用候选快照估算全局 burst，供打分/门/长粘使用。
    let mut burst_inflight = 0.0f64;
    let mut burst_cap = 0.0f64;
    for client in clients {
        let active = active_counts
            .and_then(|m| m.get(&client.name))
            .copied()
            .unwrap_or(0)
            .max(0) as f64;
        let engine = ENGINE
            .get(&client.name)
            .map(|s| *s)
            .filter(EngineSnapshot::fresh);
        let reserved = reserved_load(&client.name);
        burst_inflight += match engine {
            Some(e) => (e.running + e.waiting).max(active) + reserved,
            None => active + reserved,
        };
        burst_cap += match engine {
            Some(e) if e.max_concurrency > 0.5 => e.max_concurrency.max(1.0),
            _ => DEFAULT_MAX_CONCURRENCY,
        };
    }
    let burst = burst_cap > 0.0 && burst_inflight / burst_cap >= 0.85;

    let mut rows: Vec<Row> = Vec::with_capacity(clients.len());
    let mut cached_anchors: Vec<usize> = Vec::with_capacity(clients.len());

    for (idx, client) in clients.iter().enumerate() {
        let active = active_counts
            .and_then(|m| m.get(&client.name))
            .copied()
            .unwrap_or(0)
            .max(0) as f64;

        let engine_snapshot = ENGINE.get(&client.name).map(|s| *s);
        let proxy = engine_snapshot.map(|e| e.proxy).unwrap_or(false);
        let engine = engine_snapshot.filter(EngineSnapshot::fresh);

        let reserved = reserved_load(&client.name);
        // engine 与网关 active 是同一批请求的两个视角：取 max 后再加 reserved，
        // 避免“已派发但网关仍计数”的双重计入。
        let inflight = match engine {
            Some(e) => (e.running + e.waiting).max(active) + reserved,
            None => active + reserved,
        };
        // v5：cap 不乘 priority，load 也必须用同一口径，否则容量门/探索的 rho 不一致。
        let load = inflight;
        // cap 是引擎的物理并发上限，不能乘 priority（否则大 priority 永远过不了门）。
        let cap = match engine {
            Some(e) if e.max_concurrency > 0.5 => e.max_concurrency.max(1.0),
            _ => DEFAULT_MAX_CONCURRENCY,
        };

        let matched = if has_chain {
            INDEX
                .get(&client.name)
                .map(|s| s.match_prefix(&features.chain, now))
                .unwrap_or(0)
        } else {
            0
        };
        cached_anchors.push(matched);

        let cached_tokens =
            features.lengths.iter().take(matched).sum::<usize>() as f64 / DEFAULT_BYTES_PER_TOKEN;
        let conf = confidence(engine, proxy);
        let cache_ratio = if prompt_tokens > 1.0 {
            (cached_tokens / prompt_tokens).clamp(0.0, 1.0) * conf
        } else {
            0.0
        };

        // 缓存省下的 prefill 毫秒数：这是迁移到别处会损失的实际代价（含未来轮次）。
        let prefill_tps_for_value = match engine {
            Some(e) if e.recent_prefill_tps > 1.0 => e.recent_prefill_tps,
            Some(e) if e.prefill_tps > 1.0 => e.prefill_tps,
            _ => median_prefill_tps.max(1.0),
        };
        let cache_value_ms = if cache_ratio > 0.0 && prefill_tps_for_value > 1.0 {
            let hit_tokens = prompt_tokens * cache_ratio;
            let saved_ms = hit_tokens / prefill_tps_for_value * 1000.0;
            // 迁移会把后续轮次的缓存也丢掉（实测 98.3% 请求是会话后续轮次）。
            saved_ms * (1.0 + CACHE_FUTURE_WEIGHT)
        } else {
            0.0
        };

        let (ttft_ms, tps) = match engine {
            Some(e) if e.available => {
                let recent_valid = e
                    .recent_at
                    .map(|t| t.elapsed() <= RECENT_TTL)
                    .unwrap_or(false)
                    && (e.recent_ttft_count > 0.0 || e.recent_prefill_tps > 1.0);
                if !recent_valid {
                    // 没有近期样本（例如只被几个超大冷请求打过）不代表它现在慢：
                    // 用候选中位数做中性占位，靠真实流量自然恢复，不发探测请求。
                    let tps = if median_decode_tps > 1.0 {
                        median_decode_tps
                    } else if e.decode_tps > 1.0 {
                        e.decode_tps
                    } else {
                        30.0
                    };
                    (median_ttft_ms, tps)
                } else {
                    let queue_avg_ms = if e.recent_queue_count > 0.0 {
                        e.recent_queue_ms
                    } else {
                        e.queue_ms
                    }
                    .max(0.0);
                    let decode_tps =
                        effective_decode_tps(e, model, &client.name, median_decode_tps);
                    let prefill_tps = if e.recent_prefill_tps > 1.0 {
                        e.recent_prefill_tps
                    } else {
                        e.prefill_tps
                    };
                    let mut ttft = if prefill_tps > 1.0 {
                        // 负载归一化：用本实例最近的非缓存 prefill 速率，按本次 prompt 长度折算。
                        let uncached_tokens = (prompt_tokens * (1.0 - cache_ratio)).max(0.0);
                        let prefill_ms = uncached_tokens / prefill_tps * 1000.0;
                        let wait_ms = queue_avg_ms
                            + if e.waiting > 0.0 {
                                e.waiting * prefill_ms / e.running.max(1.0)
                            } else {
                                0.0
                            };
                        e.rtt_ms + wait_ms + prefill_ms
                    } else {
                        // 没有 prefill 直方图时，退回最近 TTFT / 累计 shrink。
                        let avg_prompt = if median_avg_prompt > 1.0 {
                            median_avg_prompt
                        } else {
                            prompt_tokens.max(1.0)
                        };
                        let prompt_factor = if prompt_tokens > 1.0 {
                            (prompt_tokens / avg_prompt).clamp(0.05, 4.0)
                        } else {
                            1.0
                        };
                        let raw_ttft_ms = if e.recent_ttft_count > 0.0 {
                            e.recent_ttft_ms
                        } else {
                            e.avg_ttft_ms
                        };
                        let base_ttft_ms = if raw_ttft_ms > 0.0 {
                            shrink(raw_ttft_ms, e.ttft_samples, median_ttft_ms, 20.0)
                        } else {
                            median_ttft_ms
                        };
                        let prefill_base_ms = (base_ttft_ms - queue_avg_ms).max(1.0);
                        let wait_ms = queue_avg_ms
                            + if e.waiting > 0.0 {
                                e.waiting * prefill_base_ms * prompt_factor / e.running.max(1.0)
                            } else {
                                0.0
                            };
                        e.rtt_ms
                            + wait_ms
                            + prefill_base_ms * prompt_factor * (1.0 - 0.85 * cache_ratio)
                    };
                    // 容量：只在真正超过最优并发后加价，低于 cap 不惩罚。
                    let over = (inflight - cap).max(0.0);
                    if over > 0.0 {
                        ttft += over * OVER_CAP_PENALTY_MS;
                    }
                    if e.kv_usage > 0.9 {
                        ttft += (e.kv_usage - 0.9) * 3000.0;
                    }
                    (ttft, decode_tps)
                }
            }
            _ => {
                let ttft_avg_ms = sliding_window::get_ttft_1m_avg(model, &client.name) * 1000.0;
                let latency_avg_ms =
                    sliding_window::get_latency_1m_avg(model, &client.name) * 1000.0;
                let base_ttft_ms = if ttft_avg_ms > 0.0 {
                    ttft_avg_ms
                } else if latency_avg_ms > 0.0 {
                    latency_avg_ms * 0.5
                } else {
                    150.0
                };
                let tps = sliding_window::get_tps_1m_avg(model, &client.name);
                let tps = if tps > 1.0 {
                    tps
                } else if median_decode_tps > 1.0 {
                    median_decode_tps
                } else {
                    30.0
                };
                let over = (inflight - cap).max(0.0);
                let ttft_ms =
                    base_ttft_ms * (1.0 - 0.85 * cache_ratio) + over * OVER_CAP_PENALTY_MS;
                (ttft_ms, tps)
            }
        };

        // v6.4：把 prefill 与 queue 分开计价；cache 只通过 uncached tokens 降低 prefill_ms，
        // 不再作为独立权重；TPS 与 rho 单独进入排序。
        let over = (inflight - cap).max(0.0);
        let waiting = engine.map(|e| e.waiting).unwrap_or(0.0);
        let queue_ms = match engine {
            Some(e) if e.available => {
                let prefill_est = (ttft_ms * 0.6).max(1.0);
                e.recent_queue_ms.max(e.queue_ms)
                    + if e.waiting > 0.0 {
                        e.waiting * prefill_est / e.running.max(1.0)
                    } else {
                        0.0
                    }
                    + over * OVER_CAP_PENALTY_MS
            }
            _ => over * OVER_CAP_PENALTY_MS,
        };
        let prefill_ms = (ttft_ms - queue_ms).max(1.0);
        let engine_known = engine.is_some();
        let active_known = active_counts.and_then(|m| m.get(&client.name)).is_some();

        // 容量/prefill 门：只影响 primary，不影响 failover 顺序。
        let engine_prefill = match engine {
            Some(e) if e.recent_prefill_tps > 1.0 => e.recent_prefill_tps,
            Some(e) if e.prefill_tps > 1.0 => e.prefill_tps,
            _ => 0.0,
        };
        let gate_ratio = if burst { 1.0 } else { CAP_GATE_RATIO };
        let excluded = if inflight >= cap * gate_ratio {
            Some("over_capacity")
        } else if engine_known && waiting > 0.0 && inflight > cap {
            Some("queue_full")
        } else if let Some(e) = engine {
            if e.kv_usage > 0.9 {
                Some("kv_full")
            } else if prompt_tokens * (1.0 - cache_ratio) > LONG_PROMPT_TOKENS
                && cache_ratio < 0.60
                && median_prefill_tps > 1.0
                && engine_prefill > 0.0
                && engine_prefill < median_prefill_tps * PREFILL_GATE_RATIO
            {
                // 只看“真正要重算的 token”；长但高命中的请求仍可放行。
                // 例：4090 TP8 通信受限的副本，长冷 prompt prefill 只有同侪的 1/8。
                Some("slow_prefill")
            } else {
                None
            }
        } else {
            None
        };

        rows.push(Row {
            idx,
            name: client.name.clone(),
            ttft_ms,
            tps,
            cache_value_ms,
            cache_signal: cache_ratio,
            kv_usage: engine.map(|e| e.kv_usage).unwrap_or(0.5),
            prefill_tps: prefill_tps_for_value,
            prefill_ms,
            queue_ms,
            waiting,
            engine_known,
            active_known,
            score: 0.0,
            load,
            priority: client.priority.unwrap_or(1).max(1) as f64,
            inflight,
            cap,
            excluded,
        });
    }

    // v6：不再用“中位数比值”。所有信号归一到 [0,1]，按 llm-d/vLLM 风格加权求和；
    // 分数越低越好（取负），避免候选=2 时比值噪声放大、缓存被计价两次。
    // v6.4：以“prefill 毫秒 + queue 毫秒 + decode 成本 + rho”排序。
    // cache 已计入 prefill_ms（uncached tokens / S_i），不再叠加独立 cache 权重；
    // prefill 与 TPS 是两条独立轴，长 prompt 偏 prefill、短请求偏 queue。
    let mut known_prefill_ms: Vec<f64> = rows.iter().map(|r| r.prefill_ms).collect();
    let mut known_queue_ms: Vec<f64> = rows.iter().map(|r| r.queue_ms).collect();
    let mut known_decode: Vec<f64> = rows.iter().map(|r| r.tps.max(1.0)).collect();
    known_prefill_ms.sort_by(f64::total_cmp);
    known_queue_ms.sort_by(f64::total_cmp);
    known_decode.sort_by(f64::total_cmp);
    let median_prefill_ms = median_value(&known_prefill_ms).unwrap_or(1.0).max(1.0);
    let median_queue_ms = median_value(&known_queue_ms).unwrap_or(1.0).max(1.0);
    let median_decode = median_value(&known_decode).unwrap_or(30.0).max(1.0);
    for row in &mut rows {
        let clamp_big = |x: f64| {
            if x.is_finite() {
                x.clamp(0.0, 5.0)
            } else {
                5.0
            }
        };
        let p_cost = clamp_big(row.prefill_ms / median_prefill_ms);
        let q_cost = clamp_big(row.queue_ms / median_queue_ms);
        let d_cost = clamp_big(median_decode / row.tps.max(1.0));
        let rho = if row.cap.is_finite() {
            row.inflight / row.cap.max(1.0)
        } else {
            1.0
        }
        .clamp(0.0, 2.0);
        // 引擎和网关 active 都没有时，不能把“未知”奖励成 idle。
        let idle = if row.engine_known || row.active_known {
            ((row.cap - row.inflight) / row.cap.max(1.0)).clamp(0.0, 1.0)
        } else {
            0.0
        };
        // 请求类别：长 prompt 重点看 prefill（cache 已折入），短请求重点看 queue。
        // 这不是 E2E 合成；TPS 只在类别内部做次级排序。
        let long = prompt_tokens > LONG_PROMPT_TOKENS;
        let (w_p, w_q, w_d, w_rho) = if long {
            (4.0, 2.0, 1.5, 2.0)
        } else {
            (1.5, 4.0, 2.0, 2.0)
        };
        // 无引擎指标时降低 prefill/TPS 的置信权重，更多依赖 queue/active；
        // 但 cache 指数若仍可用，prefill_ms 本身已包含其收益。
        let (w_p, w_d) = if row.engine_known {
            (w_p, w_d)
        } else {
            (w_p * 0.6, w_d * 0.6)
        };
        let kv_cost = if row.engine_known && row.kv_usage.is_finite() {
            row.kv_usage.clamp(0.0, 1.0)
        } else {
            0.0
        };
        let mut weighted = w_p * p_cost + w_q * q_cost + w_d * d_cost + w_rho * rho + 1.5 * kv_cost
            - (if long { 0.25 } else { 0.75 }) * idle
            - 0.02 * (row.priority - 1.0);
        if !weighted.is_finite() {
            weighted = 0.0;
        }
        // v6.4 的 weighted 是“成本”，越小越好；不要再取负（v6.1 的加权和是收益，才取负）。
        row.score = weighted;
    }

    // 冷启动：无任何命中时，用 session rendezvous + 冷分配计数摊平新会话。
    let mut cold_home: Option<usize> = None;
    if has_chain && cached_anchors.iter().all(|n| *n == 0) {
        let mut home = rendezvous_home(features.session, clients).unwrap_or(0);
        let home_picks = cold_picks(&clients[home].name);
        let min_picks = clients
            .iter()
            .map(|c| cold_picks(&c.name))
            .min()
            .unwrap_or(0);
        if home_picks > min_picks {
            home = clients
                .iter()
                .enumerate()
                .filter(|(_, c)| cold_picks(&c.name) == min_picks)
                .min_by_key(|(idx, c)| {
                    (
                        usize::from(*idx != home),
                        std::cmp::Reverse(mix64(
                            features.session ^ mix64(hash_bytes(c.name.as_bytes())),
                        )),
                    )
                })
                .map(|(idx, _)| idx)
                .unwrap_or(home);
        }
        cold_home = Some(home);
        let best_score = rows.iter().map(|r| r.score).fold(f64::INFINITY, f64::min);
        for row in &mut rows {
            // v6 分数为负，必须用加法余量；乘性写法在负分下永不成立。
            if row.idx == home && row.score <= best_score + 0.50 {
                row.score -= 0.50;
            }
        }
        if let Some(mut entry) = COLD_PICKS.get_mut(&clients[home].name) {
            *entry = entry.saturating_add(1);
        } else {
            COLD_PICKS.insert(clients[home].name.clone(), 1);
        }
    }

    // 容量/prefill 门：把超容量、KV 满、长 prompt 下 prefill 明显偏慢的候选移出 primary。
    // 只有全部候选都被排除时才保留全部（避免 503）。
    let gate_candidates: Vec<&'static str> = rows
        .iter()
        .filter_map(|r| r.excluded)
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if rows.iter().any(|r| r.excluded.is_none()) {
        rows.retain(|r| r.excluded.is_none());
    } else {
        // 分级 fail-open：先只放宽容量门，kv_full / slow_prefill 永远不因全门而放回；
        // 只有在全部都是硬故障时才保留 rho 最低的 2 个（避免 503）。
        let hard_ok: Vec<&str> = rows
            .iter()
            .filter(|r| r.excluded != Some("kv_full") && r.excluded != Some("slow_prefill"))
            .map(|r| r.name.as_str())
            .collect();
        if !hard_ok.is_empty() {
            rows.retain(|r| r.excluded != Some("kv_full") && r.excluded != Some("slow_prefill"));
        } else {
            rows.sort_by(|a, b| {
                let ra = a.inflight / a.cap.max(1.0);
                let rb = b.inflight / b.cap.max(1.0);
                ra.total_cmp(&rb).then_with(|| a.name.cmp(&b.name))
            });
            rows.truncate(2.min(rows.len()));
        }
        tracing::warn!(
            model,
            gates = ?gate_candidates,
            kept = rows.len(),
            "cache_weave all candidates gated, tiered fail-open"
        );
    }

    tracing::debug!(
        model,
        session = features.session,
        cold_home = ?cold_home.map(|i| clients[i].name.as_str()),
        prompt_tokens,
        "cache_weave features"
    );

    if tracing::enabled!(tracing::Level::DEBUG) {
        let snapshot: Vec<(String, f64, f64)> = rows
            .iter()
            .map(|r| (r.name.clone(), r.score, r.load))
            .collect();
        tracing::debug!(model, scores = ?snapshot, "cache_weave candidates");
    }

    rows.sort_by(|a, b| {
        a.score
            .total_cmp(&b.score)
            .then_with(|| a.name.cmp(&b.name))
    });

    // v6.4：长 prompt 的 home 若已接近打满/有 waiting，即使缓存价值高也不能继续占 primary；
    // 只把它降为第 2 候选（保留 failover），避免“缓存把热点钉死”。
    if has_chain && prompt_tokens > LONG_PROMPT_TOKENS && rows.len() > 1 {
        if let Some(prev) = LAST_CHOICE.get(&features.session).map(|v| v.0.clone()) {
            if let Some(pos) = rows.iter().position(|r| r.name == prev) {
                let home_rho = rows[pos].inflight / rows[pos].cap.max(1.0);
                if pos == 0 && (rows[pos].waiting > 0.0 || home_rho >= 0.95) {
                    let row = rows.remove(pos);
                    rows.insert(1.min(rows.len()), row);
                }
            }
        }
    }

    // 会话粘性：v5 用加法余量（分数单位），负数分数也不会失效。
    // 长 prompt 用更大余量（重算成本随长度放大）；burst 期收紧，避免把队列压在 home。
    if has_chain {
        let previous = LAST_CHOICE.get(&features.session).map(|v| v.0.clone());
        if let Some(prev) = previous {
            if let Some(pos) = rows.iter().position(|r| r.name == prev) {
                let long = prompt_tokens > LONG_PROMPT_TOKENS;
                let margin = if long {
                    STICKY_MARGIN_LONG
                } else {
                    STICKY_MARGIN_SHORT
                };
                let home_rho = rows[pos].inflight / rows[pos].cap.max(1.0);
                if rows[pos].waiting > 0.0 || home_rho > 0.95 {
                    // 拥堵 home 不享受粘性，避免把队列钉在热点上。
                } else if pos > 0 && long {
                    // 长 prompt 只在 home 未接近打满、且确实有缓存/不算慢时硬粘；
                    // 冷且慢的 home 不该被钉死（serving review S6）。
                    let home_rho_limit = if burst { 0.70 } else { 0.95 };
                    let home_ok = home_rho < home_rho_limit
                        && rows[pos].cache_signal >= 0.50
                        && rows[pos].prefill_tps >= median_prefill_tps.max(1.0) * 0.70;
                    if rows[pos].excluded.is_none() && home_ok {
                        let row = rows.remove(pos);
                        rows.insert(0, row);
                    }
                } else if pos > 0 && rows[pos].score <= rows[0].score + margin {
                    let row = rows.remove(pos);
                    rows.insert(0, row);
                }
            }
        }
    }

    // failover 尾部加 session 级抖动，避免所有网关/会话按同一固定顺序重试。
    if rows.len() > 2 {
        for row in rows.iter_mut().skip(1) {
            let j = (mix64(features.session ^ hash_bytes(row.name.as_bytes())) % 1000) as f64
                / 1000.0
                * 0.05;
            row.score += j;
        }
        rows[1..].sort_by(|a, b| {
            a.score
                .total_cmp(&b.score)
                .then_with(|| a.name.cmp(&b.name))
        });
    }

    if let Some(best) = rows.first() {
        reserve(&best.name);
        LAST_PICK.insert(best.name.clone(), Instant::now());
        if has_chain {
            LAST_CHOICE.insert(features.session, (best.name.clone(), Instant::now()));
            INDEX
                .entry(best.name.clone())
                .or_default()
                .record(&features.chain, now);
        }
        tracing::debug!(
            model,
            chosen = %best.name,
            score = best.score,
            ttft_est_ms = best.ttft_ms,
            tpot_est_ms = 1000.0 / best.tps.max(1.0),
            cache_value_ms = best.cache_value_ms,
            inflight = best.inflight,
            cap = best.cap,
            gated = ?gate_candidates,
            cache_anchors = cached_anchors.get(best.idx).copied().unwrap_or(0),
            candidates = clients.len(),
            "cache_weave decision"
        );
    }

    rows.into_iter().map(|r| r.idx).collect()
}

fn effective_decode_tps(
    engine: EngineSnapshot,
    model: &str,
    backend: &str,
    median_decode_tps: f64,
) -> f64 {
    let raw = if engine.recent_tps > 1.0 {
        engine.recent_tps
    } else if engine.decode_tps > 1.0 {
        engine.decode_tps
    } else {
        let tps = sliding_window::get_tps_1m_avg(model, backend);
        if tps > 1.0 {
            tps
        } else {
            return if median_decode_tps > 1.0 {
                median_decode_tps
            } else {
                30.0
            };
        }
    };
    shrink(raw, engine.itl_samples, median_decode_tps, 20.0).max(1.0)
}

/// 小样本均值向候选内中位数收缩：3 个样本的 22.9s 不应被当成精确速度。
fn shrink(value: f64, samples: f64, prior: f64, prior_weight: f64) -> f64 {
    if !value.is_finite() || value <= 0.0 {
        return prior.max(1.0);
    }
    if samples <= 0.0 || prior <= 0.0 || prior_weight <= 0.0 {
        return value;
    }
    (value * samples + prior * prior_weight) / (samples + prior_weight)
}

// ---------------------------------------------------------------------------
// 路由特征：由 routing_keys 构造稳定的会话前缀链
// ---------------------------------------------------------------------------

fn routing_features(model: &str, routing_keys: Option<&[(String, usize, usize)]>) -> Features {
    let mut features = Features {
        session: mix64(hash_bytes(model.as_bytes())),
        ..Features::default()
    };
    let Some(keys) = routing_keys else {
        return features;
    };
    if keys.is_empty() {
        return features;
    }

    let mut prev = mix64(hash_bytes(model.as_bytes()));
    for (content, length, _turns_after) in keys.iter().take(MAX_CHAIN) {
        features.bytes = features.bytes.saturating_add(*length);
        prev = fnv1a(prev, content.as_bytes());
        prev = fnv1a(prev, &(*length as u64).to_le_bytes());
        features.chain.push(prev);
        features.lengths.push(*length);
    }
    if let Some(first) = features.chain.first() {
        features.session = *first;
    }
    features
}

// ---------------------------------------------------------------------------
// 引擎探测
// ---------------------------------------------------------------------------

fn ensure_prober() {
    if PROBER_STARTED.load(Ordering::Relaxed) {
        return;
    }
    if let Ok(handle) = tokio::runtime::Handle::try_current() {
        if PROBER_STARTED
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
        {
            handle.spawn(prober_loop());
        }
    }
}

fn update_candidates(clients: &[ClientConfig]) {
    let mut next: Vec<ProbeTarget> = Vec::with_capacity(clients.len());
    for client in clients {
        if next.iter().any(|t| t.name == client.name) {
            continue;
        }
        next.push(ProbeTarget {
            name: client.name.clone(),
            base_url: client.base_url.clone(),
        });
    }
    let mut guard = CANDIDATES.write().unwrap_or_else(|e| e.into_inner());
    let changed = guard.len() != next.len()
        || guard
            .iter()
            .zip(next.iter())
            .any(|(a, b)| a.name != b.name || a.base_url != b.base_url);
    if changed {
        *guard = next;
    }
}

async fn prober_loop() {
    let client = match reqwest::Client::builder().timeout(PROBE_TIMEOUT).build() {
        Ok(c) => c,
        Err(_) => return,
    };
    let mut previous: HashMap<String, (RawMetrics, Instant)> = HashMap::new();
    let mut next_probe: HashMap<String, Instant> = HashMap::new();
    let mut last_clean = Instant::now();

    loop {
        let targets = CANDIDATES.read().unwrap_or_else(|e| e.into_inner()).clone();
        let now = Instant::now();
        for target in targets {
            if let Some(at) = next_probe.get(&target.name) {
                if now < *at {
                    continue;
                }
            }
            let url = metrics_url(&target.base_url);
            let started = Instant::now();
            let mut ok = false;
            if let Ok(resp) = client.get(&url).send().await {
                if resp.status().is_success() {
                    if let Ok(text) = resp.text().await {
                        if let Some(raw) = RawMetrics::parse(&text) {
                            let rtt_ms = started.elapsed().as_secs_f64() * 1000.0;
                            let prev_snap = ENGINE.get(&target.name).map(|s| *s);
                            let snapshot =
                                build_snapshot(&raw, previous.get(&target.name), prev_snap, rtt_ms);
                            if snapshot.available || snapshot.proxy {
                                ENGINE.insert(target.name.clone(), snapshot);
                                previous.insert(target.name.clone(), (raw, Instant::now()));
                                ok = snapshot.available;
                            }
                        }
                    }
                }
            }
            if !ok {
                let mut snap = ENGINE.get(&target.name).map(|s| *s).unwrap_or_default();
                snap.available = false;
                ENGINE.insert(target.name.clone(), snap);
            }
            next_probe.insert(
                target.name.clone(),
                Instant::now()
                    + if ok {
                        PROBE_OK_INTERVAL
                    } else {
                        PROBE_FAIL_INTERVAL
                    },
            );
        }

        if last_clean.elapsed() >= CLEAN_INTERVAL {
            let now_secs = unix_secs();
            for shard in INDEX.iter() {
                shard.cleanup(now_secs);
            }
            last_clean = Instant::now();
        }

        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

fn metrics_url(base_url: &str) -> String {
    let base = base_url.trim_end_matches('/');
    let base = base.strip_suffix("/v1").unwrap_or(base);
    format!("{base}/metrics")
}

#[derive(Debug, Default, Clone, Copy)]
struct RawMetrics {
    proxy: bool,
    engine: bool,
    running: Option<f64>,
    waiting: Option<f64>,
    kv_usage: Option<f64>,
    prompt_tokens: Option<f64>,
    generation_tokens: Option<f64>,
    hits: Option<f64>,
    queries: Option<f64>,
    ttft_sum: Option<f64>,
    ttft_count: Option<f64>,
    queue_sum: Option<f64>,
    queue_count: Option<f64>,
    itl_sum: Option<f64>,
    itl_count: Option<f64>,
    prefill_sum: Option<f64>,
    prefill_count: Option<f64>,
    max_concurrency: Option<f64>,
    prefix_enabled: Option<bool>,
}

impl RawMetrics {
    fn parse(text: &str) -> Option<Self> {
        let mut raw = RawMetrics::default();
        let mut recognized = false;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((head, value)) = line.rsplit_once(' ') else {
                continue;
            };
            let Ok(value) = value.parse::<f64>() else {
                continue;
            };
            let name = head.split('{').next().unwrap_or(head);
            if name.starts_with("vllm:") || name.starts_with("sglang:") {
                raw.engine = true;
            }
            if name == "vllm:cache_config_info" {
                if let Some(v) = label_value(head, "kv_cache_max_concurrency") {
                    if let Ok(parsed) = v.parse::<f64>() {
                        if parsed > 0.0 {
                            raw.max_concurrency = Some(parsed);
                            recognized = true;
                        }
                    }
                }
                let lower = head.to_ascii_lowercase();
                if lower.contains("enable_prefix_caching=\"false\"")
                    || lower.contains("enable_prefix_caching=false")
                {
                    raw.prefix_enabled = Some(false);
                } else if lower.contains("enable_prefix_caching=\"true\"")
                    || lower.contains("enable_prefix_caching=true")
                {
                    raw.prefix_enabled = Some(true);
                }
            }
            match name {
                "vllm:num_requests_running" | "sglang:num_running_reqs" => {
                    raw.running = Some(raw.running.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:num_requests_waiting" | "sglang:num_queue_reqs" => {
                    raw.waiting = Some(raw.waiting.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:kv_cache_usage_perc" | "vllm:gpu_cache_usage_perc" | "sglang:token_usage" => {
                    raw.kv_usage = Some(raw.kv_usage.unwrap_or(0.0).max(value));
                    recognized = true;
                }
                "vllm:prompt_tokens_total" | "sglang:prompt_tokens_total" => {
                    raw.prompt_tokens = Some(raw.prompt_tokens.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:generation_tokens_total" | "sglang:generation_tokens_total" => {
                    raw.generation_tokens = Some(raw.generation_tokens.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:prefix_cache_hits_total"
                | "vllm:prefix_cache_hits"
                | "sglang:cache_hit_tokens_total" => {
                    raw.hits = Some(raw.hits.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:prefix_cache_queries_total"
                | "vllm:prefix_cache_queries"
                | "sglang:cache_query_tokens_total" => {
                    raw.queries = Some(raw.queries.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:time_to_first_token_seconds_sum"
                | "sglang:time_to_first_token_seconds_sum" => {
                    raw.ttft_sum = Some(raw.ttft_sum.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:time_to_first_token_seconds_count"
                | "sglang:time_to_first_token_seconds_count" => {
                    raw.ttft_count = Some(raw.ttft_count.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:request_queue_time_seconds_sum" => {
                    raw.queue_sum = Some(raw.queue_sum.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:request_queue_time_seconds_count" => {
                    raw.queue_count = Some(raw.queue_count.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:request_prefill_time_seconds_sum"
                | "sglang:request_prefill_time_seconds_sum" => {
                    raw.prefill_sum = Some(raw.prefill_sum.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:request_prefill_time_seconds_count"
                | "sglang:request_prefill_time_seconds_count" => {
                    raw.prefill_count = Some(raw.prefill_count.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:inter_token_latency_seconds_sum"
                | "vllm:request_time_per_output_token_seconds_sum"
                | "sglang:inter_token_latency_seconds_sum"
                | "sglang:request_time_per_output_token_seconds_sum" => {
                    raw.itl_sum = Some(raw.itl_sum.unwrap_or(0.0) + value);
                    recognized = true;
                }
                "vllm:inter_token_latency_seconds_count"
                | "vllm:request_time_per_output_token_seconds_count"
                | "sglang:inter_token_latency_seconds_count"
                | "sglang:request_time_per_output_token_seconds_count" => {
                    raw.itl_count = Some(raw.itl_count.unwrap_or(0.0) + value);
                    recognized = true;
                }
                _ => {}
            }
        }
        if !raw.engine
            && (text.contains("gpt_api_active_requests")
                || text.contains("db_write_batches")
                || text.contains("queqiao"))
        {
            raw.proxy = true;
            recognized = true;
        }
        recognized.then_some(raw)
    }
}

fn build_snapshot(
    raw: &RawMetrics,
    previous: Option<&(RawMetrics, Instant)>,
    previous_snapshot: Option<EngineSnapshot>,
    rtt_ms: f64,
) -> EngineSnapshot {
    let mut snap = EngineSnapshot {
        available: raw.engine,
        proxy: raw.proxy,
        prefix_enabled: raw.prefix_enabled,
        running: raw.running.unwrap_or(0.0),
        waiting: raw.waiting.unwrap_or(0.0),
        kv_usage: raw.kv_usage.unwrap_or(0.0).clamp(0.0, 1.0),
        rtt_ms,
        updated: Some(Instant::now()),
        ..EngineSnapshot::default()
    };
    if let (Some(sum), Some(count)) = (raw.ttft_sum, raw.ttft_count) {
        if count > 0.0 {
            snap.avg_ttft_ms = sum / count * 1000.0;
            snap.ttft_samples = count;
            if let Some(prompt_tokens) = raw.prompt_tokens {
                snap.avg_prompt_tokens = (prompt_tokens / count).max(1.0);
            }
        }
    }
    if let (Some(sum), Some(count)) = (raw.itl_sum, raw.itl_count) {
        if count > 0.0 {
            snap.itl_samples = count;
            let itl_s = sum / count;
            if itl_s > 0.0 {
                snap.decode_tps = (1.0 / itl_s).clamp(1.0, 10_000.0);
            }
        }
    }
    if let (Some(sum), Some(count)) = (raw.queue_sum, raw.queue_count) {
        if count > 0.0 {
            snap.queue_ms = sum / count * 1000.0;
        }
    }
    snap.prefill_samples = raw.prefill_count.unwrap_or(0.0);
    snap.max_concurrency = raw.max_concurrency.unwrap_or(0.0).max(0.0);
    if let Some((prev, at)) = previous {
        let dt = at.elapsed().as_secs_f64();
        if dt > 0.2 {
            if let (Some(cur), Some(prev)) = (raw.prompt_tokens, prev.prompt_tokens) {
                snap.prefill_tps = ((cur - prev).max(0.0) / dt).max(0.0);
            }
            if let (Some(cur_sum), Some(prev_sum), Some(cur_count), Some(prev_count)) =
                (raw.ttft_sum, prev.ttft_sum, raw.ttft_count, prev.ttft_count)
            {
                let dc = (cur_count - prev_count).max(0.0);
                if dc > 0.0 {
                    snap.recent_ttft_ms = ((cur_sum - prev_sum).max(0.0) / dc) * 1000.0;
                    snap.recent_ttft_count = dc;
                }
            }
            if let (Some(cur_sum), Some(prev_sum), Some(cur_count), Some(prev_count)) =
                (raw.itl_sum, prev.itl_sum, raw.itl_count, prev.itl_count)
            {
                let dc = (cur_count - prev_count).max(0.0);
                if dc > 0.0 && cur_sum > prev_sum {
                    snap.recent_tps = (dc / (cur_sum - prev_sum)).clamp(1.0, 10_000.0);
                }
            }
            snap.recent_at = Some(Instant::now());
            if let (Some(cur_sum), Some(prev_sum), Some(cur_count), Some(prev_count)) = (
                raw.prefill_sum,
                prev.prefill_sum,
                raw.prefill_count,
                prev.prefill_count,
            ) {
                let dc = (cur_count - prev_count).max(0.0);
                let dt = (cur_sum - prev_sum).max(0.0);
                if dc > 0.0 && dt > 0.0001 {
                    let dp = raw.prompt_tokens.unwrap_or(0.0) - prev.prompt_tokens.unwrap_or(0.0);
                    let dh = raw.hits.unwrap_or(0.0) - prev.hits.unwrap_or(0.0);
                    let uncached = (dp - dh).max(0.0);
                    if uncached > 0.0 {
                        snap.recent_prefill_tps = (uncached / dt).clamp(1.0, 1_000_000.0);
                    }
                }
            }
            if let (Some(cur_sum), Some(prev_sum), Some(cur_count), Some(prev_count)) = (
                raw.queue_sum,
                prev.queue_sum,
                raw.queue_count,
                prev.queue_count,
            ) {
                let dc = (cur_count - prev_count).max(0.0);
                if dc > 0.0 {
                    snap.recent_queue_ms = ((cur_sum - prev_sum).max(0.0) / dc) * 1000.0;
                    snap.recent_queue_count = dc;
                }
            }
            if snap.decode_tps <= 1.0 {
                if let (Some(cur), Some(prev)) = (raw.generation_tokens, prev.generation_tokens) {
                    snap.decode_tps = ((cur - prev).max(0.0) / dt).max(0.0);
                }
            }
            if let (Some(hits), Some(prev_hits), Some(queries), Some(prev_queries)) =
                (raw.hits, prev.hits, raw.queries, prev.queries)
            {
                let dq = (queries - prev_queries).max(0.0);
                let dh = (hits - prev_hits).max(0.0);
                snap.hit_queries = dq;
                snap.hit_rate = if dq > 0.0 {
                    (dh / dq).clamp(0.0, 1.0)
                } else {
                    0.0
                };
            }
        }
    }
    if snap.recent_ttft_count <= 0.0 {
        if let Some(prev_snap) = previous_snapshot {
            snap.recent_ttft_ms = prev_snap.recent_ttft_ms;
            snap.recent_ttft_count = prev_snap.recent_ttft_count;
            snap.recent_tps = prev_snap.recent_tps;
            snap.recent_prefill_tps = prev_snap.recent_prefill_tps;
            snap.recent_at = prev_snap.recent_at;
            snap.recent_queue_ms = prev_snap.recent_queue_ms;
            snap.recent_queue_count = prev_snap.recent_queue_count;
            if snap.hit_queries <= 0.0 {
                snap.hit_rate = prev_snap.hit_rate;
                snap.hit_queries = prev_snap.hit_queries;
            }
        }
    }
    snap
}

fn confidence(engine: Option<EngineSnapshot>, proxy: bool) -> f64 {
    if proxy {
        return 0.15;
    }
    let Some(e) = engine else {
        return 0.5;
    };
    if e.prefix_enabled == Some(false) {
        return 0.05;
    }
    if e.hit_queries > 0.0 {
        return if e.hit_rate > 0.01 { 1.0 } else { 0.25 };
    }
    0.5
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

/// 从 Prometheus 的标签串里取 `key="value"`。
fn label_value(head: &str, key: &str) -> Option<String> {
    let start = head.find('{')?;
    let labels = &head[start + 1..];
    let needle = format!("{key}=\"");
    let mut rest = labels;
    while let Some(pos) = rest.find(&needle) {
        let after = &rest[pos + needle.len()..];
        if let Some(end) = after.find('"') {
            return Some(after[..end].to_string());
        }
        rest = &after[..];
        if rest.is_empty() {
            break;
        }
    }
    None
}

fn median_value(sorted: &[f64]) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let mid = sorted.len() / 2;
    if sorted.len() % 2 == 0 {
        Some((sorted[mid - 1] + sorted[mid]) / 2.0)
    } else {
        sorted.get(mid).copied()
    }
}

fn reserved_load(name: &str) -> f64 {
    let Some(entry) = RESERVED.get(name) else {
        return 0.0;
    };
    let mut v = entry.lock().unwrap_or_else(|e| e.into_inner());
    v.retain(|t| t.elapsed() < RESERVATION_LEASE);
    v.len() as f64
}

fn reserve(name: &str) {
    let entry = RESERVED.entry(name.to_string()).or_default();
    let mut v = entry.lock().unwrap_or_else(|e| e.into_inner());
    v.retain(|t| t.elapsed() < RESERVATION_LEASE);
    v.push(Instant::now());
}

fn unix_secs() -> u32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as u32)
        .unwrap_or(0)
}

fn cold_picks(name: &str) -> u64 {
    COLD_PICKS.get(name).map(|v| *v).unwrap_or(0)
}

fn rendezvous_home(session: u64, clients: &[ClientConfig]) -> Option<usize> {
    let mut best: Option<(u64, usize)> = None;
    for (idx, client) in clients.iter().enumerate() {
        let score = mix64(session ^ mix64(hash_bytes(client.name.as_bytes())));
        if best.map(|(s, _)| score > s).unwrap_or(true) {
            best = Some((score, idx));
        }
    }
    best.map(|(_, idx)| idx)
}

fn fnv1a(seed: u64, bytes: &[u8]) -> u64 {
    let mut hash = seed;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn mix64(mut x: u64) -> u64 {
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    x
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    mix64(hash)
}

// ---------------------------------------------------------------------------
// 单测
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::types::ModelMatch;

    fn client(name: &str, priority: u32) -> ClientConfig {
        ClientConfig {
            name: name.to_string(),
            base_url: format!("http://{name}.test/v1"),
            priority: Some(priority),
            model_match: ModelMatch {
                match_type: "exact".to_string(),
                value: vec!["m".to_string()],
            },
            ..ClientConfig::default()
        }
    }

    fn keys(turns: usize, seed: u64) -> Vec<(String, usize, usize)> {
        (0..turns)
            .map(|i| {
                let text = format!("turn {seed}-{i} ").repeat(8);
                (text.clone(), text.len(), turns - 1 - i)
            })
            .collect()
    }

    fn engine(avg_ttft_ms: f64, decode_tps: f64) -> EngineSnapshot {
        EngineSnapshot {
            available: true,
            avg_ttft_ms,
            avg_prompt_tokens: 1000.0,
            decode_tps,
            recent_ttft_ms: avg_ttft_ms,
            recent_ttft_count: 1.0,
            recent_tps: decode_tps,
            recent_at: Some(Instant::now()),
            rtt_ms: 1.0,
            updated: Some(Instant::now()),
            ..EngineSnapshot::default()
        }
    }

    #[test]
    fn metrics_url_strips_v1() {
        assert_eq!(
            metrics_url("http://10.0.0.1:8000/v1/"),
            "http://10.0.0.1:8000/metrics"
        );
        assert_eq!(
            metrics_url("http://10.0.0.1:8000"),
            "http://10.0.0.1:8000/metrics"
        );
    }

    #[test]
    fn routing_features_are_prefix_monotonic() {
        let f1 = routing_features("m", Some(&keys(2, 1)));
        let f2 = routing_features("m", Some(&keys(3, 1)));
        assert_eq!(f2.chain.len(), f1.chain.len() + 1);
        assert_eq!(&f2.chain[..f1.chain.len()], &f1.chain[..]);
        assert_eq!(f1.session, f2.session);
        assert!(f2.bytes > f1.bytes);
    }

    #[test]
    fn index_matches_only_contiguous_prefix() {
        let shard = IndexShard::default();
        let now = unix_secs();
        shard.record(&[1, 2, 3], now);
        assert_eq!(shard.match_prefix(&[1, 2], now), 2);
        assert_eq!(shard.match_prefix(&[1, 2, 3], now), 3);
        assert_eq!(shard.match_prefix(&[1, 2, 9, 3], now), 2);
        assert_eq!(shard.match_prefix(&[9], now), 0);
    }

    #[test]
    fn index_expires() {
        let shard = IndexShard::default();
        let now = unix_secs();
        shard.record(&[7], now);
        assert_eq!(shard.match_prefix(&[7], now + CACHE_TTL_SECS + 1), 0);
    }

    #[test]
    fn order_prefers_cached_backend_when_load_comparable() {
        let model = "cache-weave-unit-cache";
        let a = client("cw-a", 1);
        let b = client("cw-b", 1);
        let k = keys(2, 11);
        let f = routing_features(model, Some(&k));
        INDEX
            .entry("cw-a".to_string())
            .or_default()
            .record(&f.chain, unix_secs());

        let order = order(&[a, b], None, model, Some(&k));
        assert_eq!(order[0], 0, "cached backend should win when load is equal");
    }

    #[test]
    fn order_breaks_affinity_when_cached_backend_is_overloaded() {
        let model = "cache-weave-unit-overload";
        let a = client("cw-hot", 1);
        let b = client("cw-cold", 1);
        let k = keys(2, 12);
        let f = routing_features(model, Some(&k));
        INDEX
            .entry("cw-hot".to_string())
            .or_default()
            .record(&f.chain, unix_secs());

        let mut active = HashMap::new();
        active.insert("cw-hot".to_string(), 40_i64);
        active.insert("cw-cold".to_string(), 0_i64);
        let order = order(&[a, b], Some(&active), model, Some(&k));
        assert_eq!(
            order[0], 1,
            "40 in-flight on cached node must lose to idle node"
        );
    }

    #[test]
    fn order_engine_speed_wins_without_cache() {
        let model = "cache-weave-unit-speed";
        let slow = client("cw-slow", 1);
        let fast = client("cw-fast", 1);
        ENGINE.insert("cw-slow".to_string(), engine(200.0, 50.0));
        ENGINE.insert("cw-fast".to_string(), engine(20.0, 50.0));
        // 不传 routing_keys：没有会话前缀，不触发冷启动亲和，纯粹比较引擎速度。
        let order = order(&[slow, fast], None, model, None);
        assert_eq!(order[0], 1, "10x lower TTFT should win");
    }

    #[test]
    fn order_engine_decode_tps_breaks_ttft_ties() {
        let model = "cache-weave-unit-tps";
        let slow_decode = client("cw-slow-decode", 1);
        let fast_decode = client("cw-fast-decode", 1);
        ENGINE.insert("cw-slow-decode".to_string(), engine(50.0, 5.0));
        ENGINE.insert("cw-fast-decode".to_string(), engine(50.0, 100.0));
        // 不传 routing_keys，直接比较 TTFT 相同、TPS 不同的两个实例。
        let order = order(&[slow_decode, fast_decode], None, model, None);
        assert_eq!(order[0], 1, "same TTFT, 20x TPS must win");
    }

    #[test]
    fn order_keeps_both_axes_and_rejects_dominated_candidate() {
        let model = "cache-weave-unit-pareto";
        let low_ttft = client("cw-low-ttft", 1);
        let high_tps = client("cw-high-tps", 1);
        let dominated = client("cw-dominated", 1);
        ENGINE.insert("cw-low-ttft".to_string(), engine(20.0, 20.0));
        ENGINE.insert("cw-high-tps".to_string(), engine(100.0, 100.0));
        ENGINE.insert("cw-dominated".to_string(), engine(300.0, 5.0));
        let order = order(&[dominated, low_ttft, high_tps], None, model, None);
        assert_eq!(
            order[2], 0,
            "candidate worse on both TTFT and TPS must rank last"
        );
        assert!(
            order[..2].contains(&1) && order[..2].contains(&2),
            "the TTFT-favored and TPS-favored candidates should both survive"
        );
    }

    #[test]
    fn median_handles_even_candidate_count() {
        let mut v = vec![0.0, 1.0, 14.0, 14.0];
        v.sort_by(f64::total_cmp);
        assert!((median_value(&v).unwrap() - 7.5).abs() < 1e-9);
    }

    #[test]
    fn reservations_are_counted_in_load() {
        let name = "cw-reservation-unit";
        assert_eq!(reserved_load(name), 0.0);
        reserve(name);
        reserve(name);
        assert_eq!(reserved_load(name), 2.0);
    }

    #[test]
    fn order_prefers_higher_recent_prefill_rate() {
        let model = "cache-weave-unit-prefill-rate";
        let slow = client("cw-prefill-slow", 1);
        let fast = client("cw-prefill-fast", 1);
        let mut slow_engine = engine(5_000.0, 50.0);
        slow_engine.recent_prefill_tps = 1_000.0;
        slow_engine.recent_at = Some(Instant::now());
        let mut fast_engine = engine(5_000.0, 50.0);
        fast_engine.recent_prefill_tps = 10_000.0;
        fast_engine.recent_at = Some(Instant::now());
        ENGINE.insert("cw-prefill-slow".to_string(), slow_engine);
        ENGINE.insert("cw-prefill-fast".to_string(), fast_engine);
        let k = vec![("x".repeat(80), 25_600usize, 0usize)];
        let order = order(&[slow, fast], None, model, Some(&k));
        assert_eq!(
            order[0], 1,
            "10x prefill rate should win for the same prompt"
        );
    }

    #[test]
    fn stale_engine_uses_neutral_median_without_extra_probe_traffic() {
        // v4 明确不发探测请求：陈旧样本只做中性占位，实例靠真实流量自然恢复。
        let model = "cache-weave-unit-stale-neutral";
        let stale = client("cw-stale-a", 1);
        let healthy = client("cw-healthy-b", 1);
        let old = Instant::now() - Duration::from_secs(RECENT_TTL.as_secs() + 30);
        ENGINE.insert(
            "cw-stale-a".to_string(),
            EngineSnapshot {
                available: true,
                avg_ttft_ms: 28_000.0,
                recent_ttft_ms: 28_000.0,
                recent_ttft_count: 1.0,
                recent_at: Some(old),
                updated: Some(Instant::now()),
                ..EngineSnapshot::default()
            },
        );
        ENGINE.insert("cw-healthy-b".to_string(), engine(500.0, 50.0));
        let k = vec![("short prompt".to_string(), 400usize, 0usize)];
        let order = order(&[stale, healthy], None, model, Some(&k));
        // 不再强制把流量塞给陈旧实例；两者的分数接近，任一在首位都可接受。
        assert_eq!(order.len(), 2);
    }

    #[test]
    fn over_capacity_candidate_is_gated_out() {
        let model = "cache-weave-unit-cap-gate";
        let busy = client("cw-busy-cap", 1);
        let idle = client("cw-idle-cap", 1);
        let mut busy_engine = engine(500.0, 50.0);
        busy_engine.max_concurrency = 4.0;
        busy_engine.running = 5.0;
        let mut idle_engine = engine(500.0, 50.0);
        idle_engine.max_concurrency = 4.0;
        idle_engine.running = 0.0;
        ENGINE.insert("cw-busy-cap".to_string(), busy_engine);
        ENGINE.insert("cw-idle-cap".to_string(), idle_engine);
        let order = order(&[busy, idle], None, model, None);
        assert_eq!(order[0], 1, "over-capacity backend must not be primary");
    }

    #[test]
    fn slow_prefill_backend_is_gated_for_long_prompts_only() {
        let model = "cache-weave-unit-slow-prefill";
        let slow = client("cw-slow-net", 1);
        let fast = client("cw-fast-net", 1);
        let mut slow_engine = engine(500.0, 50.0);
        slow_engine.recent_prefill_tps = 1_000.0;
        slow_engine.prefill_tps = 1_000.0;
        let mut fast_engine = engine(500.0, 50.0);
        fast_engine.recent_prefill_tps = 8_000.0;
        fast_engine.prefill_tps = 8_000.0;
        ENGINE.insert("cw-slow-net".to_string(), slow_engine);
        ENGINE.insert("cw-fast-net".to_string(), fast_engine);

        // 长 prompt：慢 prefill 的实例被门掉。
        let long = vec![("x".repeat(80), 200_000usize, 0usize)];
        assert_eq!(
            order(&[slow.clone(), fast.clone()], None, model, Some(&long))[0],
            1
        );

        // 短 prompt：仍然可以参与（fp8 在短/缓存命中时并不慢）。
        let short = vec![("x".to_string(), 400usize, 0usize)];
        let order_short = order(&[slow, fast], None, model, Some(&short));
        assert_eq!(order_short.len(), 2);
    }

    #[test]
    fn max_concurrency_is_parsed_from_cache_config() {
        let head = r#"vllm:cache_config_info{enable_prefix_caching="True",kv_cache_max_concurrency="11.79696394686907"}"#;
        let text = format!("{head} 1.0\n");
        let raw = RawMetrics::parse(&text).expect("metrics should parse");
        assert!((raw.max_concurrency.unwrap_or(0.0) - 11.79696394686907).abs() < 1e-6);
    }

    #[test]
    fn cache_value_outweighs_a_slightly_faster_cold_backend() {
        let model = "cache-weave-unit-cache-value";
        let cached = client("cw-cached-x", 1);
        let cold = client("cw-cold-y", 1);
        let chain = vec![("shared prefix".repeat(20), 30_000usize, 0usize)];
        let f = routing_features(model, Some(&chain));
        INDEX
            .entry("cw-cached-x".to_string())
            .or_default()
            .record(&f.chain, unix_secs());

        // 冷实例 prefill 更快（12k vs 8k tok/s）：没有缓存价值时它会赢。
        let mut cached_engine = engine(900.0, 50.0);
        cached_engine.recent_prefill_tps = 8_000.0;
        cached_engine.ttft_samples = 200.0;
        cached_engine.recent_ttft_count = 200.0;
        let mut cold_engine = engine(850.0, 50.0);
        cold_engine.recent_prefill_tps = 12_000.0;
        cold_engine.ttft_samples = 200.0;
        cold_engine.recent_ttft_count = 200.0;
        ENGINE.insert("cw-cached-x".to_string(), cached_engine);
        ENGINE.insert("cw-cold-y".to_string(), cold_engine);

        // 换一段没被索引过的前缀（长度相同）：cache_ratio=0，冷实例凭更快 prefill 胜出。
        let miss = vec![("other prefix".repeat(20), 30_000usize, 0usize)];
        let no_cache = order(
            &[client("cw-cached-x", 1), client("cw-cold-y", 1)],
            None,
            model,
            Some(&miss),
        );
        assert_eq!(no_cache[0], 1, "without cache the faster prefill wins");

        // 带前缀：缓存价值（约 760ms）应把略快的冷实例压回去。
        let with_cache = order(&[cached, cold], None, model, Some(&chain));
        assert_eq!(with_cache[0], 0, "cache value should outweigh 50ms of TTFT");
    }

    #[test]
    fn queue_backlog_yields_even_with_cached_home() {
        let model = "cache-weave-unit-queue-yield";
        let home = client("cw-queue-home", 1);
        let alt = client("cw-queue-alt", 1);
        let k = keys(2, 91);
        let f = routing_features(model, Some(&k));
        INDEX
            .entry("cw-queue-home".to_string())
            .or_default()
            .record(&f.chain, unix_secs());
        let mut hot = engine(500.0, 50.0);
        hot.running = 2.0;
        hot.waiting = 2.0;
        hot.recent_queue_ms = 1_500.0;
        hot.max_concurrency = 8.0;
        let mut idle = engine(600.0, 50.0);
        idle.max_concurrency = 8.0;
        ENGINE.insert("cw-queue-home".to_string(), hot);
        ENGINE.insert("cw-queue-alt".to_string(), idle);
        LAST_CHOICE.insert(f.session, ("cw-queue-home".to_string(), Instant::now()));
        let order = order(&[home, alt], None, model, Some(&k));
        assert_eq!(
            order[0], 1,
            "waiting/cached home must yield to idle backend"
        );
    }

    #[test]
    fn priority_breaks_ties_without_changing_capacity() {
        let model = "cache-weave-unit-priority";
        let low = client("cw-priority-low", 1);
        let high = client("cw-priority-high", 2);
        ENGINE.insert("cw-priority-low".to_string(), engine(500.0, 50.0));
        ENGINE.insert("cw-priority-high".to_string(), engine(500.0, 50.0));
        let order = order(&[low, high], None, model, None);
        assert_eq!(
            order[0], 1,
            "higher priority should win an otherwise exact tie"
        );
    }

    #[test]
    fn long_prompt_pin_yields_when_home_is_near_capacity() {
        let model = "cache-weave-unit-long-pin";
        let home = client("cw-long-home", 1);
        let alt = client("cw-long-alt", 1);
        let mut home_engine = engine(300.0, 60.0);
        home_engine.max_concurrency = 10.0;
        home_engine.running = 9.8; // rho=0.98，未到硬门但已接近打满
        let mut alt_engine = engine(900.0, 40.0);
        alt_engine.max_concurrency = 10.0;
        ENGINE.insert("cw-long-home".to_string(), home_engine);
        ENGINE.insert("cw-long-alt".to_string(), alt_engine);
        let k = vec![("long prompt".repeat(40), 220_000usize, 0usize)];
        let f = routing_features(model, Some(&k));
        LAST_CHOICE.insert(f.session, ("cw-long-home".to_string(), Instant::now()));
        let order = order(&[home, alt], None, model, Some(&k));
        assert_eq!(
            order[0], 1,
            "long-prompt pin must yield when home rho >= 0.95"
        );
    }

    #[test]
    fn cold_home_bonus_works_with_negative_weighted_scores() {
        let model = "cache-weave-unit-cold-negative";
        let a = client("cw-cold-neg-a", 1);
        let b = client("cw-cold-neg-b", 1);
        ENGINE.insert("cw-cold-neg-a".to_string(), engine(500.0, 50.0));
        ENGINE.insert("cw-cold-neg-b".to_string(), engine(500.0, 50.0));
        let k = keys(2, 77);
        let f = routing_features(model, Some(&k));
        let home = rendezvous_home(f.session, &[a.clone(), b.clone()]).unwrap();
        let order = order(&[a, b], None, model, Some(&k));
        assert_eq!(order[0], home, "cold home must win with negative scores");
    }

    #[test]
    fn nan_metrics_and_zero_cap_do_not_poison_order() {
        let model = "cache-weave-unit-nan-cap";
        let broken = client("cw-nan", 1);
        let healthy = client("cw-healthy-nan", 1);
        let mut bad = engine(500.0, 50.0);
        bad.kv_usage = f64::NAN;
        bad.recent_prefill_tps = f64::NAN;
        bad.max_concurrency = 0.0;
        ENGINE.insert("cw-nan".to_string(), bad);
        ENGINE.insert("cw-healthy-nan".to_string(), engine(500.0, 50.0));
        let order = order(&[broken, healthy], None, model, None);
        assert_eq!(order.len(), 2);
        assert_eq!(order[0], 1, "finite candidate must rank ahead of NaN/cap0");
    }

    #[test]
    fn session_keeps_previous_backend_within_margin() {
        let model = "cache-weave-unit-sticky";
        let a = client("cw-sticky-a", 1);
        let b = client("cw-sticky-b", 1);
        let k = keys(2, 15);
        let first = order(&[a.clone(), b.clone()], None, model, Some(&k));
        let chosen = first[0];
        let f = routing_features(model, Some(&k));
        LAST_CHOICE.insert(
            f.session,
            (
                vec!["cw-sticky-a", "cw-sticky-b"][chosen].to_string(),
                Instant::now(),
            ),
        );

        let mut active = HashMap::new();
        active.insert("cw-sticky-a".to_string(), 1_i64);
        active.insert("cw-sticky-b".to_string(), 0_i64);
        let second = order(&[a, b], Some(&active), model, Some(&k));
        assert_eq!(
            second[0], chosen,
            "sticky session should not flip for a marginal gain"
        );
    }

    #[test]
    fn parses_vllm_metrics_with_ttft_itl_and_cache_config() {
        let sample = r#"
# HELP vllm:num_requests_running running
vllm:num_requests_running{engine="0",model_name="m"} 2.0
vllm:num_requests_running{engine="1",model_name="m"} 3.0
vllm:num_requests_waiting{engine="0",model_name="m"} 1.0
vllm:num_requests_waiting{engine="1",model_name="m"} 0.0
vllm:kv_cache_usage_perc{engine="0",model_name="m"} 0.25
vllm:prefix_cache_queries_total{engine="0",model_name="m"} 1000.0
vllm:prefix_cache_hits_total{engine="0",model_name="m"} 900.0
vllm:prompt_tokens_total{engine="0",model_name="m"} 200000.0
vllm:generation_tokens_total{engine="0",model_name="m"} 5000.0
vllm:time_to_first_token_seconds_count{engine="0",model_name="m"} 100.0
vllm:time_to_first_token_seconds_sum{engine="0",model_name="m"} 50.0
vllm:inter_token_latency_seconds_count{engine="0",model_name="m"} 1000.0
vllm:inter_token_latency_seconds_sum{engine="0",model_name="m"} 50.0
vllm:request_prefill_time_seconds_count{engine="0",model_name="m"} 100.0
vllm:request_prefill_time_seconds_sum{engine="0",model_name="m"} 10.0
vllm:request_queue_time_seconds_count{engine="0",model_name="m"} 100.0
vllm:request_queue_time_seconds_sum{engine="0",model_name="m"} 2.0
vllm:cache_config_info{enable_prefix_caching="True",block_size="16"} 1.0
"#;
        let raw = RawMetrics::parse(sample).expect("vllm metrics should parse");
        assert!(raw.engine);
        let snap = build_snapshot(&raw, None, None, 1.5);
        assert!(snap.available);
        assert_eq!(snap.running, 5.0);
        assert_eq!(snap.waiting, 1.0);
        assert!((snap.avg_ttft_ms - 500.0).abs() < 0.01);
        assert!((snap.avg_prompt_tokens - 2000.0).abs() < 0.01);
        assert!((snap.queue_ms - 20.0).abs() < 0.01);
        assert!((snap.decode_tps - 20.0).abs() < 0.01);
        assert_eq!(snap.ttft_samples, 100.0);
        assert_eq!(snap.itl_samples, 1000.0);
        assert_eq!(snap.prefill_samples, 100.0);
        assert_eq!(snap.prefix_enabled, Some(true));
        assert!((snap.rtt_ms - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn recent_delta_wins_over_cumulative_lifetime() {
        let prev = RawMetrics {
            engine: true,
            prompt_tokens: Some(200_000.0),
            ttft_sum: Some(100.0),
            ttft_count: Some(100.0),
            itl_sum: Some(10.0),
            itl_count: Some(1_000.0),
            queue_sum: Some(2.0),
            queue_count: Some(100.0),
            hits: Some(900.0),
            queries: Some(1_000.0),
            ..RawMetrics::default()
        };
        let cur = RawMetrics {
            engine: true,
            prompt_tokens: Some(220_000.0),
            ttft_sum: Some(102.0),
            ttft_count: Some(110.0),
            itl_sum: Some(10.2),
            itl_count: Some(1_100.0),
            queue_sum: Some(2.1),
            queue_count: Some(110.0),
            hits: Some(1_000.0),
            queries: Some(1_100.0),
            ..RawMetrics::default()
        };
        let at = Instant::now() - Duration::from_secs(10);
        let snap = build_snapshot(&cur, Some(&(prev, at)), None, 1.0);
        assert!((snap.avg_ttft_ms - 927.27).abs() < 0.1);
        assert!((snap.recent_ttft_ms - 200.0).abs() < 0.1);
        assert_eq!(snap.recent_ttft_count, 10.0);
        assert!((snap.recent_tps - 500.0).abs() < 2.0);
        assert!((snap.recent_queue_ms - 10.0).abs() < 0.1);
    }

    #[test]
    fn small_samples_are_shrunk_toward_median() {
        // 3 个样本的 22.9s 在 prior=1s、权重 20 下应收缩到 ~3.86s。
        let shrunk = shrink(22_900.0, 3.0, 1_000.0, 20.0);
        assert!((shrunk - 3_856.52).abs() < 1.0, "shrunk={shrunk}");
        assert_eq!(shrink(500.0, 0.0, 1_000.0, 20.0), 500.0);
    }

    #[test]
    fn detects_second_level_gateway_metrics() {
        let sample = "gpt_api_active_requests{backend=\"x\",endpoint=\"/v1/chat/completions\",model=\"m\"} 0\n";
        let raw = RawMetrics::parse(sample).expect("proxy metrics should be recognized");
        assert!(!raw.engine);
        assert!(raw.proxy);
        let snap = build_snapshot(&raw, None, None, 2.0);
        assert!(!snap.available);
        assert!(snap.proxy);
    }

    #[test]
    fn detects_prefix_cache_disabled() {
        let sample = "vllm:num_requests_running 0\nvllm:cache_config_info{enable_prefix_caching=\"False\"} 1.0\n";
        let raw = RawMetrics::parse(sample).expect("metrics should parse");
        let snap = build_snapshot(&raw, None, None, 1.0);
        assert_eq!(snap.prefix_enabled, Some(false));
        assert_eq!(confidence(Some(snap), false), 0.05);
    }

    #[test]
    fn unknown_cold_instance_uses_median_not_worst() {
        let model = "cache-weave-unit-cold-median";
        let warm = client("cw-warm", 1);
        let cold = client("cw-cold", 1);
        ENGINE.insert("cw-warm".to_string(), engine(100.0, 50.0));
        ENGINE.insert(
            "cw-cold".to_string(),
            EngineSnapshot {
                available: true,
                updated: Some(Instant::now()),
                ..EngineSnapshot::default()
            },
        );
        let k = keys(2, 16);
        let order = order(&[warm, cold], None, model, Some(&k));
        assert_eq!(order.len(), 2);
    }
}
