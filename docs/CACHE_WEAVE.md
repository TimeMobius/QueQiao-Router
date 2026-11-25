# CacheWeave：缓存感知 + prefill/TPS 的纯路由策略

> 状态：v6.4 已实现｜开关：`load_balancing.strategy: cache_weave`
> 代码：`src/client/cache_weave.rs` + `src/client/routing.rs` 的一个分派分支

## 1. 用法

只改一行，不需要新增配置：

```yaml
load_balancing:
  strategy: cache_weave   # deterministic | random | least_connections | cache_weave
```

未配置时仍是 `deterministic`。`cache_weave` 只影响候选排序，不改变模型匹配、鉴权、故障转移、熔断、审计、流式处理等业务路径。

## 2. 作用域

CacheWeave 是 `src/client/routing.rs` 里的一个策略分支，输入只有：

- `Vec<ClientConfig>`（候选后端、priority）；
- `routing_keys`（已有的多轮 user 锚点）；
- 模型名（读取已有的 `model:backend` TTFT/TPS 滑动窗口）；
- `ACTIVE_REQUESTS`（与 `least_connections` 共用的在途请求 gauge）。

它自己维护的状态（前缀索引、引擎探测结果、会话粘性、冷启动计数）全部封在 `cache_weave.rs` 内部，和策略生命周期一致。

## 3. v6.4 代价模型

不构造 E2E：prefill（首 token）与 decode（TPS/TPOT）是两条独立体验轴。

对请求 `r`、候选 `i`：

```text
P         = prompt tokens
c_i       = 连续前缀命中 token（网关索引 + 引擎命中率置信度）
u_i       = max(0, P - c_i)
prefill_i = u_i / S_i                       # S_i = 近期非缓存 prefill tok/s
queue_i   = recent_queue_i + waiting_i * prefill_i / running_i + over_cap
decode_i  = median_tps / TPS_i
rho_i     = inflight_i / cap_i
```

排序成本：

```text
cost_i = w_p * prefill_i/median_prefill
       + w_q * queue_i/median_queue
       + w_d * decode_i
       + w_rho * rho_i
       + 1.5 * kv_i
       - idle_bonus
```

- 长 prompt：`w_p=4, w_q=2, w_d=1.5, w_rho=2`；
- 短请求：`w_p=1.5, w_q=4, w_d=2, w_rho=2`；
- cache 只通过 `u_i` 影响 `prefill_i`，不再作为独立权重；
- TPS 保留在排序中；不假设输出长度。

## 4. 门控与粘性

- 容量：`inflight >= cap*1.2`（burst 时 1.0）；
- KV：已知 `kv > 0.9` 时排除；
- 长 prompt 且非缓存 prefill 明显偏慢（低于候选中位 0.4）时排除；
- `waiting>0` 且 `inflight>cap` 时排除；
- 全部候选被门时：先只放宽容量门，`kv_full`/`slow_prefill` 不放回；全硬故障才保留 rho 最低 2 个。

会话粘性只在 home 未拥堵时生效；长 prompt 的 home `waiting>0` 或 `rho>=0.95` 时降为第 2 候选，避免缓存把热点钉死。

## 5. 引擎指标与降级

引擎指标来自 vLLM/SGLang `/metrics`，延迟 30s 过期。缺失时：

- prefill/TPS 信号权重降低；
- KV 不做硬门；
- queue 使用网关 active/over-cap 近似；
- idle 只在 engine 或 active 至少一个已知时生效；
- 冷请求使用轻量 LRU 轮转，不发 synthetic probe。

兼容二级网关、外部 API、未开前缀缓存的后端；不会因为指标缺失把全部候选门掉。

## 6. 状态与清理

- 前缀索引 TTL 900s，单后端上限 300k；
- 会话/选择状态按会话 key 保存，索引定期清理；
- 引擎探测 lazy-start，成功 8s / 失败 120s，超时 400ms；
- 网关在途预留 lease 15s，避免 burst 内同一空闲快照被集中打爆。

## 7. 测试

`cargo test --lib` 覆盖：

- 缓存亲和/过载释放/长 prompt 让位；
- prefill/TPS 双轴、priority tie-break；
- 冷启动、NaN/cap0、未知 KV、无引擎降级；
- burst/queue 排队让位。

不在生产代码中保留回放/调试路径；算法模块不依赖测试专用时钟或外部 trace。
