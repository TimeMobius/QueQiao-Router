# DSH 会话标识与网关审计日志：调研与修复方案

> 基线：网关上游 `main` = `40c4e3d1`（`b679` / `0.2.679`）；DSH `0.2.1-rc.5`
> （`/usr/local/lib/node_modules/@deepseek-ai/dsh`）。本文所有 `文件:行号` 均已核对；
> 本地快照 `src/db/extract.rs`、`src/db/records.rs`、`src/client/proxy.rs`、
> `src/handlers/records_api.rs`、`src/db/records_query.rs` 与上游 `main` **逐字节一致**。
>
> 线上实测环境：`http://192.168.10.121:8000`（`/dashboard/api/records` 只读接口）。

---

## 0. 结论速览

| 问题 | 结论 |
| :--- | :--- |
| **一、`/v1/messages` 为什么没有会话 id** | 网关只认 `x-session-id`（`src/db/extract.rs:544`），而 DSH Messages 路由发的是 **`x-deepseek-harness-session-id`**。线上最近 200 条记录里 172 条 `anthropic.messages` 的 `SessionId` **全部为空**；而抽样 120 条解 `Headers` 载荷，**120/120 都带着这个头**。→ 一行别名即可修好。 |
| **二、chat 接口没有会话 id 怎么看时间线** | 面板「会话时间线」按钮在 `SessionId` 为空时**直接禁用**（`web/assets/records.js:578`），点不动。三条出路见 §3：**(A) 换 `openai-responses` 协议**（pi-ai 会发 `session_id` 头）、**(B) 开 `cacheRetention: long`**（body 带 `prompt_cache_key`）、**(C) 对话前缀指纹聚类**（本仓库已实现 `工具/session_timeline.py`，实测可用）。 |
| **三、子智能体能否取到父会话 id** | **能。** 运行期 `agent.session.header.parentSession` 可读（`dsh-session` 的 `SessionHeader` 公开字段）。而且**父会话 id 现在已经到达网关**——藏在 `/v1/messages` 请求体的 `dsh_session_log.session.parentSession` 里（线上 #49700 实测存在）。但 DSH **不把它放进任何请求头**，`GenerateOptions` 里也没有 `parentSessionId` 字段。 |

---

## 1. 问题一：`/v1/messages` 没取到 DSH 的会话 id

### 1.1 根因

网关 `header_meta` 的取值是硬编码的四个名字：

```rust
// src/db/extract.rs:544-546
session_id: get("x-session-id"),
parent_session_id: get("x-parent-session-id"),
session_affinity: get("x-session-affinity"),
```

而 DSH 的 DeepSeek Messages 适配器发的是（`dsh-llm-deepseek/lib/index.js:2234-2242`）：

```js
headers: {
  ...attributionHeaders(),                 // user-agent
  "content-type": "application/json",
  "accept": "text/event-stream",
  ...auth.headers,                         // x-api-key
  "anthropic-version": "2023-06-01",
  ...betas.length === 0 ? {} : { "anthropic-beta": betas.join(",") },
  "x-deepseek-harness-user-id": this.dependencies.resolveUserId(),
  ...options.sessionId === void 0 ? {} : { "x-deepseek-harness-session-id": String(options.sessionId) },
  ...options.purpose === "compaction" ? { "x-deepseek-harness-compact": "1" } : {}
}
```

两个名字都对不上，于是 `SessionId` / `ParentSessionId` 恒为 `NULL`。

### 1.2 线上证据

```
$ curl '.../dashboard/api/records?limit=200'
('anthropic.messages', has_sessionId=False) 172
('chat.completions',   has_sessionId=False) 27
('chat.completions',   has_sessionId=True)  1     ← 我自己注入 x-session-id 的探针

$ # 抽样 120 条 anthropic.messages，解 Headers 载荷
has_harness_sess 120   no_x_session_id 120   distinct sessions: 3
```

即：**DSH 一直在发，网关一直没接。**

### 1.3 修复（推荐，改动极小）

在 `header_meta` 里加别名回退。注意 **优先级必须是网关原生名优先**，避免客户端
同时发两个头时行为翻转：

```rust
// src/db/extract.rs 顶部（header_meta 之外）新增
/// 按给定顺序返回第一个非空请求头；全部缺失时返回 None。
fn first_header(get: &impl Fn(&str) -> Option<String>, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| get(name))
        .filter(|v| !v.is_empty())
}

// src/db/extract.rs:544-546 改为
session_id: first_header(
    &get,
    &[
        "x-session-id",                  // 网关原生约定（最高优先级）
        "x-deepseek-harness-session-id", // DSH Messages 路由
        "session_id",                    // pi-ai openai-responses 协议（见 §3.2 方案 A）
        "x-client-request-id",           // pi-ai openai-responses / codex
    ],
),
parent_session_id: first_header(
    &get,
    &[
        "x-parent-session-id",
        "x-deepseek-harness-parent-session-id", // DSH 目前不发，预留给将来的上游改动
    ],
),
```

同时建议把 `x-deepseek-harness-session-id` 加入透传白名单（可选）：

```rust
// src/client/proxy.rs:60-67
const FORWARD_HEADERS: &[&str] = &[
    "x-request-id", "x-trace-id", "x-correlation-id",
    "x-session-id", "x-parent-session-id", "x-session-affinity",
    "x-deepseek-harness-session-id",   // 新增：让上游也能关联
];
```

> ⚠️ 白名单是**白名单**：不加这一行，上游永远看不到该头。当前它既不入库也不转发。

### 1.4 顺带解决「父会话 ID」：从请求体取

`x-parent-session-id` DSH 从不发送，但 **`dsh_session_log` 请求体字段里带着它**
（见 §4.2）。所以 `ParentSessionId` 的正确来源是 body，不是 header。
建议在 `src/db/extract.rs` 增加一个 body 提取器，并在 `log_non_streaming_request`
（`src/db/records.rs:298`，`request_body` 已在手边）里做 header→body 的补齐：

```rust
// src/db/extract.rs 新增
/// DSH 官方路由在请求体里携带的会话头：`dsh_session_log.session`。
/// 这是**唯一**能把子会话的父会话 id 送到网关的通道（DSH 不发对应请求头）。
pub fn dsh_session_meta(request_body: &Value) -> (Option<String>, Option<String>) {
    let session = request_body
        .get("dsh_session_log")
        .and_then(|l| l.get("session"));
    let id = session
        .and_then(|s| s.get("id"))
        .and_then(Value::as_str)
        .map(str::to_string);
    let parent = session
        .and_then(|s| s.get("parentSession"))
        .and_then(Value::as_str)
        .map(str::to_string);
    (id, parent)
}
```

```rust
// src/db/records.rs:307 之后
let header = header_meta(headers);
let (body_session_id, body_parent_id) = dsh_session_meta(request_body);
// 请求头优先，body 仅补空缺
let session_id = header.session_id.or(body_session_id);
let parent_session_id = header.parent_session_id.or(body_parent_id);
```

这样 `SessionId` / `ParentSessionId` 双列对 `/v1/messages` 全部就位，
面板的「会话时间线」和「调用树」两个按钮对 DSH 记录立即生效——
面板 `fetchChildren()`（`web/assets/records.js:778-796`）正是靠
`sessionId` + `parentSessionId` 建调用树的，**无需改前端**。

---

## 2. DSH 各路由到底发了什么会话标识（源码核查）

| DSH 路由 | 出站会话标识 | 依据 |
| :--- | :--- | :--- |
| `deepseek`（Messages，`/v1/messages`） | **头** `x-deepseek-harness-session-id` + `x-deepseek-harness-user-id`；**体** `dsh_session_log.session`（含 `parentSession`/`origin`/`delegationDepth`） | `dsh-llm-deepseek/lib/index.js:2240-2241`；`dsh-session-log-deepseek/lib/index.js:25-37` |
| `pi-ai` → `openai-completions`（`/v1/chat/completions`） | **无头**。body 里 `prompt_cache_key` **仅当** host 含 `api.openai.com` 或 `cacheRetention: "long"` | pi-ai `dist/api/openai-completions.js:544-556`、`:580-583` |
| `pi-ai` → `openai-responses` | **头** `session_id` + `x-client-request-id`；**体** `prompt_cache_key` | pi-ai `dist/api/openai-responses.js:224-233`、`:267` |
| `pi-ai` → `anthropic-messages` | 无（除非 `compat.sendSessionAffinityHeaders`，而 DSH 把该开关 **withhold** 了） | `dsh-llm-pi-ai/lib/index.js:421`、`:462` |
| 指向 `openrouter.ai` 的 completions | 头 `x-session-id` | pi-ai `dist/api/openai-completions.js:544-555` + `:1305` |
| Web 搜索 / 模型发现 / Files API | 无 | `dsh-web-search-deepseek/lib/index.js:133-145` 等 |

两条 DSH 侧的关键事实：

1. **`sessionId` 唯一的产出点是 agent loop**：`dsh-agent-loop/lib/index.js:1284-1289`
   给每个请求打 `sessionId: this.session.id`。
2. **`GenerateOptions` 里没有 `parentSessionId`**（`dsh-llm/lib/types/types.d.ts:506-548`，
   全字段：`provider/model/reasoningEffort/messages/system/tools/toolHistory/temperature/
   maxTokens/stop/signal/sessionId/purpose`）。注意类型名是 **`GenerateOptions`**，
   不存在 `RequestOptions`。子会话的 LLM 请求只带**自己的** id。
3. 配置里的 `headers` 是**静态**的（`z.dict(z.string())`，`dsh-llm-pi-ai/lib/index.js:1060`），
   没有变量插值；`transformHeaders` 在 DSH 里**零接线**。→ **改配置拿不到活会话 id。**

---

## 3. 问题二：chat 接口没有会话 id，怎么在面板看时间线

### 3.1 为什么现在点不动

```js
// web/assets/records.js:577-579
'<button type="button" class="drawer-btn" id="timelineBtn"' +
    (d.sessionId ? "" : ' disabled title="该记录没有会话 ID"') + ">" +
    ICON.latency + "会话时间线</button>"
```

`loadTimeline(sessionId)`（`:739`）走 `?session_id=` 过滤 + 全等二次过滤；
`SessionId` 为 `NULL` 时整条链路没有入口。**这是纯数据缺失，不是前端 bug。**

### 3.2 方案 A（最省事，DSH 侧纯配置）：切到 `openai-responses` 协议

pi-ai 的 `openai-responses` 路径**无条件**发 `session_id` + `x-client-request-id`
（`dist/api/openai-responses.js:224-233`），与 host 无关：

```yaml
# ~/.dsh/profiles/web/cordis.patch.yml
- id: llm-pi-ai
  config:
    providers:
      A6:
        api: openai-responses          # 原来是 openai-completions
        baseURL: http://192.168.10.121:8000/v1
```

配合 §1.3 的 `session_id` / `x-client-request-id` 别名，`SessionId` 立刻有值。
**代价**：要求上游支持 `/v1/responses`（网关已支持，见 `docs/API_LLM.md:14`），
且 pi-ai 的 responses 协议对部分兼容网关的参数（`max_output_tokens`、
`prompt_cache_options` 等）比 completions 严格，需要实测。

### 3.3 方案 B（DSH 侧一行配置 + 网关读 body）：`cacheRetention: long`

```yaml
        cacheRetention: long     # 原来未设 → 默认 short
```

此时 pi-ai 在**非 OpenAI host** 上也会把 `prompt_cache_key` 放进 body
（`dist/api/openai-completions.js:580-583`，`supportsLongCacheRetention` 对我们的
自定义网关默认为 `true`），值就是会话 id（`clampOpenAIPromptCacheKey`，上限 64 字符，
`session-8f8597a7-…` 是 45 字符，够用）。

线上已验证该字段**不会让上游报错**：

```bash
$ curl -X POST .../v1/chat/completions -d '{...,"prompt_cache_key":"session-probe-cachekey-1","prompt_cache_retention":"24h"}'
→ 200 OK
```

> ⚠️ **但网关当前会把它吃掉。** `ChatCompletionRequest`（`src/models/requests.rs:154-187`）
> 是逐字段结构体、**没有 `#[serde(flatten)] extra`**，而
> `build_request_body_generic`（`src/handlers/request_body.rs:46`）是从结构体重建 body，
> 未知字段全部丢弃。实测：带 `prompt_cache_key` 发出的请求，入库 body 只剩
> `['max_tokens','messages','model','stream']`。
>
> 所以方案 B 需要网关配合：给 `ChatCompletionRequest` 加
> `#[serde(flatten)] pub extra: Map<String, Value>`（顺带修复「任何 OpenAI 新字段都被
> 静默吞掉」这个更大的问题），或在 `header_meta`/`records.rs` 里从**原始请求体**
> 取 `prompt_cache_key` 写入 `SessionId`。

### 3.4 方案 C（不改任何一方，立即可用）：对话前缀指纹聚类

agent 客户端每轮重发**完整历史**且前缀稳定（实测同一会话 system 提示词 8529 字节
逐字节相同），所以 `sha256(system + 前 3 条 message)` 能当会话指纹用。
已实现并实测：`工具/session_timeline.py`。

```bash
# 列出窗口内所有会话（含父子关系）
python3 工具/session_timeline.py --base http://192.168.10.121:8000 --list --last-hours 2
```

实测输出（`--session session-8f8597a7-… --last-hours 1.5`）：

```
窗口：最近 1.5h；归属 328 条记录到 2 个会话线索，0 条无法归属
● session-8f8597a7-fbeb-4529-84bd-acb7785bc851  (165 条)
  21:48:09.331  anthropic.messages deepseek-flash 200   415.5ms tok=786   #49506
  ...
  ● 99484ac5-debe-4bb3-9f16-611a73e85c0b  (165 条)   ← 父会话 session-8f8597a7-…
```

chat 侧聚类效果（40 条 `chat.completions` → 7 个指纹簇，最大簇 17 条，`--last-hours 0.75`）：

```
● prefix:22eb881aacb1424a  (17 条)
  21:42:31.293  chat.completions DeepSeek-V4.1-Flash 200  164957.2ms tok=152597 #49474
  21:47:20.700  chat.completions DeepSeek-V4.1-Flash 200  288612.7ms tok=162502 #49505
  ...
● prefix:270b7773a53259b0  (9 条)
  21:42:29.297  chat.completions DeepSeek-V4.1-Flash 200  153741.6ms tok=426142 #49473
  ...
```

同一簇内消息数与 token 单调增长，说明聚类正确（同一会话在持续推进）。

**局限**：compaction 后前缀变化 → 时间线断成多段；前缀相同的并发 fork 会被合并。

### 3.5 建议的组合

| 目标 | 做法 |
| :--- | :--- |
| 立刻能看 | 方案 C（`工具/session_timeline.py`），零改动 |
| 一次性根治 | 网关 §1.3 别名 + §1.4 body 补齐（覆盖 `/v1/messages`）**＋** DSH 侧方案 A 或 B（覆盖 `/v1/chat/completions`） |
| 长期 | 网关把 `x-deepseek-harness-*`、`session_id`、`prompt_cache_key` 纳入统一「会话标识解析链」，并写进 `docs/API_RECORDS.md` 的对应关系表 |

---

## 4. 问题三：DSH 子智能体能不能取到父会话 id

### 4.1 运行期：能

子会话创建时把父 id 写进**持久化 session header**
（`dsh-subagent/lib/types/child-agent.js:107-121`）：

```js
export function childSessionMeta(parent, childDepth, isSeeded, selectedPreset) {
    const parentHeader = parent.session.header;
    return {
        ...parentHeader.cwd !== undefined ? { cwd: parentHeader.cwd } : {},
        ...agentPreset === undefined ? {} : { agentPreset },
        parentSession: parentHeader.id,   // ← 父会话 id
        isSeeded,
        origin: 'subagent',
        delegationDepth: childDepth,
    };
}
```

`SessionHeader` 是**公开只读**的（`dsh-session/lib/types/index.d.ts:150`
`readonly header: SessionHeader`），字段含 `parentSession?` / `origin?` /
`delegationDepth?` / `agentPreset?`。DSH 内部自己在用：

- `dsh-subagent/lib/types/manager.js:701-712` `liveLineage()` 沿 `parentSession` 向上走；
- `dsh-session-telemetry/lib/index.js:244-246` 上报 `session.parent_id`。

所以**子智能体（或任何插件）都能读到自己的父会话 id**：
`agent.session.header.parentSession`，或 `ctx.sessions.get(id).header.parentSession`。

本会话实测（读本地会话日志）：

```json
{"type":"session","version":4,"id":"99484ac5-debe-4bb3-9f16-611a73e85c0b",
 "parentSession":"session-8f8597a7-fbeb-4529-84bd-acb7785bc851",
 "origin":"subagent","delegationDepth":1,"agentPreset":"standard"}
```

### 4.2 但「到网关」只有一条路，而且是 body 不是 header

`dsh_session_log` 请求扩展把整条 header 塞进请求体
（`dsh-session-log-deepseek/lib/index.js:25-37` `wireHeader`），
`parentSession` 就在里面。**线上已验证**（记录 #49700，是子智能体发的）：

```json
// POST /v1/messages 请求体里的 dsh_session_log.session
{"id":"99484ac5-debe-4bb3-9f16-611a73e85c0b",
 "parentSession":"session-8f8597a7-fbeb-4529-84bd-acb7785bc851",
 "origin":"subagent","delegationDepth":1,"agentPreset":"standard",
 "cwd":"/root/Codes/QueQiao-Router","createdAt":1793973054169,"version":4}
```

- 该扩展默认开启（`Config.enabled` 默认 `true`），由 base bundle 挂载。
- **仅 DeepSeek Messages 路由**有效：注册表是 `deepseekLlmApiExtensions`，
  只在 `dsh-llm-deepseek/lib/index.js:2208-2225` 被消费。**pi-ai 路由完全不经过它。**

### 4.3 缺口与最小改法（若要给 OpenAI chat 也带上）

现状：子会话的 LLM 请求只带自己的 `sessionId`，`GenerateOptions` 无 `parentSessionId`。

最小正确改法（两步，都不改 pi-ai）：

1. `dsh-llm/lib/types/types.d.ts` 的 `GenerateOptions` 增加
   `parentSessionId?: Branded<'SessionId'>`（可选 `delegationDepth?: number`）。
2. `dsh-agent-loop/lib/index.js:1284-1289` 的 `buildRequest` 打上
   `...this.session.header.parentSession === undefined ? {} : { parentSessionId: this.session.header.parentSession }`。
3. `dsh-llm-pi-ai/lib/index.js:1983-1991` 在传给 `streamSimple` 的 options 里合并
   per-request headers（pi-ai 的 `options.headers` 最后合并、优先级最高）：
   `headers: { ...requestHeaders(profile.headers), "x-session-id": sid, "x-parent-session-id": parent }`。

> 注意：**不要把这两个字段并进 `LlmCallConfig`/`callConfigEquals`**，
> 否则每一步都会判定为配置变更、刷出假的 `request/header` 事件。

---

## 5. 落地清单（网关侧）

| # | 文件 | 改动 | 影响 |
| :--- | :--- | :--- | :--- |
| 1 | `src/db/extract.rs:544-546` | 会话 id 改为「多别名按优先级回退」 | `/v1/messages` 的 `SessionId` 立即有值 |
| 2 | `src/db/extract.rs`（新增函数） | `dsh_session_meta(request_body)` 解析 `dsh_session_log.session` | `ParentSessionId` 有值 → 面板调用树可用 |
| 3 | `src/db/records.rs:307` | header 优先、body 补空缺 | 同上 |
| 4 | `src/models/requests.rs:154` | `ChatCompletionRequest` 加 `#[serde(flatten)] extra` | 修复「未知 OpenAI 字段被静默吞掉」，为方案 B 铺路 |
| 5 | `src/client/proxy.rs:60` | 白名单加 `x-deepseek-harness-session-id` | 上游可关联 |
| 6 | `docs/API_RECORDS.md` §「ID 字段与请求头对应关系」 | 补别名表 | 文档一致 |

改完按仓库铁律：`cargo fmt --all` → `cargo build` → `cargo test` → 约定式提交。
**`QueQiao-Router-Tests` 不得修改。**

---

## 6. 附：验证命令

```bash
# 1) 确认某条 messages 记录里带了 DSH 会话头（当前 SessionId 仍为 null）
curl -s 'http://192.168.10.121:8000/dashboard/api/records/49701?include=body' \
  | python3 -c 'import json,sys;d=json.load(sys.stdin);h=json.loads(d["headers"]);
print(d["sessionId"], h.get("x-deepseek-harness-session-id"))'

# 2) 确认子会话的父 id 在 body 里
curl -s 'http://192.168.10.121:8000/dashboard/api/records/49700?include=body' \
  | python3 -c 'import json,sys;d=json.load(sys.stdin);
print(json.loads(d["request"])["dsh_session_log"]["session"])'

# 3) 会话时间线（当前即可用的客户端方案）
python3 工具/session_timeline.py --base http://192.168.10.121:8000 \
  --session session-8f8597a7-fbeb-4529-84bd-acb7785bc851 --last-hours 2
```
