# QueQiao-Router (鹊桥路由)

[![Release](https://img.shields.io/github/v/release/TimeMobius/queqiao-router)](https://github.com/TimeMobius/queqiao-router/releases) [![CI/CD](https://github.com/TimeMobius/queqiao-router/actions/workflows/ci.yml/badge.svg)](https://github.com/TimeMobius/queqiao-router/actions/workflows/ci.yml)

**高性能大模型智能路由网关。**

QueQiao-Router 的名字源自“鹊桥相会”的传说：如同喜鹊搭桥连接银河两岸，QueQiao-Router 负责连接客户端与不同的大模型服务。它是一个基于 Rust、Axum 和 Tokio 构建的 LLM 反向代理与智能路由网关，位于客户端和多个模型服务之间；客户端只需要访问一个统一入口，网关便会根据请求中的模型名、匹配规则和负载均衡策略选择后端，转发请求并将响应返回给客户端。

项目的核心目标是把多个异构的模型服务统一成一个稳定、可观测、易运维的访问层。它可以集中管理 OpenAI、Azure、Anthropic、DeepSeek、vLLM、Llama.cpp 等不同类型的后端，并通过 OpenAI 兼容接口、Anthropic Messages API 以及部分 vLLM/Rerank 接口对外提供服务。后端可以使用关键字匹配或精确匹配接收指定模型请求，也可以配置权重、确定性路由和故障转移策略。

一次请求通常会经过以下链路：HTTP API 接收与校验 → 请求预处理和参数合并 → 后端选择与故障转移 → 上游请求转发 → 流式或非流式响应转换 → 指标上报与审计记录。对于流式响应，网关能够在保持 SSE 传输的同时累计 usage、推理内容和工具调用信息，并将完整结果异步写入 Turso 审计库，方便后续排查和统计。

除了请求转发，QueQiao-Router 还提供思考格式归一化、消息清洗、多模态请求转发、配置热加载、Prometheus 指标和按月轮转的审计存储。因此，它适合用作个人或团队的统一模型入口，也适合部署在内部模型集群、多个云厂商后端或需要逐步迁移模型服务的生产环境中。

---

## ✨ 核心特性

### 🚀 极致性能
- **Rust 驱动**: 基于 `Axum` 和 `Tokio` 异步运行时，单核轻松处理数千并发。
- **SIMD 加速**: 核心路径集成 `simd-json`，利用 AVX2/NEON 指令集加速 JSON 解析与序列化。
- **Rayon 并行预处理**: 消息清洗、思考标签移除等预处理阶段通过 `rayon` 并行化，充分利用多核性能。
- **配置缓存**: `extra_body` 在配置加载时预解析为 Value 树，运行时零开销注入。
- **内存优化**: 默认集成 `mimalloc` 分配器，在高并发场景下大幅减少内存碎片与锁竞争。
- **无锁流式处理**: 流式响应处理采用 `mpsc` 通道与后台任务分离架构，避免深拷贝 (Deep Clone)，确保首字节延迟 (TTFT) 最小化。
- **连接池调优**: TCP keepalive（30s）、空闲连接存活（15s）、每 host 最大空闲连接（64），减少高并发建连开销。以上参数可通过顶层 `connection_pool` 配置项调整（重启生效）。

### 🔀 智能流量调度
- **多策略路由**: 支持关键字匹配 (`keyword`) 和精确匹配 (`exact`)。
- **四种负载均衡策略**:
  - **Deterministic**（默认）: 基于内容的 **Multi-Anchor Voting (Rendezvous Hashing)** 算法。相同的提示词（Prompt）永远路由到同一后端，显著提升 **KV Cache 命中率**。每条 `role=user` 消息是一个锚点（前 64 字符做哈希、字节长度作票权），并叠加**轮次几何衰减**：每往回一轮票权乘以 `0.85`（约 4 轮半衰期），越近的轮次影响越大，避免首轮或早期长消息把整条会话永久锁死。集成 **Efraimidis-Spirakis** 加权采样，确保长期流量分布严格遵循 `priority` 权重比例。
  - **Random**: 纯加权随机路由，无状态调度。
  - **LeastConnections**: 加权最少连接路由，实时读取活跃请求数（Prometheus gauge），将新请求调度至当前负载最低的后端。
  - **CacheWeave**: 有状态复合路由。与其他策略同作用域：只接收候选、`routing_keys`、模型名与 `ACTIVE_REQUESTS`，不埋 handler/main/请求模型。v6.4 按「prefill 成本 + queue 成本 + decode/TPS 成本 + 实例繁忙度」排序；cache 命中通过减少 uncached tokens 折入 prefill，不再单独加权。长 prompt 偏 prefill、短请求偏 queue；引擎指标缺失时降权并回退到网关 active/审计。自动探测 vLLM/SGLang `/metrics`，兼容二级网关、外部 API 与不开前缀缓存的后端。**无额外配置**，只需 `strategy: cache_weave`。详见 [`docs/CACHE_WEAVE.md`](docs/CACHE_WEAVE.md)。
- **自动故障转移 (Failover)**: 当前端客户端返回 5xx 错误或网络错误时，自动并发尝试后续客户端（多后端场景）；若所有后端均失败且配置了 `fallback`，则切换至指定的后备模型。
- **灵活的客户端配置**: 支持提示词前缀 (`special_prefix`)、停止序列 (`stop`)、最大令牌数 (`max_tokens`) 和请求体字段注入 (`extra_body`)。

### 🔄 思考格式归一化 (Thinking Format Normalization)
- **多格式互转**: 将模型输出的推理/思考内容在 `<think>...</think>`（ThinkTag）、`reasoning` 字段、`reasoning_content` 字段之间任意转换。
- **流式有状态转换**: 内置 `ThinkingStreamTransformer` 状态机，正确处理跨 SSE chunk 的标记断裂（如 `<thi` + `nk>`），确保流式场景下格式转换零丢失。
- **灵活配置**: 支持全局默认格式 + 每后端覆盖，按需适配不同客户端的需求。

### 📊 深度可观测性
- **全接口 Metrics 覆盖**: `/v1/chat/completions`、`/v1/responses`、`/v1/messages`、`/v1/completions` 均有独立的 Token 用量和延迟指标上报。
- **流式审计**: 即使是 SSE 流式响应，也能完整记录 Token 消耗 (`usage`)、推理过程 (`reasoning_content`) 和工具调用 (`tool_calls`)。
- **Token 统计**: 兼容 OpenAI 标准 `stream_options` 与 Anthropic `input_tokens`/`output_tokens`，自动归一化已支持的上游 Token 统计格式。
- **请求回放**: 完整的请求体和响应体（包括流式拼接后的结果）异步写入 Turso 审计库，支持按月自动轮转归档。
- **统一错误日志**: 无论上游返回何种错误格式，网关层统一标准化错误响应，确保日志与客户端接收到的错误信息完全一致 (422/500)。
- **Prometheus 监控**: 内置 `/metrics` 端点，提供请求总数、活跃请求数、成功率、延迟直方图等关键指标。

### 🛠️ 企业级功能
- **零停机热重载**: 监控配置目录（带 100ms 防抖），修改 `config.yaml` 后自动重载，无需重启服务。
- **`/v1/models` 缓存**: 网关内部缓存模型列表（默认 TTL 600 秒），按凭据指纹隔离，配置热重载时自动失效，支持 TTL=0 关闭。
- **数据清洗**: 自动移除特定模型的 ` thinking... response` 思考标签，通过静态编译的正则引擎高效处理。
- **多模态支持**: 完整支持 Vision (图片) 和 Audio (Whisper) 请求转发。
- **消息优化**: 过滤空消息，减少无效 token 消耗（连续用户消息原样透传）。

---

## ⚠️ 安全注意事项

- **无默认网关认证**: 部署时应自行在前面加一层认证层（如 API Key 验证或反向代理鉴权），否则所有请求将直接放行。CORS 只能控制浏览器跨域访问，不能替代认证。
- **CORS**: 默认配置 permissive，生产环境建议按需限制。
- **审计数据**: 完整记录请求体、响应体、客户端 IP 等审计数据，敏感环境中请配置 `LOG_FULL_TOKEN_ON_ERROR=true` 以便追踪问题，同时注意日志访问权限控制。
- **无内置速率限制**: 不提供请求频率限制，必要时借助外部网关或反向代理实现。
- **API Key 优先级**: 默认（`api_key_priority: "config"`）为 客户端配置中的固定 `api_key` > 请求头 `Authorization: Bearer <token>` > 请求头 `x-api-key`（值本身即 Key，无需前缀）；按后端设置 `api_key_priority: "client"` 可反转为请求头 key 优先、配置 key 兜底。正常日志中 Token 仅显示前 8 字符（缩略）；设置为 `LOG_FULL_TOKEN_ON_ERROR=true` 时，错误日志中记录完整 Token 以便排查。该变量仅建议在受控环境中临时启用。

---

## 🐳 快速部署 (Docker)

### 1. 准备配置
在宿主机创建 `config/config.yaml`：

```yaml
# === 全局负载均衡策略 ===
load_balancing:
  strategy: deterministic   # deterministic | random | least_connections | cache_weave

# === 全局思考格式（可选，每客户端可覆盖）===
# thinking_format: "reasoning"  # passthrough | think_tag | reasoning | reasoning_content

openai_clients:
  - name: "primary_gpt4"
    api_key: "sk-xxxx"
    base_url: "https://api.openai.com/v1"
    model_match:
      type: "keyword"
      value: ["gpt-4"]
    priority: 10
    fallback: "azure_backup"

  - name: "azure_backup"
    api_key: "azure-key"
    base_url: "https://azure-endpoint.com/v1"
    model_match:
      type: "exact"
      value: ["gpt-4-backup"]
    priority: 1
```

### 2. 启动服务

```bash
docker run -d \
  --name queqiao-router \
  -p 8000:8000 \
  -v $(pwd)/config:/app/config \
  -v $(pwd)/logs:/app/logs \
  --restart always \
  ghcr.io/timemobius/queqiao-router:b742
```

> **数据库路径说明**: 源码默认 `RECD_PATH=./record.db`（数据库文件在运行目录，兼容 `sqlite:` 前缀），而 Docker 镜像默认设为 `sqlite:./logs/record.db`（写入挂载的 logs 卷）。若使用 Docker，建议将 logs 目录挂载为主机持久化路径，或显式设置 `RECD_PATH` 环境变量指向挂载路径，避免容器重启后数据丢失。

> **旧库自动升级**: 保持原有 `RECD_PATH`，更新镜像并启动即可。网关自动将旧 SQLite 审计库迁移到
> Turso v8，验证通过后保留 `.bak` 备份并切换到原路径；首次启动会等待迁移完成，无需手动运行
> `--migrate-db`。请为新库和备份预留磁盘空间。详细流程见 [数据库文档](docs/DATABASE_SCHEMA.md)。

---

## ⚙️ 环境变量

| 变量名 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `SERVER_PORT` | `8000` | 服务监听端口 |
| `MAX_REQUEST_BODY_BYTES` | `67108864`（64 MiB） | 请求体大小上限（字节）。Axum 默认仅 2 MiB，多轮 Agent 会话（全量历史 + 内联 base64 图片）会提前触发 413；该值需高于客户端侧预算之和。未设置、非法或 `0` 时回退默认值，详见[请求体大小上限](#-请求体大小上限) |
| `RECD_PATH` | `sqlite:./record.db` | 审计库路径（兼容 `sqlite:` 前缀）；Docker 镜像默认为 `sqlite:./logs/record.db` |
| `DB_ROTATION_CHECK_INTERVAL_SEC` | `60` | 数据库轮转检查间隔（秒） |
| `DB_FTS_OPTIMIZE_AT` | `04:00` | FTS `OPTIMIZE INDEX` 每日执行时刻（本地时区，`HH:MM`）；`off` 关闭 |
| `DB_FTS_OPTIMIZE_INTERVAL_SEC` | 未设置 | 显式设置时改用固定间隔语义（秒）；`0` 关闭。默认不设置，按 `DB_FTS_OPTIMIZE_AT` 每日执行 |
| `AUDIT_BATCH_SIZE` | `200` | 审计写批次行数 |
| `AUDIT_QUEUE_CAPACITY` | `10000` | 审计写队列容量 |
| `AUDIT_FTS_PROMPT_BYTES` / `_TAIL_BYTES` / `_ANSWER_BYTES` | `512` | 检索索引每段截断字节；0 为全文（详见 `docs/WRITE_BATCHING.md`） |
| `ENABLE_TEST_API` | `false` | 启用 `/internal/test/*`（仅测试环境） |
| `RUST_LOG` | `info` | 日志级别 (`error`, `warn`, `info`, `debug`, `trace`) |
| `TZ` | 镜像默认（通常为 UTC） | 日志文件名轮转与所有日志时间戳均跟随该时区；部署时应设为宿主机时区（如 `TZ=Asia/Shanghai`），或挂载 `-v /etc/localtime:/etc/localtime:ro` 自动跟随宿主机 |
| `LOG_FULL_TOKEN_ON_ERROR` | `false` | 错误日志中是否记录完整 Token（默认 false，仅缩略显示） |
| `STREAM_IDLE_TIMEOUT_SEC` | `300` | **首字节之后**、相邻两次上游 SSE 数据之间允许的最大静默（秒）：超时即中断流并返回 `upstream stream interrupted`，避免上游半开连接把请求挂到 30 分钟全局超时；`0` 关闭。**不限制 TTFT**（首 token 等待不计入此预算） |
| `STREAM_FIRST_BYTE_TIMEOUT_SEC` | `0`（关闭） | 上游 SSE **首字节**的最大等待（秒）。默认关闭，交给 60s 响应头超时与 1800s 全局超时；需要给“首 token 很慢”的模型设硬上限时再开启 |
| `STREAM_HEADER_TIMEOUT_SEC` | `60` | 流式请求等待上游**响应头**的上限（秒）；`0` 关闭（只剩 1800s 全局超时）。它决定“首 token 前”的硬上限，与 `STREAM_IDLE_TIMEOUT_SEC`（首字节之后）相互独立 |
| `ARCHIVE_LEGACY_MIGRATION` | 迁移（未设置） | 设为 `skip` 时跳过旧 SQLite 归档的自动升级；v8 归档照常查询 |
| `ANALYSIS_ROLLUP` | 开启 | 历史分析读取 `audit_aggregates.db` 预聚合；设为 `off` 时全部走原始扫描（用于对照与排查） |
| `ANALYSIS_ROLLUP_AT` | `04:00` | active 分片预聚合每日增量构建时刻（本地时区，`HH:MM`）；`off` 关闭。与 FTS 优化共用轮转锁串行执行 |

---

## 📖 支持接口

完全兼容 OpenAI API 规范、Anthropic Messages API 及 vLLM/Rerank 扩展：

| 方法 | 路径 | 描述 |
| :--- | :--- | :--- |
| `GET` | `/health` | 健康检查（配置可用性纳入判定，见[健康检查](#-健康检查)） |
| `GET` | `/v1/models` | 获取聚合模型列表 |
| `GET` | `/metrics` | Prometheus 监控指标 |
| `POST` | `/v1/chat/completions` | 对话接口 (支持流式、多模态、工具调用) |
| `POST` | `/v1/responses` | OpenAI Responses API (支持流式 + 审计日志) |
| `POST` | `/v1/messages` | Anthropic Messages API (支持流式 SSE + 审计日志) |
| `POST` | `/v1/completions` | 文本补全接口 (Legacy) |
| `POST` | `/v1/embeddings` | 向量化接口 |
| `POST` | `/v1/audio/transcriptions` | 语音转文字 (Whisper) |
| `POST` | `/v1/audio/translations` | 语音翻译 |
| `POST` | `/v1/rerank` | Rerank 重排序接口 |
| `POST` | `/rerank` | Rerank 重排序 (兼容 vLLM) |
| `POST` | `/score` | 文本评分接口 (兼容 vLLM) |
| `POST` | `/classify` | 文本分类接口 (兼容 vLLM) |
| `POST` | `/v1/classify` | 文本分类接口 (`/classify` 别名) |
| `POST` | `/v1/systemone` | System One 分类协议 (TypeSafe / OpenRouter Jev) |
| `POST` | `/systemone` | System One 分类协议 (不带 `/v1` 的写法) |
| `POST` | `/v1/tools/fetch` | 工具直通代理：原样转发到上游（缺省 DeepSeek），见 [Tools 直通代理](#tools-直通代理-tools_proxy) |
| `POST` | `/v1/tools/search` | 工具直通代理：原样转发到上游（缺省 DeepSeek），见 [Tools 直通代理](#tools-直通代理-tools_proxy) |

---

## 📋 配置参考

### 全局配置项

```yaml
# 负载均衡策略（默认：deterministic）
load_balancing:
  strategy: deterministic   # deterministic | random | least_connections | cache_weave

# 全局思考格式归一化（可选，每客户端可覆盖）
# thinking_format: "reasoning"   # passthrough | think_tag | reasoning | reasoning_content

# reqwest 连接池（默认值如下；修改后需重启服务，共享 HTTP 客户端仅在启动时构建一次）
connection_pool:
  enabled: true              # 是否启用连接池 keep-alive（false 时等价于 max_idle_per_host: 0）
  max_idle_per_host: 64      # 每 host 最大空闲连接数
  idle_timeout_seconds: 15   # 空闲连接淘汰时间（秒）；0 表示不淘汰空闲连接
  tcp_keepalive_seconds: 30  # TCP keepalive 探测间隔（秒）；0 表示禁用 TCP keepalive

# /v1/tools/fetch 与 /v1/tools/search 的直通代理（可选；缺省直连 DeepSeek）
tools_proxy:
  base_url: "https://api.deepseek.com"
```

### Tools 直通代理 (tools_proxy)

`/v1/tools/fetch` 与 `/v1/tools/search` 是**旁路端点**：请求在中间件层就被转发到上游，
不进入模型路由、调度与审计流程，也没有对应的 handler。

- **不做模型匹配、不解析请求体**：只按路径命中，请求体按字节原样上行；
- **不写审计库、不产生 Prometheus 指标**；
- 请求方法、path（含 query）与 `Authorization` 等 end-to-end 请求头原样透传；
  `Connection` / `Transfer-Encoding` 等逐跳头以及 `Host` / `Content-Length`
  由网关按实际连接重建；
- 响应状态码、响应头与响应体（含流式）原样返回；
- 缺省上游为 DeepSeek 官方端点 `https://api.deepseek.com`，可用
  `tools_proxy.base_url` 替换；`base_url` 可带结尾 `/`，也可写成
  `https://host/v1`（网关会避免拼出 `/v1/v1/...`）；
- 鉴权头**不做替换**，客户端需携带上游可接受的凭据。若编译了
  `check-api-auth` 插件，请求仍会先经过该插件鉴权，再转发到上游。

```yaml
tools_proxy:
  base_url: "https://api.deepseek.com"   # 缺省值；留空/省略同样回退 DeepSeek
```

配置项支持热重载，修改后无需重启即可生效。

### 客户端配置项

```yaml
openai_clients:
  - name: "client_name"
    api_key: "sk-xxx"            # 可选，为空时使用客户端传入的 Key
    api_key_priority: "config"   # 可选：api key 取值优先级，config(默认，配置 key 优先) | client(请求 key 优先)
    base_url: "https://api.example.com/v1"
    model_match:
      type: "keyword"            # 或 "exact"
      value: ["gpt-4", "claude"] # 匹配关键字/精确模型名
    priority: 10                 # 权重，越高越优先
    fallback: "backup_client"    # 故障转移目标
    special_prefix: "<PREFIX>"   # 可选：添加特殊前缀
    stop: ["<STOP1>"]            # 可选：停止序列
    max_tokens: 4096             # 可选：最大令牌数覆盖
    extra_body: |                # 可选：JSON 对象，仅当请求未提供同名字段时注入
      {"frequency_penalty": 1, "presence_penalty": 0.91}
    thinking_format: "reasoning" # 可选：覆盖全局思考格式
    model_rewrite:               # 可选：模型名改写（入站名 → 上游名）
      "deepseek-flash": "deepseek-v4.1-flash"
    model_echo: "upstream"       # 可选：响应体 model 回显，upstream(默认) | inbound
```

### 模型名改写 (model_rewrite) 说明

当上游只认某个模型名、但希望网关对外暴露更简短的别名时使用：

```yaml
openai_clients:
  - name: "hapiopen"
    base_url: "https://hapiopen.cc/v1"
    api_key: "sk-xxx"
    model_match:
      type: "exact"
      value: []                # 可留空：model_rewrite 的键会自动并入可路由模型集
    model_rewrite:
      "deepseek-flash": "deepseek-v4.1-flash"
```

此后客户端用 `"model": "deepseek-flash"` 调用，网关转发给上游时改写为
`deepseek-v4.1-flash`；`GET /v1/models` 也会列出 `deepseek-flash`。

| 规则 | 行为 |
| :--- | :--- |
| 命中映射 | 上游请求体的 `model` 改为映射值 |
| 未命中映射 | 原样透传入站名（等价于默认行为） |
| 路由匹配 | 入站名命中 `model_match` **或** 存在于 `model_rewrite` 的键中即可路由 |
| 日志/指标/审计库 `Model` 列 | 一律记录**入站名**，便于按客户端视角聚合 |
| `fallback` | 属入站名空间；切换后按目标后端的映射重新改写 |
| 改写模式 | 仅精确映射，不做前缀/正则替换，也不做链式映射 |
| 音频 `/v1/audio/*` | multipart 中的 `model` 字段同样被改写 |

**`model_echo`**：默认 `upstream`，响应体里的 `model` 原样透传上游返回的名字（零额外开销）。
设为 `inbound` 则改回客户端请求时用的名字——注意流式路径会因此对带 `model` 的
SSE chunk 多做一次 JSON 解析，仅在确有必要时开启。

### API Key 优先级 (api_key_priority) 说明

每个后端可独立决定“配置里的固定 `api_key`”与“客户端请求带来的 key”谁先用，
另一侧始终作为兜底（不会因为一个字都没匹配到就不发鉴权头）：

| 取值 | 行为 |
| :--- | :--- |
| `config`（默认，缺省即此值） | 优先后端配置的 `api_key`；为空或未配置时用请求 key |
| `client` | 优先请求 key（`Authorization: Bearer` > `x-api-key`）；未带时用配置 `api_key` |

```yaml
openai_clients:
  - name: "passthrough-key"
    base_url: "https://api.example.com/v1"
    api_key: "sk-fallback"       # 客户端没带 key 时使用
    api_key_priority: "client"   # 客户端带了 key 就优先用客户端的
```

无论哪种模式，网关最终都只向上游发送一个 `Authorization: Bearer <key>`，
客户端原始的 `Authorization` / `x-api-key` 不会被原样透传。

> `GET /v1/models` 是唯一例外：该入口固定为“客户端凭证优先、配置 `api_key` 兜底”，
> 不受本设置影响（便于用调用方自己的凭证查询可用模型）。

### 思考格式 (ThinkingFormat) 说明

| 值 | 效果 |
| :--- | :--- |
| `passthrough` | 不做任何转换，原样透传上游输出（默认） |
| `think_tag` | 统一封装为 `<think>...</think>` 包裹在 `content` 中 |
| `reasoning` | 统一放入独立的 `reasoning` 字段 |
| `reasoning_content` | 统一放入独立的 `reasoning_content` 字段 |

支持在流式和非流式两种模式下无损转换，正确处理跨 SSE chunk 的标记断裂。

---

## ⏱️ 超时与重试

| 超时场景 | 默认值 | 说明 |
| :--- | :--- | :--- |
| TCP 连接建立 | 10s | 快速失败，避免长时间等待不可达主机 |
| 流式响应头（首 token 前的硬上限） | 60s | 流式请求（`stream: true`）发出后等待上游**响应头**的上限；`STREAM_HEADER_TIMEOUT_SEC` 可调，`0` 关闭（仅剩 1800s 全局超时） |
| 流式空闲（首字节之后） | 300s | 上游 SSE 已开始出字节后，相邻 chunk 之间允许的最大静默；`STREAM_IDLE_TIMEOUT_SEC` 可调，`0` 关闭。**不计入 TTFT** |
| 客户端全局超时 | 1800s（30min） | 非流式请求的整体超时上限，防止永久挂起 |
| 连接池空闲淘汰 | 15s | 短于常见云 LB/网关的 keepalive 窗口（5~30s），避免取出已被对端关闭的陈旧连接 |
| TCP keepalive | 30s | 防止 NAT/负载均衡器静默切断长连接 |

连接池相关参数（每 host 最大空闲连接数、空闲淘汰时间、TCP keepalive 间隔）由顶层 `connection_pool` 配置项控制，默认值如上表。由于共享 HTTP 客户端仅在启动时构建一次，修改 `connection_pool` 后需重启服务才会生效。

**重试行为**: 仅在发送阶段遇到连接/请求错误（非超时）时自动重建连接重试一次。流式请求默认用 60s 响应头超时快速失败；非流式请求受 1800s 全局超时限制。上游失败时，调度器会尝试其他匹配客户端；所有候选客户端失败后才使用配置的 `fallback`。

---

## ❤️ 健康检查

`GET /health` 同时是**存活**和**配置就绪**探针。除进程存活外，它还检查磁盘上的配置文件当前是否可用：

| 状态 | HTTP | `status` | 触发条件 |
| :--- | :--- | :--- | :--- |
| 正常 | `200` | `ok` | 配置文件可读、可解析、校验通过，且至少配置了一个后端 |
| 降级 | `503` | `degraded` | 运行中文件被删除 / 不可读 / YAML 非法 / 校验失败 / `openai_clients: []` |

响应体（降级时带错误详情；正常时省略 `config_error*` 字段）：

```json
{
  "status": "degraded",
  "config": "unusable",
  "config_generation": 1,
  "config_failures": 4,
  "config_error": "cannot stat config/config.yaml: No such file or directory (os error 2)",
  "config_error_kind": "read"
}
```

- `config_error_kind` 取值为 `read`（读文件失败）/ `parse`（YAML 非法）/ `validate`（校验失败或后端为空），并通过 `x-config-error-kind`、`x-config-failures` 响应头暴露，便于只看状态码/头的探针使用。
- **运行中**配置失效时，内存里的旧配置继续服务（请求不受影响），`/health` 立刻降级；每 30s 重试一次，恢复后自动回到 `200` 并记录 `config file recovered`。
- **启动期**配置不可用直接 panic 退出（不 bind 端口），因此此时 `/health` 是连接被拒（`000`）而非 503——带着空配置启动只会把错误推迟到第一个请求。
- 相同的失败原因只记录一条 `WARN`（`config file is unusable (...)`），后续重复尝试降到 `debug`，避免 30s 轮询刷屏；失败次数累计在 `config_failures` / `gpt_api_config_load_failures_total` 中。

配置健康同时暴露为 Prometheus 指标（`/metrics`）：

| 指标 | 说明 |
| :--- | :--- |
| `gpt_api_config_healthy` | `1` 可用 / `0` 不可用，与 `/health` 状态一致 |
| `gpt_api_config_generation` | 当前生效的配置代数（每次成功热重载 +1） |
| `gpt_api_config_load_failures_total{kind}` | 按 `read`/`parse`/`validate` 统计的失败次数 |

熔断器的后端健康状态也按「模型 × 后端」和「后端」两个维度暴露（`/metrics`）：

| 指标 | 标签 | 说明 |
| :--- | :--- | :--- |
| `gpt_api_model_backend_health` | `model,backend` | `1` 可接流量（closed/disabled） / `0` 冷却中或半开 |
| `gpt_api_model_backend_circuit_state` | `model,backend` | `0` closed / `1` open / `2` half_open / `3` disabled |
| `gpt_api_model_backend_cooldown_remaining_seconds` | `model,backend` | 距下一次可探测的剩余秒数（0 表示无需等待） |
| `gpt_api_backend_health` | `backend` | 不依赖模型的兜底视图，含义同上 |
| `gpt_api_backend_circuit_state` | `backend` | 同上 |
| `gpt_api_backend_cooldown_remaining_seconds` | `backend` | 同上 |

- `model` 维度 = 配置中 `model_match.type: exact` 的模型名、`model_rewrite` 的别名，加上运行期实际路由过的（模型，后端）组合；`keyword` 匹配出的具体模型名只有产生流量后才会出现，冷启动阶段可用 `gpt_api_backend_*` 按后端查看。
- 从未失败过的后端不在熔断注册表里，按 `closed` 导出；`health_check.enabled: false` 时统一导出 `disabled`。
- 每次抓取会重置并重建这些序列（移除已删除的后端/模型）；序列数量上限为 512 个模型 × 每模型 64 个后端，防止异常入站模型名撑爆基数。
- 半开探测被取消/超时回收时，剩余冷却为 0 且 `state=half_open`，下一次请求会重新探测。

> 容器默认 HEALTHCHECK 为 `curl -f http://localhost:8000/health`，因此配置失效会让容器变为 `unhealthy`（配合 `--restart` 策略即可触发自动恢复）。

---

## 📦 请求体大小上限

Axum 的 `DefaultBodyLimit` 默认为 **2 MiB**。Agent 类客户端每一轮都会重发全量历史
（文本 + 内联 base64 图片），几轮之后就会越过 2 MiB；而客户端自身的请求预算
（例如 DSH 的 `maxRequestImageBytes` 默认 20 MiB，且只统计图片 base64 载荷）通常
仍有余量。如果网关先撞墙，客户端会收到一个无法自愈的 413，而历史只增不减，之后
每一轮都会继续 413。因此网关默认上限提升为 **64 MiB**，并可通过
`MAX_REQUEST_BODY_BYTES` 覆盖（单位：字节）：

```bash
# 例如放宽到 128 MiB 后启动
MAX_REQUEST_BODY_BYTES=134217728 ./queqiao_router
```

启动日志会打印当前生效值（`request body limit configured`）。超过上限的请求返回
HTTP 413 和 OpenAI 兼容的结构化错误体，`error.type` 为 `request_too_large`，
消息中带有当前生效阈值，客户端可据此压缩历史或降低图片载荷后重试：

```json
{
  "error": {
    "message": "Request body too large: the gateway accepts request bodies up to 64 MiB (67108864 bytes) and this request exceeded it. Reduce conversation history or inline image payloads and retry. The limit can be raised with the MAX_REQUEST_BODY_BYTES environment variable.",
    "type": "request_too_large",
    "param": null,
    "code": null
  },
  "error_type": "request_too_large"
}
```

> **部署提示**: 如果网关前面还有 Nginx / Envoy / Cloudflare 等反向代理，需要同步
> 调整它们自身的请求体上限（如 Nginx `client_max_body_size`），否则请求会在到达
> 网关之前就被拒绝。

---

## 🛠️ 本地编译与开发

### 环境要求
- **Rust 1.96.0**（与 CI、Dockerfile 固定版本一致；自动迁移使用标准库文件锁，最低要求 1.89）
  - **不要使用 rustc 1.100+**：`turso_core 0.8.3` 的 allocator API 与之不兼容，编译会报 `E0308`（`DynVec::new_in`）。升级工具链前请先在本地验证 `cargo build`。
- C 编译器 (gcc/clang)

### 编译 Release 版本

```bash
# 推荐：通用高性能构建 (x86-64-v3, 兼容大多数现代服务器)
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release

# 可选：针对本机 CPU 极致优化 (仅限本机运行)
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### 本地检查与测试

提交前至少通过以下三项（CI 也执行这三条）：

```bash
# 格式检查
cargo fmt --all -- --check

# Clippy 静态检查（如需覆盖测试代码，可加 --all-targets）
cargo clippy

# 运行单元测试（全量约 30 秒）
cargo test
```

> - **不要加 `--all-features`**：`check-api-auth` feature 通过 `include!` 引用仓库外的
>   `../Check_API/auth-lib`，独立克隆会编译失败。
> - CI 的 clippy 目前不阻塞构建（未使用 `-D warnings`）；新增代码请至少不要增加告警。

### CI 与发布

推送到 `main` 或创建针对 `main` 的 Pull Request 时，CI（固定 Rust 1.96.0）会依次执行 `cargo fmt --all -- --check`、`cargo clippy` 和 `cargo test --verbose`。推送到 `main` 且检查通过后，CI 还会构建并发布 `linux/amd64` 与 `linux/arm64` Docker 镜像到 GitHub Container Registry，并生成 `b<提交数>` 和 `latest` 标签。

另一个发布工作流会根据提交数量创建 GitHub Release，并自动将 README 中的 Docker 镜像标签更新为
`b<提交数>`、将 `Cargo.toml` 中的版本更新为 `0.2.<提交数>`（`Cargo.lock` 未纳入版本控制，构建时由
Cargo 自动重建）。因此，这些版本信息可能会由 CI 自动更新。

---

## 📊 性能调优

本项目已在代码层面做了深度优化 (SIMD, Rayon, 异步流式处理)，建议在生产环境配置以下参数：

- **文件描述符**: 确保宿主机 `ulimit -n` 大于 65535。
- **数据库**: `record.db`（Turso）应置于高性能 SSD 上以避免 I/O 瓶颈。
- **日志级别**: 设置 `RUST_LOG=info` 以获取关键审计信息，`error` 仅记录故障。
- **连接池**: reqwest 客户端已内置 TCP keepalive（30s）、空闲连接存活（15s）、每 host 最大空闲连接数（64），并可通过顶层 `connection_pool` 配置项调整这些参数（修改后需重启服务，共享 HTTP 客户端仅在启动时构建一次）。

---

## 🗄️ 日志与数据库

- **日志文件**: 写入 `logs/` 目录（可通过 `RUST_LOG` 筛选级别），每日自动轮转。
  - `info*.log`: 成功请求访问日志（Nginx Combined 格式）
  - `error*.log`: 错误请求访问日志（包含请求体，仅在 4xx/5xx 时记录）
  - `system*.log`: 系统运行日志（受 `RUST_LOG` 控制）
- **日志保留**: info/system 日志默认保留 10 天，error 日志默认保留 30 天。
- **数据库**: Turso 数据库文件路径由 `RECD_PATH` 控制（见环境变量）。完整审计数据（IP、请求体、响应体、Token 统计）异步写入数据库，支持按月自动轮转。
- **Token 脱敏**: 正常日志中 Token 仅显示前 8 字符；设置 `LOG_FULL_TOKEN_ON_ERROR=true` 后错误日志记录完整 Token 以便排查。

---

## 🔍 调试与排查

- **先看日志**：`logs/error*.log` 记录 4xx/5xx 请求体；`logs/system*.log` 是运行日志，级别由 `RUST_LOG` 控制（如 `RUST_LOG=debug`）。
- **看面板**：`/dashboard` 提供记录、分析与会话时间线；`/dashboard/api/error-log` 可直接取错误日志尾部。面板接口均为只读，可安全频繁轮询。
- **看指标**：`/metrics` 提供请求数、活跃请求、成功率与延迟直方图；`least_connections` 策略读取的也是这里的活跃数。
- **请求级细节**：`RUST_LOG=debug` 时，流式解析出错会额外打印上游响应尾部（最多 16 KiB），用于定位非标准 SSE。
- **写入链路**：`ENABLE_TEST_API=1` 启用 `/internal/test/records|reset|stats`，可验证审计队列的入队、批量落库与队列深度。
- **数据库诊断**：迁移与校验见 [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md)；分析结果与全量扫描对不上时，用 `ANALYSIS_ROLLUP=off` 关闭预聚合做对照。

完整的"症状 → 工具 → 判断"流程见 [DEVELOPER.md](DEVELOPER.md#5-调试与排查)。

---

## 📚 接口与数据库文档

对外模型接口、面板日志检索接口、日志分析接口，以及 Turso 审计库的表结构与索引，集中
存放在本仓库的 `docs/` 目录（随代码一起提交），入口见其中的 `README.md`。本 README 只
保留功能、部署与配置介绍。

## 🔧 可选编译功能

- **`check-api-auth`**: 门控认证插件，编译时启用 `--features check-api-auth`。此功能依赖同目录下 `Check_API/auth-lib` 外部 sibling 库，未编译时不产生任何性能影响，也无默认认证保护。

---

## 📜 许可证

本项目基于 Apache License 2.0 协议开源。
