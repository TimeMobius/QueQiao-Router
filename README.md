# QueQiao-Router (鹊桥路由)

[![Release](https://img.shields.io/github/v/release/TimeMobius/queqiao-router)](https://github.com/TimeMobius/queqiao-router/releases) [![CI/CD](https://github.com/TimeMobius/queqiao-router/actions/workflows/ci.yml/badge.svg)](https://github.com/TimeMobius/queqiao-router/actions/workflows/ci.yml)

**高性能大模型智能路由网关。**

QueQiao-Router 的名字源自“鹊桥相会”的传说：如同喜鹊搭桥连接银河两岸，QueQiao-Router 负责连接客户端与不同的大模型服务。它是一个基于 Rust、Axum 和 Tokio 构建的 LLM 反向代理与智能路由网关，位于客户端和多个模型服务之间；客户端只需要访问一个统一入口，网关便会根据请求中的模型名、匹配规则和负载均衡策略选择后端，转发请求并将响应返回给客户端。

项目的核心目标是把多个异构的模型服务统一成一个稳定、可观测、易运维的访问层。它可以集中管理 OpenAI、Azure、Anthropic、DeepSeek、vLLM、Llama.cpp 等不同类型的后端，并通过 OpenAI 兼容接口、Anthropic Messages API 以及部分 vLLM/Rerank 接口对外提供服务。后端可以使用关键字匹配或精确匹配接收指定模型请求，也可以配置权重、确定性路由和故障转移策略。

一次请求通常会经过以下链路：HTTP API 接收与校验 → 请求预处理和参数合并 → 后端选择与故障转移 → 上游请求转发 → 流式或非流式响应转换 → 指标上报与审计记录。对于流式响应，网关能够在保持 SSE 传输的同时累计 usage、推理内容和工具调用信息，并将完整结果异步写入 SQLite，方便后续排查和统计。

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
- **连接池调优**: TCP keepalive（30s）、空闲连接存活（15s）、每 host 最大空闲连接（64），减少高并发建连开销。

### 🔀 智能流量调度
- **多策略路由**: 支持关键字匹配 (`keyword`) 和精确匹配 (`exact`)。
- **三种负载均衡策略**:
  - **Deterministic**（默认）: 基于内容的 **Multi-Anchor Voting (Rendezvous Hashing)** 算法。相同的提示词（Prompt）永远路由到同一后端，显著提升 **KV Cache 命中率**。集成 **Efraimidis-Spirakis** 加权采样，确保长期流量分布严格遵循 `priority` 权重比例。
  - **Random**: 纯加权随机路由，无状态调度。
  - **LeastConnections**: 加权最少连接路由，实时读取活跃请求数（Prometheus gauge），将新请求调度至当前负载最低的后端。
- **自动故障转移 (Failover)**: 当前端客户端返回 5xx 错误或网络错误时，自动并发尝试后续客户端（多后端场景）；若所有后端均失败且配置了 `fallback`，则切换至指定的后备模型。
- **灵活的客户端配置**: 支持自定义请求头 (`headers`)、提示词前缀 (`special_prefix`)、停止序列 (`stop`)、最大令牌数 (`max_tokens`) 和请求体字段注入 (`extra_body`)。

### 🔄 思考格式归一化 (Thinking Format Normalization)
- **多格式互转**: 将模型输出的推理/思考内容在 `<think>...</think>`（ThinkTag）、`reasoning` 字段、`reasoning_content` 字段之间任意转换。
- **流式有状态转换**: 内置 `ThinkingStreamTransformer` 状态机，正确处理跨 SSE chunk 的标记断裂（如 `<thi` + `nk>`），确保流式场景下格式转换零丢失。
- **灵活配置**: 支持全局默认格式 + 每后端覆盖，按需适配不同客户端的需求。

### 📊 深度可观测性
- **全接口 Metrics 覆盖**: `/v1/chat/completions`、`/v1/responses`、`/v1/messages`、`/v1/completions` 均有独立的 Token 用量和延迟指标上报。
- **流式审计**: 即使是 SSE 流式响应，也能完整记录 Token 消耗 (`usage`)、推理过程 (`reasoning_content`) 和工具调用 (`tool_calls`)。
- **Token 统计**: 兼容 OpenAI 标准 `stream_options` 与 Anthropic `input_tokens`/`output_tokens`，自动归一化已支持的上游 Token 统计格式。
- **请求回放**: 完整的请求体和响应体（包括流式拼接后的结果）异步写入 SQLite 数据库，支持按月自动轮转归档。
- **统一错误日志**: 无论上游返回何种错误格式，网关层统一标准化错误响应，确保日志与客户端接收到的错误信息完全一致 (422/500)。
- **Prometheus 监控**: 内置 `/metrics` 端点，提供请求总数、活跃请求数、成功率、延迟直方图等关键指标。

### 🛠️ 企业级功能
- **零停机热重载**: 监控配置目录（带 100ms 防抖），修改 `config.yaml` 后自动重载，无需重启服务。
- **`/v1/models` 缓存**: 网关内部缓存模型列表（默认 TTL 600 秒），按凭据指纹隔离，配置热重载时自动失效，支持 TTL=0 关闭。
- **数据清洗**: 自动移除特定模型的 ` thinking... response` 思考标签，通过静态编译的正则引擎高效处理。
- **多模态支持**: 完整支持 Vision (图片) 和 Audio (Whisper) 请求转发。
- **消息优化**: 自动合并连续的用户消息、过滤空消息，减少 token 消耗。

---

## ⚠️ 安全注意事项

- **无默认网关认证**: 部署时应自行在前面加一层认证层（如 API Key 验证或反向代理鉴权），否则所有请求将直接放行。CORS 只能控制浏览器跨域访问，不能替代认证。
- **CORS**: 默认配置 permissive，生产环境建议按需限制。
- **审计数据**: 完整记录请求体、响应体、客户端 IP 等审计数据，敏感环境中请配置 `LOG_FULL_TOKEN_ON_ERROR=true` 以便追踪问题，同时注意日志访问权限控制。
- **无内置速率限制**: 不提供请求频率限制，必要时借助外部网关或反向代理实现。
- **API Key 优先级**: 客户端配置中的固定 `api_key` > 请求头 `Authorization: Bearer <token>` > 请求头 `x-api-key`（值本身即 Key，无需前缀）。正常日志中 Token 仅显示前 8 字符（缩略）；设置为 `LOG_FULL_TOKEN_ON_ERROR=true` 时，错误日志中记录完整 Token 以便排查。该变量仅建议在受控环境中临时启用。

---

## 🐳 快速部署 (Docker)

### 1. 准备配置
在宿主机创建 `config/config.yaml`：

```yaml
# === 全局负载均衡策略 ===
load_balancing:
  strategy: deterministic   # deterministic | random | least_connections

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
  ghcr.io/timemobius/queqiao-router:b479
```

> **数据库路径说明**: 源码默认 `RECD_PATH=sqlite:./record.db`（数据库文件在运行目录），而 Docker 镜像默认设为 `sqlite:./logs/record.db`（写入挂载的 logs 卷）。若使用 Docker，建议将 logs 目录挂载为主机持久化路径，或显式设置 `RECD_PATH` 环境变量指向挂载路径，避免容器重启后数据丢失。

---

## ⚙️ 环境变量

| 变量名 | 默认值 | 说明 |
| :--- | :--- | :--- |
| `SERVER_PORT` | `8000` | 服务监听端口 |
| `RECD_PATH` | `sqlite:./record.db` | SQLite 数据库连接字符串；Docker 镜像默认为 `sqlite:./logs/record.db` |
| `DB_ROTATION_CHECK_INTERVAL_SEC` | `60` | 数据库轮转检查间隔（秒） |
| `RUST_LOG` | `info` | 日志级别 (`error`, `warn`, `info`, `debug`, `trace`) |
| `LOG_FULL_TOKEN_ON_ERROR` | `false` | 错误日志中是否记录完整 Token（默认 false，仅缩略显示） |

---

## 📖 支持接口

完全兼容 OpenAI API 规范、Anthropic Messages API 及 vLLM/Rerank 扩展：

| 方法 | 路径 | 描述 |
| :--- | :--- | :--- |
| `GET` | `/health` | 健康检查 |
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

---

## 📋 配置参考

### 全局配置项

```yaml
# 负载均衡策略（默认：deterministic）
load_balancing:
  strategy: deterministic   # deterministic | random | least_connections

# 全局思考格式归一化（可选，每客户端可覆盖）
# thinking_format: "reasoning"   # passthrough | think_tag | reasoning | reasoning_content
```

### 客户端配置项

```yaml
openai_clients:
  - name: "client_name"
    api_key: "sk-xxx"            # 可选，为空时使用客户端传入的 Key
    base_url: "https://api.example.com/v1"
    model_match:
      type: "keyword"            # 或 "exact"
      value: ["gpt-4", "claude"] # 匹配关键字/精确模型名
    priority: 10                 # 权重，越高越优先
    fallback: "backup_client"    # 故障转移目标
    special_prefix: "<PREFIX>"   # 可选：添加特殊前缀
    stop: ["<STOP1>"]            # 可选：停止序列
    max_tokens: 4096             # 可选：最大令牌数覆盖
    headers:                     # 可选：自定义请求头
      "X-Custom-Header": "value"
    extra_body: |                # 可选：JSON 对象，仅当请求未提供同名字段时注入
      {"frequency_penalty": 1, "presence_penalty": 0.91}
    thinking_format: "reasoning" # 可选：覆盖全局思考格式
```

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
| 流式 TTFB（首字节） | 60s | 流式请求（`stream: true`）发送后 60s 内未收到响应则超时 |
| 客户端全局超时 | 1800s（30min） | 非流式请求的整体超时上限，防止永久挂起 |
| 连接池空闲淘汰 | 15s | 短于常见云 LB/网关的 keepalive 窗口（5~30s），避免取出已被对端关闭的陈旧连接 |
| TCP keepalive | 30s | 防止 NAT/负载均衡器静默切断长连接 |

**重试行为**: 仅在发送阶段遇到连接/请求错误（非超时）时自动重建连接重试一次。流式请求使用 60s TTFB 超时快速失败；非流式请求受 1800s 全局超时限制。上游失败时，调度器会尝试其他匹配客户端；所有候选客户端失败后才使用配置的 `fallback`。

---

## 🛠️ 本地编译与开发

### 环境要求
- Rust 1.75+
- C 编译器 (gcc/clang)

### 编译 Release 版本

```bash
# 推荐：通用高性能构建 (x86-64-v3, 兼容大多数现代服务器)
RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release

# 可选：针对本机 CPU 极致优化 (仅限本机运行)
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

### 本地检查与测试

```bash
# 格式检查
cargo fmt --all -- --check

# Clippy 静态检查
cargo clippy --all-targets --all-features -- -D warnings

# 运行单元测试
cargo test
```

### CI 与发布

推送到 `main` 或创建针对 `main` 的 Pull Request 时，CI 会依次执行格式检查、Clippy 和 Rust 单元测试。推送到 `main` 且检查通过后，CI 还会构建并发布 `linux/amd64` 与 `linux/arm64` Docker 镜像到 GitHub Container Registry，并生成 `b<提交数>` 和 `latest` 标签。

另一个发布工作流会根据提交数量创建 GitHub Release，并自动将 README 中的 Docker 镜像标签更新为对应版本。因此，README 中的镜像标签可能会由 CI 自动更新。

---

## 📊 性能调优

本项目已在代码层面做了深度优化 (SIMD, Rayon, 异步流式处理)，建议在生产环境配置以下参数：

- **文件描述符**: 确保宿主机 `ulimit -n` 大于 65535。
- **数据库**: `record.db` (SQLite) 应置于高性能 SSD 上以避免 I/O 瓶颈。
- **日志级别**: 设置 `RUST_LOG=info` 以获取关键审计信息，`error` 仅记录故障。
- **连接池**: reqwest 客户端已内置 TCP keepalive（30s）、空闲连接存活（15s）、每 host 最大空闲连接数（64），无需额外配置。

---

## 🗄️ 日志与数据库

- **日志文件**: 写入 `logs/` 目录（可通过 `RUST_LOG` 筛选级别），每日自动轮转。
  - `info*.log`: 成功请求访问日志（Nginx Combined 格式）
  - `error*.log`: 错误请求访问日志（包含请求体，仅在 4xx/5xx 时记录）
  - `system*.log`: 系统运行日志（受 `RUST_LOG` 控制）
- **日志保留**: info/system 日志默认保留 10 天，error 日志默认保留 30 天。
- **数据库**: SQLite 文件路径由 `RECD_PATH` 控制（见环境变量）。完整审计数据（IP、请求体、响应体、Token 统计）异步写入数据库，支持按月自动轮转。
- **Token 脱敏**: 正常日志中 Token 仅显示前 8 字符；设置 `LOG_FULL_TOKEN_ON_ERROR=true` 后错误日志记录完整 Token 以便排查。

---

## 🔧 可选编译功能

- **`check-api-auth`**: 门控认证插件，编译时启用 `--features check-api-auth`。此功能依赖同目录下 `Check_API/auth-lib` 外部 sibling 库，未编译时不产生任何性能影响，也无默认认证保护。

---

## 📜 许可证

本项目基于 Apache License 2.0 协议开源。
