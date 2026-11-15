# QueQiao-Router 开发者指南

> 工具链固定 **Rust 1.96.0**（与 CI、Dockerfile 一致）。不要使用 rustc 1.100+：
> `turso_core 0.8.3` 的 allocator API 与之不兼容，编译会报 `E0308`（`DynVec::new_in`）。
> 最低支持 Rust 1.89（数据库自动迁移使用标准库文件锁）。

## 1. 架构深度解析

QueQiao-Router 基于 Tokio 全异步运行时构建。请求主路径尽量不做阻塞操作与深拷贝，
审计落库、指标计算、配置热重载、数据库轮转都在独立任务中完成。

### 1.1 核心组件交互

```text
Request
  │
  ▼
[Axum Router]  ← access_log middleware（最外层） + metrics middleware
  │
  ├─► /health · /v1/models · /metrics · /dashboard/*
  │
  ▼
[CommonHandler]  统一入口：参数预处理 / 流式或非流式分发
                 （Chat / Embedding / Rerank / Classify / SystemOne）
  │
  ▼
[DispatcherService]  路由匹配 · 负载均衡 · 熔断 · 故障转移 · fallback
  │
  ▼
[ClientManager]  共享 reqwest Client（连接池、keepalive）
  │
  ▼
[Upstream Backend]
  │
  ├─ 非流式 ─► 响应转换 ─► 客户端
  │
  └─ 流式 ──► [StreamHandler]
                 │  热路径：SSE 原样透传（仅必要改写）
                 └─► mpsc::unbounded_channel<String>
                          │
                          ▼
                    [Stream Logger Task]  累积 usage / reasoning / tool_calls
                          │
                          ▼
                    [AuditWriter]  有界队列 10,000 · 每批 200 行 · 20ms 刷新
                          │
                          ▼
                    [Turso v8 单文件审计库]  ← 月度轮转 + FTS
```

旁路任务：

- **Metrics**：`metrics_middleware` 投递事件 → metrics worker 任务 → 滑动窗口（1m/10m/1h）→ Prometheus。
  `least_connections` 策略读取的活跃请求数也来自这里的 gauge。
- **Config**：`ConfigManager` 用 `notify` 监听 `config/config.yaml`，事件后延迟 100ms 等待写入落盘；
  另有 30s 轮询兜底（NFS 等场景 inotify 收不到远端写入）。两条路径都按 `(mtime, size)` 指纹去重，
  解析失败保留旧配置并在下个事件/轮询重试。
- **Rotation**：后台任务每 60s 检查一次（`DB_ROTATION_CHECK_INTERVAL_SEC`），跨月时
  seal（排空写队列 + WAL checkpoint）→ 把 `record.db` 改名为 `record_YYYYMM.db` → 新建空库继续写。

### 1.2 关键模块设计

#### 1.2.1 DispatcherService（调度层）

- **职责**：路由匹配、负载均衡排序、熔断、**故障转移循环**与 fallback 模型切换。
- **负载均衡**：`deterministic`（默认）使用按 `priority` 加权的多锚点 Rendezvous Hashing，
  相同 routing key 稳定命中同一后端以提升 KV Cache 命中率；请求未提供 `routing_keys` 时
  自动回退 `random`（Efraimidis–Spirakis 加权采样）；`least_connections` 按实时活跃请求数选择。
- **熔断与 4xx 语义**：只有 **429** 计入后端故障；401/403/404 会尝试下一个后端（别的后端可能
  有权限或支持该模型），但不冷却后端；其余 4xx（400/422/413）是请求本身的问题，直接透传。
  5xx 与网络错误进入故障转移链。
- **日志一致性**：无论重试多少次，最终错误体会带上 `Tried: ["A", "B"]`，与 Access Log 一致。
- **熔断配置**：`health_check` 段控制 `CircuitBreakerRegistry`；处于冷却期的后端不会被调用，
  全部冷却时返回 503（`error.type = upstream_unavailable`）。

#### 1.2.2 StreamHandler（流式处理层）

这是系统的性能热点，采用读写分离：

- **主路径（Hot Path）**：把上游 SSE 原样透传给客户端，只做必要改写（模型名回写、
  `special_prefix` 注入、空 `choices` 占位、缺失 `[DONE]` 时补发哨兵）。
- **后台任务**：通过 `mpsc::unbounded_channel<String>` 接收数据副本，在独立 Tokio 任务中
  反序列化、累积 token usage / 推理内容 / 工具调用，再交给审计 writer。
- **异常可观测**：流式解析出错时，`RUST_LOG=debug` 会打印上游响应尾部（最多 16 KiB），
  用于定位非标准 SSE 分帧。
- **思考格式**：`ThinkingStreamTransformer` 是有状态状态机，能处理跨 chunk 的标记断裂
  （如 `<thi` + `nk>`）。

#### 1.2.3 审计与存储

- **引擎**：Turso 0.8.3 内嵌、单文件、WAL；Schema 版本 v8，详见
  [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md)。
- **写入**：`AuditWriter` 是唯一写者。数据走有界队列（默认 10,000），按 200 行/批或 20ms
  超时提交；控制通道单独处理 `Seal` / `Flush` / `Reset` / `Shutdown`。队列满时等待
  `AUDIT_ENQUEUE_TIMEOUT_MS` 后丢弃并计数；SIGTERM / Ctrl-C 会先 flush 再退出。
  详见 [docs/WRITE_BATCHING.md](docs/WRITE_BATCHING.md)。
- **读取**：`ReaderPool` 用信号量限制并发（每个分片默认 8 个 reader，获取超时 5s），
  连接复用；轮转时整体替换 `Arc<AuditStore>`，在途 reader 继续使用旧 store。
- **检索**：`SearchText` 单列 ngram FTS；正文全文仍保存在表中，详情接口返回全文。
  索引文本默认按 512/512/512 字节截断，可用 `AUDIT_FTS_*` 调整。
- **维护**：默认每天 04:00（`DB_FTS_OPTIMIZE_AT`）执行一次 `OPTIMIZE INDEX`。
- **迁移**：启动时探测库版本，旧 SQLite 自动迁移到 v8（保留 `.bak`，校验通过后才切换）；
  写打开前有 probe 安全闸，拒绝未知结构或更高版本。诊断可用
  `--migrate-db` / `--verify-db`。

#### 1.2.4 指标

`metrics_middleware` 只投递轻量事件，不做计算；worker 任务维护滑动窗口并更新 Prometheus
指标，避免把统计开销压到请求路径。面板的 `/dashboard/api/metrics` 是独立的只读查询接口。

#### 1.2.5 配置热重载

- 监听父目录以兼容编辑器"临时文件 + rename"式保存。
- `connection_pool` 只在启动时构建共享 HTTP 客户端，修改后**必须重启**；其余配置热生效。
- 配置代数（`config_generation`）随每次成功重载递增，可用于判断请求看到的是哪一版配置。

## 2. 构建系统与优化

### 2.1 CPU 指令集

- **x86-64-v3 (AVX2)**：Docker amd64 镜像默认目标，覆盖绝大多数现代服务器。
- **tsv110**：ARM64 镜像目标（鲲鹏 920 等）。
- 不默认使用 `v4`（AVX-512）：部分 CI Runner 不支持，会导致 proc-macro 崩溃（`SIGILL`）。
- 本机追求极致性能可用 `RUSTFLAGS="-C target-cpu=native"`，但产物只能在本机运行。

### 2.2 内存分配器

默认链接 `mimalloc`。高并发异步场景下可减少内存碎片与分配器锁竞争。

### 2.3 发布配置

- `release` profile：`lto = true`、`codegen-units = 1`、`panic = "abort"`、`strip = true`。
- `Cargo.lock` **未纳入版本控制**（见 `.gitignore`），由 Cargo 在构建时重建；
  `build.rs` 会读取 lock 中的 reqwest 版本注入编译期变量，读不到时回退 `unknown`。
- 版本号由 CI 按提交数生成：`Cargo.toml` 为 `0.2.<提交数>`，镜像标签为 `b<提交数>`。

### 2.4 已知限制

- Tokio 运行时当前硬编码 `worker_threads(128)`（见 `src/main.rs`），在核心数较少的机器上
  线程数偏多，后续可考虑改为按 CPU 数或环境变量配置。
- `check-api-auth` feature 通过 `include!` 引用仓库外的 `../Check_API/auth-lib`，
  独立克隆时不能启用该 feature。

## 3. 开发流程与质量门禁

1. **小步增量**：一次只改一小块，改完立刻编译 + 测试，避免大范围同时变更。
2. **提交前必须通过**：
   ```bash
   cargo fmt --all -- --check
   cargo clippy        # 加 --all-targets 可额外覆盖测试代码
   cargo test
   ```
   CI 按同样顺序执行（测试为 `cargo test --verbose`），工具链固定 1.96.0。clippy 目前只告警、
   不阻塞（未加 `-D warnings`），但新增代码不应增加告警。
3. **提交**：门禁通过后立即提交；涉及接口、配置、行为变化时同步更新 README / `docs/`。
4. **发布**：推送到 `main` 后 CI 构建双架构镜像并生成 `b<提交数>` 标签，发布工作流负责
   Release 与版本号回写。
5. **外部测试套件受保护**：`../QueQiao-Router-Tests` 的代码**不得修改**，它是可选的
   黑盒验收工具，不属于每次提交的必需流程。

## 4. 测试与验证

### 4.1 单元测试（必需）

- 测试与代码同文件（`#[cfg(test)]`），当前 382 个，全量约 30 秒。
- 覆盖重点：dispatcher 的 4xx 分类/熔断/fallback、流式分帧与 `[DONE]` 补发、思考格式转换、
  Turso writer 批量/seal/reset、归档与迁移校验、面板聚合与扫描结果一致性。
- 数据库相关测试使用临时目录，不会污染仓库根目录的 `record.db`。

### 4.2 外部端到端测试（可选）

用于验证真实 HTTP/SSE 线格式、路由装配、故障转移与热重载等跨进程行为。

```bash
# 首次：创建虚拟环境并安装依赖
python3 -m venv ../QueQiao-Router-Tests/.venv
../QueQiao-Router-Tests/.venv/bin/python3 -m pip install -r ../QueQiao-Router-Tests/requirements.txt

# 运行前先清理遗留进程，避免端口冲突
pkill -f queqiao_router || true

# 全量 / 分阶段
cd ../QueQiao-Router-Tests && source .venv/bin/activate
python test_runner_enhanced.py --api-version rust --stage all
python test_runner_enhanced.py --api-version rust --stage phase1
python test_runner_enhanced.py --api-version rust --stage phase4
```

阶段划分：Phase 1 基础功能（Chat / Completion / Embedding / 输入校验 / 日志），Phase 2
数据库记录完整性，Phase 3 负载均衡与权重，Phase 4 性能基准。报告输出到
`../QueQiao-Router-Tests/test_report.md`。

**扩展 mock 行为**：不要新增 API 端点，而是用专用模型名触发；在 mock server 中识别该模型名
并返回预设响应。报告文案在 `TEST_EXPECTATIONS` / `TEST_STEP_LOGIC` 中维护。

## 5. 调试与排查

### 5.1 日志

| 文件 | 内容 | 保留 |
| :--- | :--- | :--- |
| `logs/info*.log` | 成功请求访问日志（Nginx Combined） | 10 天 |
| `logs/error*.log` | 4xx/5xx 访问日志，含请求体 | 30 天 |
| `logs/system*.log` | 运行日志，受 `RUST_LOG` 控制 | 10 天 |

级别用 `RUST_LOG` 控制（默认 `info`）。需要看上游原始响应时用 `RUST_LOG=debug`：
流式解析失败会打印响应尾部（最多 16 KiB）。`LOG_FULL_TOKEN_ON_ERROR=true` 可在错误日志中
记录完整 Token（仅限受控环境）。

### 5.2 面板与只读 API

`/dashboard`（内嵌静态资源）提供监控、记录、分析三页；排查时常用的只读接口：

- `GET /dashboard/api/records`、`GET /dashboard/api/records/{id}`：列表与详情（含请求体、响应体、Token、时延）
- `GET /dashboard/api/records/facets`：筛选项
- `GET /dashboard/api/session-timeline`：按会话/父会话聚合时间线
- `GET /dashboard/api/analysis`、`GET /dashboard/api/analysis/errors`：聚合分析
- `GET /dashboard/api/error-log`：错误日志尾部
- `POST /dashboard/api/metrics`：轻量指标查询

这些接口不写库、不产生 metrics、不写访问日志，可以安全地频繁轮询。

### 5.3 指标

`GET /metrics`（Prometheus 文本格式）：请求总数、活跃请求、成功率、延迟直方图、
RPS/TPS 及其 1m/10m/1h 滑动值。排查"是上游慢还是网关慢"时，先看延迟与活跃数，
再看 `logs/error*.log` 的具体请求。

### 5.4 请求级调试

- **流式异常**：`RUST_LOG=debug`，搜索 `captured upstream response body tail after SSE stream error`，
  对照原始分帧判断是否为非标准 SSE。
- **审计写入链路**：`ENABLE_TEST_API=1` 启动后可用
  `POST /internal/test/records`（插入样本）、`GET /internal/test/stats`（队列深度、已 flush 行数）、
  `POST /internal/test/reset`（清空并重置 id）。注意该路由只在测试环境开启。
- **`src/metrics/debug_timing.rs`**：定义了 T1–T7 延迟锚点，但当前**未在 `metrics/mod.rs` 声明，
  也未接线**，属于未启用代码；需要时先接入模块与调用点再使用，不要以为设置 `RUST_LOG=debug` 就会出现。

### 5.5 数据库诊断

- 自动迁移失败会保留原库、不使用未完成的新库启动；先看 `logs/system*.log` 的迁移错误。
- 手动演练：`queqiao-router --migrate-db --from <旧库> --to <新库> --batch 500 --defer-fts`，
  再用 `--verify-db --from <旧库> --to <新库>` 校验行数、payload 与时间范围。
- 分析结果与全量扫描对不上：`ANALYSIS_ROLLUP=off` 关闭预聚合，强制走原始扫描做对照。
- 旧归档未升级：检查 `ARCHIVE_LEGACY_MIGRATION` 是否为 `skip`。
- 轮转后新数据不见了：确认 `RECD_PATH` 指向的目录可写、磁盘未满，并检查归档文件命名
  `record_YYYYMM.db`。

### 5.6 症状分诊表

| 症状 | 优先查看 | 常见原因 |
| :--- | :--- | :--- |
| 客户端收到 413 | `MAX_REQUEST_BODY_BYTES`、启动日志、反向代理配置 | 网关或 Nginx 的请求体上限小于客户端历史 |
| 所有后端失败 / 503 | `logs/error*.log`、`/metrics` 的 failover 计数 | 后端不可达、熔断冷却、fallback 未配置或同样失败 |
| 流式响应卡住或不结束 | `RUST_LOG=debug` 的上游尾部、客户端是否收到 `[DONE]` | 上游 SSE 非标准分帧；网关已补发 `[DONE]` 时问题在客户端 |
| 记录缺失或面板查不到 | `/internal/test/stats` 队列深度、`RECD_PATH`、归档文件 | 队列满丢弃、写到了别的库、跨月归档未命中 |
| 分析结果与原始数据不一致 | `ANALYSIS_ROLLUP=off` 对照、归档分片是否存在 | 预聚合未重建、归档未升级 |
| 启动即失败 | `logs/system*.log`、库文件 `user_version` | 库版本高于二进制、结构未知、迁移失败或磁盘不足 |
| `cargo build` 报 `E0308` / `DynVec::new_in` | `rustc --version` | 工具链为 1.100+，回退到 1.96.0 |

## 6. 开发规范

1. **错误处理**：网关层生成的错误统一走 `AppError`，确保日志与响应体内容一致；
   新增错误类型时同时补齐用户可读的 `error.type` 与处置建议。
2. **依赖**：引入新 crate 前评估编译体积、启动开销与许可证；优先复用现有依赖。
3. **性能敏感路径**：主路径避免深拷贝与阻塞 I/O；重活放后台任务，用 channel 解耦。
4. **测试**：新增行为至少配一个单测；修改 dispatcher、流式处理、审计存储时必须补回归用例。
5. **文档**：接口、环境变量、配置项、默认值变化时同步更新 README 与 `docs/`。
6. **外部测试套件**：`../QueQiao-Router-Tests` 代码不可修改；产品行为与套件冲突时先确认
   产品行为是否正确，再决定是否调整套件之外的东西。
