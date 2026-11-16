# QueQiao-Router 项目指南

QueQiao-Router 是 Rust 实现的高性能大模型智能路由网关（Axum + Tokio + simd-json + mimalloc），
负责在多个上游 LLM 后端之间做路由、负载均衡、故障转移，并落地审计与指标。

```
[Client] -> [QueQiao-Router] -> [Backend Services]
```

## 0. 本机环境速查

**本机就是开发机，不需要 SSH 到任何地方**——所有命令在当前 shell 直接执行。

| 项 | 值 |
| :--- | :--- |
| 主机 | hostname `fedora`，内网 `192.168.6.169` |
| 仓库根（= 本文件所在目录） | `/mnt/data/Codes/GPT_API_gemini/QueQiao-Router` |
| 同级外部目录 | `../Check_API`（鉴权插件）、`../build_multiarch.sh`、`../dist`、`../QueQiao-Router-Tests` |
| 工具链 | rustc / cargo **1.96.0**（与 CI、`Dockerfile` 一致，勿擅自升级） |
| 分支 | `main` |

```bash
cd /mnt/data/Codes/GPT_API_gemini/QueQiao-Router
git status --short && git pull --ff-only && git fetch --tags origin
cargo fmt --all && cargo build && cargo test
```

---

## 1. 硬性约束（先读这一节）

> **铁律**：严禁修改 `../QueQiao-Router-Tests` 测试套件的任何代码，其代码不可更改。
>
> **测试定位**：以 Rust 内部单元测试（`cargo test`）为**必需的**质量门禁。
> `QueQiao-Router-Tests` 是**可选的黑盒端到端验收工具**，仅当需要验证真实 HTTP/SSE 线格式、
> 路由装配、故障转移、热重载等跨进程行为时手动运行，**不属于**日常开发或提交的硬性流程。

1. **自主性**：遇到问题先独立排查（含在线搜索），避免无谓中断等待指令
2. **范围限制**：代码修改严格限制在当前指令目标的项目内
3. **结果导向**：专注最终目标，允许迭代尝试不同方法
4. **版本控制**：每次代码调整后必须完成编译与单元测试；通过后立即提交 Git
5. **提交约束**：提交/推送前必须先 `cargo fmt`，再编译测试与单元测试验证功能。
   **不再要求每次提交前运行外部测试套件**
6. **增量变更**：小步、增量的改动，频繁编译与测试验证

---

## 2. 标准开发闭环

```text
同步 main → 小步改码 → cargo fmt --all → cargo build → cargo test
  → 手动/端到端验证（按改动类型）→ git commit（约定式）→ git push origin main
  → GitHub CI + Release 自动打 tag / 升版本
  → 需要鉴权版时：本机 bash ../build_multiarch.sh -o ../dist
```

### 2.1 提交规范

采用 **Conventional Commits**：`type(scope): 摘要`
（`feat` / `fix` / `perf` / `refactor` / `docs` / `chore`；scope 如 `gateway`、`handlers`、`db`、`config`）。

- 摘要一句话说清“改了什么、为什么”；正文补根因、方案、验证结论
- `git add <明确列出的文件>`，避免 `git add -A` 带入无关文件
- `Cargo.lock` 未纳入版本控制（见 `.gitignore`）

### 2.2 版本号（自动维护，勿手改）

- 版本号 = 提交数：tag `b<git rev-list --count HEAD>`，Cargo 版本 `0.2.<提交数>`
- 向 `main` push 一个**不含 `[skip ci]`** 的提交后：
  - `Create Release and Update README` 创建 tag + Release，并把 `Cargo.toml` 版本、
    README 镜像 tag 改成新值，提交 `chore(release): update build version to bN [skip ci]` 推回主干
  - `Rust CI & Docker Build` 构建并推送普通版多架构镜像到 GHCR
- **不要手改 `Cargo.toml` / README 里的版本行**；出现“本地落后 1 个提交”是正常的，`git pull --ff-only` 即可

### 2.3 推送后确认完成的标准

```bash
gh run list -R TimeMobius/QueQiao-Router -L 6
gh run view <run-id> -R TimeMobius/QueQiao-Router            # 作业级状态
gh run view <run-id> --log-failed -R TimeMobius/QueQiao-Router
git ls-remote --tags origin b<新版本>                          # tag 指向你的提交
```

1. `Rust CI & Docker Build`：completed / success
2. `Create Release and Update README`：completed / success
3. 新 tag 就位，且 `origin/main` 多出自动版本提交

---

## 3. 测试策略

| 层级 | 命令 | 是否必需 |
| :--- | :--- | :--- |
| 单元测试 | `cargo test` | **必需**（提交门禁） |
| 编译 | `cargo build` | **必需** |
| 格式化 | `cargo fmt --all`（CI 跑 `--check`） | **必需** |
| Clippy | `cargo clippy`（CI 只跑普通 lint，不加 `-D warnings`） | 推荐 |
| 黑盒 E2E | 见 §6 | 可选，按需 |

> `cargo clippy --all-targets --all-features -- -D warnings` 在当前工具链下有数十条**历史遗留**告警，
> 只需确认本次改动文件无新增告警，不要顺手大改无关代码。

涉及 HTTP 行为、配置、多模态、大请求体的改动，单测之外务必起本地网关验证（见 §6.3）。

---

## 4. 代码地图

| 关注点 | 位置 |
| :--- | :--- |
| 进程入口、启动装配（配置/DB/metrics/路由） | `src/main.rs`、`src/lib.rs` |
| 路由注册 | `src/routes/`（`mod.rs` + 各端点文件） |
| JSON 请求体提取器 `CustomJson` | `src/handlers/extractors.rs` |
| 请求体上限（`MAX_REQUEST_BODY_BYTES`，默认 64 MiB） | `src/config/body_limit.rs` |
| 统一错误体 / 413 文案 | `src/app_error.rs` |
| 核心调度、故障转移、熔断记账 | `src/services/dispatcher.rs` |
| 非流式转发、审计落库 | `src/handlers/common_handler.rs` |
| 流式（SSE）处理 | `src/handlers/stream_handler.rs`、`anthropic_stream_handler.rs`、`responses_stream_handler.rs` |
| 上游 HTTP 客户端、鉴权头与透传白名单 | `src/client/proxy.rs`、`client_manager.rs` |
| 配置加载 / 热重载 / 类型 | `src/config/config_manager.rs`、`types.rs` |
| 审计库（Turso v8）、轮转、FTS | `src/db/` |
| 访问日志中间件 | `src/middleware/access_log.rs` |
| Prometheus 指标 | `src/metrics/` |
| 内嵌监控面板（rust-embed） | `src/routes/dashboard.rs`、`web/` |
| 鉴权插件（编译期注入，仓库外） | `../Check_API/auth-lib/src/lib.rs` |

### 4.1 可选功能：鉴权插件

普通版 `cargo build` 不含认证；`--features check-api-auth` 时 `src/main.rs` 用 `include!`
引入 `../Check_API/auth-lib/src/lib.rs`，把鉴权中间件加在路由最外层，零运行时开销。

> 注意：**不要用 `--all-features`**——该 feature 依赖仓库外的 sibling 目录，缺目录会编译失败。

---

## 5. 交付产物

| 产物 | 构建者 | 内容 |
| :--- | :--- | :--- |
| `ghcr.io/timemobius/queqiao-router:bN` / `latest` | GitHub CI（根 `Dockerfile`） | 普通版，**无鉴权插件** |
| `../dist/queqiao-router-<ver>-{amd64,arm64}.tar.gz` | 本机 `../build_multiarch.sh`（`../Check_API/Dockerfile`） | `FEATURES=check-api-auth`，**含鉴权插件** |

```bash
# 打鉴权版多架构镜像（版本号由本地 tag 自动识别，追加 -c）
cd /mnt/data/Codes/GPT_API_gemini
git -C QueQiao-Router fetch --tags origin          # 先同步 tag，否则版本会回落 latest-c
bash build_multiarch.sh -o ./dist
```

- 产物校验：`gzip -t dist/*.tar.gz` + `sha256sum`，amd64 可 `docker load` 后冒烟
- 磁盘建议 ≥20GB 空闲（BuildKit Min Free Space 约 19.5GB，过低会 GC 缓存触发全量重建）
- arm64 走 QEMU 指令模拟（buildkit 内置，**不需要**宿主机 binfmt），耗时是 amd64 的数倍

---

## 6. 测试与环境

### 6.1 外部黑盒测试套件（可选）

```bash
pkill -f queqiao_router || true           # 先清理遗留进程，避免端口冲突
cd /mnt/data/Codes/GPT_API_gemini/QueQiao-Router-Tests
source .venv/bin/activate                 # 已存在；若缺失见下
python test_runner_enhanced.py --api-version rust --stage all
# 报告：/mnt/data/Codes/GPT_API_gemini/QueQiao-Router-Tests/test_report.md
```

首次环境准备：

```bash
apt-get update && apt-get install -y python3-pip python3.11-venv
python3 -m venv /mnt/data/Codes/GPT_API_gemini/QueQiao-Router-Tests/.venv
/mnt/data/Codes/GPT_API_gemini/QueQiao-Router-Tests/.venv/bin/python3 -m pip install \
  -r /mnt/data/Codes/GPT_API_gemini/QueQiao-Router-Tests/requirements.txt
```

> 若套件与产品当前行为不一致，**不要为其修改产品代码**；先确认产品行为是否确实有误，
> 再按需调整测试（该目录代码受 §1 铁律保护）。

### 6.2 Mock 服务器约定

需要特定或异常后端行为时，**不要**在 Mock 服务器里新增 API 端点。
正确做法是用**专用模型名**驱动分支：

1. 调用标准 `/v1/chat/completions`
2. `model` 设为专用名（如 `model-for-incomplete-stream`）
3. Mock 识别该模型名并返回预设响应

### 6.3 本地起网关验证

```bash
WORK=/tmp/qq-dev; rm -rf $WORK; mkdir -p $WORK/config
cat > $WORK/config/config.yaml <<'YAML'
openai_clients:
  - name: "mock"
    api_key: "dummy"
    base_url: "http://127.0.0.1:18001/v1"
    model_match: { type: "keyword", value: ["dev-model"] }
YAML
cd $WORK && RECD_PATH="sqlite:$WORK/record.db" SERVER_PORT=18080 \
  /mnt/data/Codes/GPT_API_gemini/QueQiao-Router/target/debug/queqiao-router
```

调试二进制名是 `target/debug/queqiao-router`（连字符）。常用环境变量见本机 `知识库/04-部署与验证.md` §5。

### 6.4 测试报告约定

在 Markdown 测试报告补充信息：`TEST_EXPECTATIONS` 加预期输出，`TEST_STEP_LOGIC` 加测试步骤。

---

## 7. API 用法示例

```bash
# 非流式
curl --location 'http://192.168.10.121:8000/v1/chat/completions' \
--header 'Authorization: Bearer sk-token' \
--header 'Content-Type: application/json' \
--data '{"model": "RWKV-v7-G0-7.2B", "messages": [{"role": "user", "content": "你好"}]}'

# 流式
curl --location 'http://192.168.10.121:8000/v1/chat/completions' \
--header 'Content-Type: application/json' \
--data '{"model": "RWKV-v7-G0-7.2B", "stream": true, "messages": [{"role": "user", "content": "你好"}]}'
```

端点总览见 [`docs/API_LLM.md`](docs/API_LLM.md)；审计接口见 [`docs/API_RECORDS.md`](docs/API_RECORDS.md)。

---

## 8. 文档索引

| 文档 | 内容 |
| :--- | :--- |
| [`README.md`](README.md) | 功能、配置参考、环境变量、部署 |
| [`DEVELOPER.md`](DEVELOPER.md) | 架构深度解析 |
| [`docs/`](docs/) | 接口协议、数据库 schema、专题调研 |
| `知识库/`（**本机，不入库**） | 开发环境、日常流程、构建发布、部署验证、案例排障 |

> `知识库/` 含内网地址与交付细节，仅保存在本机、由 `.gitignore` 排除，不随仓库分发。
> 该目录与本文件冲突时，以本文件与 `README.md`、`DEVELOPER.md` 为准。