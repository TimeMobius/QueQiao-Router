# QueQiao-Router 项目指南

本文档是 QueQiao-Router 项目的持久化记忆，整合了项目架构、开发规范和测试流程。

## 项目概述

QueQiao-Router 是 Rust 实现的高性能大模型智能路由网关。

### 部署架构
```
[Client] -> [QueQiao-Router] -> [Backend Services]
```

---

## 开发指令与约束

> **铁律**: 严禁修改 QueQiao-Router-Tests 测试套件的任何代码。测试套件是项目质量的守护者，其代码不可更改。

1. **自主性**: 遇到问题时，优先通过在线搜索等方式独立解决，避免中断等待用户指令
2. **范围限制**: 所有代码修改严格限制在当前指令目标的项目内
3. **结果导向**: 专注于最终目标的实现，允许通过不断迭代和尝试不同方法来达成目的
4. **版本控制**: 每次代码调整后，必须完成编译测试与单元测试。若通过，应立即将变更提交至 Git 仓库
5. **提交约束**: 提交和推送 Git 仓库之前，必须先执行 `cargo fmt` 格式化代码，再进行编译测试与单元测试验证功能性。**不再要求每次提交前运行外部测试套件**
6. **增量变更**: 避免一次性修改大量代码。进行小步、增量的变更，并频繁运行编译测试与单元测试以验证效果

---

## 核心特性
- **极致性能**: 基于 `Axum` 和 `Tokio` 异步运行时，利用 `simd-json` 和 `mimalloc` 优化
- **智能流量调度**: 支持关键字匹配和精确匹配的多策略路由，加权负载均衡，自动故障转移
- **深度可观测性**: 流式审计功能，完整的请求体和响应体记录，统一错误日志
- **企业级功能**: 零停机热重载，数据清洗（移除思考标签），多模态支持

### 核心架构
- **入口点**: `src/main.rs` 初始化运行时、日志系统、配置管理、客户端管理、数据库连接池和路由
- **路由层**: `src/routes/` 定义了所有支持的 API 端点
- **处理层**: `src/handlers/` 包含各端点的具体处理逻辑
- **服务层**: `src/services/dispatcher.rs` 实现核心请求分发逻辑
- **客户端层**: `src/client/` 管理上游客户端连接
- **配置层**: `src/config/` 负责加载和管理 YAML 配置文件
- **数据层**: `src/db/` 处理 SQLite 数据库操作

### 编译命令 (Debug 模式)
```bash
cd /mnt/data/Codes/GPT_API_gemini/QueQiao-Router && cargo build
```

---

## 测试套件

测试套件目录: `/mnt/data/Codes/GPT_API_gemini/QueQiao-Router-Tests`

### 环境设置
```bash
# 安装依赖
apt-get update && apt-get install -y python3-pip python3.11-venv

# 创建虚拟环境
python3 -m venv /mnt/data/Codes/GPT_API_gemini/QueQiao-Router-Tests/.venv

# 安装 Python 依赖
/mnt/data/Codes/GPT_API_gemini/QueQiao-Router-Tests/.venv/bin/python3 -m pip install -r /mnt/data/Codes/GPT_API_gemini/QueQiao-Router-Tests/requirements.txt
```

### 测试运行
**重要**: 运行测试套件前，必须先清理可能遗留的 `queqiao_router` 进程，避免端口冲突导致测试失败。

```bash
# 清理遗留进程
pkill -f queqiao_router || true

# 运行测试
cd /mnt/data/Codes/GPT_API_gemini/QueQiao-Router-Tests && source .venv/bin/activate && python test_runner_enhanced.py --api-version rust --stage all
```

### 测试报告
测试报告生成位置: `/mnt/data/Codes/GPT_API_gemini/QueQiao-Router-Tests/test_report.md`

---

### API 用法示例

#### 非流式 Chat Completion
```bash
curl --location 'http://192.168.10.121:8000/v1/chat/completions' \
--header 'Authorization: Bearer sk-token' \
--header 'Content-Type: application/json' \
--data '{"model": "RWKV-v7-G0-7.2B", "messages": [{"role": "user", "content": "你好"}]}'
```

#### 流式 Chat Completion
```bash
curl --location 'http://192.168.10.121:8000/v1/chat/completions' \
--header 'Content-Type: application/json' \
--data '{"model": "RWKV-v7-G0-7.2B", "stream": true, "messages": [{"role": "user", "content": "你好"}]}'
```

### Mock 服务器测试逻辑

添加需要特定或异常后端行为的新测试时，**不应**在 Mock 服务器中创建新的 API 端点。

正确做法：使用**专用的模型名称**来触发特殊响应。

示例：测试网关如何处理不完整的流式响应
1. 测试调用标准 `/v1/chat/completions` 端点
2. `model` 参数设为 `model-for-incomplete-stream`
3. 修改 Mock 服务器识别此模型名称并返回预设的特殊响应

### 测试报告生成

向 Markdown 测试报告添加详细信息：
1. 在 `TEST_EXPECTATIONS` 字典中添加预期输出
2. 在 `TEST_STEP_LOGIC` 字典中添加测试步骤
