# 对外模型接口

QueQiao-Router 对客户端提供统一入口，兼容 OpenAI API 规范、Anthropic Messages API 及部分
vLLM / Rerank 扩展。请求经统一路由、思考格式归一化与响应转换后转发到后端，并异步写入审计库。

## 端点一览

| 方法 | 路径 | 描述 |
| :--- | :--- | :--- |
| `GET` | `/health` | 健康检查 |
| `GET` | `/v1/models` | 获取聚合模型列表（按凭据指纹缓存，默认 TTL 600s，可配置为 0 关闭） |
| `GET` | `/metrics` | Prometheus 指标（进程内注册表，重启清零） |
| `POST` | `/v1/chat/completions` | 对话接口（支持流式、多模态、工具调用） |
| `POST` | `/v1/responses` | OpenAI Responses API（支持流式 + 审计日志） |
| `POST` | `/v1/messages` | Anthropic Messages API（支持流式 SSE + 审计日志） |
| `POST` | `/v1/completions` | 文本补全接口（Legacy） |
| `POST` | `/v1/embeddings` | 向量化接口 |
| `POST` | `/v1/audio/transcriptions` | 语音转文字（Whisper） |
| `POST` | `/v1/audio/translations` | 语音翻译 |
| `POST` | `/v1/rerank` | Rerank 重排序（OpenAI 风格路径） |
| `POST` | `/rerank` | Rerank 重排序（兼容 vLLM） |
| `POST` | `/score` | 文本评分（兼容 vLLM） |
| `POST` | `/classify` | 文本分类（兼容 vLLM） |

## 认证与凭据优先级

本网关**默认无认证**，建议在前置反向代理/网关层做鉴权。API Key 取值优先级：

1. 客户端配置中的固定 `api_key`
2. 请求头 `Authorization: Bearer <token>`
3. 请求头 `x-api-key`（值本身即 Key，无需前缀）

正常情况下日志中 Token 仅显示前 8 字符；设置 `LOG_FULL_TOKEN_ON_ERROR=true` 时错误日志
记录完整 Token（仅建议受控环境临时启用）。

## 透传请求头

以下客户端请求头会**原样透传**给上游，供后端做日志关联与客户端识别（白名单见
`src/client/proxy.rs` 的 `FORWARD_HEADERS`）：

| 请求头 | 用途 |
| :--- | :--- |
| `user-agent` | 客户端标识 |
| `x-request-id` / `x-trace-id` / `x-correlation-id` | 请求追踪 id |
| `x-session-id` / `x-parent-session-id` / `x-session-affinity` | 会话标识 |

鉴权头（`authorization` / `x-api-key`）**不**透传——网关始终用上游 key 重建
`Authorization`；其余客户端头一律不转发。上述头同时会被写入审计库（见
`DATABASE_SCHEMA.md` 的「身份字段与请求头的对应关系」）。

## 思考格式（ThinkingFormat）

| 值 | 效果 |
| :--- | :--- |
| `passthrough` | 原样透传（默认） |
| `think_tag` | 统一封装为 ` thinking...</think>` 包裹在 `content` 中 |
| `reasoning` | 统一放入独立的 `reasoning` 字段 |
| `reasoning_content` | 统一放入独立的 `reasoning_content` 字段 |

支持流式与非流式无损转换，正确处理跨 SSE chunk 的标记断裂。可在全局配置，也可按后端覆盖。

## 超时与重试

| 场景 | 默认值 | 说明 |
| :--- | :--- | :--- |
| TCP 连接建立 | 10s | 快速失败 |
| 流式 TTFB | 60s | 流式请求发送后 60s 内无响应则超时 |
| 客户端全局超时 | 1800s | 非流式请求整体上限 |
| 连接池空闲淘汰 | 15s | 短于常见云 LB 的 keepalive 窗口 |
| TCP keepalive | 30s | 防 NAT/LB 静默断链 |

仅在发送阶段遇到连接/请求错误（非超时）时重建连接重试一次。上游失败时调度器会尝试其他
匹配客户端，全部失败后才使用配置的 `fallback`。

## 请求示例

```bash
# 非流式
curl http://127.0.0.1:8000/v1/chat/completions \
  -H 'Authorization: Bearer sk-token' \
  -H 'Content-Type: application/json' \
  -d '{"model": "gpt-5", "messages": [{"role": "user", "content": "你好"}]}'

# 流式
curl http://127.0.0.1:8000/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model": "gpt-5", "stream": true, "messages": [{"role": "user", "content": "你好"}]}'
```
