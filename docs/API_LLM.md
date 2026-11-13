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
| `POST` | `/v1/classify` | `/classify` 的别名（兼容习惯带 `/v1` 前缀的客户端） |
| `POST` | `/v1/systemone` | System One 分类协议（TypeSafe / OpenRouter Jev 等分类模型） |
| `POST` | `/systemone` | System One 分类协议（不带 `/v1` 的写法） |

## 认证与凭据优先级

本网关**默认无认证**，建议在前置反向代理/网关层做鉴权。API Key 取值优先级：

1. 客户端配置中的固定 `api_key`
2. 请求头 `Authorization: Bearer <token>`
3. 请求头 `x-api-key`（值本身即 Key，无需前缀）

正常情况下日志中 Token 仅显示前 8 字符；设置 `LOG_FULL_TOKEN_ON_ERROR=true` 时错误日志
记录完整 Token（仅建议受控环境临时启用）。

## 透传请求头

以下客户端请求头会**原样透传**给上游，供后端做日志关联（白名单见 `src/client/proxy.rs`
的 `FORWARD_HEADERS`）：

| 请求头 | 用途 |
| :--- | :--- |
| `x-request-id` / `x-trace-id` / `x-correlation-id` | 请求追踪 id |
| `x-session-id` / `x-parent-session-id` / `x-session-affinity` | 会话标识 |
| `x-deepseek-harness-session-id` | DSH（deepseek-harness）Messages 路由的会话标识，与 `x-session-id` 同义 |

`authorization` / `x-api-key` **不**透传——网关始终用上游 key 重建 `Authorization`；
`user-agent` 也不透传，上游看到的是**网关自身 UA**（`QueQiao-Router/b<提交数> reqwest/<版本>`）；
其余客户端头一律不转发。上述标识头同时会被写入审计库（见 `DATABASE_SCHEMA.md`
的「身份字段与请求头的对应关系」）。

## 透传请求体字段

网关**不丢弃**客户端发来的未声明字段。`/v1/chat/completions`、`/v1/completions`、
`/v1/embeddings`、`/v1/rerank`、`/score`、`/classify`、`/v1/systemone` 的请求结构体都带
`#[serde(flatten)] extra`，未在结构体中声明的键会原样带到上游。

这对 vLLM / SGLang / OpenAI 生态的扩展参数是必需的，例如：

| 类别 | 字段 |
| :--- | :--- |
| 采样扩展 | `top_k`、`min_p`、`typical_p`、`tfs` |
| vLLM 专有 | `guided_json`、`guided_regex`、`guided_choice`、`enable_thinking`、`thinking_budget`、`truncate_prompt_tokens`、`priority`、`ignore_eos` |
| 结构化输出 | `response_format`、`json_schema` |
| 缓存/路由 | `prompt_cache_key`、`prompt_cache_retention`、`metadata`、`user` |

**网关保留最终决定权的字段**（客户端无法经未知字段绕过）：

- `model` —— 若配置了 `model_rewrite`，改写发生在重建之后，是最终值；
- `stream` —— 由请求实际形态决定，不使用客户端在 body 里声明的值；
- `stream_options` —— 流式时尊重客户端值（缺省注入 `include_usage: true`），
  非流式时**移除**（该字段对 SSE 之外无意义）；
- 后端 `extra_body` 配置 —— 仅在字段缺失时补充，不会覆盖客户端字段。

> 透传意味着上游会看到客户端发来的一切字段。若某上游对未知字段严格校验并返回 422，
> 那是客户端与上游之间的约定，网关不做过滤；可用后端 `extra_body` 反向覆盖，
> 或让客户端不要发送该字段。

## System One 分类（Jev）

网关支持 TypeSafe 的 **System One** 分类协议（OpenRouter 转售的 Jev 决策/分类模型走同一协议），
端点 `POST /v1/systemone`（兼容裸路径 `/systemone`），上游转发到 `{base_url}/systemone`。

该协议与 OpenAI Chat 完全不同：不是文本生成，而是“给一段 state 文本 + 一组带类型的
questions，返回每个问题的结构化答案（校准概率）”。

### 请求

```bash
curl http://127.0.0.1:8000/v1/systemone \
  -H 'Authorization: Bearer sk-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "model": "typesafe/jev-1.13",
    "state": "I was charged twice for my subscription.",
    "questions": {
      "refund":     {"type": "noul",   "instructions": "Is the customer asking for money back?"},
      "department": {"type": "choice", "instructions": "Which team should handle this?",
                     "criteria": {"billing": "Charges and refunds", "technical": "Bugs and outages"}}
    }
  }'
```

字段说明：

| 字段 | 必填 | 说明 |
| :--- | :--- | :--- |
| `model` | 是 | 分类模型名（OpenRouter 为 `typesafe/jev-1.13`、`~typesafe/jev-latest` 等） |
| `state` | 是 | 待分类文本；带图片时是 `[{"type":"image_url",...}, "文本"]` 的 parts 数组 |
| `questions` | 是 | id → 题型定义：`noul`（是/否，wire 层名，网关原样透传）、`choice`、`score` |
| `images` / `assets` | 否 | 图片输入（不同服务形态不同），未声明字段由 `#[serde(flatten)] extra` 原样透传 |

网关只做路由、鉴权、透传与审计，**不解析题型语义**：除 `model` 外字段一律原样到达上游，
`model_rewrite` 同样生效；该协议非流式，审计库 `Type` 列记为 `systemone`，
`answer` 列记录结构化 `answers`（便于看板直接看到分类结果）。

### 响应

上游响应体原样透传（不做格式转换）：

```json
{
  "model": "typesafe/jev-1.13",
  "answers": {
    "refund":     {"type": "noul",   "noul": 0.98},
    "department": {"type": "choice", "choice": "billing", "confidence": 0.91}
  },
  "usage": {"input_tokens": 42, "output_tokens": 0}
}
```

### 后端配置示例

```yaml
openai_clients:
  # OpenRouter 转售的 Jev（base_url 带 /v1，网关会拼成 .../v1/systemone）
  - name: "typesafe-jev-openrouter"
    api_key: "sk-or-v1-..."
    base_url: "https://openrouter.ai/api/v1"
    model_match:
      type: "exact"
      value: ["typesafe/jev-1.13", "typesafe/jev-1.14", "~typesafe/jev-latest"]

  # TypeSafe 原生端点（裸模型 id，可用 model_rewrite 做入站别名）
  - name: "typesafe-jev-native"
    api_key: "ts-..."
    base_url: "https://api.typesafe.ai/v1"
    model_match:
      type: "exact"
      value: ["typesafe/jev-1.13"]
    model_rewrite:
      "typesafe/jev-1.13": "jev-1.13"
```

> 客户端（如 DSH 的 `typesafe-system-one` 传输）把 baseUrl 指向网关的 `/v1` 即可：
> 请求路径 `{baseUrl}/systemone` → 网关 `/v1/systemone`。

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

## 请求体大小与 413

网关默认接受最大 **64 MiB** 的请求体（`MAX_REQUEST_BODY_BYTES`，单位字节；Axum 自带的
2 MiB 默认值已替换）。Agent 类客户端每轮都会重发全量历史（文本 + 内联 base64 图片），
该上限应高于客户端侧预算之和，避免网关先于客户端预算触发 413。

请求体超限时返回 `HTTP 413`，响应体与其他错误一样为 OpenAI 兼容的双格式 JSON，
`error.type` 为 `request_too_large`，消息中带有当前生效上限：

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

超限的错误日志只记录上限与说明，**不会**把整个请求体写入 `logs/error.*.log`。
前置反向代理（Nginx / Envoy 等）需要同步放宽自身的请求体上限。

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
