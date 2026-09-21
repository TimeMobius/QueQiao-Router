# 日志检索 API

监控面板（`/dashboard`）背后的只读查询接口，均为 **GET**、返回 JSON。这些接口自身不产生
metrics、不写访问日志，可安全频繁轮询。

---

## `GET /dashboard/api/records` — 列表检索

| 参数 | 类型 | 匹配方式 | 说明 |
| :--- | :--- | :--- | :--- |
| `from` / `to` | i64 | — | 起止时间（**epoch 毫秒**，含）。提供后（与任一月份归档的 `TimeMs` 范围相交时）启用跨月检索：同时查询当前库与命中的月度归档 |
| `model` | string | 前缀，大小写不敏感 | `gpt` 命中 `gpt-4o-mini`、`GPT-5.6-Sol` |
| `ip` | string | 前缀，大小写不敏感 | `192.168.10` 命中 `192.168.10.32` |
| `apikey` | string | 精确 | 需填完整 Key |
| `type` | string | 精确 | `chat.completions` / `responses` / `anthropic.messages` |
| `backend` | string | 精确 | 上游客户端名 |
| `status` | i64 | 精确 | HTTP 状态码 |
| `client` | string | 子串 | 同时匹配 `ClientName` 或 `UserAgent` |
| `session_id` / `parent_session_id` / `request_id` | string | 子串 | |
| `q` | string | 见下 | 关键词检索 |
| `errors` | `1` | — | 仅返回 `Status >= 400` |
| `cursor` | string | — | 翻页游标，对客户端**不透明**：单月为 `"<TimeMs>:<id>"`，跨月为 `"<TimeMs>:<id>:<shard>"` |
| `limit` | i64 | — | 每页条数，默认 50，范围 1–500 |

`q` 按长度分支：**≥3 字符** 走 FTS5 trigram 子串索引（快）；**<3 字符** 退化为三列
`LIKE '%x%'` 全表扫描，建议前端限制最少 3 字符，或同时附带 `from`/`to` 收窄范围。

### 返回结果

```json
{
  "items": [{
    "id": 273, "time": "2026-09-17 18:08:40.742568", "timeMs": 1789639720742,
    "type": "responses", "model": "gpt-5.6-sol", "status": 200, "ip": "192.168.10.32",
    "sessionId": "…", "requestId": "…", "toolCount": 34,
    "promptTokens": 1234, "totalTokens": 5678, "latencyMs": 20484.2, "promptPreview": "……",
    "shard": "record_202608"
  }],
  "nextCursor": "1789639720742:273:record_202608",
  "total": 273,
  "totalExact": true
}
```

- `items` 已按时间倒序（最新在前），同毫秒再按 `id` 倒序；跨月检索时以 `shard` 作为最终
  并列次序（`TimeMs DESC, id DESC, shard DESC`）。
- `shard` 字段仅跨月检索时出现：`"active"` 表示当前库，其余为归档分片 id
  （文件名去扩展名，如 `record_202608`）。
- `total` **最多统计 10000 条**；`totalExact` 为 `false` 时表示 `total` 语义是「**≥ 10000**」。
  跨月时 `total` 为各分片计数之和的上限值。
- 翻页只支持顺序前后翻：把上一页的 `nextCursor` 原样回传，`null` 表示已是最后一页。
  游标格式对客户端不透明，请勿自行拼接。翻页时筛选条件必须保持一致。
- 不提供 `from`/`to` 时行为与历史版本完全一致：只查询当前月数据库，游标为两段式。

```bash
curl 'http://127.0.0.1:8000/dashboard/api/records?model=gpt&limit=20'
curl 'http://127.0.0.1:8000/dashboard/api/records?ip=192.168.10'
curl 'http://127.0.0.1:8000/dashboard/api/records?apikey=sk-xxxxxxxx'
curl 'http://127.0.0.1:8000/dashboard/api/records?q=模型路由'
curl 'http://127.0.0.1:8000/dashboard/api/records?from=1789629022325&to=1789639720742&errors=1'
curl 'http://127.0.0.1:8000/dashboard/api/records?limit=20&cursor=1789639720742:273'
```

---

## `GET /dashboard/api/records/facets` — 筛选项字典

返回去重后的可选项（每维最多 200 个），用于下拉与自动补全：

```json
{ "models": ["gpt-5.6-sol"], "types": ["chat.completions", "responses"],
  "backends": ["alpha"], "clients": ["python-httpx/0.27.0"] }
```

---

## `GET /dashboard/api/records/{id}` — 记录详情

返回列表字段外加 `prompt`、`requestTail`、`answer`、`toolNames`、`apiKey`、`hasPayload`。
命中归档时会额外返回 `"shard"` 字段标识来源分片。

| 参数 | 说明 |
| :--- | :--- |
| `include=body` | 额外返回完整 `request` / `response` / `headers`（按需 zstd 解压，体积可能很大，建议用户展开时再请求） |
| `shard` | 指定分片：`active` 为当前库，其余为归档分片 id（如 `record_202608`）。指定后只在该分片内查找，分片不存在返回 **404** |

不指定 `shard` 时先查当前库；当前库未命中则跨所有归档按 `id` 查找：唯一命中返回记录，
命中多个归档（`id` 在不同月份重复）返回 **409**，响应体为
`["ambiguous_record_id", "<shard>", …]`；均未命中返回 **404**。未知 `id` 返回 **404**；
`hasPayload=false` 表示该记录无压缩正文。

### 遗留归档的一次性迁移

扫描到不含 `TimeMs` 列的旧归档（`user_version=0` 的遗留库）时，网关会在首次扫描时
**就地升级**该文件：补齐现代列与索引，`TimeMs` 依据本地墙钟文本 `Time` 结合当时的历史
UTC 偏移（含夏令时）回填为真实 epoch 毫秒，并把遗留明文 `Request`/`Response` 映射到
`Prompt`/`Answer` 以支持展示与检索。迁移在单个事务内完成，中断可安全重试且不会影响已
现代归档。设置环境变量 `ARCHIVE_LEGACY_MIGRATION=skip` 可禁用迁移，此时遗留归档仍会被
跳过、不参与检索；无法解析的时间戳保留为 NULL 隔离，并被时间范围查询排除。

---

## `GET /dashboard/api/error-log` — 错误日志（读文件，非数据库）

非 2xx 响应与流式中断**不写入数据库**，只存在于 `logs/error.<日期>.log`。该接口从文件
末尾**向前分块读取**，开销与页大小相关而非文件总大小。

| 参数 | 说明 |
| :--- | :--- |
| `limit` | 每页条数，默认 100，范围 1–500 |
| `before` | 字节偏移游标，回传上一页返回的 `nextBefore` |

```json
{
  "file": "error.2026-09-17.log",
  "size": 7040,
  "entries": [
    { "kind": "http_error", "raw": "10.0.0.5 - - [17/Sep/2026:07:28:00 +0000] \"POST /v1/messages HTTP/1.1\" 500 - \"-\" \"python-httpx/0.27.0\" 1.250s \"claude-x\" \"sk-abc12345…\" \"upstream boom\" \"{\\\"model\\\":\\\"claude-x\\\"}\"",
      "time": "17/Sep/2026:07:28:00 +0000", "ip": "10.0.0.5",
      "method": "POST", "path": "/v1/messages", "status": 500,
      "user_agent": "python-httpx/0.27.0", "latency": "1.250s",
      "model": "claude-x", "api_key": "sk-abc12345…", "backend": null,
      "error": "upstream boom", "error_truncated": false, "error_bytes": null,
      "request_body": "{\"model\":\"claude-x\"}" },
    { "kind": "stream_interrupted", "raw": "[17/Sep/2026:08:00:00 +0000] STREAM_INTERRUPTED client=10.0.0.9 endpoint=/v1/chat/completions model=gpt-5 backend=alpha error=\"upstream stream interrupted: connection reset\"",
      "time": "17/Sep/2026:08:00:00 +0000", "ip": "10.0.0.9",
      "path": "/v1/chat/completions", "model": "gpt-5", "backend": "alpha",
      "error": "upstream stream interrupted: connection reset" }
  ],
  "nextBefore": 4480,
  "hasMore": true
}
```

字段均为 **snake_case**。`kind` 取值 `http_error` / `stream_interrupted` / `unparsed`
（无法解析时仅保留 `raw`）。条目字段：

| 字段 | 说明 |
| :--- | :--- |
| `kind` | 条目类型 |
| `raw` | 原始日志行原文（始终存在） |
| `time` / `ip` / `method` / `path` / `status` | 解析出的请求信息；`stream_interrupted` 无 `status`/`method` |
| `user_agent` / `latency` / `model` / `api_key` | 请求标识；行内为 `-` 占位时保持 `"-"` 原样（不归一化为 null） |
| `backend` | 仅 `stream_interrupted` 有值 |
| `error` | 错误信息；超过 16 KiB 时按字符边界截断并标注 |
| `error_truncated` / `error_bytes` | 是否发生截断 / 截断前的原始字节数（未截断时为 `false`/`null`） |
| `request_body` | 请求体（`http_error` 且日志行带请求体时） |

某字段为 `null` 表示该行不含此信息，不是错误。`nextBefore` 为 `null` 表示已到文件开头。

---

## 检索性能备注

| 条件 | 索引 |
| :--- | :--- |
| `from` / `to` | `idx_records_time`（覆盖索引，兼顾倒序与统计） |
| `type` / `backend` / `apikey` | 等值索引 |
| `model` / `ip` | 前缀范围索引（`COLLATE NOCASE`） |
| `q`（≥3 字） | FTS5 trigram |
| `session_id` / `parent_session_id` / `request_id` / `client` / `status` / `q`（<3 字） | **无索引，扫描** |

- **前缀 ≠ 子串**：`model=pt` 不会命中 `gpt-4`；任意位置匹配请用 `q`（≥3 字）。
- **`total` 是带上限的估算**，键集分页不依赖它；需要精确总数请自行带相同条件统计。
- 前缀/范围条件会附加一次临时排序（范围扫描的固有代价），但候选集已被收窄，影响很小。
- **详情接口返回完整明文 `apiKey`**（列表接口永不返回）。UI 默认脱敏需点击展开，接口本身
  不脱敏；本项目默认无网关认证，暴露在网络上等于泄露全部密钥，**部署时务必给 `/dashboard`
  加认证或限制来源**。
- 读取的是 `logs/` 下日期最大的 `error.*.log`，日志目录为进程工作目录下的 `logs/`；该目录
  在进程启动时会被重建，不要在其中存放需要留存的数据。
