# 分析 API（只读）

监控面板分析页背后的只读聚合接口，均为 **GET**、返回 JSON。数据来源分两类，互不混合：

- **审计库（`source=db`）**：active `record.db` + 命中的月度归档 `record_YYYYMM.db`。
- **文本日志（`source=log`）**：`logs/error.*.log` 文件（4xx/5xx 与流式中断）。

这些接口不写数据库、不产生 metrics、不写访问日志。**零 schema 变更**：不新增表、
不新增索引、不改动 `records` 结构，全部为只读 `SELECT`。

路由挂载在 `/dashboard` 前缀下：

- `GET /dashboard/api/analysis`
- `GET /dashboard/api/analysis/errors`
- `GET /dashboard/analysis` — 返回内嵌的 `analysis.html`

---

## 通用筛选参数

以下参数与 `GET /dashboard/api/records` 的筛选语义**完全一致**（复用同一套
`build_filters`），两个分析接口通用：

| 参数 | 类型 | 匹配方式 | 说明 |
| :--- | :--- | :--- | :--- |
| `from` / `to` | i64 | — | 起止时间（**epoch 毫秒**，含）。缺省 `to=now`、`from=now-7d`，**始终有界** |
| `model` | string | 前缀，大小写不敏感 | |
| `ip` | string | 前缀，大小写不敏感 | |
| `apikey` | string | 精确 | |
| `type` | string | 精确 | |
| `backend` | string | 精确 | |
| `status` | i64 | 精确 | |
| `client` | string | 子串 | 同时匹配 `ClientName` 或 `UserAgent` |
| `errors` | `1` | — | 仅统计 `Status >= 400` |

当 `from`/`to` 与归档分片的真实时间范围相交时，会**并行**查询 active 与命中的归档
（并发由 `Semaphore` 限制为 8），并在 Rust 侧归并；响应中的 `shards` 列出本次实际
查询的分片 id（`active` 或 `record_YYYYMM`）。

---

## `GET /dashboard/api/analysis`

### 分析专用参数

| 参数 | 取值 | 默认 | 说明 |
| :--- | :--- | :--- | :--- |
| `interval` | `hour` \| `day` \| `month` | 自动 | 自动规则：跨度 ≤48h 用 `hour`，≤92d 用 `day`，否则 `month` |
| `dimension` | `model` \| `apikey` \| `ip` \| `type` \| `status` \| `backend` \| `client` \| `hour` | `model` | 分组维度；非法值回退 `model` |
| `orderBy` | `requests` \| `errors` \| `tokens` | `requests` | 维度排序指标 |
| `page` | i64 | `1` | 最小 1 |
| `pageSize` | i64 | `20` | clamp 到 `1..=200` |
| `topLimit` | i64 | `8` | clamp 到 `1..=50` |

`hour` 维度的分组标签为**一天内的小时**（`00:00`..`23:00`，本地时区），与参考 UI 一致；
`interval` 的 `hour` 则是趋势的时间桶（含日期）。

### 响应

```json
{
  "from": 1789000000000, "to": 1789600000000,
  "interval": "hour", "dimension": "model",
  "shards": ["active"],
  "warnings": [],
  "summary": {"requests":0,"success":0,"errors":0,"promptTokens":0,"completionTokens":0,"totalTokens":0,"models":0,"ips":0},
  "trend": [{"label":"2026-09-18 20:00","success":0,"errors":0,"promptTokens":0,"completionTokens":0,"totalTokens":0}],
  "dimensions": {
    "name":"model","page":1,"pageSize":20,"total":0,"totalExact":true,
    "items":[{"name":"x","requests":0,"success":0,"errors":0,"promptTokens":0,"completionTokens":0,"totalTokens":0}]
  },
  "top": [{"name":"x","value":0}]
}
```

- `summary`：每分片一次聚合，Rust 侧按字段求和。`success` 为 `200 <= Status < 400`，
  `errors` 为 `Status >= 400`。
- `trend`：按 `interval` 分桶，升序，字段为 `success/errors/promptTokens/completionTokens/totalTokens`。
- `dimensions`：`total` 为合并后的分组数（上限 10000，超出时 `totalExact=false`）；
  `items` 在 Rust 侧排序后分页。
- `top`：同一 `dimension`，始终按 `requests` 降序取 `topLimit` 项。

### 数据缺失与上限

- 空值分组统一显示为 `Unknown`（`status` 维度用 `CAST(Status AS TEXT)`）。
- 成本可控性上限：每分片维度分组最多读 5000 个（超出告警）、趋势桶超 400 时自动
  粗化 `interval` 并告警、合并总分组数上限 10000。
- 跨分片时 `models`/`ips` 不是把各分片 `COUNT(DISTINCT)` 相加（那会在跨月时重复
  计数），而是各分片取 `DISTINCT` 值后在 Rust 侧并集去重；单分片走 `COUNT(DISTINCT)`
  快路径。去重集合上限 10000，触顶时告警。

---

## `GET /dashboard/api/analysis/errors`

在通用筛选参数之外：

| 参数 | 取值 | 默认 | 说明 |
| :--- | :--- | :--- | :--- |
| `source` | `db` \| `log` | `db` | 数据来源 |
| `limit` | i64 | `20` | clamp 到 `1..=100` |

### `source=db`

在审计库中按 `Status >= 400` 分组统计：

⚠️ 注意：`source=db` 只统计**写入了数据库**的请求。非 2xx 响应与流式中断并不写入
数据库，必须使用 `source=log` 才能看到。

```json
{"source":"db","from":0,"to":0,"shards":["active"],"warnings":[],
 "items":[{"status":422,"model":"x","backend":"alpha","error":"...","count":10}],
 "total": 6}
```

`total` 为合并后的错误分组总数（上限 10000），`items` 为按 `count` 降序的前 `limit` 项。
`backend` 可能为空字符串（原值为空）。

### `source=log`

枚举 `logs/error.*.log`，**从文件开头正向读取**并逐行解析（复用 `error_log_api::parse_line`
的解析器，不重复实现）。若提供了 `from`/`to`，解析每行的 `time` 字段
（格式 `17/Sep/2026:07:28:00 +0000`）并按时段过滤；**时间无法解析的条目会被保留并计入
`unparsed`**。分组键为 `(kind, status, model, backend, error)`。

```json
{"source":"log","files":["error.2026-09-17.log"],"warnings":[],
 "items":[{"kind":"http_error","status":500,"model":"x","backend":null,"error":"...","count":7}],
 "unparsed": 4, "total": 12}
```

- `files` 为本次实际读取的日志文件名。
- `model`/`backend` 为 `null` 表示该行不含此信息；`-` 与空串会被归一化为 `null`。
- 安全上限：单次请求最多解析约 200000 行、单文件最多读取 32 MiB；超限即停止并写入
  `warnings`。
- **绝不返回**原始日志行、请求体、响应体或 API Key；只返回解析后的 `kind/status/model/
  backend/error`。

---

## 性能与索引

| 条件 | 索引 |
| :--- | :--- |
| `from` / `to` | `idx_records_time`（覆盖索引） |
| `type` / `backend` / `apikey` | 等值索引 |
| `model` / `ip` | 前缀范围索引（`COLLATE NOCASE`） |
| `status` / `client` / `dimension` 分组 / `errors` | **无索引，扫描** |

- 本接口**不允许新增索引**（零 schema 变更）。聚合查询会扫描所选时间窗内的行。
  请始终带 `from`/`to` 收窄范围（缺省窗口为 7 天）以避免大范围扫描。
- 分片查询并发上限 8；每个分片内部为单条 `GROUP BY`，不会逐行回传数据到 Rust。
- 结果带 **15s 进程内缓存**，键为规范化查询串；缓存有界（最多 256 条，插入时清理过期项）。
- 趋势桶标签使用 SQLite `strftime(..., 'localtime')`，与服务器本地时区一致。

## 错误与告警

- 查询失败返回 **HTTP 500**（响应体为通用文案），并通过 `tracing::error!` 记录原因。
- `warnings` 覆盖：自动粗化 `interval`、维度分组触顶、趋势桶触顶、日志行/字节触顶、
  遗留/不支持的归档分片被跳过、跨分片 DISTINCT 计数求和。
