# 数据库结构（SQLite 审计库）

## 概览

- 引擎：SQLite（sqlx 0.7，bundled）。
- **Schema 版本：`PRAGMA user_version = 7`**（见 `src/db/mod.rs` 的 `SCHEMA_VERSION`）。
- **当前库**：由环境变量 `RECD_PATH` 指定，默认 `sqlite:./record.db`（Docker 镜像默认
  `sqlite:./logs/record.db`）。
- **月度轮转**：数据月份落后于当前月份时，进程把 `record.db` 改名为
  `record_YYYYMM.db`（同年份冲突则追加 `_<unix秒>`），并新建空的 `record.db` 继续写入。
  轮转由「进程时钟 + 数据月份」驱动（不依赖文件 mtime），写入路径仅做一次原子边界比较。
- **归档分片**：所有 `record_<YYYYMM>[_<unix秒>].db` 以只读连接池懒加载注册
  （`ArchiveRegistry`），按各分片真实的 `MIN/MAX(TimeMs)` 范围路由查询，支持跨月检索。
- **journal 模式**：**不使用 WAL**（NFS 上不可用），即 SQLite 默认的 rollback journal
  （`delete`）。因此读写提交互斥，长时间大范围扫描会短暂阻塞写入提交。

> 迁移说明：`user_version < 7` 时逐列 `ALTER TABLE ADD COLUMN` 补齐新列，并创建依赖列
> 齐备的索引；`payloads` 表与 FTS5 索引按需创建。遗留归档（`user_version = 0`、缺
> `TimeMs`）会在首次扫描时就地迁移（详见 `API_RECORDS.md` 的「遗留归档的一次性迁移」）。

---

## 表 `records`

主审计表。早期版本仅有下列 **12 列**（测试套件兼容基线）：

| 列 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 主键，自增 |
| `Time` | TEXT | 本地墙钟时间文本 `%Y-%m-%d %H:%M:%S%.6f` |
| `IP` | TEXT | 客户端 IP |
| `Model` | TEXT | 请求模型名 |
| `Type` | TEXT | 接口类型，如 `chat.completions` / `responses` / `anthropic.messages` |
| `CompletionTokens` | INTEGER | 返回 Token |
| `PromptTokens` | INTEGER | 请求 Token |
| `TotalTokens` | INTEGER | 总 Token |
| `Tool` | BOOLEAN | 是否含工具调用 |
| `Multimodal` | BOOLEAN | 是否多模态 |
| `Headers` | TEXT | 请求头（遗留明文列） |
| `Request` | TEXT | 请求体（遗留明文列，迁移时映射到 `Prompt`） |
| `Response` | TEXT | 响应体（遗留明文列，迁移时映射到 `Answer`） |

v1 起通过迁移补齐的现代列：

| 列 | 类型 | 说明 |
| :--- | :--- | :--- |
| `TimeMs` | INTEGER | 真实 epoch 毫秒（范围查询与排序主键） |
| `Method` | TEXT | HTTP 方法 |
| `Endpoint` | TEXT | 请求端点 |
| `Backend` | TEXT | 命中的上游客户端名 |
| `SessionId` | TEXT | 会话 id |
| `ParentSessionId` | TEXT | 父会话 id |
| `RequestId` | TEXT | 请求 id |
| `SessionAffinity` | TEXT | 会话亲和标识 |
| `UserAgent` | TEXT | User-Agent |
| `ClientName` | TEXT | 客户端名 |
| `ClientVersion` | TEXT | 客户端版本 |
| `ApiKey` | TEXT | 使用的 API Key（详情接口返回明文） |
| `Status` | INTEGER | HTTP 状态码 |
| `Error` | TEXT | 错误信息 |
| `RetryCount` | INTEGER | 重试次数 |
| `FinishReason` | TEXT | 结束原因 |
| `LatencyMs` | REAL | 端到端时延（毫秒） |
| `TtftMs` | REAL | 首字时延 TTFT（毫秒） |
| `UpstreamMs` | REAL | 上游耗时（毫秒） |
| `StreamMs` | REAL | 流式耗时（毫秒） |
| `RequestBytes` | INTEGER | 请求字节数 |
| `ResponseBytes` | INTEGER | 响应字节数 |
| `PromptBytes` | INTEGER | 提问字节数 |
| `RequestTailBytes` | INTEGER | 请求尾部字节数 |
| `AnswerBytes` | INTEGER | 回答字节数 |
| `MessageCount` | INTEGER | 消息轮数 |
| `SystemCount` | INTEGER | system 消息数 |
| `ToolCount` | INTEGER | 工具调用数 |
| `AssistantCount` | INTEGER | assistant 消息数 |
| `ToolResultCount` | INTEGER | 工具结果数 |
| `ImageCount` | INTEGER | 图片数 |
| `Prompt` | TEXT | 提问明文（清洗后） |
| `RequestTail` | TEXT | 请求尾部明文 |
| `Answer` | TEXT | 回答明文（流式拼接后） |
| `ToolNames` | TEXT | 工具名列表 |
| `payload_id` | INTEGER | 关联 `payloads.record_id` |

### 身份字段与请求头的对应关系

以下字段直接取自客户端请求头，**仅用于审计入库，不**透传给上游（见
`src/db/extract.rs` 的 `header_meta`）：

| 列 | 来源请求头 |
| :--- | :--- |
| `SessionId` | `x-session-id` |
| `ParentSessionId` | `x-parent-session-id` |
| `SessionAffinity` | `x-session-affinity` |
| `RequestId` | `x-request-id` → `x-trace-id` → `request-id` → `x-correlation-id`（依次取第一个非空，trim 后截断至 200 字符） |
| `ApiKey` | `Authorization: Bearer <token>` 或 `x-api-key`；默认明文入库，设 `RECORD_STORE_RAW_CREDENTIALS=false` 时不落库 |
| `UserAgent` / `ClientName` / `ClientVersion` | `user-agent`（后两者由 UA 解析出名称/版本） |

---

## 索引

迁移会创建（依赖列齐备时）：

| 索引 | 列 |
| :--- | :--- |
| `idx_records_time` | `(TimeMs)` |
| `idx_records_type_time` | `(Type, TimeMs)` |
| `idx_records_api_key_time` | `(ApiKey, TimeMs)` |
| `idx_records_model_time` | `(Model COLLATE NOCASE, TimeMs)` |
| `idx_records_ip_time` | `(IP COLLATE NOCASE, TimeMs)` |
| `idx_records_backend_time` | `(Backend, TimeMs)` |

v6 曾删除一批低收益索引（`idx_records_status_time`、`idx_records_session`、
`idx_records_parent_session`、`idx_records_request_id`、`idx_records_list_covering`、
`idx_records_api_key` 等）。其中 `Model`/`IP`/`Backend` 相关索引由上表重建，因此
**`Status`、`ClientName`、`SessionId` 等列没有索引**，按它们筛选/分组会走全表扫描。

> 面板分析接口**不允许新增索引**（零 schema 变更），请始终带 `from`/`to` 收窄范围。

---

## 表 `payloads`（压缩正文）

```sql
CREATE TABLE IF NOT EXISTS payloads (
    record_id INTEGER PRIMARY KEY REFERENCES records(id),
    codec TEXT NOT NULL,
    dict_id TEXT,
    request BLOB,
    response BLOB,
    headers BLOB,
    request_raw_len INTEGER,
    response_raw_len INTEGER,
    headers_raw_len INTEGER
);
```

完整请求/响应/请求头以 zstd 压缩存储，仅在详情接口带 `include=body` 时按需解压。

---

## 全文检索 `records_fts`

external-content FTS5 + trigram 分词，仅索引 `Prompt` / `RequestTail` / `Answer` 三列：

```sql
CREATE VIRTUAL TABLE IF NOT EXISTS records_fts USING fts5(
    Prompt, RequestTail, Answer,
    content='records', content_rowid='id', tokenize='trigram');
```

配合 `AFTER INSERT / DELETE / UPDATE` 三个触发器保持同步；迁移与遗留归档升级后会执行
`INSERT INTO records_fts(records_fts) VALUES('rebuild')` 重建索引。

- `q` ≥ 3 字符：走 `records_fts MATCH`（trigram 子串索引）。
- `q` < 3 字符：退化为三列 `LIKE '%x%'` 全表扫描。

---

## 归档分片与轮转

- 文件名：`record_YYYYMM.db` 或 `record_YYYYMM_<unix秒>.db`。
- 分片注册表记录每个分片的 `MIN(TimeMs)` / `MAX(TimeMs)`，查询时只选取与请求时间范围
  相交的分片。
- 归档以只读单连接池打开；缺少 `TimeMs` 的遗留归档在迁移前会被跳过并告警。
- 轮转时关闭并重建连接池，`Pool::close()` 会等待在途查询归还连接后优雅排空。
