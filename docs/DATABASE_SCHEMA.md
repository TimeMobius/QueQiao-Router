# 数据库结构（Turso v8 审计库）

## 概览

- 引擎：**Turso 0.8.3**（内嵌，Rust `turso` crate），单文件数据库。
- **Schema 版本：`PRAGMA user_version = 8`**（`src/db/turso_schema.rs`）。
- **单库**：不再区分主库与索引库；记录、压缩正文、FTS 索引都在同一个文件内。
- **当前库**：环境变量 `RECD_PATH`，默认 `./record.db`（兼容 `sqlite:` 前缀；Docker 默认为
  `./logs/record.db`）。
- **访问模型**：网关是**唯一写者**；面板/分析/详情全部走网关 HTTP API。外部程序如需直接
  读取，必须使用 Turso 工具并以只读方式打开，不要同时写。
- **journal 模式**：WAL（Turso 默认）。写入由单个 writer 任务串行化，读连接不阻塞写。
- **月度轮转**：数据月份落后于当前月份时，先 `seal`（排空队列 + checkpoint），再把
  `record.db` 改名为 `record_YYYYMM.db`，新建空 v8 库继续写。归档分片以只读方式打开。
- **写入批量化**：有界队列 + 每批 200 行多行 INSERT，详见 `WRITE_BATCHING.md`。

---

## 版本与迁移

v7（SQLite + FTS5 + 死列）**不能原地升级**，必须创建新文件并复制数据：

```bash
queqiao-router --migrate-db --from /path/record.db --to /path/record.db.v8 --batch 500 --defer-fts
queqiao-router --verify-db --from /path/record.db --to /path/record.db.v8
```

- `--defer-fts`：灌数期间不维护 FTS，结束后一次性 `CREATE INDEX`，批量迁移快一个数量级。
- `--verify-db`：核对行数、payload 数、`MIN/MAX(TimeMs)`、三类 payload 原始字节，并抽样
  比对 FTS 词命中。
- 迁移源始终以 **只读** 打开；迁移完成后源文件字节不变（实测 SHA-256 一致）。

**写打开前的安全闸**：`AuditStore::open` 先只读执行 `probe_schema`：

| 探测结果 | 行为 |
| :--- | :--- |
| 文件不存在/0 字节 | 允许创建 v8 |
| `user_version=7` 或存在 `Headers` 列 | 拒绝，提示执行 `--migrate-db` |
| `user_version>8` | 拒绝（二进制过旧） |
| v8 但缺少 `SearchText` | 拒绝，提示重建 |
| 无 `records` 且版本非 0 | 拒绝（未知结构） |

启动时同样先探测；v7 活跃库会直接报错并给出精确迁移命令，**不会**再被可写打开覆盖。

---

## 表 `records`

v8 主表（完整 DDL 见 `src/db/turso_schema.rs`）：

| 列 | 类型 | 说明 |
| :--- | :--- | :--- |
| `id` | INTEGER | 主键，显式分配（writer 批量插入） |
| `Time` | TEXT | 本地墙钟时间 `%Y-%m-%d %H:%M:%S%.6f` |
| `TimeMs` | INTEGER | epoch 毫秒，范围查询与排序主键 |
| `IP` / `Method` / `Endpoint` | TEXT | 客户端与请求元信息 |
| `Type` | TEXT | `chat.completions` / `responses` / `anthropic.messages` 等 |
| `Model` / `Backend` | TEXT | 请求模型、命中的上游客户端 |
| `SessionId` / `ParentSessionId` / `RequestId` / `SessionAffinity` | TEXT | 会话与请求关联 |
| `UserAgent` / `ClientName` / `ClientVersion` | TEXT | 客户端信息 |
| `ApiKey` | TEXT | 使用的 key（受 `RECORD_STORE_RAW_CREDENTIALS` 控制） |
| `Status` / `Error` / `RetryCount` / `FinishReason` | INTEGER/TEXT | 结果状态 |
| `LatencyMs` / `TtftMs` / `UpstreamMs` / `StreamMs` | REAL | 时延 |
| `CompletionTokens` / `PromptTokens` / `TotalTokens` | INTEGER | Token |
| `Tool` / `Multimodal` | INTEGER | 0/1 |
| `RequestBytes` / `ResponseBytes` | INTEGER | 原始报文大小 |
| `PromptBytes` / `RequestTailBytes` / `AnswerBytes` | INTEGER | 正文分段大小 |
| `MessageCount` / `SystemCount` / `ToolCount` / `AssistantCount` / `ToolResultCount` / `ImageCount` | INTEGER | 结构统计 |
| `Prompt` / `RequestTail` / `Answer` | TEXT | 清洗后的明文正文（完整） |
| `ToolNames` | TEXT | 工具名列表 |
| `SearchText` | TEXT | 写入时拼接的检索文本（默认截断，见下） |

v7 的 `Headers`、`Request`、`Response` 死列不再存在；请求头整体进入压缩 payload，正文映射到
`Prompt`/`Answer`。

### `SearchText` 与截断

`SearchText = Prompt 头部 + '\n' + RequestTail 尾部 + '\n' + Answer 头部`，由写入路径生成，
不依赖触发器。默认每段 512 字节：

| 变量 | 默认 | 说明 |
| :--- | ---: | :--- |
| `AUDIT_FTS_PROMPT_BYTES` | 512 | Prompt 头部 |
| `AUDIT_FTS_TAIL_BYTES` | 512 | RequestTail 尾部 |
| `AUDIT_FTS_ANSWER_BYTES` | 512 | Answer 头部 |
| `AUDIT_FTS_MAX_BYTES` | 0 | 拼接后全局上限，0 为不限 |

完整正文仍保存在 `Prompt`/`RequestTail`/`Answer`，详情接口不受影响；只有检索索引使用截断文本。
设为 0 可索引全文，写入吞吐约从 5,185 降到 2,400 行/秒。

---

## 表 `payloads`（压缩正文）

```sql
CREATE TABLE payloads (
    record_id INTEGER PRIMARY KEY,
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

完整请求/响应/请求头以 zstd 压缩；仅在详情接口带 `include=body` 时按需解压。

---

## 索引

```sql
CREATE INDEX idx_records_time          ON records(TimeMs);
CREATE INDEX idx_records_type_time     ON records(Type, TimeMs);
CREATE INDEX idx_records_api_key_time  ON records(ApiKey, TimeMs);
CREATE INDEX idx_records_model_time    ON records(Model, TimeMs);
CREATE INDEX idx_records_status_time   ON records(Status, TimeMs);
CREATE INDEX idx_records_backend_time  ON records(Backend, TimeMs);
CREATE INDEX idx_records_ip_time       ON records(IP, TimeMs);
```

列名匹配为精确/前缀（与旧实现一致），`LIKE '%v%'` 子串过滤仍无法利用 B-tree。

---

## 全文检索

**单列 ngram FTS 索引**：

```sql
CREATE INDEX records_fts ON records
USING fts (SearchText) WITH (tokenizer = 'ngram');
```

> 单列是硬约束：Turso 的多列 `USING fts (...)` 会退化为分词匹配（`"error"` 匹配不到
> `terrordome`，中文 2 字全 miss），因此不把 `Model/Status/Type/Backend` 放进同一索引；
> 这些条件继续用 B-tree 残余谓词。

查询翻译（`src/db/records_query.rs`）：

- `q` 按空白切词，每词用裸词 AND（引号短语在 ngram 下会失效）；每个词追加
  `instr(lower(SearchText), lower(?)) > 0` 残余谓词，消除 ngram 的 OR 过匹配。
- `q` ≥ 2 字符：`fts_match(SearchText, ?)` + 残余。
- `q` 为 1 字符：`lower(SearchText) LIKE '%x%'`。
- `model`/`ip` 前缀、`type`/`backend` 精确、`status` 精确、`errors=1 → Status>=400`、
  `client` 子串等仍是 SQL 残余，语义与 v7 一致。
- 多词从 v7 的“整句短语”变为“词间 AND”；这是有意的行为变化（见基准报告）。

**维护**：运行期写入会产生 FTS 段；后台任务默认每 6 小时执行一次 `OPTIMIZE INDEX`
（`DB_FTS_OPTIMIZE_INTERVAL_SEC`，0 关闭），与轮转锁互斥。

---

## 写入路径

- `records` 与 `payloads` 在**同一事务**内提交；writer 显式分配 id。
- 队列满等待 `AUDIT_ENQUEUE_TIMEOUT_MS` 后丢弃并计数，不阻塞请求线程。
- `Flush`/`Seal`/`Shutdown` 按批排空；SIGTERM/Ctrl-C 先 flush 再退出。
- 轮转：`seal` → `wal_checkpoint(TRUNCATE)` → rename → 新库 `init_v8` → `resume`。
- 写入指标：`db_write_queue_depth`、`db_write_rows`、`db_write_batches`、
  `db_write_dropped`、`db_write_failed_batches`、`db_write_last_batch_ms`、
  `db_write_max_batch_ms`（`/metrics`）。

---

## 轮转与归档

- 文件名：`record_YYYYMM.db` 或 `record_YYYYMM_<unix秒>.db`。
- 归档以 Turso **只读** 打开；v7 归档（FTS5）同样可读，无需迁移。
- 遗留 v0 归档（缺 `TimeMs`）在首次扫描时就地迁移到 v7（sqlx 仅用于这一步），随后由
  Turso 只读打开。
- 归档分片记录各自 `MIN/MAX(TimeMs)`，查询只选相交分片；跨分片结果在 Rust 侧合并。

---

## 性能参考

| 场景 | 结果 |
| :--- | :--- |
| v7 → v8 迁移（108,895 行 / 9.2GB payload） | 灌数 104.7s + 建 FTS 145.1s |
| 批量写入（10KB payload，512/512/512） | 5,185 行/秒 |
| 中文检索（3 字词，108k 行） | 计数比 v7 快 11–39×，列表快 5–10× |
| 2 字中文 | 计数更快，列表略慢（FTS + 残余） |
