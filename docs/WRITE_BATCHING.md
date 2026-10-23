# 审计写入批量化（P1）

## 实现

- `src/db/writer.rs`：有界数据队列（默认 10,000）+ 独立控制通道。
- 单写任务按 `batch_size`（默认 200）或多行 INSERT 一次提交；空闲 20ms 自动 flush。
- 显式分配 id，records 与 payloads 各一条多行 INSERT，同一事务提交。
- `Seal`：排空队列 → flush → `wal_checkpoint(TRUNCATE)` → 暂停数据消费；轮转改名、重开、`Resume` 后继续。
- `Flush` / `Shutdown`：按批大小分块排空，不会把整个队列塞进一条 SQL。
- `Reset`：丢弃排队项并清空 records/payloads，id 从 1 重新开始。
- SIGTERM / Ctrl-C 会先 flush 写队列再退出。
- `ENABLE_TEST_API=1` 时提供 `/internal/test/records|reset|stats`。

## 吞吐

测试条件：release、同一进程、20,000 行、10KB 压缩前 request、Prompt/Tail/Answer 约 1KB/1KB/2KB、tmpfs、FTS 随写入维护（不是迁移时的延迟建索引）。

| 配置 | 索引文本量 | 行/秒 |
|---|---:|---:|
| batch 200，不截断 | ~4 KB | 2,422 |
| batch 200，512/1024/1024 | ~2.5 KB | 3,549 |
| batch 200，512/512/1024 | ~2 KB | 4,093 |
| batch 200，512/512/512（当前默认） | ~1.5 KB | **5,185** |

对照：batch 50 为 3,252，batch 150 为 4,014（旧基准的小文本）。批次增大能降低事务/语句开销，但真实数据的主要成本是 ngram 索引的文本量。

## 配置

| 变量 | 默认 | 说明 |
|---|---:|---|
| `AUDIT_BATCH_SIZE` | 200 | 每批行数 |
| `AUDIT_BATCH_TIMEOUT_MS` | 20 | 未满批的刷新间隔 |
| `AUDIT_QUEUE_CAPACITY` | 10000 | 有界队列容量 |
| `AUDIT_ENQUEUE_TIMEOUT_MS` | 5000 | 队列满时的等待上限，超时丢弃并计数 |
| `AUDIT_MAX_ATTEMPTS` | 3 | busy/conflict 重试次数 |
| `AUDIT_FTS_PROMPT_BYTES` | 512 | Prompt 头部入索引字节数，0 为不截断 |
| `AUDIT_FTS_TAIL_BYTES` | 512 | RequestTail 尾部入索引字节数 |
| `AUDIT_FTS_ANSWER_BYTES` | 512 | Answer 头部入索引字节数 |
| `AUDIT_FTS_MAX_BYTES` | 0 | 拼接后的全局上限 |

**取舍**：完整 `Prompt/RequestTail/Answer` 仍保存在 records 表中，详情接口返回全文；只有全文检索索引和 `instr` 残余匹配使用截断后的 `SearchText`。要恢复全量搜索，将三个字段上限设为 0，写入吞吐会回到约 2,400 行/秒。

## 验证

- `cargo test --lib`：274 通过（含 writer 批量、seal/resume、reset 测试）。
- 真实服务：聊天请求 200ms 内可从 dashboard 查到；35 天旧记录 1.5s 内触发归档；轮转后继续写入；SIGTERM 后重启，未 flush 的请求记录仍在。
- 真实 V7 库迁移：108,895 行，104.7s 灌数 + 145.1s 建 FTS；行数、payload 字节、时间范围完全一致。
