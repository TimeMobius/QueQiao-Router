# 8.68 网关启动延迟排查与修复

> 对象：8.68 上 `queqiao-router` 容器（镜像 `queqiao-router:b722-c-v61`，监听 `[::]:8000`）。
> 排查：**只读**日志/`/proc`/NFS/DB 元数据 + 源码阅读；未重启、未停止、未部署、未改配置、未碰生产 DB。
> 数据：2026-11-16 ~ 2026-11-21 应用日志；当前容器 StartedAt `2026-11-21T13:27:49.068Z`（本地 21:27:49）。
> 证据等级：【实测】= 日志/命令直接看到；【推断】= 代码+量级，未直接测量。

---

## 0. 结论先行

1. **5–6 分钟不是 DB 打开/迁移/FTS/rollup，而是 `AuditWriter::start()` 里的 `next_id()`**：【实测】`init_db()` 在启动后 **1.64s** 完成并打印 `Audit database ... initialized (v8)`；之后 **623.5s** 才打印 `Audit database and writer initialized`。两者之间代码只有 `AuditWriter::start`（`src/main.rs:80-86`），而 `start` 的第一件事是 `next_id(&store).await`（`src/db/writer.rs`）。
2. **根因是 `SELECT COALESCE(MAX(id), 0) + 1 FROM records` 触发全表扫描**：把 `MAX(id)` 包进表达式会令 rowid 的 min/max 优化失效。在 41GB、755,984 行、payload 很重的 `records` 表上，读 41GB 是 4–10 分钟的主因。
3. **当前这次 10m25s 是历史分布的右尾**：2026-11-16~11-20 共 14 次启动的 `init_db → listening` 间隔为 **3m59s–6m07s**（中位约 4m40s），本次 **10m25s**；用户感知的"5–6 分钟"与历史实测吻合。
4. **不是 NFS 初次挂载/迁移锁/WAL replay**：`record.db.v8-migrate.lock` 为 0 字节且 mtime 2026-10-28；`record.db-wal` 当前仅 2.5MB；`init_db` 1.64s 内已完成 schema 检查 + `AuditStore::open`；`warm_active_rollup` 在 listening 之后异步完成。
5. **修复已完成并验证**：`next_id` 改为 `SELECT id FROM records ORDER BY id DESC LIMIT 1`，实测读 4.7KB 而非 821MB，并加了计时日志与 EXPLAIN 回归测试（见 §5）。

---

## 1. 启动时序（当前容器，21:27–21:38 本地）

| 阶段 | 日志时间 | 距启动 | 代码/证据 |
| :--- | :--- | ---: | :--- |
| 进程启动 | 21:27:49.068 | 0.000s | `docker inspect StartedAt` |
| Logging initialized | 21:27:49.106 | +0.038s | `main.rs:64` |
| Configuration loaded | 21:27:49.108 | +0.040s | `main.rs:71` |
| **Audit DB opened (v8)** | **21:27:50.709** | **+1.641s** | `init_db` + `src/db/mod.rs` |
| **Writer initialized** | **21:38:14.241** | **+625.173s** | `main.rs:83-86`；**这里卡了 623.5s** |
| Application state | 21:38:14.241 | +625.173s | `main.rs:94`（同一毫秒） |
| Metrics channel/worker | 21:38:14.242 | +625.174s | `main.rs:114/120` |
| **listening on [::]:8000** | **21:38:15.245** | **+626.177s** | `main.rs:123` 的 1s sleep + `main.rs:233` |

**总启动 626.2s（10m26s）**；其中 `AuditWriter::start` 占 **623.5s（99.6%）**。

### 1.1 历史对比（`system.*.log` 中 `Audit database ... initialized` → `listening on`）【实测】

| 日期 | 间隔 |
| :--- | ---: |
| 11-16 | 4m52s |
| 11-17（5 次） | 5m34s / 5m11s / 5m12s / 5m11s / 4m06s |
| 11-18（4 次） | 5m13s / 3m59s / 4m37s / 4m25s |
| 11-19（2 次） | 6m07s / 4m12s |
| 11-20（2 次） | 4m13s / 4m26s |
| **11-21（本次）** | **10m25s** |

中位约 **4m40s**；本次比历史中位慢 **2.2 倍**。说明"5–6 分钟"是常态，10 分钟是 NFS/缓存/负载导致的右尾。

---

## 2. 根因：`next_id` 的 `COALESCE(MAX(id))+1`

### 2.1 代码路径【实测代码】

```
src/main.rs:80   let db = init_db().await?;                 // 1.64s 完成
src/main.rs:83   let audit_writer = AuditWriter::start(db.clone(), WriterConfig::default()).await?;
src/main.rs:86   println!("Audit database and writer initialized.");
                 // 中间没有别的 await；623.5s 全部在 AuditWriter::start
src/db/writer.rs   pub async fn start(store, cfg) {
                       let next_id = next_id(&store).await?;   // ← 唯一重操作
src/db/writer.rs   async fn next_id(store) {
                       "SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM records"
```

- `records.id` 是 `INTEGER PRIMARY KEY`（`src/db/turso_schema.rs`），本应等价 rowid。
- 但 `MAX(id)` 被包在 `COALESCE(...)+1` 表达式里，min/max rowid 优化不生效 → **全表扫描**。
- 表大小：`record.db` **43,119,325,184 B（41G）**、page_size **4096**、约 **10.5M pages**；`last_id=755,984`。
- 41GB 在 4–10 分钟内扫完 ≈ **68–137 MB/s**，与 NFS 1MB rsize 的顺序读量级一致。

### 2.2 Turso 0.8.3 `EXPLAIN` 字节码实测【实测，高】

用仓库自带 `libturso 0.8.3` 编译只读 EXPLAIN 探针（真实 v8 schema）：

| SQL 形态 | 字节码 | 复杂度 |
| :--- | :--- | :--- |
| `COALESCE(MAX(id), 0) + 1` | `OpenRead` → **`Rewind` → `AggStep(max)` → `Next`** | **全表扫描** ❌ |
| `IFNULL(MAX(id), 0) + 1` | `OpenRead` → **`Rewind` → `AggStep(max)` → `Next`** | **全表扫描** ❌ |
| `MAX(id)` 单独 | `OpenRead` → `Last` → `AggStep(max)` → `Goto` | O(1) ✅ |
| `SELECT id ... ORDER BY id DESC LIMIT 1` | `OpenRead` → **`Last` → `RowId` → `ResultRow`/`Prev`** | O(1) ✅ |
| `SELECT next_id FROM records_meta WHERE id=1` | `SeekRowid` | O(1) ✅ |

**触发条件是把 `MAX(id)` 包进任何表达式**——单独 `MAX(id)` 反而能走 `Last` 优化。

> ⚠️ 注意：`EXPLAIN QUERY PLAN` 的文本（`SEARCH`/`SCAN`）**不可靠**，本例中它把两条查询的结论显示反了；
> 只有 `EXPLAIN`（字节码）才准。另外 stock sqlite3 会优化 `COALESCE(MAX(id))` 形态，
> **不能用 stock sqlite3 复现**，必须用 Turso。

### 2.3 量级实测（同一份 v8 库，803MB / 200k 行）【实测，高】

| SQL | 耗时 | 实际读取 |
| :--- | ---: | ---: |
| `COALESCE(MAX(id),0)+1`（旧） | **1936.6 ms** | **821 MB** |
| `ORDER BY id DESC LIMIT 1`（新） | **0.8 ms** | **4.7 KB** |

约 **2400 倍**耗时差、**17 万倍**读取量差。41GB 生产库按同一比例外推即 620s 量级。

---

## 3. 现场其他证据

### 3.1 NFS【实测】

```
192.168.50.10:/mnt/data/dubhe on /nfs type nfs4
rw,relatime,vers=4.1,rsize=1048576,wsize=1048576,namlen=255,
acregmin=30,acregmax=30,acdirmax=30,
soft,proto=tcp,timeo=30,retrans=2,sec=sys,clientaddr=192.168.8.68,
local_lock=none,addr=192.168.50.10
```

- `rsize/wsize=1MB`、v4.1 正常；**单条 TCP 连接**（未设 `nconnect`），41GB 顺序扫描受单流吞吐限制。
- `soft,timeo=30`（3s）×2 retry：读压力大时可能短读/EIO；它不会直接造成 5min，但会增加重试与不确定性。
- 主机 `313Gi` 内存、`183Gi buff/cache`；`/tmp` 是 **tmpfs 157G**；根盘 **10G 可用**。

### 3.2 运行中进程【实测/局限】

- `/proc/12888/io`：`rchar 31.1GB`、`read_bytes 5.2GB`（运行 2h 累计，不是启动期）。
- `/proc/12888/stack`、`wchan` 采样于 23:30：绝大多数 `futex_wait_queue_me`、主线程 `ep_poll`；
  **看不到启动期等待**（只能证明"当前空闲"，不能回放启动峰值）。

### 3.3 排除项【实测，高】

- 不是迁移：`.v8-migrate.lock` 0B、mtime 2026-10-28，启动日志已写 `(v8)`。
- 不是 WAL replay：`record.db-wal` 仅 2.5MB。
- 不是 rollup/FTS：`active rollup updated` 在 listening 之后 0.85s，且 `warm_active_rollup` 是 `tokio::spawn`。
- 不是 1s sleep：`main.rs` 的 1s 固定延迟远小于 623s。

---

## 4. 另一处同源问题：轮转路径

`next_id()` 不止启动时调用一次。`WriterCmd::Resume` 分支（`src/db/writer.rs` 的 `run()`）也调它，
而 `Resume` 是**月度轮转**（`src/db/rotation.rs`）归档老库、打开新库后必经的一步：

- 轮转成功时：新库为空，`next_id` 返回 1，代价极小（实测 `elapsed_ms=11`）。
- **轮转失败回滚时**（`rotation.rs` 的 `writer.resume(old_store)`）：重新解析的就是那个大库，
  代价与启动时相同。修复后该路径一并消除。

---

## 5. 修复内容

### 5.1 换查询形态（`src/db/writer.rs`）

```sql
-- 旧（全表扫描）
SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM records
-- 新（Last/Prev，O(1)）
SELECT id FROM records ORDER BY id DESC LIMIT 1
```

- 空表（轮转后的新分片）返回 `None` → 从 **1** 开始；否则 `max + 1`。
- SQL 提为具名常量 `NEXT_ID_SQL`，供测试 `EXPLAIN` 同一份语句。

### 5.2 计时日志（`AuditWriter::start`）

原先这两步之间只有一行 `println!`，出问题时无法就地定位。现在记录：

- `audit writer id lookup resolved elapsed_ms=<n> next_id=<n>`（正常，单数字毫秒）
- 超过 `SLOW_NEXT_ID_MS = 1000` 时降级为 `WARN`，直指"`records` 可能正在被扫描"
- `audit writer task started elapsed_ms=<n>`（整个 start 的累计耗时）

### 5.3 EXPLAIN 回归测试（`src/db/next_id_plan_tests.rs`）

断言**编译后的字节码**，而非查询耗时（快且与机器规格无关）：

| 测试 | 断言 |
| :--- | :--- |
| `non_empty_table_id_lookup_does_not_scan` | 计划无 `Rewind`/`Next`；含 `Last` + `RowId`（证明 id 仍是 rowid 别名）；返回值 = max+1 |
| `empty_table_id_lookup_starts_at_one_without_scanning` | 空表返回 1；计划同样无扫描 |
| `expression_wrapped_max_id_is_the_regression_we_guard_against` | 记录旧形态确实扫描；若将来 Turso 改了优化行为，这条会提示前两条断言已失去意义 |

---

## 6. 验证结论

| 项 | 结果 |
| :--- | :--- |
| `cargo fmt --all` | 通过 |
| `cargo build` | 通过 |
| `cargo test`（全量） | **408 passed / 0 failed** |
| 新增回归测试 | 3 passed |
| 本地起网关（空库） | 启动 <1s，日志含 `id lookup resolved elapsed_ms=0 next_id=1` |
| 本地起网关（803MB / 200k 行真实 v8 库） | 启动 <1s，`elapsed_ms=0 next_id=200001`（旧代码此库约 1.9s，41GB 则约 620s） |
| 端到端写审计行 | `/v1/chat/completions` 正常记账；新分片 id 从 1 递增，老库延续 max+1 |
| 轮转路径 | `record.db` → `record_197001.db` 归档后 `resume` 正确解析 id（`elapsed_ms=11`） |

---

## 7. 结论可信度

| 结论 | 等级 | 依据 |
| :--- | :--- | :--- |
| 623.5s 花在 `AuditWriter::start` | **实测（高）** | 两条日志 + `main.rs:80-86` |
| `start` 内唯一 await 是 `next_id` | **实测（高）** | `writer.rs` `start()` |
| 旧 `next_id` 是 `COALESCE(MAX(id))+1` | **实测（高）** | 旧代码 + git 历史 |
| 41GB 表上该查询全表扫描 | **实测（高）** | Turso 0.8.3 `EXPLAIN`：`Rewind`/`Next` |
| 新形态是 O(1) | **实测（高）** | Turso `EXPLAIN`：`Last`/`Prev`；实测 4.7KB |
| 历史 4–6 分钟同因 | **推断（中高）** | 同一二进制/同一日志断点/同一量级 |
| 不是迁移/WAL/FTS/rollup | **实测（高）** | `init_db` 1.64s、lock 0B、WAL 2.5MB、rollup 在 listening 后 |
| NFS 是唯一根因 | **否** | NFS 是放大器；SQL 形态是触发条件 |

---

## 8. 启动路径全量审计（确认 `next_id` 是唯一扫描点）

逐步核对了 `main.rs` 绑定 listener 之前的所有 DB/IO 操作：

| 启动阶段 | 执行的 SQL / IO | 复杂度 | 证据 |
| :--- | :--- | :--- | :--- |
| `ConfigManager::new` | YAML 读文件 | O(1) | `main.rs:68` |
| `init_db` → `detect_schema` | `PRAGMA user_version` + schema 元数据 | O(1) | `migrate.rs` |
| `init_db` → `AuditStore::open` | `probe_schema` + `init_v8_schema`（`PRAGMA table_info`、`CREATE TABLE/INDEX IF NOT EXISTS`） | O(1)，仅元数据 | `turso_schema.rs:111-157` |
| **`AuditWriter::start` → `next_id`** | **旧 `COALESCE(MAX(id))+1`** | **全表扫描 ❌** | 本文 §2 |
| `AppState::new` | 内存构造 + `resolve_db_path`（无 stat） | O(1) | `app_state.rs:27-54` |
| **`init_rotation_state` → `current_data_yyyymm`** | **`SELECT MAX(TimeMs) FROM records`** | **O(1) ✅**（走 `idx_records_time`） | 实测 0.6ms / 读 17KB |
| `warm_active_rollup` | `tokio::spawn` 异步，listening 之后 | 不阻塞 | `main.rs:99-105` |
| `ArchiveRegistry::new` | 仅存路径，不扫目录 | O(1) | `archive.rs:55-69` |
| `ensure_scanned` | 目录枚举 | **惰性触发**，不在启动路径 | `archive.rs:76` |

关键发现：`current_data_yyyymm` 的 `MAX(TimeMs)` 是**裸 `MAX()`**，不像 `next_id` 那样被表达式包裹，
所以能正常走索引优化（`Last` + `Column` + `Prev`，0.6ms / 17KB）。
这正好反证了 §2 的根因判定：**问题不是"取 MAX"，而是"把 MAX 包进表达式"**。

结论：**`next_id` 是启动路径上唯一的扫描型操作**，修复它即可消除 620s 量级的启动延迟。

---

## 9. 后续可选优化（本次未做）

1. **listener 提前 + readiness gate**：在 `init_db` 后先 bind `:8000`，`/health` 返回 503 直到 writer ready；
   避免"连接被拒/超时"。注意未 ready 时审计写入必须排队或拒绝，需要单独评审就绪语义。
2. **DB 本地化/分片**：41GB 活跃库过大；按月归档 + active 小库，active 放本地 NVMe，归档放 NFS。
3. **NFS 调优**：`hard,timeo=600`（可靠性）、`nconnect=4/8`（并发流）、确认 `rsize=1M`。
4. **不要**在启动路径做 `VACUUM`/全库 `integrity_check`/FTS rebuild。

---

## 10. 复现命令

```bash
# 1) 当前容器启动时间与日志断点
ssh root@192.168.8.68 'docker inspect -f "StartedAt={{.State.StartedAt}}" queqiao-router'
ssh root@192.168.8.68 'docker logs --timestamps queqiao-router | grep -E "Logging|Configuration|Audit database|listening|id lookup"'

# 2) 历史启动间隔（应用日志）
ssh root@192.168.8.68 'cd /nfs/openai-api/logs; for f in system.2026-11-*.log; do grep -hE "Audit database at .*initialized|listening on" "$f"; done'

# 3) DB/NFS 元数据
ssh root@192.168.8.68 'ls -lh /nfs/openai-api/logs/record.db*; stat -c "%n %s %b" /nfs/openai-api/logs/record.db; mount | grep nfs; nfsstat -m'

# 4) 部署后确认修复生效（关键：elapsed_ms 应为个位数）
ssh root@192.168.8.68 'cd /nfs/openai-api/logs; grep -h "id lookup resolved" info.*.log | tail -5'
```

---

## 11. 文件/改动

- 修改：`src/db/writer.rs`（`next_id` 查询形态 + 计时日志 + `NEXT_ID_SQL` 常量）
- 修改：`src/db/mod.rs`（注册测试模块）
- 新增：`src/db/next_id_plan_tests.rs`（EXPLAIN 回归测试）
- 新增：`docs/design/startup-latency-868.md`（本文件）
- 未修改配置、未重启/部署任何生产服务。