//! 审计库的 schema 定义与迁移逻辑。
//!
//! 集中维护表结构版本、增量列/索引、payloads 与 FTS5 全文检索 DDL，
//! 以及 `migrate` 迁移入口。`legacy.rs` 复用这里的常量完成遗留归档迁移。

pub(crate) const SCHEMA_VERSION: i64 = 7;

/// v1 新增列；经 `PRAGMA table_info` 守卫后逐列 `ADD COLUMN`，以兼容旧库与测试套件预建的表结构。
pub(crate) const NEW_COLUMNS: &[(&str, &str)] = &[
    ("TimeMs", "INTEGER"),
    ("Method", "TEXT"),
    ("Endpoint", "TEXT"),
    ("Backend", "TEXT"),
    ("SessionId", "TEXT"),
    ("ParentSessionId", "TEXT"),
    ("RequestId", "TEXT"),
    ("SessionAffinity", "TEXT"),
    ("UserAgent", "TEXT"),
    ("ClientName", "TEXT"),
    ("ClientVersion", "TEXT"),
    ("ApiKey", "TEXT"),
    ("Status", "INTEGER"),
    ("Error", "TEXT"),
    ("RetryCount", "INTEGER"),
    ("FinishReason", "TEXT"),
    ("LatencyMs", "REAL"),
    ("TtftMs", "REAL"),
    ("UpstreamMs", "REAL"),
    ("StreamMs", "REAL"),
    ("RequestBytes", "INTEGER"),
    ("ResponseBytes", "INTEGER"),
    ("PromptBytes", "INTEGER"),
    ("RequestTailBytes", "INTEGER"),
    ("AnswerBytes", "INTEGER"),
    ("MessageCount", "INTEGER"),
    ("SystemCount", "INTEGER"),
    ("ToolCount", "INTEGER"),
    ("AssistantCount", "INTEGER"),
    ("ToolResultCount", "INTEGER"),
    ("ImageCount", "INTEGER"),
    ("Prompt", "TEXT"),
    ("RequestTail", "TEXT"),
    ("Answer", "TEXT"),
    ("ToolNames", "TEXT"),
    ("payload_id", "INTEGER"),
];

/// v1 索引：(SQL, 依赖列)。仅当依赖列齐备时创建，避免测试预建表缺列导致失败。
pub(crate) const NEW_INDEXES: &[(&str, &[&str])] = &[
    (
        "CREATE INDEX IF NOT EXISTS idx_records_time ON records(TimeMs)",
        &["TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_type_time ON records(Type, TimeMs)",
        &["Type", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_api_key_time ON records(ApiKey, TimeMs)",
        &["ApiKey", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_model_time ON records(Model COLLATE NOCASE, TimeMs)",
        &["Model", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_ip_time ON records(IP COLLATE NOCASE, TimeMs)",
        &["IP", "TimeMs"],
    ),
    (
        "CREATE INDEX IF NOT EXISTS idx_records_backend_time ON records(Backend, TimeMs)",
        &["Backend", "TimeMs"],
    ),
];

pub(crate) const PAYLOADS_DDL: &str = r#"
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
)
"#;

/// v3 全文检索：external-content + trigram（2 字中文用 LIKE，≥3 字用 MATCH）。
pub(crate) const FTS_DDL: &[&str] = &[
    "CREATE VIRTUAL TABLE IF NOT EXISTS records_fts USING fts5(\
         Prompt, RequestTail, Answer, \
         content='records', content_rowid='id', tokenize='trigram')",
    "CREATE TRIGGER IF NOT EXISTS records_fts_ai AFTER INSERT ON records BEGIN \
         INSERT INTO records_fts(rowid, Prompt, RequestTail, Answer) \
         VALUES (new.id, new.Prompt, new.RequestTail, new.Answer); END",
    "CREATE TRIGGER IF NOT EXISTS records_fts_ad AFTER DELETE ON records BEGIN \
         INSERT INTO records_fts(records_fts, rowid, Prompt, RequestTail, Answer) \
         VALUES ('delete', old.id, old.Prompt, old.RequestTail, old.Answer); END",
    "CREATE TRIGGER IF NOT EXISTS records_fts_au AFTER UPDATE ON records BEGIN \
         INSERT INTO records_fts(records_fts, rowid, Prompt, RequestTail, Answer) \
         VALUES ('delete', old.id, old.Prompt, old.RequestTail, old.Answer); \
         INSERT INTO records_fts(rowid, Prompt, RequestTail, Answer) \
         VALUES (new.id, new.Prompt, new.RequestTail, new.Answer); END",
];
