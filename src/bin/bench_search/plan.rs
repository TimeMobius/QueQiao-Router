use queqiao_router::db::engine::Value;

const LIST_COLS: &str = "id, Time, TimeMs, Type, Model, Status, Backend, IP, Method, Endpoint, \
    SessionId, ParentSessionId, RequestId, ClientName, ClientVersion, UserAgent, \
    LatencyMs, TtftMs, UpstreamMs, StreamMs, PromptTokens, CompletionTokens, TotalTokens, \
    MessageCount, SystemCount, ToolCount, AssistantCount, ToolResultCount, ImageCount, \
    Tool, Multimodal, RequestBytes, ResponseBytes, FinishReason, Error, RetryCount, \
    substr(Prompt,1,201) AS PromptPreview";

#[derive(Clone, Copy, Debug)]
pub(super) enum Workload {
    Count,
    List,
    CappedCount,
}

pub(super) struct Case<'a> {
    pub(super) name: &'a str,
    pub(super) q: &'a str,
    pub(super) days: Option<i64>,
    pub(super) status: Option<i64>,
    pub(super) model: Option<&'a str>,
}

pub(super) struct Plan {
    pub(super) sql: String,
    pub(super) params: Vec<Value>,
}

pub(super) fn plan(case: &Case<'_>, workload: Workload, target: bool, max_ms: i64) -> Plan {
    let mut params = Vec::new();
    let mut predicate = String::new();
    if target && case.q.chars().count() >= 2 {
        predicate.push_str("fts_match(SearchText, ?)");
        params.push(Value::Text(
            case.q.split_whitespace().collect::<Vec<_>>().join(" AND "),
        ));
        for term in case.q.split_whitespace() {
            predicate.push_str(" AND instr(lower(SearchText), ?) > 0");
            params.push(Value::Text(term.to_lowercase()));
        }
    } else if !target && case.q.chars().count() >= 3 {
        predicate.push_str("id IN (SELECT rowid FROM records_fts WHERE records_fts MATCH ?)");
        params.push(Value::Text(format!("\"{}\"", case.q.replace('"', "\"\""))));
    } else {
        let pattern = format!("%{}%", case.q);
        if target {
            predicate.push_str("SearchText LIKE ? ESCAPE '\\'");
            params.push(Value::Text(pattern));
        } else {
            predicate.push_str("(Prompt LIKE ? ESCAPE '\\' OR RequestTail LIKE ? ESCAPE '\\' OR Answer LIKE ? ESCAPE '\\')");
            params.extend([
                Value::Text(pattern.clone()),
                Value::Text(pattern.clone()),
                Value::Text(pattern),
            ]);
        }
    }
    if let Some(days) = case.days {
        predicate.push_str(" AND TimeMs >= ? AND TimeMs <= ?");
        params.extend([
            Value::Integer(max_ms - days * 86_400_000),
            Value::Integer(max_ms),
        ]);
    }
    if let Some(status) = case.status {
        predicate.push_str(" AND Status = ?");
        params.push(Value::Integer(status));
    }
    if let Some(model) = case.model {
        predicate.push_str(" AND Model COLLATE NOCASE >= ? AND Model COLLATE NOCASE < ?");
        params.extend([
            Value::Text(model.to_owned()),
            Value::Text(format!("{model}\u{10FFFF}")),
        ]);
    }
    let sql = match workload {
        Workload::Count => format!("SELECT COUNT(*) FROM records WHERE {predicate}"),
        Workload::List => format!("SELECT {LIST_COLS} FROM records WHERE {predicate} ORDER BY TimeMs DESC, id DESC LIMIT 51"),
        Workload::CappedCount => format!("SELECT COUNT(*) FROM (SELECT 1 FROM records WHERE {predicate} LIMIT 10000)"),
    };
    Plan { sql, params }
}
