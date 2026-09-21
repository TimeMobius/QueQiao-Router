use serde_json::{json, Value};

/// 递归截断 JSON 对象，用于日志记录
pub fn truncate_json(value: &Value) -> Value {
    match value {
        Value::String(s) => {
            if s.len() > 500 {
                let mut end = 500;
                while !s.is_char_boundary(end) {
                    end -= 1;
                }
                json!(format!("{}...[TRUNCATED]", &s[..end]))
            } else {
                value.clone()
            }
        }
        Value::Array(arr) => {
            if arr.len() > 10 {
                let mut new_arr: Vec<Value> = arr.iter().take(10).map(truncate_json).collect();
                new_arr.push(json!(format!("...[TRUNCATED: {} items]", arr.len())));
                Value::Array(new_arr)
            } else {
                Value::Array(arr.iter().map(truncate_json).collect())
            }
        }
        Value::Object(map) => {
            let new_map = map
                .iter()
                .map(|(k, v)| (k.clone(), truncate_json(v)))
                .collect();
            Value::Object(new_map)
        }
        _ => value.clone(),
    }
}

/// Records a mid-stream SSE interruption in `error.log`.
///
/// SSE responses are committed as `200` before the body is polled, so the
/// access-log middleware can never classify these failures as errors. The
/// `access_log` target is what routes the record to `error.log`; its layers keep
/// only the message, so all fields are folded into the formatted line.
pub fn log_stream_interruption(
    client_ip: &str,
    endpoint: &str,
    model: &str,
    backend: &str,
    error: &str,
) {
    let time_str = chrono::Local::now().format("%d/%b/%Y:%H:%M:%S %z");
    tracing::error!(
        target: "access_log",
        "[{}] STREAM_INTERRUPTED client={} endpoint={} model={} backend={} error={:?}",
        time_str,
        client_ip,
        endpoint,
        model,
        backend,
        error
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_truncate_json_string_short() {
        let value = json!("short string");
        let result = truncate_json(&value);
        assert_eq!(result, "short string");
    }

    #[test]
    fn test_truncate_json_string_long() {
        let long_string = "a".repeat(600);
        let value = json!(long_string);
        let result = truncate_json(&value);
        let result_str = result.as_str().unwrap();
        assert!(result_str.ends_with("...[TRUNCATED]"));
        assert!(result_str.len() < 550);
    }

    #[test]
    fn test_truncate_json_array() {
        let arr: Vec<Value> = (0..15).map(|i| json!(i)).collect();
        let value = Value::Array(arr);
        let result = truncate_json(&value);
        let arr = result.as_array().unwrap();
        assert_eq!(arr.len(), 11);
        assert!(arr[10].as_str().unwrap().contains("TRUNCATED"));
    }
}
