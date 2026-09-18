//! 跨月归档分片注册表。
//!
//! 月度轮转会把 active 的 `record.db` 归档为同目录下的
//! `record_YYYYMM.db`（同名冲突时追加 `_<unix秒>` 后缀）。本模块以只读连接
//! 懒加载这些归档文件，并按数据的真实时间范围（`MIN/MAX(TimeMs)`）而非文件名
//! 月份来路由查询，从而支持跨月检索。
//!
//! 旧归档可能不带 `TimeMs` 列（`user_version=0` 的遗留库），扫描时应记录告警并
//! 跳过，绝不能因此 panic 或影响 active 库的正常服务。

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// active 分片在查询层使用的固定标识。
pub const ACTIVE_SHARD: &str = "active";

/// 单个归档分片及其只读连接池。
pub struct ArchiveShard {
    /// 文件名去扩展名，例如 `record_202608`。
    pub id: String,
    pub path: PathBuf,
    /// 该分片内 `TimeMs` 的最小值；空表为 None。
    pub min_ms: Option<i64>,
    /// 该分片内 `TimeMs` 的最大值；空表为 None。
    pub max_ms: Option<i64>,
    pub pool: SqlitePool,
}

/// 归档分片注册表；扫描结果缓存于内存，避免每次查询都重新打开连接。
pub struct ArchiveRegistry {
    active_path: PathBuf,
    dir: PathBuf,
    shards: RwLock<HashMap<String, Arc<ArchiveShard>>>,
}

impl ArchiveRegistry {
    /// 以 active 数据库路径构造注册表；归档目录取 active 文件所在目录。
    pub fn new(active_path: PathBuf) -> Self {
        let dir = active_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        Self {
            active_path,
            dir,
            shards: RwLock::new(HashMap::new()),
        }
    }

    /// 扫描归档目录，把尚未缓存的合法分片登记进来。
    ///
    /// 任何单个文件的错误都只会记录告警并继续，绝不影响整体检索。
    pub async fn ensure_scanned(&self) {
        let entries = match std::fs::read_dir(&self.dir) {
            Ok(entries) => entries,
            Err(e) => {
                warn!(
                    "archive scan: read_dir '{}' failed: {}",
                    self.dir.display(),
                    e
                );
                return;
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if path == self.active_path {
                continue;
            }
            let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
                continue;
            };
            if parse_archive_stem(stem).is_none() {
                continue;
            }
            {
                let shards = self.shards.read().await;
                if shards.contains_key(stem) {
                    continue;
                }
            }

            match open_archive_shard(&path, stem).await {
                Ok(Some(shard)) => {
                    info!(
                        "Archive shard registered: {} (TimeMs {:?}..{:?})",
                        shard.id, shard.min_ms, shard.max_ms
                    );
                    self.shards
                        .write()
                        .await
                        .insert(stem.to_string(), Arc::new(shard));
                }
                Ok(None) => {
                    // 遗留归档（缺少 TimeMs），open_archive_shard 内已告警。
                }
                Err(e) => {
                    warn!("archive shard '{}' open failed: {}", path.display(), e);
                }
            }
        }
    }

    /// 返回时间范围与 `[from_ms, to_ms]` 相交的归档分片。
    ///
    /// 两者都为 None 时返回空（仅查询 active）；这保证了默认行为不变。
    /// 分片按数据实际边界过滤，缺失边界的空分片一律排除。
    pub async fn candidates(
        &self,
        from_ms: Option<i64>,
        to_ms: Option<i64>,
    ) -> Vec<Arc<ArchiveShard>> {
        if from_ms.is_none() && to_ms.is_none() {
            return Vec::new();
        }
        self.ensure_scanned().await;

        let lo = from_ms.unwrap_or(i64::MIN);
        let hi = to_ms.unwrap_or(i64::MAX);
        let shards = self.shards.read().await;
        shards
            .values()
            .filter(|s| match (s.min_ms, s.max_ms) {
                (Some(min), Some(max)) => max >= lo && min <= hi,
                _ => false,
            })
            .cloned()
            .collect()
    }

    /// 按分片 id 精确取出一个归档分片。
    pub async fn get(&self, id: &str) -> Option<Arc<ArchiveShard>> {
        self.ensure_scanned().await;
        self.shards.read().await.get(id).cloned()
    }

    /// 返回全部已登记的归档分片（用于详情按 id 跨归档检索）。
    pub async fn all_shards(&self) -> Vec<Arc<ArchiveShard>> {
        self.ensure_scanned().await;
        self.shards.read().await.values().cloned().collect()
    }
}

/// 解析归档文件名主干，返回 `YYYYMM`；格式非法或月份越界返回 None。
///
/// 接受 `record_202608` 与 `record_202608_1789639720` 两种形式；
/// 拒绝 `record_202613`（月份非法）、`record_20260`（位数不足）、
/// `whatever`（前缀不符）、`record_202608_x`（后缀非数字）。
fn parse_archive_stem(stem: &str) -> Option<i32> {
    let rest = stem.strip_prefix("record_")?;
    let (month_part, suffix) = match rest.split_once('_') {
        Some((month, tail)) => (month, Some(tail)),
        None => (rest, None),
    };

    if month_part.len() != 6 || !month_part.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    if let Some(tail) = suffix {
        if tail.is_empty() || !tail.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
    }

    let yyyymm: i32 = month_part.parse().ok()?;
    let month = yyyymm % 100;
    if !(1..=12).contains(&month) {
        return None;
    }
    Some(yyyymm)
}

/// 以只读单连接池打开归档；缺少 `TimeMs` 的遗留库返回 `Ok(None)` 并告警。
async fn open_archive_shard(path: &Path, id: &str) -> Result<Option<ArchiveShard>, sqlx::Error> {
    let options = SqliteConnectOptions::new()
        .filename(path)
        .read_only(true)
        .create_if_missing(false);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .acquire_timeout(Duration::from_secs(5))
        .connect_with(options)
        .await?;

    let columns = sqlx::query("PRAGMA table_info(records)")
        .fetch_all(&pool)
        .await?;
    let has_time_ms = columns
        .iter()
        .any(|row| row.get::<String, _>("name") == "TimeMs");
    if !has_time_ms {
        warn!(
            "Skipping legacy archive without TimeMs column: {}",
            path.display()
        );
        return Ok(None);
    }

    let bounds: (Option<i64>, Option<i64>) =
        sqlx::query_as("SELECT MIN(TimeMs), MAX(TimeMs) FROM records")
            .fetch_one(&pool)
            .await?;

    Ok(Some(ArchiveShard {
        id: id.to_string(),
        path: path.to_path_buf(),
        min_ms: bounds.0,
        max_ms: bounds.1,
        pool,
    }))
}

#[cfg(test)]
mod tests {
    use super::parse_archive_stem;

    #[test]
    fn parses_valid_archive_stems() {
        assert_eq!(parse_archive_stem("record_202608"), Some(202608));
        assert_eq!(parse_archive_stem("record_202608_1789639720"), Some(202608));
        assert_eq!(parse_archive_stem("record_202601"), Some(202601));
        assert_eq!(parse_archive_stem("record_202612"), Some(202612));
    }

    #[test]
    fn rejects_invalid_archive_stems() {
        assert_eq!(parse_archive_stem("record_202613"), None);
        assert_eq!(parse_archive_stem("record_202600"), None);
        assert_eq!(parse_archive_stem("record_20260"), None);
        assert_eq!(parse_archive_stem("whatever"), None);
        assert_eq!(parse_archive_stem("record_202608_x"), None);
        assert_eq!(parse_archive_stem("record_20260a"), None);
        assert_eq!(parse_archive_stem("record_"), None);
    }
}
