use once_cell::sync::Lazy;
use sha2::{Digest, Sha256};
use std::io::Read;

fn env_i32(name: &str, default: i32) -> i32 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

static LEVEL: Lazy<i32> = Lazy::new(|| env_i32("RECORD_ZSTD_LEVEL", 3));
static DICT: Lazy<Option<(String, Vec<u8>)>> = Lazy::new(load_dict);

fn load_dict() -> Option<(String, Vec<u8>)> {
    let path = std::env::var("RECORD_ZSTD_DICT_PATH").ok()?;
    let bytes = std::fs::read(&path).ok()?;
    if bytes.is_empty() {
        return None;
    }
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let id = hasher
        .finalize()
        .iter()
        .take(8)
        .map(|b| format!("{:02x}", b))
        .collect::<String>();
    Some((id, bytes))
}

pub struct Compressed {
    pub codec: String,
    pub dict_id: Option<String>,
    pub bytes: Vec<u8>,
    pub raw_len: i64,
}

pub fn compress(data: &[u8]) -> Compressed {
    let raw_len = data.len() as i64;
    if let Some((id, dict)) = DICT.as_ref() {
        if let Ok(mut compressor) = zstd::bulk::Compressor::with_dictionary(*LEVEL, dict) {
            if let Ok(bytes) = compressor.compress(data) {
                return Compressed {
                    codec: "zstd-dict".to_string(),
                    dict_id: Some(id.clone()),
                    bytes,
                    raw_len,
                };
            }
        }
    }
    match zstd::bulk::compress(data, *LEVEL) {
        Ok(bytes) => Compressed {
            codec: "zstd".to_string(),
            dict_id: None,
            bytes,
            raw_len,
        },
        Err(_) => Compressed {
            codec: "none".to_string(),
            dict_id: None,
            bytes: data.to_vec(),
            raw_len,
        },
    }
}

pub fn decompress(codec: &str, dict_id: Option<&str>, blob: &[u8]) -> Option<Vec<u8>> {
    match codec {
        "none" => Some(blob.to_vec()),
        "zstd" => zstd::stream::decode_all(blob).ok(),
        "zstd-dict" => {
            let (id, dict) = DICT.as_ref()?;
            if Some(id.as_str()) != dict_id {
                return None;
            }
            let mut decoder = zstd::stream::Decoder::with_dictionary(blob, dict).ok()?;
            let mut out = Vec::new();
            decoder.read_to_end(&mut out).ok()?;
            Some(out)
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_without_dictionary() {
        let data = b"{\"messages\":[{\"content\":\"hello world\"}]}".repeat(50);
        let c = compress(&data);
        assert_eq!(c.codec, "zstd");
        assert_eq!(c.raw_len, data.len() as i64);
        assert!(c.bytes.len() < data.len());
        let out = decompress(&c.codec, c.dict_id.as_deref(), &c.bytes).unwrap();
        assert_eq!(out, data);
    }

    #[test]
    fn unknown_codec_returns_none() {
        assert!(decompress("brotli", None, b"x").is_none());
    }
}
