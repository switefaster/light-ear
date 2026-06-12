#![allow(dead_code)]

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::header::{ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, HeaderMap, RANGE};

use crate::core::PlaybackTrack;

pub(crate) const READY_WINDOW_MS: u64 = 12_000;
pub(crate) const LOW_WATERMARK_MS: u64 = 5_000;
pub(crate) const HIGH_WATERMARK_MS: u64 = 15_000;
pub(crate) const MAX_CACHE_BYTES: u64 = 2 * 1024 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ByteRange {
    pub(crate) start: u64,
    pub(crate) end_inclusive: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ContentRange {
    pub(crate) start: u64,
    pub(crate) end_inclusive: u64,
    pub(crate) total: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RangeProbe {
    pub(crate) supports_ranges: bool,
    pub(crate) total_bytes: Option<u64>,
}

#[derive(Debug, Clone)]
pub(crate) struct CachedRange {
    pub(crate) path: PathBuf,
    pub(crate) range: ByteRange,
    pub(crate) total_bytes: Option<u64>,
}

impl ByteRange {
    pub(crate) fn new(start: u64, end_inclusive: u64) -> Result<Self> {
        if end_inclusive < start {
            bail!("invalid byte range");
        }
        Ok(Self {
            start,
            end_inclusive,
        })
    }

    pub(crate) fn len(&self) -> u64 {
        self.end_inclusive.saturating_sub(self.start) + 1
    }

    pub(crate) fn header_value(&self) -> String {
        format!("bytes={}-{}", self.start, self.end_inclusive)
    }
}

pub(crate) fn cache_root() -> PathBuf {
    std::env::temp_dir().join("link-ear-media-cache")
}

pub(crate) fn cache_key(track: &PlaybackTrack) -> String {
    let digest = md5::compute(format!("{}|{}", track.track_id, track.audio_url));
    format!("{}-{digest:x}", sanitize_cache_component(&track.track_id))
}

pub(crate) fn cache_file_path(root: &Path, track: &PlaybackTrack, range: ByteRange) -> PathBuf {
    root.join(cache_key(track))
        .join(format!("{}-{}.part", range.start, range.end_inclusive))
}

pub(crate) fn range_for_window(position_ms: u64, duration_ms: u64, total_bytes: u64) -> ByteRange {
    if duration_ms == 0 || total_bytes == 0 {
        return ByteRange {
            start: 0,
            end_inclusive: 0,
        };
    }

    let start = position_ms
        .saturating_mul(total_bytes)
        .saturating_div(duration_ms)
        .min(total_bytes.saturating_sub(1));
    let window_end_ms = position_ms
        .saturating_add(HIGH_WATERMARK_MS)
        .min(duration_ms);
    let end = window_end_ms
        .saturating_mul(total_bytes)
        .saturating_div(duration_ms)
        .saturating_add(256 * 1024)
        .min(total_bytes.saturating_sub(1));

    ByteRange {
        start,
        end_inclusive: end.max(start),
    }
}

pub(crate) fn parse_range_probe(headers: &HeaderMap) -> RangeProbe {
    let supports_ranges = headers
        .get(ACCEPT_RANGES)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("bytes"))
        || headers.get(CONTENT_RANGE).is_some();
    let total_bytes = headers
        .get(CONTENT_RANGE)
        .and_then(|value| value.to_str().ok())
        .and_then(parse_content_range)
        .and_then(|range| range.total)
        .or_else(|| {
            headers
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<u64>().ok())
        });

    RangeProbe {
        supports_ranges,
        total_bytes,
    }
}

pub(crate) fn parse_content_range(value: &str) -> Option<ContentRange> {
    let value = value.trim();
    let range = value.strip_prefix("bytes ")?;
    let (span, total) = range.split_once('/')?;
    let (start, end) = span.split_once('-')?;
    Some(ContentRange {
        start: start.parse().ok()?,
        end_inclusive: end.parse().ok()?,
        total: if total == "*" {
            None
        } else {
            Some(total.parse().ok()?)
        },
    })
}

pub(crate) async fn fetch_range(
    client: &reqwest::Client,
    track: &PlaybackTrack,
    root: &Path,
    range: ByteRange,
) -> Result<CachedRange> {
    let path = cache_file_path(root, track, range);
    if path.exists() && path.metadata()?.len() == range.len() {
        return Ok(CachedRange {
            path,
            range,
            total_bytes: None,
        });
    }

    let response = client
        .get(&track.audio_url)
        .header(RANGE, range.header_value())
        .header("referer", track.referer.as_str())
        .send()
        .await
        .with_context(|| format!("failed to fetch media range {}", range.header_value()))?
        .error_for_status()
        .context("media range request failed")?;

    let probe = parse_range_probe(response.headers());
    if !probe.supports_ranges {
        bail!("media server does not support HTTP range requests");
    }

    let bytes = response
        .bytes()
        .await
        .context("failed to read media range")?;
    if bytes.len() as u64 != range.len() {
        bail!(
            "media range returned {} bytes, expected {}",
            bytes.len(),
            range.len()
        );
    }

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = fs::File::create(&path)?;
    file.write_all(&bytes)?;

    Ok(CachedRange {
        path,
        range,
        total_bytes: probe.total_bytes,
    })
}

pub(crate) fn evict_cache(root: &Path, max_bytes: u64) -> Result<u64> {
    let mut entries = Vec::new();
    collect_cache_files(root, &mut entries)?;
    let mut total: u64 = entries.iter().map(|entry| entry.len).sum();
    if total <= max_bytes {
        return Ok(0);
    }

    entries.sort_by_key(|entry| entry.modified);
    let mut removed = 0;
    for entry in entries {
        if total <= max_bytes {
            break;
        }
        if fs::remove_file(&entry.path).is_ok() {
            total = total.saturating_sub(entry.len);
            removed += entry.len;
        }
    }
    Ok(removed)
}

#[derive(Debug)]
struct CacheFile {
    path: PathBuf,
    len: u64,
    modified: SystemTime,
}

fn collect_cache_files(root: &Path, entries: &mut Vec<CacheFile>) -> Result<()> {
    if !root.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(root).with_context(|| format!("failed to read {}", root.display()))? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_cache_files(&path, entries)?;
        } else if metadata.is_file() {
            entries.push(CacheFile {
                path,
                len: metadata.len(),
                modified: metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH),
            });
        }
    }
    Ok(())
}

fn sanitize_cache_component(value: &str) -> String {
    let sanitized = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if sanitized.is_empty() {
        "track".to_string()
    } else {
        sanitized
    }
}

pub(crate) fn range_probe_error(probe: &RangeProbe) -> Result<()> {
    if probe.supports_ranges {
        Ok(())
    } else {
        Err(anyhow!("media server does not support HTTP range requests"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn track() -> PlaybackTrack {
        PlaybackTrack {
            track_id: "bilibili:BV1/test".to_string(),
            title: "title".to_string(),
            source_kind: "bilibili".to_string(),
            bvid: "BV196Ex61ES1".to_string(),
            part: 1,
            duration_ms: 120_000,
            audio_url: "https://example.test/audio.m4s?token=secret".to_string(),
            referer: "https://www.bilibili.com/video/BV196Ex61ES1".to_string(),
        }
    }

    #[test]
    fn byte_range_formats_http_header() {
        let range = ByteRange::new(10, 20).unwrap();
        assert_eq!(range.len(), 11);
        assert_eq!(range.header_value(), "bytes=10-20");
        assert!(ByteRange::new(20, 10).is_err());
    }

    #[test]
    fn content_range_parser_accepts_known_shape() {
        assert_eq!(
            parse_content_range("bytes 10-20/100"),
            Some(ContentRange {
                start: 10,
                end_inclusive: 20,
                total: Some(100)
            })
        );
        assert_eq!(
            parse_content_range("bytes 10-20/*"),
            Some(ContentRange {
                start: 10,
                end_inclusive: 20,
                total: None
            })
        );
        assert_eq!(parse_content_range("items 10-20/100"), None);
    }

    #[test]
    fn cache_key_and_path_are_stable_and_sanitized() {
        let root = PathBuf::from("cache");
        let path = cache_file_path(&root, &track(), ByteRange::new(0, 99).unwrap());
        let text = path.to_string_lossy();
        assert!(text.contains("bilibili_BV1_test"));
        assert!(text.ends_with("0-99.part"));
    }

    #[test]
    fn window_range_maps_time_to_bytes_with_prefetch_slack() {
        let range = range_for_window(60_000, 120_000, 1_200_000);
        assert_eq!(range.start, 600_000);
        assert!(range.end_inclusive > 750_000);
        assert!(range.end_inclusive < 1_200_000);
    }

    #[test]
    fn unsupported_range_probe_is_recoverable_error() {
        assert!(
            range_probe_error(&RangeProbe {
                supports_ranges: true,
                total_bytes: Some(100),
            })
            .is_ok()
        );
        assert!(
            range_probe_error(&RangeProbe {
                supports_ranges: false,
                total_bytes: Some(100),
            })
            .unwrap_err()
            .to_string()
            .contains("does not support HTTP range")
        );
    }
}
