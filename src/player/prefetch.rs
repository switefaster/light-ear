use super::{SharedBytes, StreamingByteChunk, media_cache};
use crate::core::{PlaybackTrack, QueueItem};
use anyhow::{Result, anyhow};
use futures::FutureExt;
use std::{
    collections::VecDeque,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;

const LOOKAHEAD: usize = 2;
// A CDN URL can expire while a long track plays. Discard old openings and resolve again.
const MAX_AGE: Duration = Duration::from_secs(10 * 60);

pub(super) struct Opening {
    media: PlaybackTrack,
    total: u64,
    bytes: Arc<[u8]>,
}

impl Opening {
    pub(super) async fn fetch(client: reqwest::Client, media: PlaybackTrack) -> Result<Self> {
        let (bytes, probe) = media_cache::fetch_opening_bytes(&client, &media).await?;
        let total = probe
            .total_bytes
            .ok_or_else(|| anyhow!("missing media length"))?;
        Ok(Self {
            media,
            total,
            bytes: bytes.into(),
        })
    }

    pub(super) fn install(self, cache: &SharedBytes) -> PlaybackTrack {
        let range = media_cache::ByteRange {
            start: 0,
            end_inclusive: self.bytes.len() as u64 - 1,
        };
        cache.set_total(Some(self.total));
        cache.append_chunk(StreamingByteChunk {
            range,
            bytes: self.bytes,
        });
        self.media
    }
}

struct Entry {
    track: PlaybackTrack,
    started: Instant,
    job: JoinHandle<Result<Opening>>,
}

impl Drop for Entry {
    fn drop(&mut self) {
        self.job.abort();
    }
}

fn same_media(a: &PlaybackTrack, b: &PlaybackTrack) -> bool {
    a.track_id == b.track_id
        && a.bvid == b.bvid
        && a.part == b.part
        && a.source_kind == b.source_kind
        && a.duration_ms == b.duration_ms
}

#[derive(Default)]
pub(super) struct QueuePrefetch {
    entries: Vec<Entry>,
}

impl QueuePrefetch {
    pub(super) fn update<F: Future<Output = Result<Opening>> + Send + 'static>(
        &mut self,
        queue: &VecDeque<QueueItem>,
        allowed: bool,
        fetch: impl FnOnce(&PlaybackTrack) -> F,
    ) {
        let upcoming = || queue.iter().take(LOOKAHEAD).map(|item| &item.track);
        self.entries.retain(|entry| {
            entry.started.elapsed() < MAX_AGE
                && upcoming().any(|track| same_media(track, &entry.track))
                && (allowed || entry.job.is_finished())
        });
        if !allowed || self.entries.iter().any(|e| !e.job.is_finished()) {
            return;
        }
        let Some(track) = upcoming().find(|t| {
            t.source_kind == "bilibili"
                && !t.bvid.is_empty()
                && t.part > 0
                && !self.entries.iter().any(|entry| same_media(t, &entry.track))
        }) else {
            return;
        };
        self.entries.push(Entry {
            track: track.clone(),
            started: Instant::now(),
            job: tokio::spawn(fetch(track)),
        });
    }

    pub(super) fn pause(&mut self) {
        self.entries.retain(|entry| entry.job.is_finished());
    }

    pub(super) fn take(&mut self, track: &PlaybackTrack) -> Option<Opening> {
        let index = self
            .entries
            .iter()
            .position(|entry| same_media(&entry.track, track))?;
        let mut entry = self.entries.remove(index);
        if entry.started.elapsed() >= MAX_AGE {
            return None;
        }
        // A real prepare never waits for speculative work or inherits its failure.
        (&mut entry.job).now_or_never()?.ok()?.ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::{pending, ready};

    fn item(id: &str) -> QueueItem {
        QueueItem {
            item_id: id.into(),
            requested_by: "local".into(),
            added_at_micros: 0,
            track: PlaybackTrack {
                track_id: id.into(),
                bvid: id.into(),
                part: 1,
                title: id.into(),
                source_kind: "bilibili".into(),
                duration_ms: 60_000,
                audio_url: String::new(),
                referer: String::new(),
                media_init_range: None,
                media_index_range: None,
                media_is_progressive: false,
            },
        }
    }

    fn opening(track: &PlaybackTrack) -> std::future::Ready<Result<Opening>> {
        ready(Ok(Opening {
            media: track.clone(),
            total: 20,
            bytes: vec![42; 20].into(),
        }))
    }

    fn unexpected_fetch(_: &PlaybackTrack) -> std::future::Pending<Result<Opening>> {
        panic!("unexpected speculative download")
    }

    #[tokio::test]
    async fn queue_prefetch_foreground_download_continues_at_next_chunk() -> Result<()> {
        use super::super::{DecodeToken, download_streaming_bytes};
        use std::{
            io::{BufRead, BufReader, Write},
            net::TcpListener,
            sync::{Arc, atomic::AtomicU64},
        };
        let listener = TcpListener::bind("127.0.0.1:0")?;
        let mut media = item("next").track;
        media.audio_url = format!("http://{}/clip", listener.local_addr()?);
        let chunk = media_cache::STREAM_CHUNK_BYTES;
        let total = 2 * chunk;
        let server = tokio::task::spawn_blocking(move || -> Result<Vec<String>> {
            listener.set_nonblocking(true)?;
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut requests = Vec::new();
            for start in [0, chunk] {
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(e)
                            if e.kind() == std::io::ErrorKind::WouldBlock
                                && Instant::now() < deadline =>
                        {
                            std::thread::sleep(Duration::from_millis(5))
                        }
                        Err(e) => return Err(e.into()),
                    }
                };
                socket.set_nonblocking(false)?;
                socket.set_read_timeout(Some(Duration::from_secs(5)))?;
                socket.set_write_timeout(Some(Duration::from_secs(5)))?;
                for line in BufReader::new(&socket).lines() {
                    let header = line?.to_ascii_lowercase();
                    if header.is_empty() {
                        break;
                    }
                    if let Some(range) = header.strip_prefix("range: ") {
                        requests.push(range.to_owned());
                    }
                }
                let end = start + chunk - 1;
                write!(
                    socket,
                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{total}\r\nContent-Length: {chunk}\r\nConnection: close\r\n\r\n"
                )?;
                socket.write_all(&vec![42; chunk as usize])?;
            }
            Ok(requests)
        });
        let client = reqwest::Client::new();
        let bytes = SharedBytes::new();
        let playable = Opening::fetch(client.clone(), media).await?.install(&bytes);
        let mut sample = [0];
        assert_eq!(bytes.read_at(0, &mut sample, &DecodeToken::new())?, 1);
        assert_eq!(sample, [42]);
        let (events, _rx) = tokio::sync::mpsc::unbounded_channel();
        let task = tokio::spawn(download_streaming_bytes(
            client,
            playable,
            0,
            Arc::new(AtomicU64::new(0)),
            bytes.clone(),
            events,
            None,
            "active".into(),
        ));
        let requests = tokio::time::timeout(Duration::from_secs(5), server).await???;
        bytes.cancel();
        task.await??;
        assert_eq!(
            requests,
            [
                format!("bytes=0-{}", chunk - 1),
                format!("bytes={chunk}-{}", total - 1)
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn queue_prefetch_follows_reorders_and_bounds_concurrent_work() {
        let mut queue = VecDeque::from([item("a"), item("b"), item("c")]);
        let mut prefetch = QueuePrefetch::default();
        prefetch.update(&queue, true, |_| pending());
        prefetch.update(&queue, true, unexpected_fetch);
        assert_eq!(prefetch.entries.len(), 1);
        queue.swap(0, 2);
        prefetch.update(&queue, true, opening);
        tokio::task::yield_now().await;
        prefetch.update(&queue, true, opening);
        tokio::task::yield_now().await;
        prefetch.update(&queue, true, unexpected_fetch);
        assert!(prefetch.take(&item("a").track).is_none());
        let bytes = SharedBytes::new();
        let media = prefetch.take(&queue[0].track).unwrap().install(&bytes);
        assert_eq!(media.track_id, "c");
        assert_eq!(bytes.total_bytes(), Some(20));
        assert_eq!(bytes.in_memory_bytes(), 20);
        queue.clear();
        prefetch.update(&queue, true, opening);
        assert!(prefetch.entries.is_empty());
    }

    #[tokio::test]
    async fn queue_prefetch_yields_to_playback_and_failures_allow_cold_prepare() {
        let queue = VecDeque::from([item("a"), item("b")]);
        let mut prefetch = QueuePrefetch::default();
        prefetch.update(&queue, false, unexpected_fetch);
        prefetch.update(&queue, true, |_| pending());
        prefetch.update(&queue, false, opening);
        assert!(prefetch.entries.is_empty());
        prefetch.update(&queue, true, |_| ready(Err(anyhow!("CDN busy"))));
        tokio::task::yield_now().await;
        prefetch.update(&queue, true, opening);
        tokio::task::yield_now().await;
        prefetch.pause();
        assert!(prefetch.take(&queue[0].track).is_none());
        assert!(prefetch.take(&queue[1].track).is_some());
    }
}
