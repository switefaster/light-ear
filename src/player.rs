mod prefetch;

use std::{
    collections::{HashSet, VecDeque},
    io::{Cursor, Read, Seek, SeekFrom},
    num::{NonZeroU16, NonZeroU32},
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use cpal::traits::{DeviceTrait, HostTrait};
use futures::{StreamExt, stream::FuturesUnordered};
use rodio::{DeviceSinkBuilder, DeviceSinkError, MixerDeviceSink, Player, Source};
#[cfg(not(feature = "fdk-aac-decoder"))]
use symphonia::default::get_codecs;
use symphonia::{
    core::{
        audio::SampleBuffer,
        codecs::{CODEC_TYPE_NULL, CodecParameters, CodecRegistry, Decoder, DecoderOptions},
        errors::Error as SymphoniaError,
        formats::{FormatOptions, SeekMode, SeekTo, Track},
        io::{MediaSource, MediaSourceStream},
        meta::MetadataOptions,
        probe::Hint,
        units::Time,
    },
    default::get_probe,
};
use tokio::sync::mpsc;

use crate::{
    core::{PlaybackCacheStatus, PlaybackCacheView, PlaybackTrack, QueueItem},
    media_cache,
};

const MEDIA_RANGE_FETCH_ATTEMPTS: usize = 3;
const STREAM_BUFFER_TARGET_MS: u64 = media_cache::HIGH_WATERMARK_MS * 2;
const STREAM_BUFFER_RESUME_MS: u64 = media_cache::HIGH_WATERMARK_MS;
const STREAM_PCM_RETAIN_BEHIND_MS: u64 = media_cache::LOW_WATERMARK_MS;
const STREAM_PCM_TRIM_GRANULARITY_MS: u64 = 1_000;
const STREAM_BYTE_RETAIN_BEHIND_BYTES: u64 = media_cache::STREAM_CHUNK_BYTES * 4;
const STREAM_MAX_PENDING_OFFSETS: usize = 6;
const LOCAL_PIECES_IN_FLIGHT: usize = 4;
const STREAM_SMALL_FULL_FETCH_LIMIT_BYTES: u64 = 8 * 1024 * 1024;
const STREAM_DOWNLOAD_IDLE_SLEEP_MS: u64 = 250;
const STREAM_RANGE_REQUEST_SPACING_MS: u64 = 750;
const STREAM_THROTTLE_RETRY_DELAY_MS: u64 = 10_000;
const STREAM_TRANSIENT_RETRY_BASE_MS: u64 = 750;
const STREAM_DECODE_IDLE_SLEEP_MS: u64 = 100;

pub struct AudioPlayer {
    stream: MixerDeviceSink,
    sink: Option<Player>,
    track_id: Option<String>,
    session_id: Option<String>,
    audio: Option<DecodedAudio>,
    streaming: Option<StreamingSession>,
    pub(crate) pieces: Option<crate::local_audio::Client>,
    queue_prefetch: prefetch::QueuePrefetch,
    position_ms: u64,
    started_at_micros: i64,
    playing: bool,
    preferred_device: Option<String>,
    device_watcher: AudioDeviceWatcher,
    volume: Arc<AtomicU32>,
    event_tx: mpsc::UnboundedSender<AudioPlayerEvent>,
    event_rx: mpsc::UnboundedReceiver<AudioPlayerEvent>,
}

#[derive(Clone)]
struct DecodedAudio {
    samples: Arc<[f32]>,
    channels: NonZeroU16,
    sample_rate: NonZeroU32,
}

struct PcmSource {
    audio: DecodedAudio,
    pos: usize,
    volume: Arc<AtomicU32>,
}

struct StreamingPcmSource {
    shared: SharedPcm,
    pos: usize,
    channels: NonZeroU16,
    sample_rate: NonZeroU32,
    base_position_ms: u64,
    dropped_samples: u64,
    volume: Arc<AtomicU32>,
    position_ms: Arc<AtomicU64>,
    event_tx: mpsc::UnboundedSender<AudioPlayerEvent>,
    session_id: String,
    track_id: String,
    duration_ms: u64,
    last_underrun_at: Option<Instant>,
}

struct MemoryMediaSource {
    cursor: Cursor<Arc<[u8]>>,
    byte_len: u64,
}

struct StreamingMediaSource {
    shared: SharedBytes,
    decoder: DecodeToken,
    cursor: u64,
    /// Virtual header length and original frame offset for indexed MP3/FLAC seeks.
    splice: Option<(u64, u64)>,
    /// Last indexed MP3 byte known to be needed for this decoder's readiness window.
    ready_through: Option<u64>,
    seekable: Arc<AtomicBool>,
}

#[derive(Clone)]
struct SharedBytes {
    inner: Arc<(Mutex<StreamingBytesState>, Condvar)>,
    demand_changed: Arc<tokio::sync::Notify>,
}

struct StreamingBytesState {
    chunks: Vec<StreamingByteChunk>,
    total_bytes: Option<u64>,
    canceled: bool,
    error: Option<String>,
    ranges: media_cache::RangeIndex,
    pinned_ranges: Vec<media_cache::ByteRange>,
    requested_offsets: VecDeque<u64>,
    decoder_offset: Option<u64>,
    prepared_once: bool,
    local: bool,
    generation: u64,
}

#[derive(Clone)]
struct StreamingByteChunk {
    range: media_cache::ByteRange,
    bytes: Arc<[u8]>,
}

#[derive(Clone)]
struct SharedPcm {
    inner: Arc<(Mutex<StreamingPcmState>, Condvar)>,
}

struct StreamingPcmState {
    samples: Vec<f32>,
    channels: Option<NonZeroU16>,
    sample_rate: Option<NonZeroU32>,
    base_position_ms: u64,
    dropped_samples: u64,
    complete: bool,
    canceled: bool,
    error: Option<String>,
}

struct StreamingSession {
    decoder_operation: Option<String>,
    session_id: String,
    track_id: String,
    track: PlaybackTrack,
    duration_ms: u64,
    pcm: SharedPcm,
    bytes: SharedBytes,
    decoder: DecodeToken,
    position_ms: Arc<AtomicU64>,
}

#[derive(Clone)]
struct DecodeToken {
    canceled: Arc<AtomicBool>,
}

struct StreamRequestPacer {
    last_started_at: Option<Instant>,
}

struct AudioDeviceWatcher {
    known_device_id: Option<String>,
    last_checked: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct OutputDeviceChange {
    previous: Option<String>,
    current: Option<String>,
}

#[derive(Debug, Clone)]
pub enum AudioPlayerEvent {
    Prepared {
        operation_id: Option<String>,
        session_id: String,
        track_id: String,
        buffered_until_ms: u64,
    },
    Cache {
        operation_id: Option<String>,
        decoder_finished: bool,
        view: PlaybackCacheView,
    },
    Buffering {
        operation_id: Option<String>,
        session_id: String,
        track_id: String,
        buffered_from_ms: u64,
        buffered_until_ms: u64,
    },
    Failed {
        operation_id: Option<String>,
        session_id: String,
        track_id: String,
        title: String,
        error: String,
    },
    DownloadStatus {
        operation_id: Option<String>,
        session_id: String,
        track_id: String,
        message: String,
    },
    OutputDeviceError {
        error: String,
    },
    OutputDeviceChanged {
        previous: Option<String>,
        current: Option<String>,
    },
    Ended {
        session_id: String,
        track_id: String,
    },
}

impl AudioPlayer {
    pub fn new() -> Result<Self> {
        Self::with_output(&OutputSettings::default(), true)
    }

    pub(crate) fn with_output(settings: &OutputSettings, allow_missing: bool) -> Result<Self> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (stream, device_id) =
            open_output(settings.device.as_deref(), allow_missing, event_tx.clone())?;
        Ok(Self {
            stream,
            sink: None,
            track_id: None,
            session_id: None,
            audio: None,
            streaming: None,
            pieces: None,
            queue_prefetch: prefetch::QueuePrefetch::default(),
            position_ms: 0,
            started_at_micros: 0,
            playing: false,
            preferred_device: settings.device.clone(),
            device_watcher: AudioDeviceWatcher::new(device_id),
            volume: Arc::new(AtomicU32::new(
                volume_percent_to_gain(settings.volume).to_bits(),
            )),
            event_tx,
            event_rx,
        })
    }

    pub fn current_track_id(&self) -> Option<&str> {
        self.streaming
            .as_ref()
            .map(|session| session.track_id.as_str())
            .or(self.track_id.as_deref())
    }

    pub(crate) fn decoder_operation(&self) -> Option<&str> {
        self.streaming.as_ref()?.decoder_operation.as_deref()
    }

    pub fn current_session_id(&self) -> Option<&str> {
        self.streaming
            .as_ref()
            .map(|session| session.session_id.as_str())
            .or(self.session_id.as_deref())
    }

    pub fn position_ms(&self, now_micros: i64) -> u64 {
        if let Some(streaming) = &self.streaming {
            return streaming
                .position_ms
                .load(Ordering::Relaxed)
                .min(streaming.duration_ms);
        }

        let position = if self.playing {
            let elapsed = now_micros.saturating_sub(self.started_at_micros).max(0) as u64 / 1000;
            self.position_ms.saturating_add(elapsed)
        } else {
            self.position_ms
        };

        self.audio
            .as_ref()
            .map_or(position, |audio| audio.clamp_position_ms(position))
    }

    pub fn is_finished(&self, now_micros: i64) -> bool {
        if !self.playing {
            return false;
        }

        if let Some(streaming) = &self.streaming {
            return streaming.pcm.is_complete()
                && streaming.position_ms.load(Ordering::Relaxed) >= streaming.duration_ms;
        }

        let Some(audio) = &self.audio else {
            return false;
        };

        self.position_ms(now_micros) >= audio.duration_ms()
    }

    pub fn is_playing(&self) -> bool {
        self.playing
    }

    pub(crate) fn prefetch_queue<
        F: std::future::Future<Output = Result<PlaybackTrack>> + Send + 'static,
    >(
        &mut self,
        queue: &VecDeque<QueueItem>,
        playing_session: Option<&str>,
        client: &reqwest::Client,
        resolve: impl FnOnce(&PlaybackTrack) -> F,
    ) {
        if queue.is_empty() {
            self.clear_queue_prefetch();
            return;
        }
        let allowed = self.playing
            && self.streaming.as_ref().is_some_and(|s| {
                if playing_session != Some(&s.session_id) {
                    return false;
                }
                let position = s.position_ms.load(Ordering::Relaxed);
                let (start, end) = stream_pcm_cache_window(&s.pcm, s.duration_ms);
                start <= position
                    && end > position
                    && (end >= position.saturating_add(media_cache::HIGH_WATERMARK_MS)
                        || media_cache::covers_track_end(end, s.duration_ms, s.pcm.is_complete()))
            });
        self.queue_prefetch.update(queue, allowed, |track| {
            let media = resolve(track);
            let client = client.clone();
            let pieces = self.pieces.clone();
            async move {
                let track = media.await?;
                if let Some(d) = &track.local_audio {
                    let client = pieces.ok_or_else(|| anyhow!("room closed"))?;
                    let mut opening = Vec::new();
                    for i in 0..d.count().min(32) {
                        opening.extend(client.fetch(d, i, true).await?.bytes);
                    }
                    return Ok(prefetch::Opening::local(track, opening));
                }
                prefetch::Opening::fetch(client, track).await
            }
        });
    }

    pub(crate) fn clear_queue_prefetch(&mut self) {
        self.queue_prefetch = prefetch::QueuePrefetch::default();
    }

    pub fn set_volume(&mut self, percent: u8, _now_micros: i64) -> Result<()> {
        let gain = volume_percent_to_gain(percent);
        let old = f32::from_bits(self.volume.load(Ordering::Relaxed));
        if (old - gain).abs() < f32::EPSILON {
            return Ok(());
        }

        self.volume.store(gain.to_bits(), Ordering::Relaxed);
        Ok(())
    }

    pub fn stop(&mut self) {
        if let Some(sink) = self.sink.take() {
            sink.stop();
        }
        if let Some(streaming) = self.streaming.take() {
            streaming.cancel();
        }

        self.track_id = None;
        self.session_id = None;
        self.audio = None;
        self.position_ms = 0;
        self.started_at_micros = 0;
        self.playing = false;
    }

    pub fn load(
        &mut self,
        track_id: String,
        audio: Arc<[u8]>,
        position_ms: u64,
        playing: bool,
        now_micros: i64,
    ) -> Result<()> {
        if let Some(streaming) = self.streaming.take() {
            streaming.cancel();
        }
        let decoded = decode_audio(audio).context("failed to decode audio")?;
        self.track_id = Some(track_id);
        self.session_id = None;
        self.audio = Some(decoded);
        self.restart(position_ms, playing, now_micros)
    }

    pub fn prepare_stream(
        &mut self,
        client: &reqwest::Client,
        operation_id: Option<String>,
        session_id: String,
        track: PlaybackTrack,
        position_ms: u64,
    ) -> Result<()> {
        let media = std::future::ready(Ok(track.clone()));
        self.prepare_resolved_stream(client, operation_id, session_id, track, position_ms, media)
    }

    pub(crate) fn prepare_resolved_stream(
        &mut self,
        client: &reqwest::Client,
        operation_id: Option<String>,
        session_id: String,
        track: PlaybackTrack,
        position_ms: u64,
        media: impl std::future::Future<Output = Result<PlaybackTrack>> + Send + 'static,
    ) -> Result<()> {
        self.queue_prefetch.pause();
        let position_ms = position_ms.min(track.duration_ms);
        if let Some(session) = self.streaming.as_mut().filter(|session| {
            session.session_id == session_id && session.track_id == track.track_id
        }) {
            if let Some(sink) = self.sink.take() {
                sink.stop();
            }
            self.track_id = Some(track.track_id.clone());
            self.session_id = Some(session_id.clone());
            self.position_ms = position_ms;
            self.started_at_micros = 0;
            self.playing = false;
            session.position_ms.store(position_ms, Ordering::Relaxed);

            let (buffered_from_ms, buffered_until_ms) =
                stream_pcm_cache_window(&session.pcm, track.duration_ms);
            if stream_pcm_ready_for_position(&session.pcm, position_ms, track.duration_ms) {
                let _ = self.event_tx.send(AudioPlayerEvent::Prepared {
                    operation_id: operation_id.clone(),
                    session_id: session_id.clone(),
                    track_id: track.track_id.clone(),
                    buffered_until_ms,
                });
                let _ = self.event_tx.send(AudioPlayerEvent::Cache {
                    operation_id: operation_id.clone(),
                    decoder_finished: session.pcm.is_complete(),
                    view: PlaybackCacheView {
                        session_id,
                        track_id: track.track_id,
                        status: PlaybackCacheStatus::Ready,
                        buffered_from_ms,
                        buffered_until_ms,
                        duration_ms: track.duration_ms,
                        error: None,
                    },
                });
                return Ok(());
            }

            if session.decoder_operation != operation_id
                || position_ms < session.pcm.base_position_ms()
                || position_ms > buffered_until_ms
            {
                session.restart_decoder(
                    operation_id.clone(),
                    session_id.clone(),
                    track.clone(),
                    position_ms,
                    self.event_tx.clone(),
                );
            }

            let (buffered_from_ms, buffered_until_ms) =
                stream_pcm_cache_window(&session.pcm, track.duration_ms);
            let _ = self.event_tx.send(AudioPlayerEvent::Cache {
                operation_id,
                decoder_finished: false,
                view: PlaybackCacheView {
                    session_id,
                    track_id: track.track_id,
                    status: PlaybackCacheStatus::Buffering,
                    buffered_from_ms,
                    buffered_until_ms,
                    duration_ms: track.duration_ms,
                    error: None,
                },
            });
            return Ok(());
        }

        if let Some(sink) = self.sink.take() {
            sink.stop();
        }
        if let Some(streaming) = self.streaming.take() {
            streaming.cancel();
        }

        self.track_id = Some(track.track_id.clone());
        self.session_id = Some(session_id.clone());
        self.audio = None;
        self.position_ms = position_ms;
        self.started_at_micros = 0;
        self.playing = false;

        let opening = if position_ms == 0 {
            self.queue_prefetch.take(&track)
        } else {
            None
        };
        let bytes = SharedBytes::new();
        if let Some(d) = &track.local_audio {
            bytes.inner.0.lock().unwrap().local = true;
            bytes.set_total(Some(d.info.length));
        }
        let pieces = self.pieces.clone();
        let warmed_media = opening.map(|opening| opening.install(&bytes));
        let pcm = SharedPcm::new();
        pcm.set_base_position(position_ms);
        let decoder = DecodeToken::new();
        let position = Arc::new(AtomicU64::new(self.position_ms));
        let session = StreamingSession {
            decoder_operation: operation_id.clone(),
            session_id: session_id.clone(),
            track_id: track.track_id.clone(),
            track: track.clone(),
            duration_ms: track.duration_ms,
            pcm: pcm.clone(),
            bytes: bytes.clone(),
            decoder: decoder.clone(),
            position_ms: Arc::clone(&position),
        };
        self.streaming = Some(session);

        let event_tx = self.event_tx.clone();
        let download_client = client.clone();
        let download_track = track.clone();
        // Local resolution can select DASH even when the requester received progressive video.
        let decode_track = warmed_media.as_ref().cloned().unwrap_or(track);
        let download_bytes = bytes.clone();
        let download_fail_bytes = bytes.clone();
        let download_operation_id = operation_id.clone();
        let download_session_id = session_id.clone();
        let download_position_ms = position_ms;
        let download_position = Arc::clone(&position);
        tokio::spawn(async move {
            let result = async {
                let playable = match warmed_media {
                    Some(playable) => playable,
                    None => media.await?,
                };
                if download_bytes.is_canceled() {
                    return Ok(());
                }
                if let Some(d) = playable.local_audio {
                    return download_local_bytes(
                        pieces.ok_or_else(|| anyhow!("no room media service"))?,
                        d,
                        download_bytes,
                    )
                    .await;
                }
                download_streaming_bytes(
                    download_client,
                    playable,
                    download_position_ms,
                    download_position,
                    download_bytes,
                    event_tx.clone(),
                    download_operation_id.clone(),
                    download_session_id.clone(),
                )
                .await
            }
            .await;
            if let Err(err) = result {
                if download_fail_bytes.is_canceled() {
                    return;
                }
                let message = format!("{err:#}");
                download_fail_bytes.fail(message.clone());
                let _ = event_tx.send(AudioPlayerEvent::Failed {
                    operation_id: download_operation_id.clone(),
                    session_id: download_session_id.clone(),
                    track_id: download_track.track_id.clone(),
                    title: download_track.title.clone(),
                    error: message.clone(),
                });
            }
        });

        let decode_tx = self.event_tx.clone();
        let decode_bytes = bytes;
        let decode_pcm = pcm;
        let decode_token = decoder;
        let decode_position = Arc::clone(&position);
        tokio::task::spawn_blocking(move || {
            decode_streaming_audio(
                decode_bytes,
                decode_pcm,
                decode_token,
                decode_tx,
                operation_id,
                session_id,
                decode_track,
                position_ms,
                decode_position,
            );
        });

        Ok(())
    }

    pub fn drain_events(&mut self) -> Vec<AudioPlayerEvent> {
        let mut events = Vec::new();
        if let Some(change) = self
            .device_watcher
            .poll(self.playing, self.preferred_device.as_deref())
        {
            events.push(AudioPlayerEvent::OutputDeviceChanged {
                previous: change.previous,
                current: change.current,
            });
        }
        while let Ok(event) = self.event_rx.try_recv() {
            events.push(event);
        }
        events
    }

    pub fn seek(&mut self, position_ms: u64, playing: bool, now_micros: i64) -> Result<()> {
        if self.streaming.is_some() {
            let position_ms = position_ms.min(
                self.streaming
                    .as_ref()
                    .map(|streaming| streaming.duration_ms)
                    .unwrap_or(position_ms),
            );
            let streaming = self.streaming.as_ref().unwrap();
            let previous_target = streaming.position_ms.swap(position_ms, Ordering::Relaxed);
            let ready =
                stream_pcm_ready_for_position(&streaming.pcm, position_ms, streaming.duration_ms);
            // A follower catching up after quorum commit receives a moving clock target
            // every tick. Let the same decoder reach it while its timeline advances.
            let catching_up = !self.playing
                && streaming.track.local_audio.is_some()
                && (previous_target..=previous_target.saturating_add(media_cache::READY_WINDOW_MS))
                    .contains(&position_ms);
            self.position_ms = position_ms;
            if ready {
                return self.restart_streaming(position_ms, playing);
            }
            if let Some(sink) = self.sink.take() {
                sink.stop();
            }

            let cache_view = self.streaming.as_mut().map(|streaming| {
                if !catching_up
                    && (position_ms < streaming.pcm.base_position_ms()
                        || position_ms
                            > streaming.pcm.buffered_until_ms().min(streaming.duration_ms))
                {
                    streaming.restart_decoder(
                        None,
                        streaming.session_id.clone(),
                        streaming.track.clone(),
                        position_ms,
                        self.event_tx.clone(),
                    );
                }
                let (buffered_from_ms, buffered_until_ms) =
                    stream_pcm_cache_window(&streaming.pcm, streaming.duration_ms);
                PlaybackCacheView {
                    session_id: streaming.session_id.clone(),
                    track_id: streaming.track_id.clone(),
                    status: PlaybackCacheStatus::Buffering,
                    buffered_from_ms,
                    buffered_until_ms,
                    duration_ms: streaming.duration_ms,
                    error: None,
                }
            });
            self.playing = false;
            if let Some(view) = cache_view {
                let _ = self.event_tx.send(AudioPlayerEvent::Cache {
                    operation_id: None,
                    decoder_finished: false,
                    view,
                });
            }
            return Ok(());
        }
        self.restart(position_ms, playing, now_micros)
    }

    pub fn start_prepared_stream(
        &mut self,
        session_id: &str,
        track_id: &str,
        position_ms: u64,
        playing: bool,
    ) -> Result<bool> {
        let ready = self.streaming.as_ref().is_some_and(|streaming| {
            streaming.session_id == session_id
                && streaming.track_id == track_id
                && stream_pcm_ready_for_position(
                    &streaming.pcm,
                    position_ms.min(streaming.duration_ms),
                    streaming.duration_ms,
                )
        });
        if !ready {
            return Ok(false);
        }

        self.restart_streaming(position_ms, playing)?;
        Ok(true)
    }

    pub fn set_playing(&mut self, playing: bool, now_micros: i64) -> Result<()> {
        if self.streaming.is_some() {
            self.playing = playing;
            if let Some(sink) = &self.sink {
                if playing {
                    sink.play();
                } else {
                    sink.pause();
                }
            } else if playing {
                let position_ms = self.position_ms(now_micros);
                if self.streaming.as_ref().is_some_and(|session| {
                    stream_pcm_ready_for_position(&session.pcm, position_ms, session.duration_ms)
                }) {
                    self.restart_streaming(position_ms, true)?;
                } else if let Some(session) = &self.streaming {
                    self.playing = false;
                    let (buffered_from_ms, buffered_until_ms) =
                        stream_pcm_cache_window(&session.pcm, session.duration_ms);
                    let _ = self.event_tx.send(AudioPlayerEvent::Cache {
                        operation_id: None,
                        decoder_finished: false,
                        view: PlaybackCacheView {
                            session_id: session.session_id.clone(),
                            track_id: session.track_id.clone(),
                            status: PlaybackCacheStatus::Buffering,
                            buffered_from_ms,
                            buffered_until_ms,
                            duration_ms: session.duration_ms,
                            error: None,
                        },
                    });
                }
            }
            return Ok(());
        }

        let current_position = self.position_ms(now_micros);
        let playing = self
            .audio
            .as_ref()
            .is_some_and(|audio| playing && current_position < audio.duration_ms());
        self.position_ms = current_position;
        self.started_at_micros = now_micros;
        self.playing = playing;

        if playing && self.sink.as_ref().is_some_and(Player::empty) {
            if let Some(sink) = self.sink.take() {
                sink.stop();
            }
            return self.restart(current_position, playing, now_micros);
        }

        if playing && self.sink.is_none() && self.audio.is_some() {
            return self.restart(current_position, playing, now_micros);
        }

        if let Some(sink) = &self.sink {
            if playing {
                sink.play();
            } else {
                sink.pause();
            }
        }

        Ok(())
    }

    pub fn recover_output_device(&mut self, now_micros: i64) -> Result<bool> {
        self.switch_output(self.preferred_device.clone(), true, now_micros)
    }

    pub(crate) fn switch_output(
        &mut self,
        device: Option<String>,
        allow_missing: bool,
        now_micros: i64,
    ) -> Result<bool> {
        // Open first so a failed selection leaves the current stream playing.
        let (stream, device_id) =
            open_output(device.as_deref(), allow_missing, self.event_tx.clone())?;
        let position_ms = self.position_ms(now_micros);
        let playing = self.playing;

        if let Some(sink) = self.sink.take() {
            sink.stop();
        }

        self.stream = stream;
        self.preferred_device = device;
        self.device_watcher.reset(device_id, Instant::now());

        if self.streaming.is_some() {
            if self.streaming.as_ref().is_some_and(|session| {
                stream_pcm_ready_for_position(&session.pcm, position_ms, session.duration_ms)
            }) {
                self.restart_streaming(position_ms, playing)?;
                return Ok(true);
            }
            self.position_ms = position_ms;
            self.playing = false;
            return Ok(false);
        }

        if self.audio.is_some() {
            self.restart(position_ms, playing, now_micros)?;
            return Ok(true);
        }

        self.playing = false;
        Ok(false)
    }

    fn restart_streaming(&mut self, position_ms: u64, playing: bool) -> Result<()> {
        let Some(streaming) = &self.streaming else {
            return Err(anyhow!("no streaming audio loaded"));
        };
        let (channels, sample_rate) = streaming
            .pcm
            .spec()
            .ok_or_else(|| anyhow!("streaming audio is not ready"))?;
        let position_ms = position_ms.min(streaming.duration_ms);

        if let Some(old_sink) = self.sink.take() {
            old_sink.stop();
        }

        streaming.position_ms.store(position_ms, Ordering::Relaxed);
        let source = StreamingPcmSource::new(
            streaming.pcm.clone(),
            position_ms,
            channels,
            sample_rate,
            streaming.pcm.base_position_ms(),
            Arc::clone(&self.volume),
            Arc::clone(&streaming.position_ms),
            self.event_tx.clone(),
            streaming.session_id.clone(),
            streaming.track_id.clone(),
            streaming.duration_ms,
        );
        let sink = Player::connect_new(self.stream.mixer());
        sink.append(source);
        if playing {
            sink.play();
        } else {
            sink.pause();
        }

        self.sink = Some(sink);
        self.track_id = Some(streaming.track_id.clone());
        self.session_id = Some(streaming.session_id.clone());
        self.position_ms = position_ms;
        self.playing = playing && position_ms < streaming.duration_ms;
        Ok(())
    }

    fn restart(&mut self, position_ms: u64, playing: bool, now_micros: i64) -> Result<()> {
        let Some(audio) = self.audio.clone() else {
            return Err(anyhow!("no audio loaded"));
        };
        let position_ms = audio.clamp_position_ms(position_ms);
        let playing = playing && position_ms < audio.duration_ms();

        if let Some(old_sink) = self.sink.take() {
            old_sink.stop();
        }

        let sink = match self.build_sink(audio.clone(), position_ms, playing) {
            Ok(sink) => sink,
            Err(first_err) => {
                self.reopen_output_device().with_context(|| {
                    format!("failed to reopen default audio output after sink failure: {first_err}")
                })?;
                self.build_sink(audio, position_ms, playing)
                    .context("failed to create audio sink after reopening output device")?
            }
        };

        self.sink = Some(sink);
        self.position_ms = position_ms;
        self.started_at_micros = now_micros;
        self.playing = playing;
        Ok(())
    }

    fn build_sink(&self, audio: DecodedAudio, position_ms: u64, playing: bool) -> Result<Player> {
        let source = PcmSource::new(audio, position_ms, Arc::clone(&self.volume));
        let sink = Player::connect_new(self.stream.mixer());
        sink.append(source);

        if playing {
            sink.play();
        } else {
            sink.pause();
        }

        Ok(sink)
    }

    fn reopen_output_device(&mut self) -> Result<()> {
        let (stream, device_id) = open_output(
            self.preferred_device.as_deref(),
            true,
            self.event_tx.clone(),
        )?;
        self.stream = stream;
        self.device_watcher.reset(device_id, Instant::now());
        Ok(())
    }
}

pub(crate) struct OutputSettings {
    pub device: Option<String>,
    pub volume: u8,
}

impl Default for OutputSettings {
    fn default() -> Self {
        Self {
            device: None,
            volume: 100,
        }
    }
}

#[derive(serde::Serialize)]
pub struct OutputDevice {
    id: String,
    name: String,
    is_default: bool,
}

pub fn list_output_devices() -> Result<Vec<OutputDevice>> {
    let default_id = default_output_device_id();
    Ok(cpal::default_host()
        .output_devices()?
        .filter_map(|d| {
            let id = d.id().ok()?.to_string();
            let name = d.description().ok()?.name().to_owned();
            Some(OutputDevice {
                is_default: Some(&id) == default_id.as_ref(),
                id,
                name,
            })
        })
        .collect())
}

fn find_device(id: &str) -> Option<cpal::Device> {
    cpal::default_host().device_by_id(&id.parse().ok()?)
}

fn open_output(
    preferred: Option<&str>,
    allow_missing: bool,
    event_tx: mpsc::UnboundedSender<AudioPlayerEvent>,
) -> Result<(MixerDeviceSink, Option<String>)> {
    let callback = move |err: cpal::StreamError| {
        let _ = event_tx.send(AudioPlayerEvent::OutputDeviceError {
            error: err.to_string(),
        });
    };
    if let Some(id) = preferred {
        if let Some(device) = find_device(id) {
            let stream = DeviceSinkBuilder::from_device(device)?
                .with_error_callback(callback)
                .open_sink_or_fallback()
                .context("failed to open selected audio output")?;
            return Ok((stream, Some(id.to_owned())));
        }
        if !allow_missing {
            bail!("Selected output device is no longer connected");
        }
    }
    let stream = open_default_sink_with_callback(callback).with_context(|| {
        format!(
            "failed to open default audio output ({})",
            default_output_diagnostics()
        )
    })?;
    Ok((stream, default_output_device_id()))
}

fn open_default_sink_with_callback<E>(callback: E) -> Result<MixerDeviceSink, DeviceSinkError>
where
    E: FnMut(cpal::StreamError) + Clone + Send + 'static,
{
    let original_error = match DeviceSinkBuilder::from_default_device() {
        Ok(builder) => match builder
            .with_error_callback(callback.clone())
            .open_sink_or_fallback()
        {
            Ok(sink) => return Ok(sink),
            Err(err) => err,
        },
        Err(err) => err,
    };

    let devices = match cpal::default_host().output_devices() {
        Ok(devices) => devices,
        Err(_) => return Err(original_error),
    };

    for device in devices.filter(|device| {
        device
            .description()
            .map(|description| description.driver().is_some_and(|driver| driver != "null"))
            .unwrap_or(false)
    }) {
        if let Ok(sink) = DeviceSinkBuilder::from_device(device)
            .map(|builder| builder.with_error_callback(callback.clone()))
            .and_then(|builder| builder.open_sink_or_fallback())
        {
            return Ok(sink);
        }
    }

    Err(original_error)
}

fn default_output_device_id() -> Option<String> {
    cpal::default_host()
        .default_output_device()
        .and_then(|device| device.id().ok())
        .map(|id| id.to_string())
}

fn default_output_diagnostics() -> String {
    let host = cpal::default_host();
    let host_id = format!("{:?}", host.id());
    let Some(device) = host.default_output_device() else {
        return format!("host={host_id}, default_device=none");
    };

    let description = device
        .description()
        .map(|description| format!("{description:?}"))
        .unwrap_or_else(|err| format!("unknown ({err})"));
    let id = device
        .id()
        .map(|id| id.to_string())
        .unwrap_or_else(|err| format!("unknown ({err})"));
    format!("host={host_id}, default_device={description}, device_id={id}")
}

impl AudioDeviceWatcher {
    const PLAYING_INTERVAL: Duration = Duration::from_secs(1);
    const IDLE_INTERVAL: Duration = Duration::from_secs(5);

    fn new(known_device_id: Option<String>) -> Self {
        Self {
            known_device_id,
            last_checked: Instant::now(),
        }
    }

    fn poll(&mut self, playing: bool, preferred: Option<&str>) -> Option<OutputDeviceChange> {
        let now = Instant::now();
        let interval = if playing {
            Self::PLAYING_INTERVAL
        } else {
            Self::IDLE_INTERVAL
        };
        if now.saturating_duration_since(self.last_checked) < interval {
            return None;
        }
        self.last_checked = now;
        let current = preferred
            .filter(|id| find_device(id).is_some())
            .map(str::to_owned)
            .or_else(default_output_device_id);
        self.observe(current)
    }

    fn reset(&mut self, known_device_id: Option<String>, now: Instant) {
        self.known_device_id = known_device_id;
        self.last_checked = now;
    }

    fn observe(&mut self, current: Option<String>) -> Option<OutputDeviceChange> {
        output_device_change(&mut self.known_device_id, current)
    }
}

fn output_device_change(
    previous: &mut Option<String>,
    current: Option<String>,
) -> Option<OutputDeviceChange> {
    if *previous == current {
        return None;
    }
    let change = OutputDeviceChange {
        previous: previous.clone(),
        current: current.clone(),
    };
    *previous = current;
    Some(change)
}

pub fn volume_percent_to_gain(percent: u8) -> f32 {
    (percent.min(100) as f32) / 100.0
}

fn stream_ready_for_position(
    position_ms: u64,
    duration_ms: u64,
    buffered_until_ms: u64,
    complete: bool,
) -> bool {
    let position_ms = position_ms.min(duration_ms);
    let ready_until = position_ms
        .saturating_add(media_cache::READY_WINDOW_MS)
        .min(duration_ms);
    buffered_until_ms >= ready_until
        || (complete && buffered_until_ms >= position_ms)
        || media_cache::covers_track_end(buffered_until_ms, duration_ms, complete)
}

fn stream_pcm_ready_for_position(pcm: &SharedPcm, position_ms: u64, duration_ms: u64) -> bool {
    position_ms >= pcm.base_position_ms()
        && pcm.spec().is_some()
        && stream_ready_for_position(
            position_ms,
            duration_ms,
            pcm.buffered_until_ms().min(duration_ms),
            pcm.is_complete(),
        )
}

fn stream_pcm_cache_window(pcm: &SharedPcm, duration_ms: u64) -> (u64, u64) {
    let buffered_from_ms = pcm.base_position_ms().min(duration_ms);
    let buffered_until_ms = pcm
        .buffered_until_ms()
        .min(duration_ms)
        .max(buffered_from_ms);
    (buffered_from_ms, buffered_until_ms)
}

fn should_emit_stream_cache_event(
    buffered_until_ms: u64,
    last_cache_event_ms: u64,
    ready_sent: bool,
    ready_until_ms: u64,
) -> bool {
    buffered_until_ms >= last_cache_event_ms.saturating_add(1000)
        || (!ready_sent && buffered_until_ms >= ready_until_ms)
}

fn stream_byte_offset_for_position(position_ms: u64, duration_ms: u64, total_bytes: u64) -> u64 {
    if duration_ms == 0 || total_bytes == 0 {
        return 0;
    }

    position_ms
        .min(duration_ms)
        .saturating_mul(total_bytes)
        .saturating_div(duration_ms)
        .min(total_bytes.saturating_sub(1))
}

fn stream_byte_range_for_window(
    position_ms: u64,
    window_ms: u64,
    duration_ms: u64,
    total_bytes: u64,
) -> media_cache::ByteRange {
    if total_bytes == 0 {
        return media_cache::ByteRange {
            start: 0,
            end_inclusive: 0,
        };
    }

    let start = stream_byte_offset_for_position(position_ms, duration_ms, total_bytes);
    let end_position_ms = position_ms.saturating_add(window_ms).min(duration_ms);
    let end = stream_byte_offset_for_position(end_position_ms, duration_ms, total_bytes)
        .saturating_add(256 * 1024)
        .min(start.saturating_add(4 * media_cache::STREAM_CHUNK_BYTES - 1))
        .min(total_bytes.saturating_sub(1));
    media_cache::ByteRange {
        start,
        end_inclusive: end.max(start),
    }
}

fn align_stream_offset(offset: u64) -> u64 {
    offset - (offset % media_cache::STREAM_CHUNK_BYTES)
}

fn stream_fetch_range(start: u64, total_bytes: u64) -> Result<media_cache::ByteRange> {
    let start = align_stream_offset(start.min(total_bytes.saturating_sub(1)));
    let end = start
        .saturating_add(media_cache::STREAM_CHUNK_BYTES)
        .saturating_sub(1)
        .min(total_bytes.saturating_sub(1));
    media_cache::ByteRange::new(start, end)
}

fn media_range_retry_delay(
    error: &media_cache::MediaRangeError,
    attempt: usize,
) -> Option<Duration> {
    if let Some(status) = error.status() {
        if status == reqwest::StatusCode::FORBIDDEN
            || status == reqwest::StatusCode::TOO_MANY_REQUESTS
        {
            return (attempt == 0).then(|| Duration::from_millis(STREAM_THROTTLE_RETRY_DELAY_MS));
        }

        if status.is_server_error() {
            return (attempt + 1 < MEDIA_RANGE_FETCH_ATTEMPTS)
                .then(|| transient_range_retry_delay(attempt));
        }

        return None;
    }

    match error {
        media_cache::MediaRangeError::Request { .. }
        | media_cache::MediaRangeError::Read { .. } => {
            (attempt + 1 < MEDIA_RANGE_FETCH_ATTEMPTS).then(|| transient_range_retry_delay(attempt))
        }
        media_cache::MediaRangeError::UnsupportedRange { .. }
        | media_cache::MediaRangeError::UnexpectedLength { .. }
        | media_cache::MediaRangeError::HttpStatus { .. } => None,
    }
}

fn transient_range_retry_delay(attempt: usize) -> Duration {
    let multiplier = 1_u64 << attempt.min(4);
    Duration::from_millis(STREAM_TRANSIENT_RETRY_BASE_MS.saturating_mul(multiplier))
}

fn describe_retry_delay(delay: Duration) -> String {
    if delay.as_secs() > 0 && delay.subsec_millis() == 0 {
        format!("{}s", delay.as_secs())
    } else {
        format!("{}ms", delay.as_millis())
    }
}

fn should_full_fetch_missing_metadata(
    metadata_ranges: &[media_cache::ByteRange],
    total_bytes: u64,
) -> bool {
    metadata_ranges.is_empty()
        && total_bytes > 0
        && total_bytes <= STREAM_SMALL_FULL_FETCH_LIMIT_BYTES
}

fn stream_metadata_prefetch_ranges(
    metadata_ranges: &[media_cache::ByteRange],
    total_bytes: u64,
) -> Result<Vec<media_cache::ByteRange>> {
    if total_bytes == 0 {
        return Ok(Vec::new());
    }
    if !metadata_ranges.is_empty() {
        return Ok(metadata_ranges.to_vec());
    }

    let mut ranges = Vec::with_capacity(3);
    push_unique_stream_range(&mut ranges, stream_fetch_range(0, total_bytes)?);
    if total_bytes > media_cache::STREAM_CHUNK_BYTES {
        let final_chunk_start = align_stream_offset(total_bytes.saturating_sub(1));
        if final_chunk_start > 0 {
            push_unique_stream_range(
                &mut ranges,
                stream_fetch_range(
                    final_chunk_start.saturating_sub(media_cache::STREAM_CHUNK_BYTES),
                    total_bytes,
                )?,
            );
        }
        push_unique_stream_range(
            &mut ranges,
            stream_fetch_range(final_chunk_start, total_bytes)?,
        );
    }
    Ok(ranges)
}

fn push_unique_stream_range(
    ranges: &mut Vec<media_cache::ByteRange>,
    range: media_cache::ByteRange,
) {
    if !ranges.contains(&range) {
        ranges.push(range);
    }
}

fn retain_streaming_byte_window(
    shared: &SharedBytes,
    position_ms: u64,
    duration_ms: u64,
    total_bytes: u64,
) {
    if total_bytes == 0 {
        return;
    }

    let anchor = stream_byte_offset_for_position(position_ms, duration_ms, total_bytes);
    let target = stream_byte_range_for_window(
        position_ms,
        STREAM_BUFFER_TARGET_MS,
        duration_ms,
        total_bytes,
    );
    let retain_start = anchor.saturating_sub(STREAM_BYTE_RETAIN_BEHIND_BYTES);
    let retain_end = target
        .end_inclusive
        .saturating_add(media_cache::STREAM_CHUNK_BYTES)
        .min(total_bytes.saturating_sub(1));
    shared.retain_window(retain_start, retain_end);
}

fn ranges_overlap(left: media_cache::ByteRange, right: media_cache::ByteRange) -> bool {
    left.start <= right.end_inclusive && right.start <= left.end_inclusive
}

fn is_audio_track(track: &Track) -> bool {
    track.codec_params.codec != CODEC_TYPE_NULL
        && (track.codec_params.channels.is_some() || track.codec_params.sample_rate.is_some())
}

fn make_audio_decoder(
    codec_params: &CodecParameters,
    decoder_opts: &DecoderOptions,
) -> symphonia::core::errors::Result<Box<dyn Decoder>> {
    codec_registry().make(codec_params, decoder_opts)
}

fn decoded_to_interleaved_f32(decoded: symphonia::core::audio::AudioBufferRef<'_>) -> Vec<f32> {
    let spec = *decoded.spec();
    let mut sample_buffer = SampleBuffer::<f32>::new(decoded.capacity() as u64, spec);
    sample_buffer.copy_interleaved_ref(decoded);
    sample_buffer.samples().to_vec()
}

#[cfg(not(feature = "fdk-aac-decoder"))]
fn codec_registry() -> &'static CodecRegistry {
    get_codecs()
}

#[cfg(feature = "fdk-aac-decoder")]
fn codec_registry() -> &'static CodecRegistry {
    use std::sync::OnceLock;

    static REGISTRY: OnceLock<CodecRegistry> = OnceLock::new();
    REGISTRY.get_or_init(|| {
        let mut registry = CodecRegistry::new();
        use symphonia::default::codecs::{FlacDecoder, MpaDecoder, PcmDecoder};
        registry.register_all::<MpaDecoder>();
        registry.register_all::<FlacDecoder>();
        registry.register_all::<PcmDecoder>();
        registry.register_all::<symphonia_adapter_fdk_aac::AacDecoder>();
        registry
    })
}

#[cfg(all(test, not(feature = "fdk-aac-decoder")))]
fn decoder_backend_name() -> &'static str {
    "symphonia-native-aac"
}

#[cfg(all(test, feature = "fdk-aac-decoder"))]
fn decoder_backend_name() -> &'static str {
    "fdk-aac"
}

impl DecodedAudio {
    fn duration_ms(&self) -> u64 {
        (self.samples.len() as u64)
            .saturating_mul(1000)
            .saturating_div(self.sample_rate.get() as u64)
            .saturating_div(self.channels.get() as u64)
    }

    fn clamp_position_ms(&self, position_ms: u64) -> u64 {
        position_ms.min(self.duration_ms())
    }

    fn position_to_sample_index(&self, position_ms: u64) -> usize {
        let frames = (position_ms as u128)
            .saturating_mul(self.sample_rate.get() as u128)
            .saturating_div(1000);
        let samples = frames
            .saturating_mul(self.channels.get() as u128)
            .min(self.samples.len() as u128) as usize;
        samples - samples % self.channels.get() as usize
    }
}

impl PcmSource {
    fn new(audio: DecodedAudio, position_ms: u64, volume: Arc<AtomicU32>) -> Self {
        let pos = audio.position_to_sample_index(position_ms);
        Self { audio, pos, volume }
    }
}

impl StreamingPcmSource {
    fn new(
        shared: SharedPcm,
        position_ms: u64,
        channels: NonZeroU16,
        sample_rate: NonZeroU32,
        base_position_ms: u64,
        volume: Arc<AtomicU32>,
        position: Arc<AtomicU64>,
        event_tx: mpsc::UnboundedSender<AudioPlayerEvent>,
        session_id: String,
        track_id: String,
        duration_ms: u64,
    ) -> Self {
        let frames = (position_ms as u128)
            .saturating_sub(base_position_ms as u128)
            .saturating_mul(sample_rate.get() as u128)
            .saturating_div(1000);
        let pos = frames
            .saturating_mul(channels.get() as u128)
            .min(usize::MAX as u128) as usize;
        let dropped_samples = shared.dropped_samples();
        Self {
            shared,
            pos: pos - pos % channels.get() as usize,
            channels,
            sample_rate,
            base_position_ms,
            dropped_samples,
            volume,
            position_ms: position,
            event_tx,
            session_id,
            track_id,
            duration_ms,
            last_underrun_at: None,
        }
    }

    fn update_position(&self) {
        let frame = self.pos / self.channels.get() as usize;
        let position_ms = (frame as u128)
            .saturating_mul(1000)
            .saturating_div(self.sample_rate.get() as u128) as u64;
        self.position_ms.store(
            self.base_position_ms
                .saturating_add(position_ms)
                .min(self.duration_ms),
            Ordering::Relaxed,
        );
    }

    fn sync_shared_base_position(&mut self) {
        let (next_base_position_ms, next_dropped_samples) = self.shared.window_start();
        if next_dropped_samples <= self.dropped_samples
            && next_base_position_ms <= self.base_position_ms
        {
            return;
        }

        let dropped_samples = next_dropped_samples.saturating_sub(self.dropped_samples);
        self.pos = self
            .pos
            .saturating_sub(dropped_samples.min(usize::MAX as u64) as usize);
        self.pos -= self.pos % self.channels.get() as usize;
        self.base_position_ms = next_base_position_ms;
        self.dropped_samples = next_dropped_samples;
    }
}

impl Iterator for StreamingPcmSource {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            self.sync_shared_base_position();
            match self.shared.sample(self.pos) {
                PcmSample::Ready(sample) => {
                    self.pos += 1;
                    self.update_position();
                    let volume = f32::from_bits(self.volume.load(Ordering::Relaxed));
                    return Some(sample * volume);
                }
                PcmSample::Finished => {
                    let _ = self.event_tx.send(AudioPlayerEvent::Ended {
                        session_id: self.session_id.clone(),
                        track_id: self.track_id.clone(),
                    });
                    return None;
                }
                PcmSample::Failed(error) => {
                    let _ = self.event_tx.send(AudioPlayerEvent::Failed {
                        operation_id: None,
                        session_id: self.session_id.clone(),
                        track_id: self.track_id.clone(),
                        title: self.track_id.clone(),
                        error,
                    });
                    return None;
                }
                PcmSample::Canceled => return None,
                PcmSample::Waiting => {
                    let now = Instant::now();
                    if self.last_underrun_at.is_none_or(|last| {
                        now.saturating_duration_since(last) >= Duration::from_secs(1)
                    }) {
                        self.last_underrun_at = Some(now);
                        let (buffered_from_ms, buffered_until_ms) =
                            stream_pcm_cache_window(&self.shared, self.duration_ms);
                        let _ = self.event_tx.send(AudioPlayerEvent::Buffering {
                            operation_id: None,
                            session_id: self.session_id.clone(),
                            track_id: self.track_id.clone(),
                            buffered_from_ms,
                            buffered_until_ms,
                        });
                    }
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl Source for StreamingPcmSource {
    fn current_span_len(&self) -> Option<usize> {
        None
    }

    fn channels(&self) -> NonZeroU16 {
        self.channels
    }

    fn sample_rate(&self) -> NonZeroU32 {
        self.sample_rate
    }

    fn total_duration(&self) -> Option<Duration> {
        Some(Duration::from_millis(self.duration_ms))
    }
}

impl Iterator for PcmSource {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        let sample = *self.audio.samples.get(self.pos)?;
        self.pos += 1;
        let volume = f32::from_bits(self.volume.load(Ordering::Relaxed));
        Some(sample * volume)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.audio.samples.len().saturating_sub(self.pos);
        (remaining, Some(remaining))
    }
}

impl Source for PcmSource {
    fn current_span_len(&self) -> Option<usize> {
        Some(self.audio.samples.len().saturating_sub(self.pos))
    }

    fn channels(&self) -> NonZeroU16 {
        self.audio.channels
    }

    fn sample_rate(&self) -> NonZeroU32 {
        self.audio.sample_rate
    }

    fn total_duration(&self) -> Option<Duration> {
        Some(Duration::from_millis(self.audio.duration_ms()))
    }
}

impl MemoryMediaSource {
    fn new(audio: Arc<[u8]>) -> Self {
        let byte_len = audio.len() as u64;
        Self {
            cursor: Cursor::new(audio),
            byte_len,
        }
    }
}

impl Read for MemoryMediaSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.cursor.read(buf)
    }
}

impl Seek for MemoryMediaSource {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.cursor.seek(pos)
    }
}

impl MediaSource for MemoryMediaSource {
    fn is_seekable(&self) -> bool {
        true
    }

    fn byte_len(&self) -> Option<u64> {
        Some(self.byte_len)
    }
}

impl StreamingMediaSource {
    fn new(shared: SharedBytes, decoder: DecodeToken) -> Self {
        Self {
            shared,
            decoder,
            cursor: 0,
            splice: None,
            ready_through: None,
            seekable: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl Read for StreamingMediaSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let (offset, max) = match self.splice {
            Some((header, frame)) if self.cursor >= header => {
                (frame + self.cursor - header, buf.len())
            }
            Some((header, _)) => (self.cursor, buf.len().min((header - self.cursor) as usize)),
            None => (self.cursor, buf.len()),
        };
        let bytes = self.shared.read_for_decoder(
            offset,
            &mut buf[..max],
            &self.decoder,
            self.ready_through,
        )?;
        self.cursor = self.cursor.saturating_add(bytes as u64);
        Ok(bytes)
    }
}

impl Seek for StreamingMediaSource {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let next = match pos {
            SeekFrom::Start(pos) => pos,
            SeekFrom::Current(delta) => self.cursor.checked_add_signed(delta).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid seek")
            })?,
            SeekFrom::End(delta) => {
                let len = self.byte_len().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::Unsupported, "unknown stream length")
                })?;
                len.checked_add_signed(delta).ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid seek")
                })?
            }
        };
        self.cursor = next;
        Ok(self.cursor)
    }
}

impl MediaSource for StreamingMediaSource {
    fn is_seekable(&self) -> bool {
        self.seekable.load(Ordering::Relaxed)
    }

    fn byte_len(&self) -> Option<u64> {
        self.shared.total_bytes().map(|len| {
            self.splice
                .map_or(len, |(header, start)| len - start + header)
        })
    }
}

impl DecodeToken {
    fn new() -> Self {
        Self {
            canceled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn cancel(&self) {
        self.canceled.store(true, Ordering::Relaxed);
    }

    fn is_canceled(&self) -> bool {
        self.canceled.load(Ordering::Relaxed)
    }
}

impl StreamRequestPacer {
    fn new() -> Self {
        Self {
            last_started_at: None,
        }
    }

    async fn wait_for_turn(&mut self, shared: &SharedBytes) -> bool {
        if let Some(last_started_at) = self.last_started_at {
            let elapsed = last_started_at.elapsed();
            let spacing = Duration::from_millis(STREAM_RANGE_REQUEST_SPACING_MS);
            if elapsed < spacing
                && !sleep_streaming_download(shared, spacing.saturating_sub(elapsed)).await
            {
                return false;
            }
        }
        if shared.is_canceled() {
            return false;
        }
        self.last_started_at = Some(Instant::now());
        true
    }
}

fn normalized_stream_request_offset(offset: u64, total_bytes: Option<u64>) -> u64 {
    let offset = total_bytes.map_or(offset, |total| offset.min(total.saturating_sub(1)));
    align_stream_offset(offset)
}

fn queue_requested_offset(queue: &mut VecDeque<u64>, offset: u64, priority: bool) {
    if !priority && queue.contains(&offset) {
        return;
    }
    queue.retain(|queued| *queued != offset);
    if priority {
        queue.push_front(offset);
    } else {
        queue.push_back(offset);
    }
    queue.truncate(STREAM_MAX_PENDING_OFFSETS);
}

async fn sleep_streaming_download(shared: &SharedBytes, duration: Duration) -> bool {
    let deadline = Instant::now() + duration;
    loop {
        if shared.is_canceled() {
            return false;
        }
        let now = Instant::now();
        if now >= deadline {
            return true;
        }
        let remaining = deadline.saturating_duration_since(now);
        tokio::time::sleep(remaining.min(Duration::from_millis(STREAM_DOWNLOAD_IDLE_SLEEP_MS)))
            .await;
    }
}

impl StreamingBytesState {
    fn request_start(&self, offset: u64) -> u64 {
        if self.local {
            offset / (crate::local_audio::PIECE as u64) * (crate::local_audio::PIECE as u64)
        } else {
            normalized_stream_request_offset(offset, self.total_bytes)
        }
    }
}

async fn download_local_bytes(
    client: crate::local_audio::Client,
    d: crate::local_audio::Descriptor,
    bytes: SharedBytes,
) -> Result<()> {
    let piece = crate::local_audio::PIECE as u64;
    let mut requests = FuturesUnordered::new();
    let mut in_flight = HashSet::new();
    let mut generation = 0;
    let fetch = |start| {
        let request = client.fetch(&d, start / piece, false);
        async move { (start, request.await) }
    };
    loop {
        {
            let mut state = bytes.inner.0.lock().unwrap();
            if state.canceled {
                return Ok(());
            }
            if generation != state.generation {
                requests.clear();
                in_flight.clear();
                generation = state.generation;
            }
            while requests.len() < LOCAL_PIECES_IN_FLIGHT {
                let Some(start) = state.requested_offsets.pop_front() else {
                    break;
                };
                if !state.ranges.ranges().iter().any(|r| r.contains(start))
                    && in_flight.insert(start)
                {
                    requests.push(fetch(start));
                }
            }
        }
        let (start, result) = tokio::select! {
            Some(response) = requests.next(), if !requests.is_empty() => response,
            _ = bytes.demand_changed.notified() => continue,
        };
        in_flight.remove(&start);
        if bytes.inner.0.lock().unwrap().generation != generation {
            continue;
        }
        let data = result?.bytes;
        let end = start + data.len() as u64 - 1;
        bytes.append_chunk(StreamingByteChunk {
            range: media_cache::ByteRange {
                start,
                end_inclusive: end,
            },
            bytes: data.into(),
        });
        bytes.retain_window(start.saturating_sub(32 * piece), end + 32 * piece);
    }
}

impl SharedBytes {
    fn new() -> Self {
        Self {
            demand_changed: Default::default(),
            inner: Arc::new((
                Mutex::new(StreamingBytesState {
                    chunks: Vec::new(),
                    total_bytes: None,
                    canceled: false,
                    error: None,
                    ranges: media_cache::RangeIndex::default(),
                    pinned_ranges: Vec::new(),
                    requested_offsets: VecDeque::new(),
                    decoder_offset: None,
                    prepared_once: false,
                    local: false,
                    generation: 0,
                }),
                Condvar::new(),
            )),
        }
    }

    fn set_total(&self, total: Option<u64>) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        state.total_bytes = total;
        condvar.notify_all();
    }

    fn append(&self, bytes: &[u8], range: media_cache::ByteRange) {
        self.append_chunk(StreamingByteChunk {
            range,
            bytes: Arc::from(bytes),
        });
    }

    fn append_chunk(&self, chunk: StreamingByteChunk) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        let range = chunk.range;
        if !chunk.bytes.is_empty() && !state.ranges.covers(range) {
            state.chunks.push(chunk);
            state.chunks.sort_by_key(|chunk| chunk.range.start);
            state.ranges.insert(range);
        }
        let ranges = state.ranges.ranges().to_vec();
        state
            .requested_offsets
            .retain(|offset| !ranges.iter().any(|range| range.contains(*offset)));
        condvar.notify_all();
    }

    fn pin_ranges(&self, ranges: &[media_cache::ByteRange]) {
        if ranges.is_empty() {
            return;
        }

        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        for range in ranges {
            if !state.pinned_ranges.contains(range) {
                state.pinned_ranges.push(*range);
            }
        }
        condvar.notify_all();
    }

    fn retain_window(&self, retain_start: u64, retain_end: u64) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        let window = media_cache::ByteRange {
            start: retain_start,
            end_inclusive: retain_end.max(retain_start),
        };
        let pinned_ranges = state.pinned_ranges.clone();
        let decoder_offset = state.decoder_offset;
        state.chunks.retain(|chunk| {
            decoder_offset.is_some_and(|offset| chunk.range.contains(offset))
                || ranges_overlap(chunk.range, window)
                || pinned_ranges
                    .iter()
                    .any(|pinned| ranges_overlap(chunk.range, *pinned))
        });
        let mut ranges = media_cache::RangeIndex::default();
        for chunk in &state.chunks {
            ranges.insert(chunk.range);
        }
        state.ranges = ranges;
        condvar.notify_all();
    }

    fn fail(&self, error: String) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        state.error = Some(error);
        condvar.notify_all();
    }

    fn cancel(&self) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        state.canceled = true;
        condvar.notify_all();
        self.demand_changed.notify_one();
    }

    fn is_canceled(&self) -> bool {
        let (lock, _) = &*self.inner;
        lock.lock().expect("streaming bytes lock poisoned").canceled
    }

    fn prepared_once(&self) -> bool {
        self.inner
            .0
            .lock()
            .expect("streaming bytes lock poisoned")
            .prepared_once
    }

    fn mark_prepared(&self) {
        self.inner
            .0
            .lock()
            .expect("streaming bytes lock poisoned")
            .prepared_once = true;
    }

    fn total_bytes(&self) -> Option<u64> {
        let (lock, _) = &*self.inner;
        lock.lock()
            .expect("streaming bytes lock poisoned")
            .total_bytes
    }

    fn has_opening_index(&self) -> bool {
        let state = self.inner.0.lock().expect("streaming bytes lock poisoned");
        state
            .chunks
            .first()
            .is_some_and(|chunk| chunk.range.start == 0 && has_front_loaded_mp4_index(&chunk.bytes))
    }

    fn covers(&self, range: media_cache::ByteRange) -> bool {
        let (lock, _) = &*self.inner;
        lock.lock()
            .expect("streaming bytes lock poisoned")
            .ranges
            .covers(range)
    }

    fn request_offset(&self, offset: u64) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        let offset = state.request_start(offset);
        if !state
            .ranges
            .ranges()
            .iter()
            .any(|range| range.contains(offset))
        {
            queue_requested_offset(&mut state.requested_offsets, offset, false);
        }
        condvar.notify_all();
    }

    fn prioritize_offset(&self, offset: u64) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        let offset = state.request_start(offset);
        if !state
            .ranges
            .ranges()
            .iter()
            .any(|range| range.contains(offset))
        {
            queue_requested_offset(&mut state.requested_offsets, offset, true);
        }
        condvar.notify_all();
    }

    fn take_requested_offset(&self, total: u64) -> Option<u64> {
        let (lock, _) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        while let Some(offset) = state.requested_offsets.pop_front() {
            let offset = state.request_start(offset.min(total.saturating_sub(1)));
            if !state
                .ranges
                .ranges()
                .iter()
                .any(|range| range.contains(offset))
            {
                return Some(offset);
            }
        }
        None
    }

    fn first_missing_offset(&self, range: media_cache::ByteRange) -> Option<u64> {
        let (lock, _) = &*self.inner;
        let state = lock.lock().expect("streaming bytes lock poisoned");
        let mut cursor = range.start;
        for cached in state.ranges.ranges() {
            if cached.end_inclusive < cursor {
                continue;
            }
            if cached.start > cursor {
                return Some(cursor);
            }
            cursor = cached.end_inclusive.saturating_add(1);
            if cursor > range.end_inclusive {
                return None;
            }
        }
        Some(cursor).filter(|offset| *offset <= range.end_inclusive)
    }

    #[cfg(test)]
    fn in_memory_bytes(&self) -> u64 {
        let (lock, _) = &*self.inner;
        lock.lock()
            .expect("streaming bytes lock poisoned")
            .chunks
            .iter()
            .map(|chunk| chunk.bytes.len() as u64)
            .sum()
    }

    #[cfg(test)]
    fn read_at(
        &self,
        offset: u64,
        buf: &mut [u8],
        decoder: &DecodeToken,
    ) -> std::io::Result<usize> {
        self.read_for_decoder(offset, buf, decoder, None)
    }

    fn read_for_decoder(
        &self,
        offset: u64,
        buf: &mut [u8],
        decoder: &DecodeToken,
        ready_through: Option<u64>,
    ) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        state.decoder_offset = Some(offset);
        loop {
            if state.canceled || decoder.is_canceled() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "stream canceled",
                ));
            }
            if let Some(error) = &state.error {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    error.clone(),
                ));
            }
            if state.total_bytes.is_some_and(|total| offset >= total) {
                return Ok(0);
            }
            if let Some(chunk) = state
                .chunks
                .iter()
                .find(|chunk| chunk.range.contains(offset))
            {
                let start = offset.saturating_sub(chunk.range.start) as usize;
                let len = chunk.bytes.len().saturating_sub(start).min(buf.len());
                buf[..len].copy_from_slice(&chunk.bytes[start..start + len]);
                return Ok(len);
            }
            if !decoder.is_canceled() {
                let offset = state.request_start(offset);
                queue_requested_offset(&mut state.requested_offsets, offset, false);
                if state.local {
                    if let Some(end) = ready_through {
                        for n in 1..LOCAL_PIECES_IN_FLIGHT {
                            let next = offset + (n * crate::local_audio::PIECE) as u64;
                            if next > end {
                                break;
                            }
                            queue_requested_offset(&mut state.requested_offsets, next, false);
                        }
                    }
                    self.demand_changed.notify_one();
                }
            }
            condvar.notify_all();
            let (next, _) = condvar
                .wait_timeout(state, Duration::from_millis(250))
                .expect("streaming bytes lock poisoned while waiting");
            state = next;
        }
    }
}

enum PcmSample {
    Ready(f32),
    Waiting,
    Finished,
    Failed(String),
    Canceled,
}

impl SharedPcm {
    fn new() -> Self {
        Self {
            inner: Arc::new((
                Mutex::new(StreamingPcmState {
                    samples: Vec::new(),
                    channels: None,
                    sample_rate: None,
                    base_position_ms: 0,
                    dropped_samples: 0,
                    complete: false,
                    canceled: false,
                    error: None,
                }),
                Condvar::new(),
            )),
        }
    }

    fn set_base_position(&self, position_ms: u64) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming pcm lock poisoned");
        if state.samples.is_empty() {
            state.base_position_ms = position_ms;
        }
        condvar.notify_all();
    }

    fn set_spec(&self, channels: NonZeroU16, sample_rate: NonZeroU32) -> Result<()> {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming pcm lock poisoned");
        match (state.channels, state.sample_rate) {
            (Some(existing_channels), Some(existing_rate))
                if existing_channels != channels || existing_rate != sample_rate =>
            {
                bail!("audio format changed while streaming")
            }
            _ => {
                state.channels = Some(channels);
                state.sample_rate = Some(sample_rate);
            }
        }
        condvar.notify_all();
        Ok(())
    }

    fn push_samples(&self, samples: &[f32]) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming pcm lock poisoned");
        state.samples.extend_from_slice(samples);
        condvar.notify_all();
    }

    fn trim_before(&self, position_ms: u64, retain_behind_ms: u64) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming pcm lock poisoned");
        let (Some(channels), Some(sample_rate)) = (state.channels, state.sample_rate) else {
            return;
        };
        let retain_from_ms = position_ms.saturating_sub(retain_behind_ms);
        if retain_from_ms <= state.base_position_ms {
            return;
        }

        let elapsed_ms = retain_from_ms.saturating_sub(state.base_position_ms);
        if elapsed_ms < STREAM_PCM_TRIM_GRANULARITY_MS {
            return;
        }
        let frames_to_drop = (elapsed_ms as u128)
            .saturating_mul(sample_rate.get() as u128)
            .saturating_div(1000);
        let available_frames = (state.samples.len() / channels.get() as usize) as u128;
        let frames_to_drop = frames_to_drop.min(available_frames);
        if frames_to_drop == 0 {
            return;
        }

        let samples_to_drop = frames_to_drop
            .saturating_mul(channels.get() as u128)
            .min(state.samples.len() as u128) as usize;
        state.samples.drain(0..samples_to_drop);
        state.dropped_samples = state.dropped_samples.saturating_add(samples_to_drop as u64);
        let advanced_ms = frames_to_drop
            .saturating_mul(1000)
            .saturating_div(sample_rate.get() as u128) as u64;
        state.base_position_ms = state.base_position_ms.saturating_add(advanced_ms);
        condvar.notify_all();
    }

    fn fail(&self, error: String) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming pcm lock poisoned");
        state.error = Some(error);
        condvar.notify_all();
    }

    fn complete(&self) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming pcm lock poisoned");
        state.complete = true;
        condvar.notify_all();
    }

    fn cancel(&self) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming pcm lock poisoned");
        state.canceled = true;
        condvar.notify_all();
    }

    fn spec(&self) -> Option<(NonZeroU16, NonZeroU32)> {
        let (lock, _) = &*self.inner;
        let state = lock.lock().expect("streaming pcm lock poisoned");
        Some((state.channels?, state.sample_rate?))
    }

    fn base_position_ms(&self) -> u64 {
        let (lock, _) = &*self.inner;
        lock.lock()
            .expect("streaming pcm lock poisoned")
            .base_position_ms
    }

    fn dropped_samples(&self) -> u64 {
        let (lock, _) = &*self.inner;
        lock.lock()
            .expect("streaming pcm lock poisoned")
            .dropped_samples
    }

    fn window_start(&self) -> (u64, u64) {
        let (lock, _) = &*self.inner;
        let state = lock.lock().expect("streaming pcm lock poisoned");
        (state.base_position_ms, state.dropped_samples)
    }

    fn duration_ms(&self) -> u64 {
        let (lock, _) = &*self.inner;
        let state = lock.lock().expect("streaming pcm lock poisoned");
        let (Some(channels), Some(sample_rate)) = (state.channels, state.sample_rate) else {
            return 0;
        };
        (state.samples.len() as u64)
            .saturating_mul(1000)
            .saturating_div(sample_rate.get() as u64)
            .saturating_div(channels.get() as u64)
    }

    fn buffered_until_ms(&self) -> u64 {
        self.base_position_ms().saturating_add(self.duration_ms())
    }

    fn is_complete(&self) -> bool {
        let (lock, _) = &*self.inner;
        lock.lock().expect("streaming pcm lock poisoned").complete
    }

    fn sample(&self, index: usize) -> PcmSample {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming pcm lock poisoned");
        loop {
            if state.canceled {
                return PcmSample::Canceled;
            }
            if let Some(error) = &state.error {
                return PcmSample::Failed(error.clone());
            }
            if let Some(sample) = state.samples.get(index) {
                return PcmSample::Ready(*sample);
            }
            if state.complete {
                return PcmSample::Finished;
            }
            let (next, timeout) = condvar
                .wait_timeout(state, Duration::from_millis(250))
                .expect("streaming pcm lock poisoned while waiting");
            state = next;
            if timeout.timed_out() {
                return PcmSample::Waiting;
            }
        }
    }
}

impl StreamingSession {
    fn cancel(self) {
        self.decoder.cancel();
        self.bytes.cancel();
        self.pcm.cancel();
    }

    fn restart_decoder(
        &mut self,
        operation_id: Option<String>,
        session_id: String,
        track: PlaybackTrack,
        position_ms: u64,
        event_tx: mpsc::UnboundedSender<AudioPlayerEvent>,
    ) {
        self.decoder.cancel();
        self.pcm.cancel();
        if track.local_audio.is_some() {
            let mut s = self.bytes.inner.0.lock().unwrap();
            s.generation += 1;
            s.requested_offsets.clear();
            self.bytes.demand_changed.notify_one();
        }

        let pcm = SharedPcm::new();
        pcm.set_base_position(position_ms);
        let decoder = DecodeToken::new();
        self.track_id = track.track_id.clone();
        self.track = track.clone();
        self.duration_ms = track.duration_ms;
        self.pcm = pcm.clone();
        self.decoder = decoder.clone();
        self.decoder_operation = operation_id.clone();
        if track.local_audio.is_some() {
            // An audio callback finishing on the retired sink still owns its old clock.
            self.position_ms = Arc::new(AtomicU64::new(position_ms));
        } else {
            self.position_ms.store(position_ms, Ordering::Relaxed);
        }
        if let Some(total) = self.bytes.total_bytes()
            && track.local_audio.is_none()
        {
            let seek_window = media_cache::range_for_window(position_ms, track.duration_ms, total);
            self.bytes.prioritize_offset(seek_window.start);
        }

        let bytes = self.bytes.clone();
        let playback_position = Arc::clone(&self.position_ms);
        tokio::task::spawn_blocking(move || {
            decode_streaming_audio(
                bytes,
                pcm,
                decoder,
                event_tx,
                operation_id,
                session_id,
                track,
                position_ms,
                playback_position,
            );
        });
    }
}

fn decode_audio(audio: Arc<[u8]>) -> Result<DecodedAudio> {
    let source = MemoryMediaSource::new(audio);
    let mss = MediaSourceStream::new(Box::new(source), Default::default());

    let mut hint = Hint::new();
    hint.with_extension("m4a");

    let format_opts = FormatOptions::default();
    let metadata_opts = MetadataOptions::default();
    let probed = get_probe()
        .format(&hint, mss, &format_opts, &metadata_opts)
        .context("failed to probe audio format")?;

    let mut format = probed.format;
    let track = format
        .tracks()
        .iter()
        .find(|track| is_audio_track(track))
        .ok_or_else(|| anyhow!("audio stream does not contain a supported track"))?;
    let track_id = track.id;
    let codec_params = track.codec_params.clone();
    let decoder_opts = DecoderOptions::default();
    let mut decoder = make_audio_decoder(&codec_params, &decoder_opts)
        .context("failed to create audio decoder")?;

    let mut channels = None;
    let mut sample_rate = None;
    let mut samples = Vec::new();
    let mut decode_errors = 0usize;
    let mut first_decode_error = None;

    loop {
        let packet = match format.next_packet() {
            Ok(packet) => packet,
            Err(SymphoniaError::IoError(err))
                if err.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Err(SymphoniaError::ResetRequired) => bail!("audio stream reset required"),
            Err(err) => bail!("failed to read audio packet: {err}"),
        };

        while !format.metadata().is_latest() {
            format.metadata().pop();
        }

        if packet.track_id() != track_id {
            continue;
        }

        match decoder.decode(&packet) {
            Ok(decoded) => {
                let spec = decoded.spec();
                let packet_channels = NonZeroU16::new(spec.channels.count() as u16)
                    .ok_or_else(|| anyhow!("decoded audio has invalid channel count"))?;
                let packet_sample_rate = NonZeroU32::new(spec.rate)
                    .ok_or_else(|| anyhow!("decoded audio has invalid sample rate"))?;

                match (channels, sample_rate) {
                    (Some(channels), Some(sample_rate))
                        if channels != packet_channels || sample_rate != packet_sample_rate =>
                    {
                        bail!("audio format changed while decoding")
                    }
                    _ => {
                        channels = Some(packet_channels);
                        sample_rate = Some(packet_sample_rate);
                    }
                }

                let packet_samples = decoded_to_interleaved_f32(decoded);
                samples.extend(packet_samples);
                decode_errors = 0;
            }
            Err(SymphoniaError::DecodeError(err)) => {
                decode_errors += 1;
                first_decode_error.get_or_insert_with(|| err.to_string());
                if decode_errors > 3 {
                    bail!(
                        "too many audio decode errors: {}",
                        first_decode_error
                            .as_deref()
                            .unwrap_or("unknown decode error")
                    );
                }
            }
            Err(SymphoniaError::IoError(err))
                if err.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Err(err) => bail!("failed to decode audio packet: {err}"),
        }
    }

    if samples.is_empty() {
        bail!("decoded audio has no samples");
    }

    Ok(DecodedAudio {
        samples: Arc::from(samples.into_boxed_slice()),
        channels: channels.ok_or_else(|| anyhow!("decoded audio has no channel info"))?,
        sample_rate: sample_rate.ok_or_else(|| anyhow!("decoded audio has no sample rate"))?,
    })
}

async fn download_streaming_bytes(
    client: reqwest::Client,
    track: PlaybackTrack,
    position_ms: u64,
    playback_position: Arc<AtomicU64>,
    shared: SharedBytes,
    event_tx: mpsc::UnboundedSender<AudioPlayerEvent>,
    operation_id: Option<String>,
    session_id: String,
) -> Result<()> {
    let mut request_pacer = StreamRequestPacer::new();
    let opening_total = shared.total_bytes();
    let total = if let Some(total) = opening_total {
        total
    } else {
        let probe = probe_stream_range_with_retry(
            &client,
            &track,
            &shared,
            &mut request_pacer,
            &event_tx,
            operation_id.as_deref(),
            &session_id,
        )
        .await?;
        let total = probe
            .total_bytes
            .ok_or_else(|| anyhow!("media range probe did not report total length"))?;
        shared.set_total(Some(total));
        total
    };

    let metadata_ranges = media_cache::metadata_ranges(&track, total);
    if opening_total.is_none() && should_full_fetch_missing_metadata(&metadata_ranges, total) {
        let bytes = fetch_full_media_with_retry(
            &client,
            &track,
            total,
            &shared,
            &mut request_pacer,
            &event_tx,
            operation_id.as_deref(),
            &session_id,
        )
        .await?;
        if bytes.len() as u64 != total {
            return Err(anyhow!(
                "full media download returned {} bytes, expected {total}",
                bytes.len()
            ));
        }
        let full_range = media_cache::ByteRange::new(0, total.saturating_sub(1))?;
        shared.pin_ranges(&[full_range]);
        shared.append(&bytes, full_range);
    } else {
        let prefetch_ranges = stream_metadata_prefetch_ranges(&metadata_ranges, total)?;
        shared.pin_ranges(&prefetch_ranges);
        for range in &prefetch_ranges {
            shared.request_offset(range.start);
        }
    }
    let initial_window = stream_byte_range_for_window(
        position_ms,
        STREAM_BUFFER_TARGET_MS,
        track.duration_ms,
        total,
    );
    shared.prioritize_offset(initial_window.start);

    let mut downloaded = opening_total
        .unwrap_or(0)
        .min(media_cache::STREAM_CHUNK_BYTES);
    let mut prefetch_paused = false;
    loop {
        if shared.is_canceled() {
            return Ok(());
        }

        let current_position_ms = playback_position
            .load(Ordering::Relaxed)
            .min(track.duration_ms);
        retain_streaming_byte_window(&shared, current_position_ms, track.duration_ms, total);

        let target_window = stream_byte_range_for_window(
            current_position_ms,
            STREAM_BUFFER_TARGET_MS,
            track.duration_ms,
            total,
        );
        let resume_window = stream_byte_range_for_window(
            current_position_ms,
            STREAM_BUFFER_RESUME_MS,
            track.duration_ms,
            total,
        );
        let start = if let Some(requested) = shared.take_requested_offset(total) {
            prefetch_paused = false;
            requested.min(total.saturating_sub(1))
        } else {
            if prefetch_paused && shared.covers(resume_window) {
                tokio::time::sleep(Duration::from_millis(STREAM_DOWNLOAD_IDLE_SLEEP_MS)).await;
                continue;
            }

            if shared.covers(target_window) {
                prefetch_paused = true;
                tokio::time::sleep(Duration::from_millis(STREAM_DOWNLOAD_IDLE_SLEEP_MS)).await;
                continue;
            }

            prefetch_paused = false;
            let Some(missing) = shared.first_missing_offset(target_window) else {
                tokio::time::sleep(Duration::from_millis(STREAM_DOWNLOAD_IDLE_SLEEP_MS)).await;
                continue;
            };
            missing.min(total.saturating_sub(1))
        };
        let range = stream_fetch_range(start, total)?;
        if shared.covers(range) {
            continue;
        }
        if track.media_is_progressive
            && downloaded + range.len() > 32 * 1024 * 1024
            && !shared.prepared_once()
        {
            bail!(
                "Bilibili supplied combined video and audio; the first playback window exceeded 32 MiB"
            );
        }
        let (bytes, _) = fetch_stream_range_with_retry(
            &client,
            &track,
            range,
            &shared,
            &mut request_pacer,
            &event_tx,
            operation_id.as_deref(),
            &session_id,
        )
        .await?;
        downloaded += bytes.len() as u64;
        shared.append(&bytes, range);
        let current_position_ms = playback_position
            .load(Ordering::Relaxed)
            .min(track.duration_ms);
        retain_streaming_byte_window(&shared, current_position_ms, track.duration_ms, total);
    }
}

async fn fetch_full_media_with_retry(
    client: &reqwest::Client,
    track: &PlaybackTrack,
    total: u64,
    shared: &SharedBytes,
    request_pacer: &mut StreamRequestPacer,
    event_tx: &mpsc::UnboundedSender<AudioPlayerEvent>,
    operation_id: Option<&str>,
    session_id: &str,
) -> Result<Vec<u8>> {
    let mut attempt = 0;
    loop {
        if !request_pacer.wait_for_turn(shared).await {
            return Err(anyhow!("stream canceled"));
        }

        match media_cache::fetch_full_media_bytes(client, track, total).await {
            Ok(result) => return Ok(result),
            Err(err) => {
                let Some(delay) = media_range_retry_delay(&err, attempt) else {
                    return Err(anyhow::Error::from(err)).context("failed to download full media");
                };
                send_media_retry_status(
                    event_tx,
                    operation_id,
                    session_id,
                    &track.track_id,
                    &err,
                    delay,
                );
                if !sleep_streaming_download(shared, delay).await {
                    return Err(anyhow!("stream canceled"));
                }
                attempt += 1;
            }
        }
    }
}

async fn probe_stream_range_with_retry(
    client: &reqwest::Client,
    track: &PlaybackTrack,
    shared: &SharedBytes,
    request_pacer: &mut StreamRequestPacer,
    event_tx: &mpsc::UnboundedSender<AudioPlayerEvent>,
    operation_id: Option<&str>,
    session_id: &str,
) -> Result<media_cache::RangeProbe> {
    let mut attempt = 0;
    loop {
        if !request_pacer.wait_for_turn(shared).await {
            return Err(anyhow!("stream canceled"));
        }

        match media_cache::probe_range_support(client, track).await {
            Ok(result) => return Ok(result),
            Err(err) => {
                let Some(delay) = media_range_retry_delay(&err, attempt) else {
                    return Err(anyhow::Error::from(err)).context("failed to probe media range");
                };
                send_media_retry_status(
                    event_tx,
                    operation_id,
                    session_id,
                    &track.track_id,
                    &err,
                    delay,
                );
                if !sleep_streaming_download(shared, delay).await {
                    return Err(anyhow!("stream canceled"));
                }
                attempt += 1;
            }
        }
    }
}

async fn fetch_stream_range_with_retry(
    client: &reqwest::Client,
    track: &PlaybackTrack,
    range: media_cache::ByteRange,
    shared: &SharedBytes,
    request_pacer: &mut StreamRequestPacer,
    event_tx: &mpsc::UnboundedSender<AudioPlayerEvent>,
    operation_id: Option<&str>,
    session_id: &str,
) -> Result<(Vec<u8>, media_cache::RangeProbe)> {
    let mut attempt = 0;
    loop {
        if !request_pacer.wait_for_turn(shared).await {
            return Err(anyhow!("stream canceled"));
        }

        match media_cache::fetch_range_bytes(client, track, range).await {
            Ok(result) => return Ok(result),
            Err(err) => {
                let Some(delay) = media_range_retry_delay(&err, attempt) else {
                    return Err(anyhow::Error::from(err)).with_context(|| {
                        format!(
                            "failed to read media range {} after {} attempt(s)",
                            range.header_value(),
                            attempt + 1
                        )
                    });
                };

                send_media_retry_status(
                    event_tx,
                    operation_id,
                    session_id,
                    &track.track_id,
                    &err,
                    delay,
                );
                if !sleep_streaming_download(shared, delay).await {
                    return Err(anyhow!("stream canceled"));
                }
                attempt += 1;
            }
        }
    }
}

fn send_media_retry_status(
    event_tx: &mpsc::UnboundedSender<AudioPlayerEvent>,
    operation_id: Option<&str>,
    session_id: &str,
    track_id: &str,
    error: &media_cache::MediaRangeError,
    delay: Duration,
) {
    let status = error
        .status()
        .map(|status| format!("HTTP {status}"))
        .unwrap_or_else(|| "transport error".to_string());
    let message = format!(
        "media range {} {status}; retrying in {}",
        error.range(),
        describe_retry_delay(delay)
    );
    send_download_status(event_tx, operation_id, session_id, track_id, message);
}

fn send_download_status(
    event_tx: &mpsc::UnboundedSender<AudioPlayerEvent>,
    operation_id: Option<&str>,
    session_id: &str,
    track_id: &str,
    message: String,
) {
    let _ = event_tx.send(AudioPlayerEvent::DownloadStatus {
        operation_id: operation_id.map(str::to_string),
        session_id: session_id.to_string(),
        track_id: track_id.to_string(),
        message,
    });
}

fn throttle_streaming_decode(
    pcm: &SharedPcm,
    playback_position: &AtomicU64,
    duration_ms: u64,
    decoder_token: &DecodeToken,
) {
    loop {
        if decoder_token.is_canceled() {
            return;
        }

        let current_position_ms = playback_position.load(Ordering::Relaxed).min(duration_ms);
        pcm.trim_before(current_position_ms, STREAM_PCM_RETAIN_BEHIND_MS);
        let buffered_until_ms = pcm.buffered_until_ms().min(duration_ms);
        let target_until_ms = current_position_ms
            .saturating_add(STREAM_BUFFER_TARGET_MS)
            .min(duration_ms);
        if target_until_ms >= duration_ms || buffered_until_ms < target_until_ms {
            return;
        }

        let resume_until_ms = current_position_ms
            .saturating_add(STREAM_BUFFER_RESUME_MS)
            .min(duration_ms);
        if buffered_until_ms <= resume_until_ms {
            return;
        }

        std::thread::sleep(Duration::from_millis(STREAM_DECODE_IDLE_SLEEP_MS));
    }
}

fn decode_streaming_audio(
    bytes: SharedBytes,
    pcm: SharedPcm,
    decoder_token: DecodeToken,
    event_tx: mpsc::UnboundedSender<AudioPlayerEvent>,
    operation_id: Option<String>,
    session_id: String,
    track: PlaybackTrack,
    position_ms: u64,
    playback_position: Arc<AtomicU64>,
) {
    if let Err(err) = decode_streaming_audio_inner(
        bytes,
        pcm.clone(),
        decoder_token.clone(),
        event_tx.clone(),
        operation_id.clone(),
        session_id.clone(),
        track.clone(),
        position_ms,
        playback_position,
    ) {
        if decoder_token.is_canceled() {
            return;
        }
        let error = format!("{err:#}");
        pcm.fail(error.clone());
        let _ = event_tx.send(AudioPlayerEvent::Failed {
            operation_id,
            session_id,
            track_id: track.track_id,
            title: track.title,
            error,
        });
    }
}

// Only select incremental MP4 opening when the complete file type, movie header
// and segment index are present before the first fragment. Other layouts use the
// usual seekable probe, including files with a trailing movie header.
fn has_front_loaded_mp4_index(mut prefix: &[u8]) -> bool {
    let mut has_ftyp = false;
    let mut has_moov = false;
    for _ in 0..64 {
        if prefix.len() < 8 {
            return false;
        }
        let size = u32::from_be_bytes(prefix[..4].try_into().unwrap()) as usize;
        if size < 8 || size > prefix.len() {
            return false;
        }
        match &prefix[4..8] {
            b"ftyp" => has_ftyp = true,
            b"moov" => has_moov = true,
            b"sidx" => return has_ftyp && has_moov,
            b"moof" | b"mdat" => return false,
            _ => {}
        }
        prefix = &prefix[size..];
    }
    false
}

#[derive(Clone, Copy)]
struct IndexedSeek {
    header_bytes: u64,
    frame_offset: u64,
    timestamp_base: u64,
    ready_through: Option<u64>,
}

fn indexed_seek(track: &PlaybackTrack, position_ms: u64, ready_until: u64) -> Option<IndexedSeek> {
    let info = &track.local_audio.as_ref()?.info;
    let sample_at = |ms: u64| (ms as u128 * info.sample_rate as u128 / 1000) as u64;
    let preroll = sample_at(position_ms).saturating_sub(u64::from(info.sample_rate));
    let &[timestamp, frame_offset] = info.seek_points.iter().rev().find(|p| p[0] <= preroll)?;
    match info.format.as_str() {
        // The first indexed frame also skips any leading ID3 artwork.
        "mp3" => Some(IndexedSeek {
            header_bytes: 0,
            frame_offset,
            timestamp_base: timestamp,
            // Only overlap requests through an audio frame required for readiness.
            ready_through: info
                .seek_points
                .iter()
                .rev()
                .find(|p| p[0] < sample_at(ready_until))
                .map(|p| p[1]),
        }),
        // FLAC needs its original stream header and carries absolute timestamps.
        "flac" if timestamp > 0 => Some(IndexedSeek {
            header_bytes: info.seek_points.first()?[1],
            frame_offset,
            timestamp_base: 0,
            ready_through: None,
        }),
        _ => None,
    }
}

fn decode_streaming_audio_inner(
    bytes: SharedBytes,
    pcm: SharedPcm,
    decoder_token: DecodeToken,
    event_tx: mpsc::UnboundedSender<AudioPlayerEvent>,
    operation_id: Option<String>,
    session_id: String,
    track: PlaybackTrack,
    position_ms: u64,
    playback_position: Arc<AtomicU64>,
) -> Result<()> {
    let mut hint = Hint::new();
    hint.with_extension(track.local_audio.as_ref().map_or("m4a", |d| &d.info.format));

    let format_opts = FormatOptions::default();
    let metadata_opts = MetadataOptions::default();
    let ready_until = position_ms
        .saturating_add(media_cache::READY_WINDOW_MS)
        .min(track.duration_ms);
    let indexed = indexed_seek(&track, position_ms, ready_until);
    let open = |incremental: bool| {
        let mut source = StreamingMediaSource::new(bytes.clone(), decoder_token.clone());
        source.splice = indexed.map(|s| (s.header_bytes, s.frame_offset));
        source.ready_through = indexed.and_then(|s| s.ready_through);
        let seekable = source.seekable.clone();
        seekable.store(!incremental && indexed.is_none(), Ordering::Relaxed);
        let mss = MediaSourceStream::new(Box::new(source), Default::default());
        let result = get_probe().format(&hint, mss, &format_opts, &metadata_opts);
        // Once open, packet reads still have full access to HTTP Range seeks.
        seekable.store(true, Ordering::Relaxed);
        result
    };
    let probed = if position_ms == 0 && !track.media_is_progressive && bytes.has_opening_index() {
        open(true).or_else(|_| open(false))
    } else {
        open(false)
    }
    .context("failed to probe streaming audio format")?;

    let mut format = probed.format;
    let track_info = format
        .tracks()
        .iter()
        .find(|track| is_audio_track(track))
        .ok_or_else(|| anyhow!("streaming audio does not contain a supported track"))?;
    let track_id = track_info.id;
    let codec_params = track_info.codec_params.clone();
    let decoder_opts = DecoderOptions::default();
    let mut decoder = make_audio_decoder(&codec_params, &decoder_opts)
        .context("failed to create streaming audio decoder")?;
    let preroll = track
        .local_audio
        .as_ref()
        .is_some_and(|d| d.info.format == "m4a");
    let seek_ms = if preroll {
        position_ms.saturating_sub(1000)
    } else {
        position_ms
    };
    let decode_base_position_ms = if indexed.is_some() {
        Some(position_ms)
    } else {
        seek_streaming_format(&mut *format, track_id, seek_ms).map(|_| position_ms)
    }
    .with_context(|| format!("failed to seek streaming audio to {position_ms}ms"))?;
    pcm.set_base_position(decode_base_position_ms);

    let mut ready_sent = false;
    let mut last_cache_event_ms: u64 = 0;
    let mut decode_errors = 0usize;
    let mut first_decode_error = None;

    loop {
        if decoder_token.is_canceled() {
            return Ok(());
        }

        let packet = match format.next_packet() {
            Ok(packet) => packet,
            Err(SymphoniaError::IoError(err))
                if err.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Err(SymphoniaError::IoError(err)) if err.kind() == std::io::ErrorKind::Interrupted => {
                return Ok(());
            }
            Err(SymphoniaError::ResetRequired) => bail!("streaming audio reset required"),
            Err(err) => bail!("failed to read streaming audio packet: {err}"),
        };

        while !format.metadata().is_latest() {
            format.metadata().pop();
        }

        if packet.track_id() != track_id {
            continue;
        }

        match decoder.decode(&packet) {
            Ok(decoded) => {
                let spec = decoded.spec();
                let channels = NonZeroU16::new(spec.channels.count() as u16)
                    .ok_or_else(|| anyhow!("decoded streaming audio has invalid channel count"))?;
                let sample_rate = NonZeroU32::new(spec.rate)
                    .ok_or_else(|| anyhow!("decoded streaming audio has invalid sample rate"))?;
                pcm.set_spec(channels, sample_rate)?;

                let packet_samples = decoded_to_interleaved_f32(decoded);
                // Demux seeks may land before the requested sample (including MP3 reservoir preroll).
                let skip = if track.local_audio.is_some() && position_ms > 0 {
                    let rate = sample_rate.get() as u64;
                    let timestamp = codec_params.time_base.map_or(packet.ts(), |tb| {
                        (packet.ts() as u128 * tb.numer as u128 * rate as u128 / tb.denom as u128)
                            as u64
                    });
                    let base = indexed.map_or(0, |s| s.timestamp_base);
                    let target = (position_ms as u128 * rate as u128 / 1000) as u64;
                    target
                        .saturating_sub(timestamp.saturating_add(base))
                        .saturating_mul(channels.get() as u64)
                        .min(packet_samples.len() as u64) as usize
                } else {
                    0
                };
                pcm.push_samples(&packet_samples[skip..]);
                pcm.trim_before(
                    playback_position
                        .load(Ordering::Relaxed)
                        .min(track.duration_ms),
                    STREAM_PCM_RETAIN_BEHIND_MS,
                );
                decode_errors = 0;

                let (buffered_from_ms, buffered_until_ms) =
                    stream_pcm_cache_window(&pcm, track.duration_ms);
                if should_emit_stream_cache_event(
                    buffered_until_ms,
                    last_cache_event_ms,
                    ready_sent,
                    ready_until,
                ) {
                    last_cache_event_ms = buffered_until_ms;
                    let _ = event_tx.send(AudioPlayerEvent::Cache {
                        operation_id: operation_id.clone(),
                        decoder_finished: false,
                        view: PlaybackCacheView {
                            session_id: session_id.clone(),
                            track_id: track.track_id.clone(),
                            status: if ready_sent {
                                PlaybackCacheStatus::Ready
                            } else {
                                PlaybackCacheStatus::Preparing
                            },
                            buffered_from_ms,
                            buffered_until_ms,
                            duration_ms: track.duration_ms,
                            error: None,
                        },
                    });
                }

                if !ready_sent && buffered_until_ms >= ready_until {
                    ready_sent = true;
                    bytes.mark_prepared();
                    let _ = event_tx.send(AudioPlayerEvent::Prepared {
                        operation_id: operation_id.clone(),
                        session_id: session_id.clone(),
                        track_id: track.track_id.clone(),
                        buffered_until_ms,
                    });
                    let _ = event_tx.send(AudioPlayerEvent::Cache {
                        operation_id: operation_id.clone(),
                        decoder_finished: false,
                        view: PlaybackCacheView {
                            session_id: session_id.clone(),
                            track_id: track.track_id.clone(),
                            status: PlaybackCacheStatus::Ready,
                            buffered_from_ms,
                            buffered_until_ms,
                            duration_ms: track.duration_ms,
                            error: None,
                        },
                    });
                }

                throttle_streaming_decode(
                    &pcm,
                    &playback_position,
                    track.duration_ms,
                    &decoder_token,
                );
            }
            Err(SymphoniaError::DecodeError(err)) => {
                decode_errors += 1;
                first_decode_error.get_or_insert_with(|| err.to_string());
                if decode_errors > 3 {
                    bail!(
                        "too many streaming audio decode errors: {}",
                        first_decode_error
                            .as_deref()
                            .unwrap_or("unknown decode error")
                    );
                }
            }
            Err(SymphoniaError::IoError(err))
                if err.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Err(SymphoniaError::IoError(err)) if err.kind() == std::io::ErrorKind::Interrupted => {
                return Ok(());
            }
            Err(err) => bail!("failed to decode streaming audio packet: {err}"),
        }
    }

    pcm.complete();
    let (buffered_from_ms, buffered_until_ms) = stream_pcm_cache_window(&pcm, track.duration_ms);
    if !ready_sent {
        if !stream_pcm_ready_for_position(&pcm, position_ms, track.duration_ms) {
            bail!("audio ended before the requested playback window");
        }
        bytes.mark_prepared();
        let _ = event_tx.send(AudioPlayerEvent::Prepared {
            operation_id: operation_id.clone(),
            session_id: session_id.clone(),
            track_id: track.track_id.clone(),
            buffered_until_ms,
        });
    }
    let _ = event_tx.send(AudioPlayerEvent::Cache {
        operation_id,
        decoder_finished: true,
        view: PlaybackCacheView {
            session_id,
            track_id: track.track_id,
            status: PlaybackCacheStatus::Ready,
            buffered_from_ms,
            buffered_until_ms,
            duration_ms: track.duration_ms,
            error: None,
        },
    });
    Ok(())
}

fn seek_streaming_format(
    format: &mut dyn symphonia::core::formats::FormatReader,
    track_id: u32,
    position_ms: u64,
) -> Option<u64> {
    if position_ms == 0 {
        return Some(0);
    }

    let time = time_from_millis(position_ms);
    format
        .seek(
            SeekMode::Accurate,
            SeekTo::Time {
                time,
                track_id: Some(track_id),
            },
        )
        .ok()
        .map(|_| position_ms)
}

fn time_from_millis(position_ms: u64) -> Time {
    Time::new(position_ms / 1000, (position_ms % 1000) as f64 / 1000.0)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Run the production piece downloader and decoder through readiness without an audio device.
    pub(crate) async fn measure_local_seek(
        client: crate::local_audio::Client,
        descriptor: &crate::local_audio::Descriptor,
        position: u64,
    ) -> Result<(Vec<u64>, Duration)> {
        let bytes = SharedBytes::new();
        bytes.inner.0.lock().unwrap().local = true;
        bytes.set_total(Some(descriptor.info.length));
        let download = tokio::spawn(download_local_bytes(
            client,
            descriptor.clone(),
            bytes.clone(),
        ));
        let started = Instant::now();
        let token = DecodeToken::new();
        let (tx, mut events) = mpsc::unbounded_channel();
        let track = descriptor.track();
        let input = bytes.clone();
        let decoder = token.clone();
        let decode = tokio::task::spawn_blocking(move || {
            decode_streaming_audio(
                input,
                SharedPcm::new(),
                decoder,
                tx,
                Some("seek".into()),
                "session".into(),
                track,
                position,
                Arc::new(AtomicU64::new(position)),
            );
        });
        let ready = tokio::time::timeout(Duration::from_secs(20), async {
            while let Some(event) = events.recv().await {
                match event {
                    AudioPlayerEvent::Prepared {
                        buffered_until_ms, ..
                    } => {
                        anyhow::ensure!(
                            buffered_until_ms
                                >= (position + media_cache::READY_WINDOW_MS)
                                    .min(descriptor.info.duration_ms)
                        );
                        return Ok(());
                    }
                    AudioPlayerEvent::Failed { error, .. } => bail!("{error}"),
                    _ => (),
                }
            }
            bail!("decoder stopped before readiness")
        })
        .await;
        let elapsed = started.elapsed();
        token.cancel();
        bytes.cancel();
        decode.await?;
        download.await??;
        ready??;
        let pieces = bytes
            .inner
            .0
            .lock()
            .unwrap()
            .chunks
            .iter()
            .map(|chunk| chunk.range.start / crate::local_audio::PIECE as u64)
            .collect();
        Ok((pieces, elapsed))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn mp3_requests_overlap_only_inside_the_indexed_window() {
        use crate::local_audio::{self, Client, Need, PIECE};
        let file = local_audio::tests::long_mp3();
        let (d, source) = local_audio::tests::scan(&file);
        let mut store = local_audio::Store::default();
        store.source(&d, source);
        for position in [0u64, 503_123] {
            let (tx, mut rx) = mpsc::channel::<Need>(8);
            let first = d
                .info
                .seek_points
                .iter()
                .rev()
                .find(|p| {
                    p[0] <= position.saturating_sub(1000) * u64::from(d.info.sample_rate) / 1000
                })
                .unwrap()[1]
                / PIECE as u64;
            let client = Client {
                tx,
                wake: Default::default(),
            };
            let mut requested = Vec::new();
            let serve = async {
                // Withhold the first response until four distinct demands arrive. A
                // stop-and-wait downloader cannot prepare this window.
                let mut batch = Vec::new();
                for _ in 0..LOCAL_PIECES_IN_FLIGHT {
                    batch.push(rx.recv().await.unwrap());
                }
                batch.sort_by_key(|n| n.index);
                assert_eq!(
                    batch.iter().map(|n| n.index).collect::<Vec<_>>(),
                    (first..first + LOCAL_PIECES_IN_FLIGHT as u64).collect::<Vec<_>>()
                );
                // Complete in reverse order, then service subsequent decoder reads normally.
                while let Some(n) = batch.pop() {
                    requested.push(n.index);
                    let _ = n.reply.send(store.read(&d, n.index));
                }
                while let Some(n) = rx.recv().await {
                    requested.push(n.index);
                    let _ = n.reply.send(store.read(&d, n.index));
                }
            };
            let (result, ()) = tokio::join!(measure_local_seek(client, &d, position), serve);
            result.unwrap();
            requested.sort_unstable();
            assert_eq!(
                requested,
                (first..first + requested.len() as u64).collect::<Vec<_>>()
            );
            assert!(requested.len() <= 12, "{requested:?}");
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mp3_seek_preempts_unanswered_opening_requests() {
        use crate::local_audio::{self, Client, Need, PIECE};
        let file = local_audio::tests::long_mp3();
        let (descriptor, _) = local_audio::tests::scan(&file);
        let track = descriptor.track();
        let (tx, mut needs) = mpsc::channel::<Need>(8);
        let bytes = SharedBytes::new();
        bytes.inner.0.lock().unwrap().local = true;
        bytes.set_total(Some(descriptor.info.length));
        let download = tokio::spawn(download_local_bytes(
            Client {
                tx,
                wake: Default::default(),
            },
            descriptor,
            bytes.clone(),
        ));
        let mut session = StreamingSession {
            decoder_operation: None,
            session_id: "session".into(),
            track_id: track.track_id.clone(),
            track: track.clone(),
            duration_ms: track.duration_ms,
            pcm: SharedPcm::new(),
            bytes,
            decoder: DecodeToken::new(),
            position_ms: Arc::new(AtomicU64::new(0)),
        };
        let (events, _rx) = mpsc::unbounded_channel();
        session.restart_decoder(
            Some("opening".into()),
            "session".into(),
            track.clone(),
            0,
            events.clone(),
        );
        let observed = tokio::time::timeout(Duration::from_secs(5), async {
            let mut opening = Vec::new();
            for _ in 0..LOCAL_PIECES_IN_FLIGHT {
                opening.push(needs.recv().await.unwrap());
            }
            let position = 503_123;
            let first = indexed_seek(&track, position, position + media_cache::READY_WINDOW_MS)
                .unwrap()
                .frame_offset
                / PIECE as u64;
            session.restart_decoder(
                Some("seek".into()),
                "session".into(),
                track,
                position,
                events,
            );
            let new = needs.recv().await.unwrap();
            (
                opening.iter().all(|n| n.reply.is_closed()),
                new.index,
                first,
            )
        })
        .await;
        session.cancel();
        download.await.unwrap().unwrap();
        let (old_demands_canceled, next, first) = observed.unwrap();
        assert!(old_demands_canceled);
        assert!((first..first + LOCAL_PIECES_IN_FLIGHT as u64).contains(&next));
    }

    #[tokio::test]
    #[ignore = "reads a public Bilibili audio stream"]
    async fn live_bilibili_prepares_playable_audio_window() -> Result<()> {
        let (client, track) = live_bilibili_track().await?;
        live_bilibili_window(client, track, false).await.map(|_| ())
    }

    #[tokio::test]
    #[ignore = "reads a public Bilibili audio stream"]
    async fn live_bilibili_prefetched_opening_matches_cold_audio() -> Result<()> {
        let (client, track) = live_bilibili_track().await?;
        let cold = live_bilibili_window(client.clone(), track.clone(), false).await?;
        let warm = live_bilibili_window(client, track, true).await?;
        assert!(!cold.is_empty() && cold == warm, "opening PCM differs");
        Ok(())
    }

    async fn live_bilibili_track() -> Result<(reqwest::Client, PlaybackTrack)> {
        let client = crate::bilibili::client()?;
        let bvid = std::env::var("LIGHT_EAR_TEST_BVID").unwrap_or_else(|_| "BV1xx411c7mD".into());
        let track = crate::bilibili::resolve_track(&client, &bvid, 0).await?;
        Ok((client, track))
    }

    async fn live_bilibili_window(
        client: reqwest::Client,
        track: PlaybackTrack,
        prefetched: bool,
    ) -> Result<Vec<f32>> {
        let bvid = track.bvid.clone();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let bytes = SharedBytes::new();
        let track = if prefetched {
            prefetch::Opening::fetch(client.clone(), track)
                .await?
                .install(&bytes)
        } else {
            track
        };
        let decoder = DecodeToken::new();
        let position = Arc::new(AtomicU64::new(0));
        let download = download_streaming_bytes(
            client,
            track.clone(),
            0,
            position.clone(),
            bytes.clone(),
            tx.clone(),
            None,
            "live".into(),
        );
        let input = bytes.clone();
        let token = decoder.clone();
        let duration = track.duration_ms;
        let progressive = track.media_is_progressive;
        let pcm = SharedPcm::new();
        let output = pcm.clone();
        let started = Instant::now();
        let decode = tokio::task::spawn_blocking(move || {
            decode_streaming_audio(
                input,
                output,
                token,
                tx,
                None,
                "live".into(),
                track,
                0,
                position,
            )
        });
        let result = tokio::select! {
            r = download => r.and_then(|_| Err(anyhow!("download stopped before readiness"))),
            _ = tokio::time::sleep(Duration::from_secs(75)) => Err(anyhow!("audio prepare timed out")),
            result = async {
                while let Some(event) = rx.recv().await {
                    match event {
                        AudioPlayerEvent::Prepared { buffered_until_ms, .. } => return Ok(buffered_until_ms),
                        AudioPlayerEvent::Failed { error, .. } => bail!("{error}"),
                        _ => {}
                    }
                }
                bail!("decoder closed before readiness")
            } => result,
        };
        let elapsed = started.elapsed();
        decoder.cancel();
        bytes.cancel();
        decode.await?;
        let ready = result?;
        eprintln!(
            "{bvid}: {duration} ms track, progressive {progressive}, prefetched {prefetched}, {ready} ms ready in {:?}",
            elapsed
        );
        assert!(ready >= media_cache::READY_WINDOW_MS.min(duration));
        let state = pcm.inner.0.lock().unwrap();
        let frames = media_cache::READY_WINDOW_MS.min(duration)
            * u64::from(state.sample_rate.unwrap().get())
            / 1000;
        let n = frames as usize * usize::from(state.channels.unwrap().get());
        Ok(state.samples[..n].to_vec())
    }

    #[test]
    fn incremental_mp4_opening_requires_complete_headers_before_audio() {
        let ftyp: &[u8] = b"\0\0\0\x08ftyp";
        let moov: &[u8] = b"\0\0\0\x08moov";
        let sidx: &[u8] = b"\0\0\0\x08sidx";
        let mdat: &[u8] = b"\0\0\0\x08mdat";
        let fragment: &[u8] = b"\0\0\0\x08moof";
        assert!(has_front_loaded_mp4_index(
            &[ftyp, moov, sidx, fragment].concat()
        ));
        for atoms in [
            vec![ftyp, sidx],
            vec![ftyp, moov],
            vec![ftyp, moov, mdat, sidx],
            vec![ftyp, fragment, moov, sidx],
            vec![ftyp, moov, b"\0\0\0\x10sidx"],
            vec![ftyp, moov, b"\0\0\0\x01sidx"],
            vec![ftyp, moov, b"\0\0\0\x07free", sidx],
            vec![ftyp, moov, b"\0\0\0\x10free", sidx],
        ] {
            assert!(!has_front_loaded_mp4_index(&atoms.concat()));
        }
    }

    #[test]
    fn volume_percent_to_gain_maps_ui_range() {
        assert_eq!(volume_percent_to_gain(0), 0.0);
        assert_eq!(volume_percent_to_gain(50), 0.5);
        assert_eq!(volume_percent_to_gain(100), 1.0);
    }

    #[test]
    fn stream_fetch_range_uses_aligned_full_chunks() {
        let total = media_cache::STREAM_CHUNK_BYTES * 10;
        let range = stream_fetch_range(3_622_827, total).unwrap();

        assert_eq!(range.start, media_cache::STREAM_CHUNK_BYTES);
        assert_eq!(range.len(), media_cache::STREAM_CHUNK_BYTES);
    }

    #[test]
    fn stream_fetch_range_clamps_at_media_end() {
        let total = media_cache::STREAM_CHUNK_BYTES + 1;
        let range = stream_fetch_range(media_cache::STREAM_CHUNK_BYTES, total).unwrap();

        assert_eq!(range.start, media_cache::STREAM_CHUNK_BYTES);
        assert_eq!(range.end_inclusive, media_cache::STREAM_CHUNK_BYTES);
        assert_eq!(range.len(), 1);
    }

    #[test]
    fn media_range_retry_policy_backs_off_once_for_cdn_throttle() {
        let err = media_cache::MediaRangeError::HttpStatus {
            range: "bytes=0-0".to_string(),
            status: reqwest::StatusCode::FORBIDDEN,
        };

        assert_eq!(
            media_range_retry_delay(&err, 0),
            Some(Duration::from_millis(STREAM_THROTTLE_RETRY_DELAY_MS))
        );
        assert_eq!(media_range_retry_delay(&err, 1), None);
    }

    #[test]
    fn media_range_retry_policy_uses_exponential_backoff_for_server_errors() {
        let err = media_cache::MediaRangeError::HttpStatus {
            range: "bytes=0-0".to_string(),
            status: reqwest::StatusCode::BAD_GATEWAY,
        };

        assert_eq!(
            media_range_retry_delay(&err, 0),
            Some(Duration::from_millis(STREAM_TRANSIENT_RETRY_BASE_MS))
        );
        assert_eq!(
            media_range_retry_delay(&err, 1),
            Some(Duration::from_millis(STREAM_TRANSIENT_RETRY_BASE_MS * 2))
        );
        assert_eq!(media_range_retry_delay(&err, 2), None);
    }

    #[test]
    fn full_fetch_fallback_only_applies_to_small_media_without_metadata() {
        assert!(should_full_fetch_missing_metadata(
            &[],
            STREAM_SMALL_FULL_FETCH_LIMIT_BYTES
        ));
        assert!(!should_full_fetch_missing_metadata(
            &[],
            STREAM_SMALL_FULL_FETCH_LIMIT_BYTES + 1
        ));
        assert!(!should_full_fetch_missing_metadata(
            &[media_cache::ByteRange::new(0, 99).unwrap()],
            10_000
        ));
        assert!(!should_full_fetch_missing_metadata(&[], 0));
    }

    #[test]
    fn missing_metadata_prefetches_head_and_tail_chunks() {
        let total = media_cache::STREAM_CHUNK_BYTES * 5 + 17;
        let ranges = stream_metadata_prefetch_ranges(&[], total).unwrap();

        assert_eq!(ranges.len(), 3);
        assert_eq!(
            ranges[0],
            media_cache::ByteRange::new(0, media_cache::STREAM_CHUNK_BYTES - 1).unwrap()
        );
        assert!(ranges[2].contains(total - 1));
        assert!(ranges[1].start >= total.saturating_sub(media_cache::STREAM_CHUNK_BYTES * 3));
    }

    #[test]
    fn pcm_source_reads_updated_volume_without_restart() {
        let audio = DecodedAudio {
            samples: Arc::from([1.0_f32, 1.0, 1.0]),
            channels: NonZeroU16::new(1).unwrap(),
            sample_rate: NonZeroU32::new(1).unwrap(),
        };
        let volume = Arc::new(AtomicU32::new(1.0_f32.to_bits()));
        let mut source = PcmSource::new(audio, 0, Arc::clone(&volume));

        assert_eq!(source.next(), Some(1.0));
        volume.store(0.25_f32.to_bits(), Ordering::Relaxed);
        assert_eq!(source.next(), Some(0.25));
    }

    #[test]
    fn shared_pcm_reports_duration_and_samples() {
        let pcm = SharedPcm::new();
        pcm.set_spec(
            NonZeroU16::new(2).unwrap(),
            NonZeroU32::new(48_000).unwrap(),
        )
        .unwrap();
        pcm.push_samples(&vec![0.0; 96_000]);

        assert_eq!(pcm.duration_ms(), 1_000);
        assert!(matches!(pcm.sample(0), PcmSample::Ready(_)));
    }

    #[test]
    fn shared_pcm_trims_consumed_samples_and_moves_base() {
        let pcm = SharedPcm::new();
        pcm.set_spec(NonZeroU16::new(1).unwrap(), NonZeroU32::new(10).unwrap())
            .unwrap();
        let samples = (0..100).map(|sample| sample as f32).collect::<Vec<_>>();
        pcm.push_samples(&samples);

        pcm.trim_before(8_000, 2_000);

        assert_eq!(pcm.base_position_ms(), 6_000);
        assert_eq!(pcm.duration_ms(), 4_000);
        assert!(matches!(pcm.sample(0), PcmSample::Ready(sample) if sample == 60.0));
    }

    #[test]
    fn stream_ready_for_position_reuses_buffered_seek_window() {
        assert!(stream_ready_for_position(30_000, 120_000, 45_000, false));
        assert!(!stream_ready_for_position(30_000, 120_000, 39_000, false));
        assert!(stream_ready_for_position(118_000, 120_000, 120_000, true));
    }

    // Original FFmpeg sine fixtures: 317 Hz, 16 kHz mono, ten seconds.
    // The MP3 has no Xing header, and the M4A has a trailing movie header.
    #[test]
    fn original_codec_pieces_decode_like_the_file_at_start_and_seek() {
        use crate::local_audio::{PIECE, Source as AudioSource};
        for ext in ["mp3", "flac", "m4a"] {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join(format!("src/local_audio/fixtures/tone.{ext}"));
            let (d, s) = AudioSource::import(
                &path,
                "test".into(),
                &libp2p::identity::Keypair::generate_ed25519(),
                Arc::new(AtomicBool::new(false)),
                |_| {},
            )
            .unwrap();
            let mut store = crate::local_audio::Store::default();
            store.source(&d, s);
            let reference = decode_audio(std::fs::read(&path).unwrap().into()).unwrap();
            let bytes = SharedBytes::new();
            bytes.set_total(Some(d.info.length));
            for i in 0..d.count() {
                let p = store.read(&d, i).unwrap();
                let start = i * PIECE as u64;
                bytes.append_chunk(StreamingByteChunk {
                    range: media_cache::ByteRange {
                        start,
                        end_inclusive: start + p.bytes.len() as u64 - 1,
                    },
                    bytes: p.bytes.into(),
                });
            }
            for ms in [0, 6323] {
                let pcm = SharedPcm::new();
                let (tx, _rx) = mpsc::unbounded_channel();
                decode_streaming_audio_inner(
                    bytes.clone(),
                    pcm.clone(),
                    DecodeToken::new(),
                    tx,
                    None,
                    "session".into(),
                    d.track(),
                    ms,
                    Arc::new(AtomicU64::new(ms)),
                )
                .unwrap();
                let expected = (ms * d.info.sample_rate as u64 / 1000) as usize;
                let state = pcm.inner.0.lock().unwrap();
                assert!(state.samples.len() > 8192);
                let original = &reference.samples[expected..expected + 8192];
                let difference = original
                    .iter()
                    .zip(&state.samples)
                    .map(|(a, b)| (a - b).abs())
                    .fold(0f32, f32::max);
                assert!(
                    difference <= 1. / 32768.,
                    "{ext} {ms} ms max sample difference {difference}"
                );
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn long_local_wav_starts_and_seeks_with_sample_accurate_partial_download() {
        use crate::local_audio::{self, Client};
        let f = local_audio::tests::wav(600);
        let (d, s) = local_audio::tests::scan(&f);
        let (tx, mut rx) = mpsc::channel::<local_audio::Need>(8);
        let store = Arc::new(Mutex::new(local_audio::Store::default()));
        store.lock().unwrap().source(&d, s);
        let client = Client {
            tx,
            wake: Default::default(),
        };
        let fetched = Arc::new(Mutex::new(Vec::new()));
        let observed = fetched.clone();
        tokio::spawn(async move {
            while let Some(n) = rx.recv().await {
                observed.lock().unwrap().push(n.index);
                let _ = n
                    .reply
                    .send(store.lock().unwrap().read(&n.descriptor, n.index));
            }
        });
        for pos in [0u64, 503_123] {
            fetched.lock().unwrap().clear();
            let bytes = SharedBytes::new();
            bytes.inner.0.lock().unwrap().local = true;
            bytes.set_total(Some(d.info.length));
            let download = tokio::spawn(download_local_bytes(
                client.clone(),
                d.clone(),
                bytes.clone(),
            ));
            let token = DecodeToken::new();
            let stop = token.clone();
            let pcm = SharedPcm::new();
            let decoded = pcm.clone();
            let track = d.track();
            let input = bytes.clone();
            let (tx, mut events) = mpsc::unbounded_channel();
            let decode = tokio::task::spawn_blocking(move || {
                decode_streaming_audio(
                    input,
                    decoded,
                    token,
                    tx,
                    None,
                    "session".into(),
                    track,
                    pos,
                    Arc::new(AtomicU64::new(pos)),
                )
            });
            tokio::time::timeout(Duration::from_secs(10), async {
                while let Some(e) = events.recv().await {
                    match e {
                        AudioPlayerEvent::Prepared {
                            buffered_until_ms, ..
                        } => {
                            assert!(buffered_until_ms >= pos + 12_000);
                            return;
                        }
                        AudioPlayerEvent::Failed { error, .. } => panic!("{error}"),
                        _ => (),
                    }
                }
                panic!("decoder stopped before prepare")
            })
            .await
            .unwrap();
            {
                let p = pcm.inner.0.lock().unwrap();
                assert_eq!(p.base_position_ms, pos);
                let frame = pos * 8;
                for (i, sample) in p.samples.iter().take(512).enumerate() {
                    let expected = ((((frame + i as u64) % 8000) * 17) as i16) as f32 / 32768.;
                    assert_eq!(*sample, expected, "sample {i} at {pos}ms");
                }
            }
            stop.cancel();
            bytes.cancel();
            decode.await.unwrap();
            download.await.unwrap().unwrap();
            let pieces = fetched.lock().unwrap();
            assert!(pieces.len() < d.count() as usize / 4, "{pieces:?}");
            if pos > 0 {
                assert!(pieces.iter().any(|i| *i > 110));
                assert!(!pieces.iter().any(|i| (5..100).contains(i)));
            }
        }
    }

    #[test]
    fn seek_into_rounded_track_end_reuses_the_finished_decoder() {
        let pcm = SharedPcm::new();
        pcm.set_base_position(28_000);
        pcm.set_spec(NonZeroU16::new(1).unwrap(), NonZeroU32::new(10).unwrap())
            .unwrap();
        pcm.push_samples(&[1.0; 13]); // Actual audio ends at 29.3s.
        assert!(!stream_pcm_ready_for_position(&pcm, 29_900, 30_000));
        pcm.complete();
        assert!(stream_pcm_ready_for_position(&pcm, 29_900, 30_000));
        assert!(!stream_pcm_ready_for_position(&pcm, 29_900, 31_000));
        assert!(!stream_pcm_ready_for_position(&pcm, 27_500, 30_000));
        assert_eq!(stream_pcm_cache_window(&pcm, 30_000), (28_000, 29_300));
    }

    #[test]
    fn stream_cache_event_is_not_packet_hot_after_ready() {
        assert!(should_emit_stream_cache_event(
            12_000, 11_500, false, 12_000
        ));
        assert!(!should_emit_stream_cache_event(
            12_100, 12_000, true, 12_000
        ));
        assert!(should_emit_stream_cache_event(13_000, 12_000, true, 12_000));
    }

    #[cfg(not(feature = "fdk-aac-decoder"))]
    #[test]
    fn decoder_backend_defaults_to_native_aac() {
        assert_eq!(decoder_backend_name(), "symphonia-native-aac");
    }

    #[cfg(feature = "fdk-aac-decoder")]
    #[test]
    fn decoder_backend_uses_fdk_feature() {
        assert_eq!(decoder_backend_name(), "fdk-aac");
    }

    #[test]
    fn streaming_source_updates_position_from_consumed_samples() {
        let pcm = SharedPcm::new();
        let channels = NonZeroU16::new(1).unwrap();
        let sample_rate = NonZeroU32::new(10).unwrap();
        pcm.set_spec(channels, sample_rate).unwrap();
        pcm.push_samples(&[1.0; 20]);
        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        let position = Arc::new(AtomicU64::new(0));
        let mut source = StreamingPcmSource::new(
            pcm,
            0,
            channels,
            sample_rate,
            0,
            Arc::new(AtomicU32::new(1.0_f32.to_bits())),
            Arc::clone(&position),
            event_tx,
            "session".to_string(),
            "track".to_string(),
            2_000,
        );

        for _ in 0..10 {
            assert_eq!(source.next(), Some(1.0));
        }

        assert_eq!(position.load(Ordering::Relaxed), 1_000);
    }

    #[test]
    fn streaming_source_position_includes_pcm_base_position() {
        let pcm = SharedPcm::new();
        pcm.set_base_position(60_000);
        let channels = NonZeroU16::new(1).unwrap();
        let sample_rate = NonZeroU32::new(10).unwrap();
        pcm.set_spec(channels, sample_rate).unwrap();
        pcm.push_samples(&[1.0; 20]);
        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        let position = Arc::new(AtomicU64::new(60_000));
        let mut source = StreamingPcmSource::new(
            pcm,
            60_000,
            channels,
            sample_rate,
            60_000,
            Arc::new(AtomicU32::new(1.0_f32.to_bits())),
            Arc::clone(&position),
            event_tx,
            "session".to_string(),
            "track".to_string(),
            120_000,
        );

        for _ in 0..10 {
            assert_eq!(source.next(), Some(1.0));
        }

        assert_eq!(position.load(Ordering::Relaxed), 61_000);
    }

    #[test]
    fn streaming_source_continues_after_pcm_window_is_trimmed() {
        let pcm = SharedPcm::new();
        let channels = NonZeroU16::new(1).unwrap();
        let sample_rate = NonZeroU32::new(10).unwrap();
        pcm.set_spec(channels, sample_rate).unwrap();
        let samples = (0..100).map(|sample| sample as f32).collect::<Vec<_>>();
        pcm.push_samples(&samples);
        let (event_tx, _event_rx) = mpsc::unbounded_channel();
        let position = Arc::new(AtomicU64::new(0));
        let mut source = StreamingPcmSource::new(
            pcm.clone(),
            0,
            channels,
            sample_rate,
            0,
            Arc::new(AtomicU32::new(1.0_f32.to_bits())),
            Arc::clone(&position),
            event_tx,
            "session".to_string(),
            "track".to_string(),
            10_000,
        );

        for _ in 0..50 {
            assert!(source.next().is_some());
        }
        pcm.trim_before(7_000, 2_000);

        assert_eq!(source.next(), Some(50.0));
        assert_eq!(position.load(Ordering::Relaxed), 5_100);
    }

    #[test]
    fn decoder_demand_survives_eviction_outside_estimated_time_window() {
        let bytes = SharedBytes::new();
        let offset = 50_000_000;
        let range = media_cache::ByteRange::new(offset, offset).unwrap();
        bytes.append(&[7], range);
        assert_eq!(
            bytes
                .read_at(offset, &mut [0], &DecodeToken::new())
                .unwrap(),
            1
        );
        bytes.retain_window(0, 10_000);
        assert!(bytes.covers(range));
        assert!(
            stream_byte_range_for_window(0, 30000, 120000, 4_000_000_000).len() <= 8 * 1024 * 1024
        );
    }

    #[test]
    fn shared_bytes_wakes_readers_on_cancel() {
        let bytes = SharedBytes::new();
        bytes.cancel();
        let mut source = StreamingMediaSource::new(bytes, DecodeToken::new());
        let mut buf = [0_u8; 1];

        let err = source.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
    }

    #[test]
    fn shared_bytes_wakes_readers_on_decoder_cancel() {
        let bytes = SharedBytes::new();
        let decoder = DecodeToken::new();
        decoder.cancel();
        let mut source = StreamingMediaSource::new(bytes, decoder);
        let mut buf = [0_u8; 1];

        let err = source.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
    }

    #[test]
    fn shared_bytes_reads_sparse_seek_range() {
        let bytes = SharedBytes::new();
        bytes.set_total(Some(1_000));
        bytes.append(
            &[10, 11, 12, 13],
            media_cache::ByteRange::new(500, 503).unwrap(),
        );
        let mut buf = [0_u8; 2];

        assert_eq!(
            bytes.read_at(501, &mut buf, &DecodeToken::new()).unwrap(),
            2
        );
        assert_eq!(buf, [11, 12]);
    }

    #[test]
    fn shared_bytes_retain_window_drops_old_unpinned_chunks() {
        let bytes = SharedBytes::new();
        bytes.set_total(Some(2_000));
        bytes.append(&[1; 100], media_cache::ByteRange::new(0, 99).unwrap());
        bytes.append(&[2; 100], media_cache::ByteRange::new(500, 599).unwrap());
        bytes.append(
            &[3; 100],
            media_cache::ByteRange::new(1_000, 1_099).unwrap(),
        );

        bytes.retain_window(450, 650);

        assert_eq!(bytes.in_memory_bytes(), 100);
        assert_eq!(
            bytes.first_missing_offset(media_cache::ByteRange::new(500, 700).unwrap()),
            Some(600)
        );
        let mut buf = [0_u8; 2];
        assert_eq!(
            bytes.read_at(500, &mut buf, &DecodeToken::new()).unwrap(),
            2
        );
        assert_eq!(buf, [2, 2]);
    }

    #[test]
    fn shared_bytes_retain_window_keeps_pinned_metadata_chunks() {
        let bytes = SharedBytes::new();
        bytes.set_total(Some(2_000));
        bytes.pin_ranges(&[media_cache::ByteRange::new(0, 9).unwrap()]);
        bytes.append(&[1; 100], media_cache::ByteRange::new(0, 99).unwrap());
        bytes.append(&[2; 100], media_cache::ByteRange::new(500, 599).unwrap());

        bytes.retain_window(450, 650);

        assert_eq!(bytes.in_memory_bytes(), 200);
        let mut buf = [0_u8; 2];
        assert_eq!(bytes.read_at(0, &mut buf, &DecodeToken::new()).unwrap(), 2);
        assert_eq!(buf, [1, 1]);
    }

    #[test]
    fn shared_bytes_keeps_multiple_requested_offsets_in_order() {
        let bytes = SharedBytes::new();
        let total = media_cache::STREAM_CHUNK_BYTES * 4;
        bytes.set_total(Some(total));

        bytes.request_offset(media_cache::STREAM_CHUNK_BYTES + 500);
        bytes.request_offset(20);
        bytes.request_offset(media_cache::STREAM_CHUNK_BYTES + 900);

        assert_eq!(
            bytes.take_requested_offset(total),
            Some(media_cache::STREAM_CHUNK_BYTES)
        );
        assert_eq!(bytes.take_requested_offset(total), Some(0));
        assert_eq!(bytes.take_requested_offset(total), None);

        bytes.request_offset(40);
        bytes.request_offset(media_cache::STREAM_CHUNK_BYTES * 2);
        bytes.append(
            &[1, 2, 3, 4],
            media_cache::ByteRange::new(0, media_cache::STREAM_CHUNK_BYTES - 1).unwrap(),
        );
        assert_eq!(
            bytes.take_requested_offset(total),
            Some(media_cache::STREAM_CHUNK_BYTES * 2)
        );
    }

    #[test]
    fn shared_bytes_prioritizes_seek_offset_over_stale_requests() {
        let bytes = SharedBytes::new();
        let total = media_cache::STREAM_CHUNK_BYTES * 5;
        bytes.set_total(Some(total));

        bytes.request_offset(media_cache::STREAM_CHUNK_BYTES + 100);
        bytes.request_offset(media_cache::STREAM_CHUNK_BYTES * 2 + 200);
        bytes.prioritize_offset(media_cache::STREAM_CHUNK_BYTES * 3 + 800);

        assert_eq!(
            bytes.take_requested_offset(total),
            Some(media_cache::STREAM_CHUNK_BYTES * 3)
        );
        assert_eq!(
            bytes.take_requested_offset(total),
            Some(media_cache::STREAM_CHUNK_BYTES)
        );
        assert_eq!(
            bytes.take_requested_offset(total),
            Some(media_cache::STREAM_CHUNK_BYTES * 2)
        );

        bytes.request_offset(media_cache::STREAM_CHUNK_BYTES * 4 + 900);
        bytes.prioritize_offset(media_cache::STREAM_CHUNK_BYTES * 4 + 1_000);
        assert_eq!(
            bytes.take_requested_offset(total),
            Some(media_cache::STREAM_CHUNK_BYTES * 4)
        );
        assert_eq!(bytes.take_requested_offset(total), None);
    }

    #[test]
    fn shared_bytes_deduplicates_decoder_offsets_by_network_chunk() {
        let bytes = SharedBytes::new();
        let total = media_cache::STREAM_CHUNK_BYTES * 2;
        bytes.set_total(Some(total));

        bytes.request_offset(65_535);
        bytes.request_offset(131_070);
        bytes.request_offset(media_cache::STREAM_CHUNK_BYTES + 5);
        bytes.request_offset(media_cache::STREAM_CHUNK_BYTES + 65_535);

        assert_eq!(bytes.take_requested_offset(total), Some(0));
        assert_eq!(
            bytes.take_requested_offset(total),
            Some(media_cache::STREAM_CHUNK_BYTES)
        );
        assert_eq!(bytes.take_requested_offset(total), None);
    }

    #[test]
    fn shared_bytes_caps_pending_offsets_and_keeps_seek_priority() {
        let bytes = SharedBytes::new();
        let total = media_cache::STREAM_CHUNK_BYTES * 12;
        bytes.set_total(Some(total));

        for index in 0..(STREAM_MAX_PENDING_OFFSETS + 3) {
            bytes.request_offset(media_cache::STREAM_CHUNK_BYTES * index as u64);
        }
        bytes.prioritize_offset(media_cache::STREAM_CHUNK_BYTES * 10);

        assert_eq!(
            bytes.take_requested_offset(total),
            Some(media_cache::STREAM_CHUNK_BYTES * 10)
        );
        for index in 0..(STREAM_MAX_PENDING_OFFSETS - 1) {
            assert_eq!(
                bytes.take_requested_offset(total),
                Some(media_cache::STREAM_CHUNK_BYTES * index as u64)
            );
        }
        assert_eq!(bytes.take_requested_offset(total), None);
    }

    #[test]
    fn shared_bytes_returns_eof_at_total_even_with_sparse_gaps() {
        let bytes = SharedBytes::new();
        bytes.set_total(Some(1_000));
        bytes.append(
            &[10, 11, 12, 13],
            media_cache::ByteRange::new(996, 999).unwrap(),
        );
        let mut buf = [0_u8; 2];

        assert_eq!(
            bytes.read_at(1_000, &mut buf, &DecodeToken::new()).unwrap(),
            0
        );
    }

    #[test]
    fn selecting_a_missing_output_reports_failure_before_opening_the_default() {
        let (events, _rx) = mpsc::unbounded_channel();
        let result = open_output(Some("missing-device-for-test"), false, events);
        assert!(
            result
                .err()
                .is_some_and(|error| error.to_string().contains("no longer connected"))
        );
    }

    #[test]
    fn output_device_change_tracks_default_output_id_changes() {
        let mut previous = Some("speaker".to_string());

        assert_eq!(
            output_device_change(&mut previous, Some("speaker".to_string())),
            None
        );
        assert_eq!(
            output_device_change(&mut previous, Some("headphones".to_string())),
            Some(OutputDeviceChange {
                previous: Some("speaker".to_string()),
                current: Some("headphones".to_string()),
            })
        );
        assert_eq!(previous, Some("headphones".to_string()));
        assert_eq!(
            output_device_change(&mut previous, None),
            Some(OutputDeviceChange {
                previous: Some("headphones".to_string()),
                current: None,
            })
        );
        assert_eq!(previous, None);
    }

    #[test]
    fn pcm_ready_requires_target_inside_current_window() {
        let pcm = SharedPcm::new();
        pcm.set_base_position(60_000);
        pcm.set_spec(NonZeroU16::new(1).unwrap(), NonZeroU32::new(1_000).unwrap())
            .unwrap();
        pcm.push_samples(&vec![0.0; 20_000]);

        assert!(stream_pcm_ready_for_position(&pcm, 62_000, 120_000));
        assert!(!stream_pcm_ready_for_position(&pcm, 30_000, 120_000));
        assert!(!stream_pcm_ready_for_position(&pcm, 79_000, 120_000));
    }
}
