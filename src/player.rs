use std::{
    fs::{self, OpenOptions},
    io::{Cursor, Read, Seek, SeekFrom, Write},
    num::{NonZeroU16, NonZeroU32},
    path::PathBuf,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use rodio::{DeviceSinkBuilder, MixerDeviceSink, Player, Source};
use symphonia::{
    core::{
        codecs::{
            CodecParameters,
            audio::{AudioDecoderOptions, CODEC_ID_NULL_AUDIO},
        },
        errors::Error as SymphoniaError,
        formats::{FormatOptions, probe::Hint},
        io::{MediaSource, MediaSourceStream},
        meta::MetadataOptions,
    },
    default::{get_codecs, get_probe},
};
use tokio::sync::mpsc;

use crate::{
    core::{PlaybackCacheStatus, PlaybackCacheView, PlaybackTrack},
    media_cache,
};

pub struct AudioPlayer {
    stream: MixerDeviceSink,
    sink: Option<Player>,
    track_id: Option<String>,
    session_id: Option<String>,
    audio: Option<DecodedAudio>,
    streaming: Option<StreamingSession>,
    position_ms: u64,
    started_at_micros: i64,
    playing: bool,
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
    cursor: u64,
}

#[derive(Clone)]
struct SharedBytes {
    inner: Arc<(Mutex<StreamingBytesState>, Condvar)>,
}

struct StreamingBytesState {
    bytes: Vec<u8>,
    total_bytes: Option<u64>,
    complete: bool,
    canceled: bool,
    error: Option<String>,
    ranges: media_cache::RangeIndex,
}

#[derive(Clone)]
struct SharedPcm {
    inner: Arc<(Mutex<StreamingPcmState>, Condvar)>,
}

struct StreamingPcmState {
    samples: Vec<f32>,
    channels: Option<NonZeroU16>,
    sample_rate: Option<NonZeroU32>,
    complete: bool,
    canceled: bool,
    error: Option<String>,
}

struct StreamingSession {
    session_id: String,
    track_id: String,
    duration_ms: u64,
    pcm: SharedPcm,
    bytes: SharedBytes,
    position_ms: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub enum AudioPlayerEvent {
    Prepared {
        operation_id: Option<String>,
        session_id: String,
        track_id: String,
        buffered_until_ms: u64,
    },
    Cache(PlaybackCacheView),
    Buffering {
        session_id: String,
        track_id: String,
        buffered_until_ms: u64,
    },
    Failed {
        operation_id: Option<String>,
        session_id: String,
        track_id: String,
        title: String,
        error: String,
    },
    Ended {
        session_id: String,
        track_id: String,
    },
}

impl AudioPlayer {
    pub fn new() -> Result<Self> {
        let stream = open_default_output()?;
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        Ok(Self {
            stream,
            sink: None,
            track_id: None,
            session_id: None,
            audio: None,
            streaming: None,
            position_ms: 0,
            started_at_micros: 0,
            playing: false,
            volume: Arc::new(AtomicU32::new(volume_percent_to_gain(100).to_bits())),
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
        let position_ms = position_ms.min(track.duration_ms);
        if let Some(session) = self.streaming.as_ref().filter(|session| {
            session.session_id == session_id && session.track_id == track.track_id
        }) {
            self.track_id = Some(track.track_id.clone());
            self.session_id = Some(session_id.clone());
            self.position_ms = position_ms;
            self.started_at_micros = 0;
            self.playing = false;
            session.position_ms.store(position_ms, Ordering::Relaxed);

            let buffered_until_ms = session.pcm.duration_ms().min(track.duration_ms);
            if stream_ready_for_position(
                position_ms,
                track.duration_ms,
                buffered_until_ms,
                session.pcm.is_complete(),
            ) {
                let _ = self.event_tx.send(AudioPlayerEvent::Prepared {
                    operation_id: operation_id.clone(),
                    session_id: session_id.clone(),
                    track_id: track.track_id.clone(),
                    buffered_until_ms,
                });
                let _ = self
                    .event_tx
                    .send(AudioPlayerEvent::Cache(PlaybackCacheView {
                        session_id,
                        track_id: track.track_id,
                        status: PlaybackCacheStatus::Ready,
                        buffered_until_ms,
                        duration_ms: track.duration_ms,
                        error: None,
                    }));
                return Ok(());
            }

            let _ = self
                .event_tx
                .send(AudioPlayerEvent::Cache(PlaybackCacheView {
                    session_id,
                    track_id: track.track_id,
                    status: PlaybackCacheStatus::Buffering,
                    buffered_until_ms,
                    duration_ms: track.duration_ms,
                    error: None,
                }));
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

        let bytes = SharedBytes::new();
        let pcm = SharedPcm::new();
        let position = Arc::new(AtomicU64::new(self.position_ms));
        let session = StreamingSession {
            session_id: session_id.clone(),
            track_id: track.track_id.clone(),
            duration_ms: track.duration_ms,
            pcm: pcm.clone(),
            bytes: bytes.clone(),
            position_ms: Arc::clone(&position),
        };
        self.streaming = Some(session);

        let event_tx = self.event_tx.clone();
        let download_client = client.clone();
        let download_track = track.clone();
        let download_bytes = bytes.clone();
        let download_fail_bytes = bytes.clone();
        let download_operation_id = operation_id.clone();
        let download_session_id = session_id.clone();
        tokio::spawn(async move {
            if let Err(err) =
                download_streaming_bytes(download_client, download_track.clone(), download_bytes)
                    .await
            {
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
        tokio::task::spawn_blocking(move || {
            decode_streaming_audio(
                decode_bytes,
                decode_pcm,
                decode_tx,
                operation_id,
                session_id,
                track,
                position_ms,
            );
        });

        Ok(())
    }

    pub fn drain_events(&mut self) -> Vec<AudioPlayerEvent> {
        let mut events = Vec::new();
        while let Ok(event) = self.event_rx.try_recv() {
            events.push(event);
        }
        events
    }

    pub fn seek(&mut self, position_ms: u64, playing: bool, now_micros: i64) -> Result<()> {
        if self.streaming.is_some() {
            return self.restart_streaming(position_ms, playing);
        }
        self.restart(position_ms, playing, now_micros)
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
                self.restart_streaming(self.position_ms(now_micros), true)?;
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
        let stream = open_default_output()?;
        self.stream = stream;
        Ok(())
    }
}

fn open_default_output() -> Result<MixerDeviceSink> {
    DeviceSinkBuilder::open_default_sink().context("failed to open default audio output")
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
    buffered_until_ms >= ready_until || (complete && buffered_until_ms >= position_ms)
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
        volume: Arc<AtomicU32>,
        position: Arc<AtomicU64>,
        event_tx: mpsc::UnboundedSender<AudioPlayerEvent>,
        session_id: String,
        track_id: String,
        duration_ms: u64,
    ) -> Self {
        let frames = (position_ms as u128)
            .saturating_mul(sample_rate.get() as u128)
            .saturating_div(1000);
        let pos = frames
            .saturating_mul(channels.get() as u128)
            .min(usize::MAX as u128) as usize;
        Self {
            shared,
            pos: pos - pos % channels.get() as usize,
            channels,
            sample_rate,
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
        self.position_ms
            .store(position_ms.min(self.duration_ms), Ordering::Relaxed);
    }
}

impl Iterator for StreamingPcmSource {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
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
                        let buffered_until_ms = self.shared.duration_ms().min(self.duration_ms);
                        let _ = self.event_tx.send(AudioPlayerEvent::Buffering {
                            session_id: self.session_id.clone(),
                            track_id: self.track_id.clone(),
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
    fn new(shared: SharedBytes) -> Self {
        Self { shared, cursor: 0 }
    }
}

impl Read for StreamingMediaSource {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes = self.shared.read_at(self.cursor, buf)?;
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
                let len = self.shared.total_bytes().ok_or_else(|| {
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
        true
    }

    fn byte_len(&self) -> Option<u64> {
        self.shared.total_bytes()
    }
}

impl SharedBytes {
    fn new() -> Self {
        Self {
            inner: Arc::new((
                Mutex::new(StreamingBytesState {
                    bytes: Vec::new(),
                    total_bytes: None,
                    complete: false,
                    canceled: false,
                    error: None,
                    ranges: media_cache::RangeIndex::default(),
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
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        if state.bytes.len() as u64 == range.start {
            state.bytes.extend_from_slice(bytes);
            state.ranges.insert(range);
        }
        condvar.notify_all();
    }

    fn fail(&self, error: String) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        state.error = Some(error);
        condvar.notify_all();
    }

    fn complete(&self) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        state.complete = true;
        condvar.notify_all();
    }

    fn cancel(&self) {
        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        state.canceled = true;
        condvar.notify_all();
    }

    fn is_canceled(&self) -> bool {
        let (lock, _) = &*self.inner;
        lock.lock().expect("streaming bytes lock poisoned").canceled
    }

    fn total_bytes(&self) -> Option<u64> {
        let (lock, _) = &*self.inner;
        lock.lock()
            .expect("streaming bytes lock poisoned")
            .total_bytes
    }

    fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        let (lock, condvar) = &*self.inner;
        let mut state = lock.lock().expect("streaming bytes lock poisoned");
        loop {
            if state.canceled {
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
            if offset < state.bytes.len() as u64 {
                let start = offset as usize;
                let available = state.bytes.len().saturating_sub(start);
                let len = available.min(buf.len());
                buf[..len].copy_from_slice(&state.bytes[start..start + len]);
                return Ok(len);
            }
            if state.complete {
                return Ok(0);
            }
            state = condvar
                .wait(state)
                .expect("streaming bytes lock poisoned while waiting");
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
                    complete: false,
                    canceled: false,
                    error: None,
                }),
                Condvar::new(),
            )),
        }
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
        self.bytes.cancel();
        self.pcm.cancel();
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
        .probe(&hint, mss, format_opts, metadata_opts)
        .context("failed to probe audio format")?;

    let mut format = probed;
    let track = format
        .tracks()
        .iter()
        .find(|track| {
            matches!(
                track.codec_params,
                Some(CodecParameters::Audio(ref params))
                    if params.codec != CODEC_ID_NULL_AUDIO
            )
        })
        .ok_or_else(|| anyhow!("audio stream does not contain a supported track"))?;
    let track_id = track.id;
    let codec_params = match track.codec_params.clone() {
        Some(CodecParameters::Audio(params)) => params,
        _ => bail!("audio stream does not contain audio codec parameters"),
    };
    let decoder_opts = AudioDecoderOptions::default();
    let mut decoder = get_codecs()
        .make_audio_decoder(&codec_params, &decoder_opts)
        .context("failed to create audio decoder")?;

    let mut channels = None;
    let mut sample_rate = None;
    let mut samples = Vec::new();
    let mut decode_errors = 0usize;
    let mut first_decode_error = None;

    loop {
        let packet = match format.next_packet() {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
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

        if packet.track_id != track_id {
            continue;
        }

        match decoder.decode(&packet) {
            Ok(decoded) => {
                let spec = decoded.spec();
                let packet_channels = NonZeroU16::new(spec.channels().count() as u16)
                    .ok_or_else(|| anyhow!("decoded audio has invalid channel count"))?;
                let packet_sample_rate = NonZeroU32::new(spec.rate())
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

                let mut packet_samples = Vec::new();
                decoded.copy_to_vec_interleaved::<f32>(&mut packet_samples);
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
    shared: SharedBytes,
) -> Result<()> {
    let root = media_cache::cache_root();
    fs::create_dir_all(root.join(media_cache::cache_key(&track)))
        .context("failed to create media cache directory")?;
    let media_path = media_cache::media_file_path(&root, &track);

    let probe = media_cache::probe_range_support(&client, &track).await?;
    let total = probe
        .total_bytes
        .ok_or_else(|| anyhow!("media range probe did not report total length"))?;
    shared.set_total(Some(total));

    let mut start = 0;
    if media_path.exists() {
        let existing = fs::read(&media_path).unwrap_or_default();
        let len = (existing.len() as u64).min(total);
        if len > 0 {
            shared.append(
                &existing[..len as usize],
                media_cache::ByteRange::new(0, len.saturating_sub(1))?,
            );
            start = len;
        }
    }

    while start < total {
        if shared.is_canceled() {
            return Ok(());
        }
        let end = start
            .saturating_add(media_cache::STREAM_CHUNK_BYTES)
            .saturating_sub(1)
            .min(total.saturating_sub(1));
        let range = media_cache::ByteRange::new(start, end)?;
        let (bytes, _) = media_cache::fetch_range_bytes(&client, &track, range).await?;
        write_cache_chunk(&media_path, start, &bytes)?;
        shared.append(&bytes, range);
        start = end.saturating_add(1);
        let _ = media_cache::evict_cache(&root, media_cache::MAX_CACHE_BYTES);
    }

    shared.complete();
    Ok(())
}

fn write_cache_chunk(path: &PathBuf, start: u64, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(start == 0)
        .append(start != 0)
        .open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

fn decode_streaming_audio(
    bytes: SharedBytes,
    pcm: SharedPcm,
    event_tx: mpsc::UnboundedSender<AudioPlayerEvent>,
    operation_id: Option<String>,
    session_id: String,
    track: PlaybackTrack,
    position_ms: u64,
) {
    if let Err(err) = decode_streaming_audio_inner(
        bytes,
        pcm.clone(),
        event_tx.clone(),
        operation_id.clone(),
        session_id.clone(),
        track.clone(),
        position_ms,
    ) {
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

fn decode_streaming_audio_inner(
    bytes: SharedBytes,
    pcm: SharedPcm,
    event_tx: mpsc::UnboundedSender<AudioPlayerEvent>,
    operation_id: Option<String>,
    session_id: String,
    track: PlaybackTrack,
    position_ms: u64,
) -> Result<()> {
    let source = StreamingMediaSource::new(bytes);
    let mss = MediaSourceStream::new(Box::new(source), Default::default());

    let mut hint = Hint::new();
    hint.with_extension("m4a");

    let format_opts = FormatOptions::default();
    let metadata_opts = MetadataOptions::default();
    let probed = get_probe()
        .probe(&hint, mss, format_opts, metadata_opts)
        .context("failed to probe streaming audio format")?;

    let mut format = probed;
    let track_info = format
        .tracks()
        .iter()
        .find(|track| {
            matches!(
                track.codec_params,
                Some(CodecParameters::Audio(ref params))
                    if params.codec != CODEC_ID_NULL_AUDIO
            )
        })
        .ok_or_else(|| anyhow!("streaming audio does not contain a supported track"))?;
    let track_id = track_info.id;
    let codec_params = match track_info.codec_params.clone() {
        Some(CodecParameters::Audio(params)) => params,
        _ => bail!("streaming audio does not contain audio codec parameters"),
    };
    let decoder_opts = AudioDecoderOptions::default();
    let mut decoder = get_codecs()
        .make_audio_decoder(&codec_params, &decoder_opts)
        .context("failed to create streaming audio decoder")?;

    let ready_until = position_ms
        .saturating_add(media_cache::READY_WINDOW_MS)
        .min(track.duration_ms);
    let mut ready_sent = false;
    let mut last_cache_event_ms: u64 = 0;
    let mut decode_errors = 0usize;
    let mut first_decode_error = None;

    loop {
        let packet = match format.next_packet() {
            Ok(Some(packet)) => packet,
            Ok(None) => break,
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

        if packet.track_id != track_id {
            continue;
        }

        match decoder.decode(&packet) {
            Ok(decoded) => {
                let spec = decoded.spec();
                let channels = NonZeroU16::new(spec.channels().count() as u16)
                    .ok_or_else(|| anyhow!("decoded streaming audio has invalid channel count"))?;
                let sample_rate = NonZeroU32::new(spec.rate())
                    .ok_or_else(|| anyhow!("decoded streaming audio has invalid sample rate"))?;
                pcm.set_spec(channels, sample_rate)?;

                let mut packet_samples = Vec::new();
                decoded.copy_to_vec_interleaved::<f32>(&mut packet_samples);
                pcm.push_samples(&packet_samples);
                decode_errors = 0;

                let buffered_until_ms = pcm.duration_ms().min(track.duration_ms);
                if buffered_until_ms >= last_cache_event_ms.saturating_add(1000)
                    || buffered_until_ms >= ready_until
                {
                    last_cache_event_ms = buffered_until_ms;
                    let _ = event_tx.send(AudioPlayerEvent::Cache(PlaybackCacheView {
                        session_id: session_id.clone(),
                        track_id: track.track_id.clone(),
                        status: if ready_sent {
                            PlaybackCacheStatus::Ready
                        } else {
                            PlaybackCacheStatus::Preparing
                        },
                        buffered_until_ms,
                        duration_ms: track.duration_ms,
                        error: None,
                    }));
                }

                if !ready_sent && buffered_until_ms >= ready_until {
                    ready_sent = true;
                    let _ = event_tx.send(AudioPlayerEvent::Prepared {
                        operation_id: operation_id.clone(),
                        session_id: session_id.clone(),
                        track_id: track.track_id.clone(),
                        buffered_until_ms,
                    });
                    let _ = event_tx.send(AudioPlayerEvent::Cache(PlaybackCacheView {
                        session_id: session_id.clone(),
                        track_id: track.track_id.clone(),
                        status: PlaybackCacheStatus::Ready,
                        buffered_until_ms,
                        duration_ms: track.duration_ms,
                        error: None,
                    }));
                }
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
    let buffered_until_ms = pcm.duration_ms().min(track.duration_ms);
    if !ready_sent && buffered_until_ms >= position_ms.min(track.duration_ms) {
        let _ = event_tx.send(AudioPlayerEvent::Prepared {
            operation_id,
            session_id: session_id.clone(),
            track_id: track.track_id.clone(),
            buffered_until_ms,
        });
    }
    let _ = event_tx.send(AudioPlayerEvent::Cache(PlaybackCacheView {
        session_id,
        track_id: track.track_id,
        status: PlaybackCacheStatus::Ready,
        buffered_until_ms,
        duration_ms: track.duration_ms,
        error: None,
    }));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn volume_percent_to_gain_maps_ui_range() {
        assert_eq!(volume_percent_to_gain(0), 0.0);
        assert_eq!(volume_percent_to_gain(50), 0.5);
        assert_eq!(volume_percent_to_gain(100), 1.0);
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
    fn stream_ready_for_position_reuses_buffered_seek_window() {
        assert!(stream_ready_for_position(30_000, 120_000, 45_000, false));
        assert!(!stream_ready_for_position(30_000, 120_000, 39_000, false));
        assert!(stream_ready_for_position(118_000, 120_000, 120_000, true));
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
    fn shared_bytes_wakes_readers_on_cancel() {
        let bytes = SharedBytes::new();
        bytes.cancel();
        let mut source = StreamingMediaSource::new(bytes);
        let mut buf = [0_u8; 1];

        let err = source.read(&mut buf).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::Interrupted);
    }
}
