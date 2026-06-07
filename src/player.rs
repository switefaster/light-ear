use std::{
    io::{Cursor, Read, Seek, SeekFrom},
    num::{NonZeroU16, NonZeroU32},
    sync::Arc,
    time::Duration,
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

pub struct AudioPlayer {
    stream: MixerDeviceSink,
    sink: Option<Player>,
    track_id: Option<String>,
    audio: Option<DecodedAudio>,
    position_ms: u64,
    started_at_micros: i64,
    playing: bool,
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
}

struct MemoryMediaSource {
    cursor: Cursor<Arc<[u8]>>,
    byte_len: u64,
}

impl AudioPlayer {
    pub fn new() -> Result<Self> {
        let stream = open_default_output()?;
        Ok(Self {
            stream,
            sink: None,
            track_id: None,
            audio: None,
            position_ms: 0,
            started_at_micros: 0,
            playing: false,
        })
    }

    pub fn current_track_id(&self) -> Option<&str> {
        self.track_id.as_deref()
    }

    pub fn position_ms(&self, now_micros: i64) -> u64 {
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

        let Some(audio) = &self.audio else {
            return false;
        };

        self.position_ms(now_micros) >= audio.duration_ms()
    }

    pub fn is_playing(&self) -> bool {
        self.playing
    }

    pub fn stop(&mut self) {
        if let Some(sink) = self.sink.take() {
            sink.stop();
        }

        self.track_id = None;
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
        let decoded = decode_audio(audio).context("failed to decode audio")?;
        self.track_id = Some(track_id);
        self.audio = Some(decoded);
        self.restart(position_ms, playing, now_micros)
    }

    pub fn seek(&mut self, position_ms: u64, playing: bool, now_micros: i64) -> Result<()> {
        self.restart(position_ms, playing, now_micros)
    }

    pub fn set_playing(&mut self, playing: bool, now_micros: i64) -> Result<()> {
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
        let source = PcmSource::new(audio, position_ms);
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
    fn new(audio: DecodedAudio, position_ms: u64) -> Self {
        let pos = audio.position_to_sample_index(position_ms);
        Self { audio, pos }
    }
}

impl Iterator for PcmSource {
    type Item = f32;

    fn next(&mut self) -> Option<Self::Item> {
        let sample = *self.audio.samples.get(self.pos)?;
        self.pos += 1;
        Some(sample)
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
            Err(SymphoniaError::DecodeError(_)) => {
                decode_errors += 1;
                if decode_errors > 3 {
                    bail!("too many audio decode errors");
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
