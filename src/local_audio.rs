//! Room-scoped, independently verifiable pieces of original audio files.
#[cfg(test)]
pub(crate) mod tests;
use crate::core::PlaybackTrack;
use anyhow::{Result, anyhow, bail, ensure};
use libp2p::identity;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use symphonia::core::{formats::FormatOptions, io::MediaSourceStream, probe::Hint};
use tokio::sync::{mpsc, oneshot};

pub(crate) const PIECE: usize = 64 * 1024;
const CACHE_LIMIT: usize = 512 * 1024 * 1024;
pub(crate) type Hash = [u8; 32];
pub(crate) type PieceKey = (String, Hash, u64);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AudioInfo {
    pub room: String,
    pub owner: String,
    pub title: String,
    pub format: String,
    pub duration_ms: u64,
    pub sample_rate: u32,
    pub length: u64,
    pub root: Hash,
    /// [sample-frame timestamp, absolute byte offset] for independently decodable seek starts.
    pub seek_points: Vec<[u64; 2]>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Descriptor {
    pub info: AudioInfo,
    public_key: Vec<u8>,
    signature: Vec<u8>,
}
impl Descriptor {
    fn signing_bytes(info: &AudioInfo) -> Vec<u8> {
        let mut bytes = b"light-ear.local-audio/1\0".to_vec();
        serde_json::to_writer(&mut bytes, info).expect("serializable manifest");
        bytes
    }
    pub(crate) fn valid(&self, room: &str) -> bool {
        let i = &self.info;
        i.room == room
            && crate::directory::valid_room(room)
            && (1..=1 << 40).contains(&i.length)
            && i.duration_ms > 0
            && i.duration_ms <= u64::from(u32::MAX) * 1000
            && i.sample_rate > 0
            && !i.title.is_empty()
            && i.title.len() <= 1024
            && i.seek_points.len() <= 96
            && i.seek_points.iter().all(|p| p[1] < i.length)
            && i.seek_points
                .windows(2)
                .all(|w| w[0][0] < w[1][0] && w[0][1] < w[1][1])
            && matches!(i.format.as_str(), "mp3" | "wav" | "flac" | "m4a")
            && identity::PublicKey::try_decode_protobuf(&self.public_key).is_ok_and(|key| {
                key.to_peer_id().to_string() == i.owner
                    && key.verify(&Self::signing_bytes(i), &self.signature)
            })
    }
    pub(crate) fn track(&self) -> PlaybackTrack {
        PlaybackTrack {
            track_id: self.id(),
            title: self.info.title.clone(),
            source_kind: "local".into(),
            bvid: String::new(),
            part: 0,
            duration_ms: self.info.duration_ms,
            audio_url: String::new(),
            referer: String::new(),
            media_init_range: None,
            media_index_range: None,
            media_is_progressive: false,
            local_audio: Some(self.clone()),
        }
    }
    fn id(&self) -> String {
        format!(
            "local-{:x}",
            Sha256::digest(Self::signing_bytes(&self.info))
        )
    }
    pub(crate) fn key(&self, index: u64) -> PieceKey {
        (self.info.room.clone(), self.info.root, index)
    }
    pub(crate) fn count(&self) -> u64 {
        self.info.length.div_ceil(PIECE as u64)
    }
}
pub(crate) fn valid_track(t: &PlaybackTrack, room: &str, requester: Option<&str>) -> bool {
    match &t.local_audio {
        None => t.source_kind != "local",
        Some(d) => {
            t.source_kind == "local"
                && t.audio_url.is_empty()
                && t.referer.is_empty()
                && d.valid(room)
                && t.track_id == d.id()
                && t.title == d.info.title
                && t.duration_ms == d.info.duration_ms
                && requester.is_none_or(|p| p == d.info.owner)
        }
    }
}

fn leaf(bytes: &[u8]) -> Hash {
    Sha256::new()
        .chain_update([0])
        .chain_update(bytes)
        .finalize()
        .into()
}
fn node(a: Hash, b: Hash) -> Hash {
    Sha256::new()
        .chain_update([1])
        .chain_update(a)
        .chain_update(b)
        .finalize()
        .into()
}
#[derive(Clone, Debug)]
pub(crate) struct Piece {
    pub bytes: Vec<u8>,
    pub proof: Vec<Hash>,
}
impl Piece {
    pub(crate) fn valid(&self, d: &Descriptor, mut i: u64) -> bool {
        let mut n = d.count();
        if i >= n || self.bytes.len() as u64 != (d.info.length - i * PIECE as u64).min(PIECE as u64)
        {
            return false;
        }
        let mut h = leaf(&self.bytes);
        let mut proof = self.proof.iter();
        while n > 1 {
            if i % 2 == 1 || i + 1 < n {
                let Some(sibling) = proof.next() else {
                    return false;
                };
                h = if i % 2 == 1 {
                    node(*sibling, h)
                } else {
                    node(h, *sibling)
                };
            }
            i /= 2;
            n = n.div_ceil(2);
        }
        proof.next().is_none() && h == d.info.root
    }
}
/// The demuxer and the sequential scanner share the current page and the leaf index.
/// A tail metadata seek can fill any leaf before the scanner reaches it.
struct IndexInput {
    file: File,
    index: File,
    length: u64,
    present: Vec<u64>,
    page: Vec<u8>,
    page_index: Option<u64>,
    canceled: Arc<AtomicBool>,
}
impl IndexInput {
    fn load(&mut self, i: u64) -> std::io::Result<()> {
        if self.canceled.load(Ordering::Relaxed) {
            return Err(std::io::Error::other("import canceled"));
        }
        if self.page_index == Some(i) {
            return Ok(());
        }
        let start = i * PIECE as u64;
        self.file.seek(SeekFrom::Start(start))?;
        self.page
            .resize((self.length - start).min(PIECE as u64) as usize, 0);
        self.file.read_exact(&mut self.page)?;
        let word = &mut self.present[(i / 64) as usize];
        let mask = 1u64 << (i % 64);
        if *word & mask == 0 {
            self.index.seek(SeekFrom::Start(i * 32))?;
            self.index.write_all(&leaf(&self.page))?;
            *word |= mask;
        }
        self.page_index = Some(i);
        Ok(())
    }
}
struct ScanInput {
    input: Arc<Mutex<IndexInput>>,
    pos: u64,
    length: u64,
    seekable: bool,
}
impl Read for ScanInput {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.pos >= self.length || buf.is_empty() {
            return Ok(0);
        }
        let mut input = self.input.lock().unwrap();
        input.load(self.pos / PIECE as u64)?;
        let offset = (self.pos % PIECE as u64) as usize;
        let n = buf.len().min(input.page.len() - offset);
        buf[..n].copy_from_slice(&input.page[offset..offset + n]);
        self.pos += n as u64;
        Ok(n)
    }
}
impl Seek for ScanInput {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = match pos {
            SeekFrom::Start(n) => Some(n),
            SeekFrom::End(n) => self.length.checked_add_signed(n),
            SeekFrom::Current(n) => self.pos.checked_add_signed(n),
        }
        .ok_or(std::io::ErrorKind::InvalidInput)?;
        Ok(self.pos)
    }
}
impl symphonia::core::io::MediaSource for ScanInput {
    fn is_seekable(&self) -> bool {
        self.seekable
    }
    fn byte_len(&self) -> Option<u64> {
        self.seekable.then_some(self.length)
    }
}
#[derive(Debug)]
pub(crate) struct Source {
    data: File,
    index: File,
    levels: Vec<(u64, u64)>,
}
fn hash_at(f: &mut File, offset: u64) -> Result<Hash> {
    let mut h = [0; 32];
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(&mut h)?;
    Ok(h)
}
impl Source {
    pub(crate) fn import(
        path: &Path,
        room: String,
        key: &identity::Keypair,
        canceled: Arc<AtomicBool>,
        mut progress: impl FnMut(u8),
    ) -> Result<(Descriptor, Self)> {
        let data = File::open(path)?;
        let metadata = data.metadata()?;
        let length = metadata.len();
        ensure!((1..=1 << 40).contains(&length), "unsupported audio length");
        let format = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        ensure!(
            matches!(format.as_str(), "mp3" | "wav" | "flac" | "m4a"),
            "unsupported audio format"
        );
        let mut hint = Hint::new();
        hint.with_extension(&format);
        let count = length.div_ceil(PIECE as u64);
        let indexed = matches!(format.as_str(), "mp3" | "flac");
        let scan = Arc::new(Mutex::new(IndexInput {
            file: data,
            index: tempfile::tempfile()?,
            length,
            present: vec![0; count.div_ceil(64) as usize],
            page: Vec::with_capacity(PIECE),
            page_index: None,
            canceled: canceled.clone(),
        }));
        let probe = || {
            let source = ScanInput {
                input: scan.clone(),
                pos: 0,
                length,
                seekable: !indexed,
            };
            let mss = MediaSourceStream::new(Box::new(source), Default::default());
            symphonia::default::get_probe()
                .format(&hint, mss, &FormatOptions::default(), &Default::default())
                .map(|r| r.format)
        };
        let mut reader = probe()?;
        let track = reader
            .default_track()
            .ok_or_else(|| anyhow!("no audio track"))?;
        let p = &track.codec_params;
        use symphonia::core::codecs::*;
        let codec_ok = match format.as_str() {
            "mp3" => p.codec == CODEC_TYPE_MP3,
            "m4a" => p.codec == CODEC_TYPE_AAC,
            "flac" => p.codec == CODEC_TYPE_FLAC,
            _ => symphonia::default::get_codecs()
                .get_codec(p.codec)
                .is_some_and(|c| c.short_name.starts_with("pcm")),
        };
        ensure!(codec_ok, "audio codec does not match the selected format");
        let tb = p
            .time_base
            .ok_or_else(|| anyhow!("audio has no time base"))?;
        let id = track.id;
        let sample_rate = p
            .sample_rate
            .ok_or_else(|| anyhow!("missing audio sample rate"))?;
        let mut seek_points = Vec::new();
        let mut last = 0;
        let mut frames = p.n_frames.unwrap_or(0);
        if indexed {
            use symphonia::core::io::ReadBytes;
            let mut offset = reader.into_inner().pos();
            reader = probe()?;
            let mut stride = u64::from(
                reader
                    .default_track()
                    .unwrap()
                    .codec_params
                    .sample_rate
                    .unwrap_or(48000),
            ) * 2;
            frames = 0;
            loop {
                ensure!(!canceled.load(Ordering::Relaxed), "import canceled");
                match reader.next_packet() {
                    Ok(pkt) => {
                        if seek_points
                            .last()
                            .is_none_or(|point: &[u64; 2]| pkt.ts() >= point[0] + stride)
                            && offset < length
                        {
                            seek_points.push([pkt.ts(), offset]);
                            if seek_points.len() == 96 {
                                seek_points = seek_points.into_iter().step_by(2).collect();
                                stride *= 2;
                            }
                        }
                        frames = pkt.ts() + pkt.dur();
                        offset += pkt.data.len() as u64;
                        let percent = (offset.min(length) * 95 / length) as u8;
                        if percent != last {
                            progress(percent);
                            last = percent
                        }
                    }
                    Err(symphonia::core::errors::Error::IoError(e))
                        if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
        } else if frames == 0 {
            let mut end = 0;
            loop {
                ensure!(!canceled.load(Ordering::Relaxed), "import canceled");
                match reader.next_packet() {
                    Ok(pkt) if pkt.track_id() == id => end = end.max(pkt.ts() + pkt.dur()),
                    Ok(_) => (),
                    Err(symphonia::core::errors::Error::IoError(e))
                        if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                    {
                        break;
                    }
                    Err(e) => return Err(e.into()),
                }
            }
            frames = end;
        }
        let time = tb.calc_time(frames);
        let duration_ms = time.seconds * 1000 + (time.frac * 1000.).round() as u64;
        ensure!(duration_ms > 0, "empty audio track");
        drop(reader);
        let mut input = Arc::try_unwrap(scan)
            .map_err(|_| anyhow!("audio index still open"))?
            .into_inner()
            .unwrap();
        for i in 0..count {
            ensure!(!canceled.load(Ordering::Relaxed), "import canceled");
            if input.present[(i / 64) as usize] & (1u64 << (i % 64)) == 0 {
                input.load(i)?;
            }
            let percent = ((i + 1) * 95 / count) as u8;
            if percent > last {
                progress(percent);
                last = percent;
            }
        }
        let IndexInput {
            file: data,
            mut index,
            ..
        } = input;
        let after = data.metadata()?;
        ensure!(
            after.len() == length && metadata.modified().ok() == after.modified().ok(),
            "audio changed during import"
        );
        let mut levels = vec![(0, count)];
        let mut offset = 0;
        let mut n = count;
        while n > 1 {
            let next = offset + n * 32;
            for i in (0..n).step_by(2) {
                ensure!(!canceled.load(Ordering::Relaxed), "import canceled");
                let a = hash_at(&mut index, offset + i * 32)?;
                let h = if i + 1 < n {
                    node(a, hash_at(&mut index, offset + (i + 1) * 32)?)
                } else {
                    a
                };
                index.seek(SeekFrom::Start(next + i / 2 * 32))?;
                index.write_all(&h)?;
            }
            offset = next;
            n = n.div_ceil(2);
            levels.push((offset, n));
        }
        let root = hash_at(&mut index, offset)?;
        let info = AudioInfo {
            room,
            owner: key.public().to_peer_id().to_string(),
            title: path
                .file_stem()
                .unwrap_or_default()
                .to_string_lossy()
                .to_string(),
            format,
            duration_ms,
            length,
            root,
            seek_points,
            sample_rate,
        };
        let d = Descriptor {
            signature: key.sign(&Descriptor::signing_bytes(&info))?,
            info,
            public_key: key.public().encode_protobuf(),
        };
        ensure!(d.valid(&d.info.room), "invalid local audio descriptor");
        progress(100);
        Ok((
            d,
            Self {
                data,
                index,
                levels,
            },
        ))
    }
    fn read(&mut self, d: &Descriptor, i: u64) -> Result<Piece> {
        ensure!(i < d.count(), "invalid piece");
        self.data.seek(SeekFrom::Start(i * PIECE as u64))?;
        let mut bytes = vec![0; (d.info.length - i * PIECE as u64).min(PIECE as u64) as usize];
        self.data.read_exact(&mut bytes)?;
        let mut at = i;
        let mut proof = Vec::new();
        for &(start, n) in &self.levels {
            if at ^ 1 < n {
                proof.push(hash_at(&mut self.index, start + (at ^ 1) * 32)?);
            }
            at /= 2;
        }
        let piece = Piece { bytes, proof };
        ensure!(piece.valid(d, i), "source changed; import again");
        Ok(piece)
    }
}

struct Entry {
    path: tempfile::TempPath,
    size: usize,
    at: Instant,
}
#[derive(Default)]
pub(crate) struct Store {
    sources: HashMap<(String, Hash), Source>,
    entries: HashMap<PieceKey, Entry>,
    used: usize,
    pinned: Option<(PieceKey, u64)>,
}
impl Store {
    pub(crate) fn source(&mut self, d: &Descriptor, s: Source) {
        self.sources.insert((d.info.room.clone(), d.info.root), s);
    }
    pub(crate) fn read(&mut self, d: &Descriptor, i: u64) -> Option<Piece> {
        if let Some(p) = self
            .sources
            .get_mut(&(d.info.room.clone(), d.info.root))
            .and_then(|s| s.read(d, i).ok())
        {
            return Some(p);
        }
        let key = d.key(i);
        let entry = self.entries.get_mut(&key)?;
        let p = (|| {
            let mut f = File::open(&entry.path).ok()?;
            let bytes_len = (d.info.length.checked_sub(i.checked_mul(PIECE as u64)?)?)
                .min(PIECE as u64) as usize;
            let proof_len = entry.size.checked_sub(bytes_len)?;
            if proof_len > 32 * 32 || proof_len % 32 != 0 {
                return None;
            }
            let mut proof = vec![[0; 32]; proof_len / 32];
            for h in &mut proof {
                f.read_exact(h).ok()?;
            }
            let mut bytes = vec![0; bytes_len];
            f.read_exact(&mut bytes).ok()?;
            Some(Piece { bytes, proof })
        })();
        if let Some(p) = p.filter(|p| p.valid(d, i)) {
            entry.at = Instant::now();
            return Some(p);
        }
        self.used -= self.entries.remove(&key).unwrap().size;
        None
    }
    pub(crate) fn put(&mut self, d: &Descriptor, i: u64, p: &Piece) -> Result<()> {
        self.put_with_limit(d, i, p, CACHE_LIMIT)
    }
    fn put_with_limit(&mut self, d: &Descriptor, i: u64, p: &Piece, limit: usize) -> Result<()> {
        ensure!(p.valid(d, i), "invalid piece proof");
        let k = d.key(i);
        if self.entries.contains_key(&k) {
            return Ok(());
        }
        let size = p.bytes.len() + p.proof.len() * 32;
        while self.used + size > limit {
            let Some(key) = self
                .entries
                .iter()
                .filter(|(k, _)| {
                    !self.pinned.as_ref().is_some_and(|(start, end)| {
                        k.0 == start.0 && k.1 == start.1 && (start.2..=*end).contains(&k.2)
                    })
                })
                .min_by_key(|(_, e)| e.at)
                .map(|(k, _)| k.clone())
            else {
                bail!("active audio cache is full")
            };
            self.used -= self.entries.remove(&key).unwrap().size;
        }
        let mut f = tempfile::NamedTempFile::new()?;
        for h in &p.proof {
            f.write_all(h)?;
        }
        f.write_all(&p.bytes)?;
        self.entries.insert(
            k,
            Entry {
                path: f.into_temp_path(),
                size,
                at: Instant::now(),
            },
        );
        self.used += size;
        Ok(())
    }
    pub(crate) fn retain(&mut self, room: &str, roots: &HashSet<Hash>) {
        self.sources
            .retain(|(r, h), _| r != room || roots.contains(h));
        self.entries.retain(|(r, h, _), e| {
            let keep = r != room || roots.contains(h);
            if !keep {
                self.used -= e.size;
            }
            keep
        });
    }
    pub(crate) fn expire_transit(&mut self, joined: Option<&String>, leased: &HashSet<String>) {
        self.entries.retain(|(r, _, _), e| {
            let keep = Some(r) == joined || (leased.contains(r) && e.at.elapsed().as_secs() < 90);
            if !keep {
                self.used -= e.size
            }
            keep
        });
    }
    pub(crate) fn pin(&mut self, d: &Descriptor, i: u64) {
        let window = (d.info.length as u128 * 15000 / d.info.duration_ms as u128 / PIECE as u128)
            .min(1024) as u64;
        self.pinned = Some((d.key(i.saturating_sub(1)), i + window + 1));
    }
}
pub(crate) struct Need {
    pub descriptor: Descriptor,
    pub index: u64,
    pub speculative: bool,
    pub started: tokio::time::Instant,
    pub retry: bool,
    pub reply: oneshot::Sender<Option<Piece>>,
}
#[derive(Clone)]
pub struct Client {
    pub(crate) wake: Arc<tokio::sync::Notify>,
    pub(crate) tx: mpsc::Sender<Need>,
}
impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MediaClient").finish_non_exhaustive()
    }
}
impl Client {
    pub(crate) async fn fetch(&self, d: &Descriptor, i: u64, speculative: bool) -> Result<Piece> {
        let started = tokio::time::Instant::now();
        let mut retry = false;
        tokio::time::timeout(std::time::Duration::from_secs(90), async {
            loop {
                let (reply, rx) = oneshot::channel();
                self.tx
                    .send(Need {
                        descriptor: d.clone(),
                        index: i,
                        speculative,
                        started,
                        retry,
                        reply,
                    })
                    .await
                    .map_err(|_| anyhow!("room closed"))?;
                self.wake.notify_one();
                retry = true;
                if let Ok(Some(p)) = rx.await
                    && p.valid(d, i)
                {
                    return Ok(p);
                }

                tokio::time::sleep(std::time::Duration::from_millis(if speculative {
                    1000
                } else {
                    400
                }))
                .await;
            }
        })
        .await
        .map_err(|_| anyhow!("Waiting for an audio source timed out"))?
    }
}
