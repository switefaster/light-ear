//! On-demand, hop-by-hop transfer. Binary framing bounds each field before allocation.
use super::*;
use crate::local_audio::{self, Client, Descriptor, Need, PIECE, Piece, PieceKey};
use futures::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use std::sync::Arc;
use tokio::sync::oneshot;

pub(in crate::backend) type Exchange = rr::Behaviour<Codec>;
#[derive(Clone, Default)]
pub(in crate::backend) struct Codec;
#[derive(Debug, Clone)]
pub(in crate::backend) struct Request {
    d: Descriptor,
    i: u64,
    lane: usize,
    trail: Vec<String>,
    speculative: bool,
    have: Vec<u64>,
}
#[async_trait::async_trait]
impl rr::Codec for Codec {
    type Protocol = libp2p::StreamProtocol;
    type Request = Request;
    type Response = Option<Piece>;
    async fn read_request<T: AsyncRead + Unpin + Send>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Request> {
        let mut buf = Vec::new();
        const MAX_HEADER: usize = 48 * 1024;
        io.take(MAX_HEADER as u64 + 1).read_to_end(&mut buf).await?;
        if buf.len() > MAX_HEADER {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let invalid = || io::Error::from(io::ErrorKind::InvalidData);
        let mut input = buf.as_slice();
        fn take<'a>(input: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
            let (v, tail) = input
                .split_at_checked(n)
                .ok_or(io::ErrorKind::UnexpectedEof)?;
            *input = tail;
            Ok(v)
        }
        let flags = take(&mut input, 1)?[0];
        if flags > 3 {
            return Err(invalid());
        }
        let i = u64::from_be_bytes(take(&mut input, 8)?.try_into().unwrap());
        let len = u16::from_be_bytes(take(&mut input, 2)?.try_into().unwrap()) as usize;
        let d = serde_json::from_slice(take(&mut input, len)?)?;
        let hops = u16::from_be_bytes(take(&mut input, 2)?.try_into().unwrap()) as usize;
        if hops > directory::MAX_PEERS {
            return Err(invalid());
        }
        let mut trail = Vec::new();
        for _ in 0..hops {
            let n = take(&mut input, 1)?[0] as usize;
            trail.push(
                PeerId::from_bytes(take(&mut input, n)?)
                    .map_err(|_| invalid())?
                    .to_string(),
            );
        }
        if input.len() > 32 * 8 || !input.len().is_multiple_of(8) {
            return Err(invalid());
        }
        let have = input
            .chunks_exact(8)
            .map(|v| u64::from_be_bytes(v.try_into().unwrap()))
            .collect();
        Ok(Request {
            d,
            i,
            lane: usize::from(flags & 1),
            speculative: flags & 2 != 0,
            trail,
            have,
        })
    }
    async fn write_request<T: AsyncWrite + Unpin + Send>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        r: Request,
    ) -> io::Result<()> {
        let descriptor = serde_json::to_vec(&r.d)?;
        let mut buf = vec![r.lane as u8 | ((r.speculative as u8) << 1)];
        buf.extend(r.i.to_be_bytes());
        buf.extend(
            u16::try_from(descriptor.len())
                .map_err(io::Error::other)?
                .to_be_bytes(),
        );
        buf.extend(descriptor);
        buf.extend((r.trail.len() as u16).to_be_bytes());
        for hop in r.trail {
            let p = hop.parse::<PeerId>().map_err(io::Error::other)?.to_bytes();
            buf.push(p.len() as u8);
            buf.extend(p)
        }
        for i in r.have {
            buf.extend(i.to_be_bytes())
        }
        io.write_all(&buf).await
    }
    async fn read_response<T: AsyncRead + Unpin + Send>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Option<Piece>> {
        let mut header = [0u8; 1];
        io.read_exact(&mut header).await?;
        if header[0] == 255 {
            return Ok(None);
        }
        if header[0] > 32 {
            return Err(io::ErrorKind::InvalidData.into());
        }
        let mut proof = vec![[0; 32]; header[0] as usize];
        for h in &mut proof {
            io.read_exact(h).await?
        }
        let mut bytes = Vec::new();
        io.take(PIECE as u64 + 1).read_to_end(&mut bytes).await?;
        if bytes.is_empty() || bytes.len() > PIECE {
            return Err(io::ErrorKind::InvalidData.into());
        }
        Ok(Some(Piece { proof, bytes }))
    }
    async fn write_response<T: AsyncWrite + Unpin + Send>(
        &mut self,
        _: &Self::Protocol,
        io: &mut T,
        p: Option<Piece>,
    ) -> io::Result<()> {
        let Some(p) = p else {
            return io.write_all(&[255]).await;
        };
        io.write_all(&[p.proof.len() as u8]).await?;
        for h in p.proof {
            io.write_all(&h).await?
        }
        io.write_all(&p.bytes).await
    }
}

type Channel = rr::ResponseChannel<Option<Piece>>;
enum Waiter {
    Local(oneshot::Sender<Option<Piece>>, Instant),
    Remote(PeerId, rr::InboundRequestId, Channel, usize),
}
struct Flight {
    circuit_retries: usize,
    scope: Arc<std::sync::atomic::AtomicBool>,
    req: Request,
    wait: Vec<Waiter>,
    tried: HashSet<PeerId>,
    started: Instant,
    generation: u64,
}
impl Drop for Flight {
    fn drop(&mut self) {
        self.scope.store(false, Ordering::Release)
    }
}
struct Upload {
    room: String,
    root: local_audio::Hash,
    peer: PeerId,
    id: rr::InboundRequestId,
    channel: Channel,
    lane: usize,
    p: Arc<Piece>,
    speculative: bool,
}
struct DiskResult(PieceKey, u64, Option<Piece>);
type DiskJob = Box<dyn FnOnce(&mut local_audio::Store) + Send>;
/// A single ordered worker owns files and cache accounting. Slow removable storage and
/// unlinking an entire room cannot block the swarm or race a later import of the same file.
struct Disk(std::sync::mpsc::Sender<DiskJob>);
impl Default for Disk {
    fn default() -> Self {
        let (tx, rx) = std::sync::mpsc::channel::<DiskJob>();
        std::thread::spawn(move || {
            let mut store = local_audio::Store::default();
            for job in rx {
                job(&mut store)
            }
        });
        Self(tx)
    }
}
impl Disk {
    fn submit(&self, job: impl FnOnce(&mut local_audio::Store) + Send + 'static) {
        let _ = self.0.send(Box::new(job));
    }
}
#[derive(Default)]
struct LinkStats {
    rx: u64,
    tx: u64,
    relay: u64,
    relay_rx: u64,
    latency_ms: u64,
    outstanding_bytes: u64,
}
struct PendingRequest {
    key: PieceKey,
    generation: u64,
    peer: PeerId,
    lane: usize,
    started: Instant,
    bytes: u64,
}
#[derive(Default)]
struct TransitReferences {
    queue_version: (u64, i64),
    queue: HashSet<local_audio::Hash>,
    playback_version: (u64, i64),
    playing: Option<local_audio::Hash>,
}
pub(in crate::backend) struct Media {
    disk: Disk,
    disk_slots: Arc<tokio::sync::Semaphore>,
    transit_refs: HashMap<String, TransitReferences>,
    pub(in crate::backend) client: Client,
    pub(in crate::backend) needs: mpsc::Receiver<Need>,
    disk_tx: mpsc::Sender<DiskResult>,
    disk_rx: mpsc::Receiver<DiskResult>,
    flights: HashMap<PieceKey, Flight>,
    outbound: HashMap<rr::OutboundRequestId, PendingRequest>,
    uploads: VecDeque<Upload>,
    sent: HashMap<rr::InboundRequestId, (usize, usize)>,
    links: BTreeMap<String, LinkStats>,
    copies: HashMap<(PeerId, PieceKey), Instant>,
    pub(in crate::backend) limit: usize,
    credit: f64,
    tick: Instant,
    report: Instant,
    generation: u64,
    retries: u64,
    lane_bytes: [u64; 2],
    assigned: [u64; 2],
    samples: VecDeque<(Instant, u64)>,
    pub(in crate::backend) refs: HashSet<local_audio::Hash>,
    pub(super) p95: Option<u64>,
    pub(super) upload_utilization: u8,
    rate_samples: VecDeque<(Instant, Instant, u64)>,
    shortfall: Option<u64>,
    last_tx: u64,
    waiting: Option<Instant>,
    foreground: Option<(String, local_audio::Hash)>,
    report_changed: bool,
    have_updates: HashMap<(PeerId, String, local_audio::Hash), Request>,
    next_have: Instant,
    notices: VecDeque<String>,
}
impl Default for Media {
    fn default() -> Self {
        let (tx, needs) = mpsc::channel(64);
        let (disk_tx, disk_rx) = mpsc::channel(128);
        Self {
            disk: Default::default(),
            disk_slots: Arc::new(tokio::sync::Semaphore::new(128)),
            transit_refs: Default::default(),
            client: Client {
                tx,
                wake: Default::default(),
            },
            needs,
            disk_tx,
            disk_rx,
            flights: Default::default(),
            outbound: Default::default(),
            uploads: Default::default(),
            sent: Default::default(),
            links: Default::default(),
            copies: Default::default(),
            limit: 1024 * 1024,
            credit: PIECE as f64,
            tick: Instant::now(),
            report: Instant::now(),
            generation: 0,
            retries: 0,
            lane_bytes: [0; 2],
            assigned: [0; 2],
            samples: Default::default(),
            refs: Default::default(),
            p95: None,
            upload_utilization: 0,
            rate_samples: Default::default(),
            shortfall: None,
            last_tx: 0,
            waiting: None,
            foreground: None,
            report_changed: false,
            have_updates: Default::default(),
            next_have: Instant::now(),
            notices: Default::default(),
        }
    }
}

impl Media {
    pub(in crate::backend) fn register_source(
        &mut self,
        d: Descriptor,
        source: local_audio::Source,
    ) {
        self.refs.insert(d.info.root);
        self.disk.submit(move |store| store.source(&d, source));
    }
    pub(in crate::backend) fn retain_room(
        &mut self,
        room: &str,
        roots: &HashSet<local_audio::Hash>,
    ) {
        self.flights
            .retain(|k, _| k.0 != room || roots.contains(&k.1));
        self.uploads
            .retain(|u| u.room != room || roots.contains(&u.root));
        let name = room.to_owned();
        let retained = roots.clone();
        self.disk.submit(move |s| s.retain(&name, &retained));
        self.have_updates
            .retain(|(_, r, h), _| r != room || roots.contains(h));
        if self
            .foreground
            .as_ref()
            .is_some_and(|(r, h)| r == room && !roots.contains(h))
        {
            self.foreground = None;
            self.report_changed = true;
            self.waiting = None;
            self.shortfall = None;
            self.rate_samples.clear();
        }
    }
    pub(in crate::backend) fn busy(&self) -> bool {
        !self.flights.is_empty() || !self.uploads.is_empty()
    }
    pub(super) fn received(&self, p: &PeerId) -> (u64, u64) {
        self.links
            .get(&p.to_string())
            .map_or((0, 0), |s| (s.rx, s.relay_rx))
    }
}
impl Network {
    /// Helpers retain only byte-cache references. They never install remote queue or player state.
    pub(super) fn observe_transit_audio(&mut self, room: &str, wire: &WireMessage) {
        let view = self.media.transit_refs.entry(room.into()).or_default();
        match wire {
            WireMessage::QueueState { state, .. } | WireMessage::QueueResponse { state, .. }
                if (state.version, state.updated_at_micros) > view.queue_version =>
            {
                view.queue_version = (state.version, state.updated_at_micros);
                view.queue = state
                    .items
                    .iter()
                    .filter_map(|q| {
                        q.track
                            .local_audio
                            .as_ref()
                            .filter(|d| d.info.room == room)
                            .map(|d| d.info.root)
                    })
                    .collect();
            }
            WireMessage::PlaybackState { state, .. }
            | WireMessage::PlaybackPrepare { state, .. }
                if (state.state_version, state.issued_at_micros) > view.playback_version =>
            {
                view.playback_version = (state.state_version, state.issued_at_micros);
                view.playing = state
                    .track
                    .as_ref()
                    .and_then(|t| t.local_audio.as_ref())
                    .filter(|d| d.info.room == room)
                    .map(|d| d.info.root);
            }
            _ => return,
        }
        let mut roots = view.queue.clone();
        roots.extend(view.playing);
        self.media.retain_room(room, &roots);
    }
    pub(in crate::backend) fn signer(&self) -> identity::Keypair {
        self.key.clone()
    }
    fn media_admits(&self, room: &str, lane: usize, p: &PeerId) -> bool {
        if lane > 1
            || !self
                .directory
                .records
                .get(&self.local_peer_id().to_string())
                .is_some_and(|s| {
                    s.record.issued + 90 > clock()
                        && (s.record.room.as_deref() == Some(room)
                            || s.record.leases[lane].iter().any(|r| r == room))
                })
            || !self.plan.rooms[lane].contains(room)
            || !self.plan.neighbors[lane].contains(&p.to_string())
            || !self.inner.is_connected(p)
        {
            return false;
        }
        self.directory.records.get(&p.to_string()).is_some_and(|s| {
            s.record.issued + 90 > clock()
                && (s.record.room.as_deref() == Some(room)
                    || s.record.leases[lane].iter().any(|r| r == room))
                && s.record.links.iter().any(|l| {
                    l.peer == self.local_peer_id().to_string() && l.lanes & (1 << lane) != 0
                })
        })
    }
    fn media_authorized(&self, d: &Descriptor) -> bool {
        if self.room.as_ref() == Some(&d.info.room) {
            self.media.refs.contains(&d.info.root)
        } else {
            self.plan
                .rooms
                .iter()
                .any(|rooms| rooms.contains(&d.info.room))
        }
    }
    pub(in crate::backend) fn media_need(&mut self, n: Need) {
        if n.reply.is_closed() {
            return;
        }
        if self.room.as_ref() != Some(&n.descriptor.info.room)
            || !self.media_authorized(&n.descriptor)
        {
            let _ = n.reply.send(None);
            return;
        }
        if !n.speculative {
            let root = (n.descriptor.info.room.clone(), n.descriptor.info.root);
            if self.media.foreground.as_ref() != Some(&root) {
                self.media.foreground = Some(root);
                self.media.waiting = None;
                self.media.shortfall = None;
                self.media.rate_samples.clear();
            }
        }
        let lane = if self.media.assigned[0] == self.media.assigned[1] {
            (n.index % 2) as usize
        } else {
            usize::from(self.media.assigned[1] < self.media.assigned[0])
        };
        let req = Request {
            d: n.descriptor,
            i: n.index,
            lane,
            trail: vec![],
            speculative: n.speculative,
            have: vec![],
        };
        if n.retry {
            self.media.retries += 1
        }
        self.media_begin(req, Waiter::Local(n.reply, n.started));
    }
    fn media_begin(&mut self, req: Request, w: Waiter) {
        let key = req.d.key(req.i);
        if let Some(f) = self.media.flights.get_mut(&key) {
            if f.req.d.info.length != req.d.info.length {
                return;
            }
            if let Waiter::Remote(peer, _, _, _) = &w
                && f.tried.contains(peer)
            {
                return;
            }
            if !req.speculative {
                f.req.speculative = false
            }
            if f.wait.len() < 64 {
                f.wait.push(w)
            }
            return;
        }
        if self.media.flights.len() >= 96 {
            return;
        }
        self.media.generation += 1;
        let gen_id = self.media.generation;
        let Ok(slot) = self.media.disk_slots.clone().try_acquire_owned() else {
            return;
        };
        let tx = self.media.disk_tx.clone();
        let d = req.d.clone();
        let index = req.i;
        let pin = matches!(w, Waiter::Local(..)) && !req.speculative;
        self.media.flights.insert(
            key.clone(),
            Flight {
                circuit_retries: 0,
                scope: Arc::new(std::sync::atomic::AtomicBool::new(true)),
                req,
                wait: vec![w],
                tried: Default::default(),
                started: Instant::now(),
                generation: gen_id,
            },
        );
        self.media.disk.submit(move |s| {
            let _slot = slot;
            if pin {
                s.pin(&d, index)
            }
            let p = s.read(&d, index);
            let _ = tx.blocking_send(DiskResult(key, gen_id, p));
        });
    }
    fn media_route(&mut self, key: &PieceKey) {
        let Some(f) = self.media.flights.get(key) else {
            return;
        };
        if !self.media_authorized(&f.req.d) || f.req.trail.len() >= directory::MAX_PEERS {
            self.media_finish(key, None);
            return;
        }
        let local = *self.local_peer_id();
        let mut choices = Vec::new();
        for lane in 0..2 {
            // Distances from the owner along each tree, excluding the requesting branch.
            let mut dist = HashMap::new();
            let mut todo = VecDeque::from([(f.req.d.info.owner.clone(), 0usize)]);
            let mut neighbors: HashMap<&str, Vec<&str>> = HashMap::new();
            for (a, b) in &self.plan.trees[lane] {
                neighbors.entry(a).or_default().push(b);
                neighbors.entry(b).or_default().push(a)
            }
            while let Some((v, n)) = todo.pop_front() {
                if dist.contains_key(&v) || v == local.to_string() {
                    continue;
                }
                for next in neighbors.get(v.as_str()).into_iter().flatten() {
                    todo.push_back((next.to_string(), n + 1))
                }
                dist.insert(v, n);
            }
            for id in &self.plan.neighbors[lane] {
                let Ok(p) = id.parse::<PeerId>() else {
                    continue;
                };
                if f.tried.contains(&p)
                    || f.req.trail.contains(id)
                    || !self.media_admits(&key.0, lane, &p)
                {
                    continue;
                }
                let near = self
                    .media
                    .copies
                    .get(&(p, key.clone()))
                    .is_some_and(|t| t.elapsed() < Duration::from_secs(60));
                let cost = self.media.links.get(id).map_or(0, |s| {
                    s.latency_ms / 150 + s.outstanding_bytes.div_ceil(PIECE as u64)
                });
                let utilization = self
                    .directory
                    .records
                    .get(id)
                    .map_or(0, |s| s.record.upload_utilization as u64 / 40);
                let direct = self.routes.get(&p).is_some_and(|r| r.values().any(|v| v.0));
                // A cached neighbor and an owner-facing branch can both supply the piece.
                // Outstanding work and confirmed transfer time can outweigh locality.
                let exploration = key.2 % 32 == 0 && lane == f.req.lane;
                let delay = if exploration { 0 } else { cost };
                choices.push((
                    !near && !dist.contains_key(id),
                    !direct,
                    delay + utilization + u64::from(lane != f.req.lane) + u64::from(!near),
                    dist.get(id).copied().unwrap_or(512),
                    lane,
                    p,
                ));
            }
        }
        let Some((_, _, _, _, lane, p)) = choices.into_iter().min() else {
            self.media_finish(key, None);
            return;
        };
        let f = self.media.flights.get_mut(key).unwrap();
        let mut req = f.req.clone();
        req.lane = lane;
        req.trail.push(local.to_string());
        if !f.tried.is_empty() {
            self.media.retries += 1
        }
        let bytes = (f.req.d.info.length - key.2 * PIECE as u64).min(PIECE as u64);
        if f.req.trail.is_empty() && f.tried.is_empty() {
            self.media.assigned[lane] += bytes;
        }
        f.tried.insert(p);
        let id = self.inner.behaviour_mut().media.send_request(&p, req);
        self.media.outbound.insert(
            id,
            PendingRequest {
                key: key.clone(),
                generation: f.generation,
                peer: p,
                lane,
                started: Instant::now(),
                bytes,
            },
        );
        self.media
            .links
            .entry(p.to_string())
            .or_default()
            .outstanding_bytes += bytes;
    }
    fn media_finish(&mut self, key: &PieceKey, p: Option<Piece>) {
        let Some(mut f) = self.media.flights.remove(key) else {
            return;
        };
        let p = p.map(Arc::new);
        if let Some(piece) = p.as_ref()
            && !f.req.speculative
            && !f.tried.is_empty()
            && f.wait
                .iter()
                .any(|w| matches!(w, Waiter::Local(tx, _) if !tx.is_closed()))
        {
            let now = Instant::now();
            let samples = &mut self.media.rate_samples;
            samples.push_back((f.started, now, piece.bytes.len() as u64));
            samples.retain(|(_, end, _)| now - *end < Duration::from_secs(15));
            // Completions are ordered by end time. Charge overlapping transfers for
            // their shared wall-clock interval only once, and exclude idle gaps.
            let mut frontier = now;
            let mut busy = Duration::ZERO;
            let mut bytes = 0;
            for &(start, end, size) in samples.iter().rev() {
                busy += end.min(frontier).saturating_duration_since(start);
                frontier = frontier.min(start);
                bytes += size;
            }
            if busy >= Duration::from_secs(2) {
                let demand =
                    (f.req.d.info.length as u128 * 1000 / f.req.d.info.duration_ms as u128) as u64;
                let measured = bytes * 1000 / busy.as_millis() as u64;
                self.media.shortfall =
                    (measured < demand).then(|| (demand - measured).div_ceil(1024));
            } else {
                self.media.shortfall = None;
            }
        }
        for w in std::mem::take(&mut f.wait) {
            match w {
                Waiter::Local(tx, requested) => {
                    if p.is_some() && !f.req.speculative {
                        self.media.waiting = None;
                        self.media
                            .samples
                            .push_back((Instant::now(), requested.elapsed().as_millis() as u64));
                    } else if p.is_none()
                        && !f.req.speculative
                        && self
                            .media
                            .waiting
                            .is_none_or(|t| t.elapsed() > Duration::from_secs(8))
                    {
                        self.media.waiting = Some(Instant::now());
                        self.media
                            .notices
                            .push_back("Waiting for an audio source".into());
                    }
                    let _ = tx.send(p.as_deref().cloned());
                }
                Waiter::Remote(peer, id, channel, lane) => {
                    if let Some(piece) = p.as_ref().filter(|_| self.media.uploads.len() < 128) {
                        self.media.uploads.push_back(Upload {
                            room: key.0.clone(),
                            root: key.1,
                            peer,
                            id,
                            channel,
                            lane,
                            p: piece.clone(),
                            speculative: f.req.speculative,
                        });
                    } else {
                        let _ = self
                            .inner
                            .behaviour_mut()
                            .media
                            .send_response(channel, None);
                    }
                }
            }
        }
    }
    pub(in crate::backend) fn media_event(&mut self, e: rr::Event<Request, Option<Piece>>) {
        match e {
            rr::Event::Message {
                peer,
                connection_id,
                message:
                    rr::Message::Request {
                        request: r,
                        channel,
                        request_id,
                    },
                ..
            } => {
                let direct = self
                    .routes
                    .get(&peer)
                    .and_then(|r| r.get(&connection_id))
                    .is_some_and(|r| r.0);
                let bypass = !direct
                    && !self.degraded(&peer)
                    && self
                        .routes
                        .get(&peer)
                        .is_some_and(|r| r.values().any(|r| r.0));
                if bypass
                    || !self.media_admits(&r.d.info.room, r.lane, &peer)
                    || !self.media_authorized(&r.d)
                    || r.i >= r.d.count()
                    || r.trail.len() > directory::MAX_PEERS
                    || r.trail.last() != Some(&peer.to_string())
                    || r.trail.contains(&self.local_peer_id().to_string())
                    || !r.d.valid(&r.d.info.room)
                    || r.have.len() > 32
                {
                    let _ = self
                        .inner
                        .behaviour_mut()
                        .media
                        .send_response(channel, None);
                    return;
                }
                if !r.have.is_empty() {
                    if self.media.copies.len() < 8192 {
                        for i in r.have {
                            if i < r.d.count() {
                                self.media.copies.insert((peer, r.d.key(i)), Instant::now());
                            }
                        }
                    }
                    let _ = self
                        .inner
                        .behaviour_mut()
                        .media
                        .send_response(channel, None);
                    return;
                }
                let lane = r.lane;
                self.media_begin(r, Waiter::Remote(peer, request_id, channel, lane))
            }
            rr::Event::Message {
                peer,
                connection_id,
                message:
                    rr::Message::Response {
                        request_id,
                        response,
                    },
                ..
            } => {
                let Some(PendingRequest {
                    key,
                    generation: g,
                    lane,
                    started: start,
                    bytes,
                    ..
                }) = self.media.outbound.remove(&request_id)
                else {
                    return;
                };
                let s = self.media.links.entry(peer.to_string()).or_default();
                s.outstanding_bytes = s.outstanding_bytes.saturating_sub(bytes);
                let ms = start.elapsed().as_millis() as u64;
                s.latency_ms = (3 * s.latency_ms + ms) / 4;
                let direct = self
                    .routes
                    .get(&peer)
                    .and_then(|r| r.get(&connection_id))
                    .is_some_and(|v| v.0);
                if let Some(p) = &response {
                    s.rx += p.bytes.len() as u64;
                    if !direct {
                        s.relay += p.bytes.len() as u64;
                        s.relay_rx += p.bytes.len() as u64
                    }
                }
                let Some(f) = self.media.flights.get(&key).filter(|f| f.generation == g) else {
                    return;
                };
                if !self.media_authorized(&f.req.d) {
                    self.media_finish(&key, None);
                    return;
                }
                if !self.media_admits(&key.0, lane, &peer) {
                    self.media_route(&key);
                    return;
                }
                if let Some(p) = response.filter(|p| p.valid(&f.req.d, key.2)) {
                    let d = f.req.d.clone();
                    let scope = f.scope.clone();
                    let tx = self.media.disk_tx.clone();
                    let Ok(slot) = self.media.disk_slots.clone().try_acquire_owned() else {
                        self.media_finish(&key, None);
                        return;
                    };
                    self.media.disk.submit(move |s| {
                        let _slot = slot;
                        let stored = scope.load(Ordering::Acquire) && s.put(&d, key.2, &p).is_ok();
                        let _ = tx.blocking_send(DiskResult(key, g, stored.then_some(p)));
                    });
                } else {
                    // request-response round-robins sockets. A healthy dual-connected supplier
                    // refuses media on its circuit. Retry until its direct socket is selected.
                    if !direct
                        && self
                            .routes
                            .get(&peer)
                            .is_some_and(|r| r.values().any(|v| v.0))
                        && let Some(f) = self.media.flights.get_mut(&key)
                        && f.circuit_retries < self.routes[&peer].len()
                    {
                        f.circuit_retries += 1;
                        f.tried.remove(&peer);
                    }
                    self.media_route(&key)
                }
            }
            rr::Event::OutboundFailure {
                request_id, error, ..
            } => {
                if let Some(PendingRequest {
                    key,
                    generation: g,
                    peer: p,
                    bytes,
                    ..
                }) = self.media.outbound.remove(&request_id)
                {
                    if matches!(
                        error,
                        rr::OutboundFailure::Timeout | rr::OutboundFailure::ConnectionClosed
                    ) {
                        self.congestion(p)
                    }
                    let s = self.media.links.entry(p.to_string()).or_default();
                    s.outstanding_bytes = s.outstanding_bytes.saturating_sub(bytes);
                    s.latency_ms = s.latency_ms.max(500);
                    if self
                        .media
                        .flights
                        .get(&key)
                        .is_some_and(|f| f.generation == g)
                    {
                        self.media_route(&key)
                    }
                }
            }
            rr::Event::ResponseSent {
                peer,
                connection_id,
                request_id,
                ..
            } => {
                if let Some((bytes, lane)) = self.media.sent.remove(&request_id) {
                    self.media.lane_bytes[lane] += bytes as u64;
                    let s = self.media.links.entry(peer.to_string()).or_default();
                    s.tx += bytes as u64;
                    if !self
                        .routes
                        .get(&peer)
                        .and_then(|r| r.get(&connection_id))
                        .is_some_and(|r| r.0)
                    {
                        s.relay += bytes as u64
                    }
                }
            }
            rr::Event::InboundFailure { request_id, .. } => {
                self.media.sent.remove(&request_id);
            }
        }
    }
    pub(in crate::backend) fn media_tick(&mut self, ui: &mpsc::Sender<UiEvent>) {
        let now = Instant::now();
        self.media.flights.retain(|_, f| {
            f.wait.retain(|w| match w {
                Waiter::Local(tx, _) => !tx.is_closed(),
                Waiter::Remote(_, _, ch, _) => ch.is_open(),
            });
            !f.wait.is_empty()
        });
        while let Ok(DiskResult(key, g, p)) = self.media.disk_rx.try_recv() {
            let Some(f) = self.media.flights.get(&key).filter(|f| f.generation == g) else {
                continue;
            };
            if !self.media_authorized(&f.req.d) {
                self.media_finish(&key, None);
                continue;
            }
            if p.is_none() {
                self.media_route(&key);
                continue;
            }
            if !f.tried.is_empty() {
                let req = &f.req;
                let mut announcements = Vec::new();
                for lane in 0..2 {
                    for id in &self.plan.neighbors[lane] {
                        if let Ok(peer) = id.parse::<PeerId>()
                            && !f.tried.contains(&peer)
                            && self.media_admits(&key.0, lane, &peer)
                        {
                            announcements.push((
                                peer,
                                Request {
                                    d: req.d.clone(),
                                    i: req.i,
                                    have: vec![req.i],
                                    lane,
                                    trail: vec![self.local_peer_id().to_string()],
                                    speculative: true,
                                },
                            ));
                        }
                    }
                }
                announcements.sort_by_key(|v| v.0);
                announcements.dedup_by_key(|v| v.0);
                for (peer, request) in announcements {
                    let key = (peer, request.d.info.room.clone(), request.d.info.root);
                    if let Some(batch) = self.media.have_updates.get_mut(&key) {
                        if batch.have.len() < 32 {
                            batch.have.push(request.i);
                        }
                    } else if self.media.have_updates.len() < 256 {
                        self.media.have_updates.insert(key, request);
                    }
                }
            }
            self.media_finish(&key, p)
        }
        if now >= self.media.next_have {
            self.media.next_have = now + Duration::from_millis(250);
            for ((peer, room, _), req) in std::mem::take(&mut self.media.have_updates) {
                if self.media_admits(&room, req.lane, &peer) && self.media_authorized(&req.d) {
                    self.inner.behaviour_mut().media.send_request(&peer, req);
                }
            }
        }
        self.media.credit = (self.media.credit
            + (now - self.media.tick).as_secs_f64() * self.media.limit as f64)
            .min(PIECE.max(self.media.limit / 4) as f64);
        self.media.tick = now;
        self.media
            .uploads
            .make_contiguous()
            .sort_by_key(|u| (u.speculative, self.room.as_deref() != Some(&u.room)));
        let count = self.media.uploads.len();
        for _ in 0..count {
            let u = self.media.uploads.pop_front().unwrap();
            let bytes = u.p.bytes.len();
            if !u.channel.is_open()
                || !self.media_admits(&u.room, u.lane, &u.peer)
                || (self.room.as_ref() == Some(&u.room) && !self.media.refs.contains(&u.root))
            {
                continue;
            }
            if self.media.credit < bytes as f64 {
                self.media.uploads.push_front(u);
                break;
            }
            if self.room.as_ref() != Some(&u.room) {
                let helpers = self
                    .plan
                    .rooms
                    .iter()
                    .flatten()
                    .filter(|r| Some(*r) != self.room.as_ref())
                    .collect::<HashSet<_>>()
                    .len()
                    .max(1);
                let rate = self.transit_rate / helpers;
                let slot = self.credits.entry(u.room.clone()).or_insert((now, 0));
                let drained = ((now - slot.0).as_secs_f64() * rate as f64) as usize;
                if drained > 0 {
                    slot.0 = now;
                    slot.1 = slot.1.saturating_sub(drained)
                }
                if rate == 0 || (slot.1 > 0 && slot.1 + bytes > (rate * 9 / 10).max(PIECE)) {
                    self.media.uploads.push_back(u);
                    continue;
                }
                slot.1 += bytes;
                self.interval_charge += bytes;
            }
            if self
                .inner
                .behaviour_mut()
                .media
                .send_response(u.channel, Some((*u.p).clone()))
                .is_ok()
            {
                self.media.sent.insert(u.id, (bytes, u.lane));
                self.media.credit -= bytes as f64;
            }
        }
        if let Some(text) = self.media.notices.pop_front() {
            let _ = ui.try_send(UiEvent::Status(text));
        }
        if now - self.media.report > Duration::from_secs(2) {
            let elapsed = (now - self.media.report).as_secs_f64();
            let leases = self
                .plan
                .rooms
                .iter()
                .flatten()
                .cloned()
                .collect::<HashSet<_>>();
            self.media.transit_refs.retain(|r, _| leases.contains(r));
            let joined = self.room.clone();
            self.media
                .disk
                .submit(move |s| s.expire_transit(joined.as_ref(), &leases));
            self.media.report = now;
            self.media
                .copies
                .retain(|_, at| at.elapsed() < Duration::from_secs(60));
            self.media
                .samples
                .retain(|(t, _)| t.elapsed() < Duration::from_secs(60));
            if !self.media.links.is_empty()
                || !self.media.samples.is_empty()
                || self.media.waiting.is_some()
                || std::mem::take(&mut self.media.report_changed)
            {
                let mut samples = self.media.samples.iter().map(|v| v.1).collect::<Vec<_>>();
                samples.sort_unstable();
                let p95 = samples
                    .get((samples.len() * 95).div_ceil(100).saturating_sub(1))
                    .copied();
                self.media.p95 = p95;
                let tx = self.media.links.values().map(|s| s.tx).sum::<u64>();
                self.media.upload_utilization = ((tx.saturating_sub(self.media.last_tx)) as f64
                    * 100.
                    / elapsed
                    / self.media.limit as f64)
                    .min(100.) as u8;
                self.media.last_tx = tx;
                let worst = self
                    .directory
                    .records
                    .values()
                    .filter(|r| {
                        r.record.room == self.room
                            && r.record.owner != self.local_peer_id().to_string()
                    })
                    .filter_map(|r| r.record.media_p95_ms)
                    .chain(p95)
                    .max();
                let _ = ui.try_send(UiEvent::MediaMetrics {
                    neighbors: self
                        .media
                        .links
                        .iter()
                        .map(|(p, s)| (p.clone(), [s.tx, s.rx, s.relay]))
                        .collect(),
                    lane_bytes: self.media.lane_bytes,
                    retries: self.media.retries,
                    receiver_p95_ms: p95,
                    worst_receiver_p95_ms: worst,
                    waiting: self.media.waiting.is_some(),
                    shortfall_kib_s: self.media.shortfall,
                });
            }
        }
    }
}
