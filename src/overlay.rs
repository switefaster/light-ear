use super::*;
use crate::{
    directory::{self, Directory, Link, Page, Presence, Signed, Summary},
    topology,
};
use libp2p::{identity, request_response as rr};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    ops::{Deref, DerefMut},
};

const TRANSIT_BYTES_PER_SECOND: usize = 256 * 1024;
const HELPER_ROOMS: usize = 8;

pub(super) type Exchange = rr::json::Behaviour<Summary, Page>;
pub(super) struct Network {
    inner: libp2p::Swarm<Behaviour>,
    key: identity::Keypair,
    received: BTreeMap<String, [u64; 2]>,
    room_received: BTreeMap<String, [u64; 2]>,
    circuit_received: HashMap<PeerId, u64>,
    control_bytes: u64,
    pressure: HashMap<PeerId, VecDeque<Instant>>,
    missed: HashMap<(usize, String), u32>,
    missed_total: u64,
    confirmation_neighbors: [BTreeMap<String, String>; 2],
    repairs: BTreeSet<String>,
    lost: HashMap<String, Instant>,
    quota_report: bool,
    exhausted: bool,
    reported_exhausted: bool,
    direct_proof: HashMap<PeerId, Instant>,
    handoffs: HashMap<PeerId, Instant>,
    alternate_proof: HashMap<PeerId, Instant>,
    repair_probe: Option<Instant>,
    interval_charge: usize,
    utilization: u8,
    pub(super) transit_rate: usize,
    published_bytes: [u64; 2],
    metrics_at: Instant,
    pub directory: Directory,
    pub room: Option<String>,
    plan: topology::Plan,
    routes: HashMap<PeerId, HashMap<libp2p::swarm::ConnectionId, (bool, u32)>>,
    pending: HashSet<PeerId>,
    dirty: bool,
    pub(super) room_routes_changed: bool,
    seed_addresses: HashMap<String, Vec<Multiaddr>>,
    announce: bool,
    seq: u64,
    refresh: Instant,
    dialed: HashMap<String, (Instant, u32)>,
    next_dial: Instant,
    view: Option<(Option<String>, BTreeMap<String, usize>)>,
    topology_view: Option<crate::core::TopologyView>,
    credits: HashMap<String, (Instant, usize)>,
    seen: HashMap<(String, String, u64), Instant>,
    samples: [HashMap<String, VecDeque<(Instant, u64)>>; 2],
    probes: HashMap<u64, (usize, String, Instant, bool)>,
    next_probe: Instant,
    probe_index: usize,
    assigned: [u64; 2],
    proposed: Option<(topology::Plan, Instant)>,
    tail_window: Instant,
    tail_target: Option<String>,
    shortcut: Option<String>,
}
impl Deref for Network {
    type Target = libp2p::Swarm<Behaviour>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for Network {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
pub(super) fn clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
#[derive(Serialize, Deserialize)]
pub(super) struct Packet {
    pub id: u64,
    pub body: Payload,
}
#[derive(Serialize, Deserialize)]
pub(super) enum Payload {
    Room(WireMessage),
    Probe(String),
    Ack(u64, String, bool, bool),
}
impl Network {
    pub fn new(
        inner: libp2p::Swarm<Behaviour>,
        key: identity::Keypair,
        room: Option<String>,
    ) -> Self {
        let now = Instant::now();
        Self {
            inner,
            key,
            room,
            received: Default::default(),
            room_received: Default::default(),
            circuit_received: Default::default(),
            control_bytes: 0,
            pressure: Default::default(),
            missed: Default::default(),
            missed_total: 0,
            confirmation_neighbors: Default::default(),
            repairs: Default::default(),
            lost: Default::default(),
            quota_report: false,
            exhausted: false,
            reported_exhausted: false,
            direct_proof: Default::default(),
            handoffs: Default::default(),
            alternate_proof: Default::default(),
            repair_probe: None,
            interval_charge: 0,
            utilization: 0,
            transit_rate: TRANSIT_BYTES_PER_SECOND,
            published_bytes: [0; 2],
            metrics_at: now,
            directory: Directory::default(),
            plan: Default::default(),
            routes: Default::default(),
            pending: Default::default(),
            dirty: true,
            room_routes_changed: false,
            announce: true,
            seq: 0,
            refresh: now,
            dialed: Default::default(),
            next_dial: now,
            view: None,
            topology_view: None,
            credits: Default::default(),
            seen: Default::default(),
            samples: Default::default(),
            probes: Default::default(),
            next_probe: now,
            probe_index: 0,
            assigned: [0; 2],
            proposed: None,
            seed_addresses: Default::default(),
            tail_window: now,
            tail_target: None,
            shortcut: None,
        }
    }
    pub fn members(&self) -> BTreeSet<String> {
        self.room
            .as_deref()
            .map(|r| self.directory.members(r))
            .unwrap_or_default()
    }
    pub fn remember_seed(&mut self, addr: Multiaddr) {
        if let Some(p) = peer_id_from_multiaddr(&addr) {
            self.seed_addresses
                .entry(p.to_string())
                .or_default()
                .push(addr);
        }
    }
    fn addresses(&self, p: &str) -> Vec<Multiaddr> {
        let mut addresses = self.seed_addresses.get(p).cloned().unwrap_or_default();
        if let Some(r) = self.directory.records.get(p) {
            addresses.extend(
                r.record
                    .addresses
                    .iter()
                    .filter_map(|s| s.parse::<Multiaddr>().ok()),
            );
        }
        addresses
    }
    pub fn reachable_without_seed(&self, peer: PeerId) -> bool {
        self.plan.reachable.contains(&peer.to_string()) && !self.repairs.contains(&peer.to_string())
    }
    pub fn member_count(&self) -> usize {
        self.members().len()
    }
    pub fn lane(&mut self, i: usize) -> &mut Observed {
        if i == 0 {
            &mut self.inner.behaviour_mut().gossipsub
        } else {
            &mut self.inner.behaviour_mut().alternate
        }
    }
    pub fn change_room(&mut self, room: Option<String>) {
        self.room = room;
        self.room_received.clear();
        self.published_bytes = [0; 2];
        self.missed_total = 0;
        self.direct_proof.clear();
        self.alternate_proof.clear();
        self.handoffs.clear();
        self.announce = true;
        self.dirty = true;
        self.probes.clear();
        self.samples = Default::default();
        self.confirmation_neighbors = Default::default();
        self.missed.clear();
        self.lost.clear();
        self.repairs.clear();
        self.assigned = [0; 2];
    }
    pub fn at_bootstrap_budget(&self) -> bool {
        self.routes.len() >= 12
            || self
                .network_info()
                .connection_counters()
                .num_pending_outgoing()
                >= 4
    }
    pub fn bootstrap_dial(&mut self, p: PeerId, addresses: Vec<Multiaddr>) {
        if !self.at_bootstrap_budget() && !self.is_connected(&p) {
            let _ = self.dial(
                DialOpts::peer_id(p)
                    .addresses(addresses)
                    .condition(PeerCondition::DisconnectedAndNotDialing)
                    .build(),
            );
        }
    }
    pub fn congestion(&mut self, p: PeerId) {
        let q = self.pressure.entry(p).or_default();
        if q.len() >= 3 {
            q.pop_front();
        }
        q.push_back(Instant::now());
        if q.len() == 3 {
            self.announce = true;
        }
    }
    fn degraded(&self, p: &PeerId) -> bool {
        self.pressure.get(p).is_some_and(|q| {
            q.len() == 3
                && q.front()
                    .is_some_and(|t| t.elapsed() < Duration::from_secs(60))
        })
    }
    pub fn request_handoff(&mut self, p: PeerId) {
        self.handoffs.entry(p).or_insert_with(Instant::now);
    }
    pub fn handoff_ready(&self, p: PeerId) -> bool {
        self.room_path_confirmed()
            && !self.degraded(&p)
            && self
                .direct_proof
                .get(&p)
                .is_some_and(|t| t.elapsed() < Duration::from_secs(30))
            && self
                .routes
                .get(&p)
                .is_some_and(|links| links.values().any(|(d, _)| *d))
    }
    pub fn link(&mut self, peer: PeerId, id: libp2p::swarm::ConnectionId, direct: Option<bool>) {
        self.direct_proof.remove(&peer);
        self.alternate_proof.remove(&peer);
        self.handoffs.remove(&peer);
        if let Some(direct) = direct {
            self.routes
                .entry(peer)
                .or_default()
                .insert(id, (direct, 50));
        } else if let Some(links) = self.routes.get_mut(&peer) {
            let lost_direct = links.remove(&id).is_some_and(|(direct, _)| direct)
                && !links.values().any(|(direct, _)| *direct);
            if links.is_empty() {
                self.routes.remove(&peer);
                self.pending.remove(&peer);
            }
            if lost_direct {
                self.congestion(peer);
            }
        }
        self.announce = true;
    }
    pub fn ping(&mut self, e: ping::Event) {
        if e.result.is_err() {
            self.congestion(e.peer);
        }
        if let (Ok(d), Some(link)) = (
            e.result,
            self.routes
                .get_mut(&e.peer)
                .and_then(|v| v.get_mut(&e.connection)),
        ) {
            link.1 = (d.as_millis() as u32).max(1);
        }
    }
    fn pull(&mut self, peer: PeerId, after: Option<String>) {
        if self.pending.insert(peer) {
            let q = self.directory.summary(after);
            self.behaviour_mut().directory.send_request(&peer, q);
        }
    }
    pub fn exchange(&mut self, event: rr::Event<Summary, Page>) {
        match event {
            rr::Event::Message {
                peer,
                message:
                    rr::Message::Request {
                        request, channel, ..
                    },
                ..
            } => {
                self.control_bytes += serde_json::to_vec(&request).map_or(0, |v| v.len() as u64);
                let page = self.directory.page(&request);
                let behind = request.known.len() <= directory::MAX_PEERS
                    && request.known.iter().any(|(id, seq)| {
                        self.directory
                            .records
                            .get(id)
                            .is_none_or(|r| r.record.sequence < *seq)
                    });
                let _ = self.behaviour_mut().directory.send_response(channel, page);
                if behind {
                    self.pull(peer, None);
                }
            }
            rr::Event::Message {
                peer,
                message: rr::Message::Response { response, .. },
                ..
            } => {
                self.control_bytes += serde_json::to_vec(&response).map_or(0, |v| v.len() as u64);
                self.pending.remove(&peer);
                if response.records.len() > 8 {
                    return;
                }
                let mut changed = false;
                for record in response.records {
                    changed |= self.directory.merge(record, clock());
                }
                self.dirty |= changed;
                if changed && response.next.is_some() {
                    self.pull(peer, response.next);
                }
            }
            rr::Event::OutboundFailure { peer, .. } => {
                self.pending.remove(&peer);
            }
            _ => {}
        }
    }
    pub async fn tick(&mut self, ui: &mpsc::Sender<UiEvent>) {
        let now = Instant::now();
        let local = self.local_peer_id().to_string();
        self.refresh_presence(now, &local);
        self.apply_directory(now, &local, ui).await;
        self.observe_partition(now);
        self.publish_metrics(now, ui).await;
        self.check_handoffs(now, &local);
        self.repair_connections(now, &local);
        self.sample_delivery(now, &local);
    }
    fn refresh_presence(&mut self, now: Instant, local: &str) {
        self.pressure.retain(|_, q| {
            q.back()
                .is_some_and(|t| now.duration_since(*t) < Duration::from_secs(60))
        });
        let count = self.directory.records.len();
        self.directory.expire(clock());
        self.dirty |= count != self.directory.records.len();
        if self.announce || now >= self.refresh || !self.directory.records.contains_key(local) {
            self.seq += 1;
            let mut addresses: Vec<_> = self
                .external_addresses()
                .chain(self.listeners())
                .filter(|a| directory::shareable(a))
                .map(ToString::to_string)
                .collect();
            addresses.sort();
            addresses.dedup();
            addresses.truncate(8);
            let mut links: Vec<_> = self
                .routes
                .iter()
                .map(|(id, links)| {
                    let direct = links.values().any(|(d, _)| *d);
                    Link {
                        peer: id.to_string(),
                        direct,
                        lanes: u8::from(self.plan.neighbors[0].contains(&id.to_string()))
                            + 2 * u8::from(self.plan.neighbors[1].contains(&id.to_string())),
                        payload_bytes: self
                            .received
                            .get(&id.to_string())
                            .copied()
                            .unwrap_or_default(),
                        circuit_bytes: *self.circuit_received.get(id).unwrap_or(&0),
                        degraded: self.degraded(id),
                        rtt_ms: links
                            .values()
                            .filter(|(d, _)| *d == direct)
                            .map(|(_, r)| *r)
                            .min()
                            .unwrap_or(50),
                    }
                })
                .collect();
            links.sort_by(|a, b| a.peer.cmp(&b.peer));
            links.truncate(directory::MAX_PEERS);
            let record = Presence {
                owner: local.to_owned(),
                sequence: self.seq,
                issued: clock(),
                room: self.room.clone(),
                addresses,
                links,
                transit_rooms: HELPER_ROOMS,
                upload_utilization: if self.reported_exhausted {
                    100
                } else {
                    self.utilization
                },
                repairs: self.repairs.iter().cloned().collect(),
                leases: std::array::from_fn(|i| self.plan.rooms[i].iter().cloned().collect()),
            };
            self.directory
                .records
                .insert(local.to_owned(), Signed::new(record, &self.key));
            self.refresh = now + Duration::from_secs(30);
            self.announce = false;
            self.dirty = true;
        }
    }
    async fn apply_directory(&mut self, now: Instant, local: &str, ui: &mpsc::Sender<UiEvent>) {
        if self.dirty {
            let next = topology::plan(&self.directory, local);
            if next.constrained != self.plan.constrained && next.constrained {
                send_status(
                    ui,
                    "single peer bridge detected; searching for another path".into(),
                )
                .await;
            }
            let over = next.rooms[0]
                .union(&next.rooms[1])
                .filter(|r| Some(*r) != self.room.as_ref())
                .count()
                > HELPER_ROOMS;
            if over && !self.quota_report {
                send_status(
                    ui,
                    "forwarding slots full; some rooms need another path".into(),
                )
                .await;
            }
            self.quota_report = over;
            self.reconcile(next, now);
            let peers: Vec<_> = self.routes.keys().copied().collect();
            for peer in peers {
                self.pull(peer, None);
            }
            self.dirty = false;
            let graph = topology::view(&self.directory);
            if self.topology_view.as_ref() != Some(&graph) {
                let _ = ui.send(UiEvent::Topology(graph.clone())).await;
                self.topology_view = Some(graph);
            }
            let view = (self.room.clone(), self.directory.rooms());
            if self.view.as_ref() != Some(&view) {
                let _ = ui
                    .send(UiEvent::RoomDirectory {
                        active: view.0.clone(),
                        rooms: view.1.clone(),
                    })
                    .await;
                self.view = Some(view);
            }
            let n = self
                .room
                .as_deref()
                .map_or(0, |r| self.directory.members(r).len().saturating_sub(1));
            let _ = ui.send(UiEvent::PeerCount(n)).await;
        }
    }
    fn observe_partition(&mut self, now: Instant) {
        if let Some(r) = &self.room {
            let members = self.directory.members(r);
            for p in members.difference(&self.plan.reachable) {
                self.lost.entry(p.clone()).or_insert(now);
            }
            self.lost
                .retain(|p, _| members.contains(p) && !self.plan.reachable.contains(p));
        }
    }
    async fn publish_metrics(&mut self, now: Instant, ui: &mpsc::Sender<UiEvent>) {
        if now >= self.metrics_at {
            self.metrics_at = now + Duration::from_secs(5);
            if self.reported_exhausted != self.exhausted {
                send_status(
                    ui,
                    if self.exhausted {
                        "transit upload budget exhausted; repairing room delivery"
                    } else {
                        "transit upload capacity recovered"
                    }
                    .into(),
                )
                .await;
                self.announce = true;
            }
            self.reported_exhausted = self.exhausted;
            self.exhausted = false;
            self.utilization = (self.interval_charge.saturating_mul(100)
                / (5 * TRANSIT_BYTES_PER_SECOND))
                .min(100) as u8;
            self.interval_charge = 0;
            let _ = ui
                .send(UiEvent::NetworkMetrics {
                    received_bytes: self.room_received.clone(),
                    circuit_payload_bytes: self.circuit_received.values().sum(),
                    directory_bytes: self.control_bytes,
                    published_bytes: self.published_bytes,
                    worst_receiver_p95_ms: self.lane_delays(),
                    missed_probes: self.missed_total,
                    confirmation_neighbors: self.confirmation_neighbors.clone(),
                })
                .await;
        }
    }
    fn room_path_confirmed(&self) -> bool {
        let members = self.members();
        let local = self.local_peer_id().to_string();
        let fresh = Duration::from_secs((members.len() * 4).max(12) as u64);
        (0..2).any(|lane| {
            members.iter().filter(|m| **m != local).all(|m| {
                self.samples[lane]
                    .get(m)
                    .and_then(|s| s.back())
                    .is_some_and(|(at, _)| at.elapsed() < fresh)
            })
        })
    }
    fn check_handoffs(&mut self, now: Instant, local: &str) {
        let check = directory::room_topic(directory::ROUTE_PROBE);
        if self.repair_probe.is_none() {
            for i in 0..2 {
                let _ = self.lane(i).subscribe(&check);
            }
        }
        if self
            .repair_probe
            .is_none_or(|t| t.elapsed() >= Duration::from_secs(5))
        {
            self.repair_probe = Some(now);
            let circuits: Vec<_> = self
                .routes
                .iter()
                .filter(|(_, ls)| ls.values().any(|(d, _)| !*d))
                .map(|(p, _)| *p)
                .collect();
            for p in circuits {
                let name = p.to_string();
                let parallel = self.routes[&p].values().any(|(d, _)| *d);
                let unused = self.plan.neighbors.iter().all(|n| !n.contains(&name))
                    && self.directory.records.get(&name).is_some_and(|r| {
                        r.record
                            .links
                            .iter()
                            .any(|l| l.peer == local && l.lanes == 0)
                    });
                if parallel || unused {
                    if parallel {
                        self.handoffs.entry(p).or_insert(now);
                    }
                    let ticket = new_nonce(*self.local_peer_id());
                    let pkt = Packet {
                        id: ticket,
                        body: Payload::Probe(name.clone()),
                    };
                    if self
                        .lane(0)
                        .publish(check.clone(), serde_json::to_vec(&pkt).unwrap())
                        .is_ok()
                    {
                        self.probes.insert(ticket, (0, name.clone(), now, true));
                    }
                    if unused
                        && self.room_path_confirmed()
                        && self
                            .alternate_proof
                            .get(&p)
                            .is_some_and(|t| t.elapsed() < Duration::from_secs(10))
                    {
                        let since = self.handoffs.entry(p).or_insert(now);
                        if since.elapsed() >= Duration::from_secs(10)
                            && !self.repairs.contains(&name)
                        {
                            let circuits = self.routes[&p]
                                .iter()
                                .filter(|(_, v)| !v.0)
                                .map(|(id, _)| *id)
                                .collect::<Vec<_>>();
                            for id in circuits {
                                self.close_connection(id);
                            }
                        }
                    }
                } else {
                    self.handoffs.remove(&p);
                }
            }
        }
        let closable = self
            .handoffs
            .keys()
            .copied()
            .filter(|p| {
                self.handoffs[p].elapsed() >= Duration::from_secs(10) && self.handoff_ready(*p)
            })
            .collect::<Vec<_>>();
        for p in closable {
            self.handoffs.remove(&p);
            let ids = self.routes[&p]
                .iter()
                .filter_map(|(id, (direct, _))| (!direct).then_some(*id))
                .collect::<Vec<_>>();
            for id in ids {
                self.close_connection(id);
            }
        }
    }
    fn repair_connections(&mut self, now: Instant, local: &str) {
        if now >= self.next_dial
            && self
                .network_info()
                .connection_counters()
                .num_pending_outgoing()
                < 2
        {
            let overdue = self
                .room
                .as_deref()
                .map(|r| self.directory.members(r))
                .unwrap_or_default()
                .into_iter()
                .find(|id| {
                    id != local
                        && (self.lost_member(id)
                            || (!self.plan.healthy_peer_paths.contains(id)
                                && id.parse().is_ok_and(|p| self.degraded(&p)))
                            || (0..2).all(|i| {
                                self.missed.get(&(i, id.clone())).copied().unwrap_or(0) >= 3
                            }))
                        && self.dialed.get(id).is_none_or(|(t, _)| *t <= now)
                });
            if let Some(id) = overdue {
                if let Ok(peer) = id.parse::<PeerId>() {
                    let addrs = self
                        .addresses(&id)
                        .into_iter()
                        .filter(is_relay_address)
                        .collect::<Vec<_>>();
                    if !addrs.is_empty()
                        && !self
                            .routes
                            .get(&peer)
                            .is_some_and(|rs| rs.values().any(|v| !v.0))
                    {
                        self.next_dial = now + Duration::from_secs(5);
                        self.dialed.insert(id, (now + Duration::from_secs(60), 1));
                        let _ = self.dial(
                            DialOpts::peer_id(peer)
                                .addresses(addrs)
                                .condition(PeerCondition::Always)
                                .build(),
                        );
                    }
                }
            }
        }
        if now >= self.next_dial
            && self
                .network_info()
                .connection_counters()
                .num_pending_outgoing()
                < 2
        {
            self.next_dial = now + Duration::from_secs(5);
            self.dialed
                .retain(|id, _| self.directory.records.contains_key(id));
            if let Some((id, peer, addresses)) = self
                .shortcut
                .iter()
                .chain(self.plan.candidates.iter())
                .filter_map(|id| {
                    let p = id.parse::<PeerId>().ok()?;
                    if self.is_connected(&p) || self.dialed.get(id).is_some_and(|(t, _)| *t > now) {
                        return None;
                    }
                    let attempts = self.dialed.get(id).map_or(0, |(_, n)| *n);
                    let mut addresses = self.addresses(id);
                    if self.shortcut.as_ref() == Some(id) && !self.plan.candidates.contains(id) {
                        addresses.retain(|a| !is_relay_address(a));
                    }
                    if attempts == 0 && addresses.iter().any(|a| !is_relay_address(a)) {
                        addresses.retain(|a| !is_relay_address(a));
                    }
                    addresses = prioritize_multiaddrs(addresses);
                    (!addresses.is_empty()).then(|| (id.clone(), p, addresses))
                })
                .next()
            {
                let attempt = self.dialed.get(&id).map_or(1, |(_, n)| *n + 1);
                self.dialed.insert(
                    id,
                    (now + Duration::from_secs(10 << attempt.min(6)), attempt),
                );
                let _ = self.dial(
                    DialOpts::peer_id(peer)
                        .addresses(addresses)
                        .condition(PeerCondition::DisconnectedAndNotDialing)
                        .build(),
                );
            }
        }
    }
    fn sample_delivery(&mut self, now: Instant, local: &str) {
        let members = self
            .room
            .as_ref()
            .map(|r| self.directory.members(r))
            .unwrap_or_default();
        if now >= self.tail_window {
            self.tail_window = now + Duration::from_secs(30);
            let tail = members
                .iter()
                .filter(|p| !self.plan.neighbors.iter().any(|n| n.contains(*p)))
                .filter_map(|p| {
                    let best = (0..2)
                        .filter_map(|i| {
                            self.samples[i]
                                .get(p)
                                .and_then(|s| p95(s, Duration::from_secs(120)))
                        })
                        .min()?;
                    (best >= 100).then_some((best, p.clone()))
                })
                .max()
                .map(|(_, p)| p);
            self.shortcut = tail.clone().filter(|_| tail == self.tail_target);
            self.tail_target = tail;
        }
        for lane in 0..2 {
            self.samples[lane].retain(|id, _| members.contains(id));
            self.confirmation_neighbors[lane].retain(|id, _| members.contains(id));
        }
        self.missed.retain(|(_, p), _| members.contains(p));
        let mut requests = self.repairs.clone();
        requests.retain(|p| {
            members.contains(p) && self.missed.get(&(0, p.clone())).copied().unwrap_or(0) > 0
        });
        for p in &members {
            if (0..2).all(|i| self.missed.get(&(i, p.clone())).copied().unwrap_or(0) >= 3) {
                requests.insert(p.clone());
            }
        }
        if requests != self.repairs {
            self.repairs = requests;
            self.announce = true;
        }
        self.credits
            .retain(|r, _| self.plan.rooms.iter().any(|rooms| rooms.contains(r)));
        self.seen
            .retain(|_, t| now.duration_since(*t) < Duration::from_secs(120));
        self.probes.retain(|_, (lane, p, t, diagnostic)| {
            if now.duration_since(*t) < Duration::from_secs(10) {
                return true;
            }
            if !*diagnostic {
                *self.missed.entry((*lane, p.clone())).or_default() += 1;
                self.missed_total += 1;
            }
            false
        });
        if now >= self.next_probe {
            self.next_probe = now + Duration::from_secs(2);
            if let Some(room) = self.room.clone() {
                let peers: Vec<_> = self
                    .directory
                    .members(&room)
                    .into_iter()
                    .filter(|id| id != local)
                    .collect();
                if !peers.is_empty() {
                    let target = &peers[self.probe_index % peers.len()];
                    self.probe_index = self.probe_index.wrapping_add(1);
                    for i in 0..2 {
                        let id = new_nonce(*self.local_peer_id());
                        let packet = Packet {
                            id,
                            body: Payload::Probe(target.clone()),
                        };
                        let _ = self.lane(i).publish(
                            directory::room_topic(&room),
                            serde_json::to_vec(&packet).unwrap(),
                        );
                        self.probes.insert(id, (i, target.clone(), now, false));
                    }
                }
            }
        }
    }
    pub fn lost_member(&self, p: &str) -> bool {
        self.room.as_ref().is_none_or(|r| {
            self.directory
                .records
                .get(p)
                .is_none_or(|v| v.record.room.as_ref() != Some(r))
        }) || self
            .lost
            .get(p)
            .is_some_and(|t| t.elapsed() >= Duration::from_secs(5))
    }
    fn reconcile(&mut self, mut next: topology::Plan, now: Instant) {
        // Lease slots rotate under excess demand, so a room name cannot starve another.
        let demand = next.rooms[0]
            .union(&next.rooms[1])
            .filter(|r| Some(*r) != self.room.as_ref())
            .cloned()
            .collect::<Vec<_>>();
        let slots = self.directory.records[&self.local_peer_id().to_string()]
            .record
            .transit_rooms
            .min(HELPER_ROOMS);
        let offset = if demand.len() > slots {
            (clock() / 90) as usize % demand.len()
        } else {
            0
        };
        let helpers = demand
            .iter()
            .cycle()
            .skip(offset)
            .take(demand.len().min(slots))
            .collect::<BTreeSet<_>>();
        for rooms in &mut next.rooms {
            rooms.retain(|r| Some(r) == self.room.as_ref() || helpers.contains(r));
        }
        let valid_edges = self.plan.trees.iter().flatten().all(|(a, b)| {
            self.directory
                .records
                .get(a)
                .is_some_and(|r| r.record.links.iter().any(|l| l.peer == *b))
                && self
                    .directory
                    .records
                    .get(b)
                    .is_some_and(|r| r.record.links.iter().any(|l| l.peer == *a))
        });
        let replacement = valid_edges
            && self.plan.rooms == next.rooms
            && (0..2).all(|i| {
                self.plan.trees[i].len() == next.trees[i].len()
                    && self.plan.neighbors[i]
                        .iter()
                        .all(|p| p.parse().is_ok_and(|p: PeerId| self.is_connected(&p)))
            });
        if !self
            .directory
            .records
            .values()
            .any(|r| !r.record.repairs.is_empty())
            && next != self.plan
            && replacement
            && self.plan.trees != next.trees
        {
            let pending = self.proposed.get_or_insert_with(|| (next.clone(), now));
            if pending.0.trees != next.trees {
                *pending = (next.clone(), now);
            }
            if now.duration_since(pending.1) < Duration::from_secs(60) {
                return;
            }
        }
        self.proposed = None;
        if self.plan.trees != next.trees {
            self.samples = Default::default();
            self.confirmation_neighbors = Default::default();
            self.probes.clear();
        }
        for i in 0..2 {
            let old = self.plan.neighbors[i].clone();
            let rooms = self.plan.rooms[i].clone();
            for r in next.rooms[i].difference(&rooms) {
                let _ = self.lane(i).subscribe(&directory::room_topic(r));
            }
            for p in next.neighbors[i].difference(&old) {
                if let Ok(p) = p.parse() {
                    self.lane(i).add_explicit_peer(&p);
                }
            }
            for p in old.difference(&next.neighbors[i]) {
                if let Ok(p) = p.parse() {
                    self.lane(i).remove_explicit_peer(&p);
                }
            }
            for r in rooms.difference(&next.rooms[i]) {
                self.lane(i).unsubscribe(&directory::room_topic(r));
            }
        }
        if self.plan.trees != next.trees || self.plan.rooms != next.rooms {
            self.room_routes_changed = true;
        }
        if self.plan.neighbors != next.neighbors || self.plan.rooms != next.rooms {
            self.announce = true;
        }
        if self.plan.trees != next.trees {
            self.alternate_proof.clear();
        }
        self.plan = next;
    }
    pub fn receive(
        &mut self,
        i: usize,
        via: PeerId,
        id: gossipsub::MessageId,
        msg: gossipsub::Message,
    ) -> Option<(PeerId, WireMessage)> {
        use gossipsub::MessageAcceptance::*;
        let room = msg.topic.as_str().strip_prefix("light-ear.room.v2/")?;
        let joined = self.room.as_deref() == Some(room);
        let packet = serde_json::from_slice::<Packet>(&msg.data).ok();
        let now = Instant::now();
        let source = msg.source?;
        let diagnostics = room == directory::ROUTE_PROBE
            && packet
                .as_ref()
                .is_some_and(|p| !matches!(p.body, Payload::Room(_)));
        let authenticated_member = self
            .directory
            .records
            .get(&source.to_string())
            .is_some_and(|v| v.record.room.as_deref() == Some(room));
        let admitted = (diagnostics || (authenticated_member && self.plan.rooms[i].contains(room)))
            && self.plan.neighbors[i].contains(&via.to_string());
        let connection = self.lane(i).connection(&id);
        let direct = connection.and_then(|c| self.routes.get(&via)?.get(&c).map(|v| v.0));
        // Explicit neighbors are the sole eager recipients. Charge only subscribed
        // outgoing branches, excluding the author and the incoming connection.
        let router = if i == 0 {
            &self.inner.behaviour().gossipsub
        } else {
            &self.inner.behaviour().alternate
        };
        let copies = router
            .all_peers()
            .filter(|(p, topics)| {
                **p != via
                    && **p != source
                    && self.plan.neighbors[i].contains(&p.to_string())
                    && topics.contains(&&msg.topic)
            })
            .count();
        let charge = msg.data.len().saturating_mul(copies);
        let helpers = self.plan.rooms[0]
            .union(&self.plan.rooms[1])
            .filter(|r| Some(*r) != self.room.as_ref())
            .count()
            .max(1);
        let budget = self.transit_rate / helpers;
        let accept = if !admitted {
            Ignore
        } else if packet.is_none() {
            Reject
        } else if joined || diagnostics {
            Accept
        } else {
            let slot = self.credits.entry(room.to_owned()).or_insert((now, 0));
            let drained = (now.duration_since(slot.0).as_secs_f64() * budget as f64) as usize;
            if drained > 0 {
                slot.1 = slot.1.saturating_sub(drained);
                slot.0 = now;
            }
            // A full bucket can admit one large publication, then pays off its bytes.
            if budget == 0 || (slot.1 > 0 && slot.1.saturating_add(charge) > budget) {
                self.exhausted = true;
                Ignore
            } else {
                slot.1 += charge;
                self.interval_charge += charge;
                Accept
            }
        };
        let apply = matches!(accept, Accept) && (joined || diagnostics);
        if matches!(accept, Accept)
            && matches!(packet.as_ref().map(|p| &p.body), Some(Payload::Room(_)))
        {
            self.received.entry(via.to_string()).or_default()[i] += msg.data.len() as u64;
            if joined {
                self.room_received.entry(via.to_string()).or_default()[i] += msg.data.len() as u64;
            }
            if direct == Some(false) {
                *self.circuit_received.entry(via).or_default() += msg.data.len() as u64;
            }
        }
        self.lane(i)
            .report_message_validation_result(&id, &via, accept);
        if !apply {
            return None;
        }
        let packet = packet?;
        let local = self.local_peer_id().to_string();
        match packet.body {
            Payload::Room(wire) => {
                if self
                    .seen
                    .insert((room.into(), source.to_string(), packet.id), now)
                    .is_some()
                {
                    return None;
                }
                Some((source, wire))
            }
            Payload::Probe(target) if target == local => {
                let ack = Packet {
                    id: new_nonce(*self.local_peer_id()),
                    body: Payload::Ack(
                        packet.id,
                        source.to_string(),
                        via == source && direct == Some(true),
                        via != source,
                    ),
                };
                let _ = self
                    .lane(i)
                    .publish(directory::room_topic(room), serde_json::to_vec(&ack).ok()?);
                None
            }
            Payload::Ack(id, target, direct_arrival, indirect_arrival) if target == local => {
                if self
                    .probes
                    .get(&id)
                    .is_some_and(|(lane, p, _, for_diagnostics)| {
                        *lane == i && *p == source.to_string() && *for_diagnostics == diagnostics
                    })
                {
                    let (_, p, start, _) = self.probes.remove(&id)?;
                    if diagnostics {
                        if via != source && indirect_arrival {
                            self.alternate_proof.insert(source, now);
                        } else if via == source
                            && direct == Some(true)
                            && direct_arrival
                            && !self.members().contains(&source.to_string())
                        {
                            // Same-room endpoints prove this socket with a room probe below.
                            self.direct_proof.insert(via, now);
                        }
                        return None;
                    }
                    self.missed.remove(&(i, p.clone()));
                    if via == source && direct == Some(true) && direct_arrival {
                        self.direct_proof.insert(via, now);
                    }
                    self.confirmation_neighbors[i].insert(p.clone(), via.to_string());
                    let series = self.samples[i].entry(p).or_default();
                    if series.len() >= 20 {
                        series.pop_front();
                    }
                    series.push_back((now, now.duration_since(start).as_millis() as u64));
                }
                None
            }
            _ => None,
        }
    }
    fn lane_delays(&self) -> [Option<u64>; 2] {
        let local = self.local_peer_id().to_string();
        let members = self.members();
        let freshness = Duration::from_secs((members.len() * 4).max(12) as u64);
        std::array::from_fn(|i| {
            members
                .iter()
                .filter(|m| **m != local)
                .map(|m| {
                    if self.missed.get(&(i, m.clone())).copied().unwrap_or(0) >= 2 {
                        return None;
                    }
                    let series = self.samples[i].get(m)?;
                    p95(series, freshness)
                })
                .collect::<Option<Vec<_>>>()?
                .into_iter()
                .max()
        })
    }
    pub fn publish_room(
        &mut self,
        topic: &gossipsub::IdentTopic,
        wire: &WireMessage,
    ) -> Result<RoomPublishOutcome> {
        let Some(room) = self.room.as_deref() else {
            return Ok(RoomPublishOutcome::NoSubscribers);
        };
        if directory::room_topic(room).hash() != topic.hash() {
            return Ok(RoomPublishOutcome::NoSubscribers);
        }
        let weights = self.lane_delays().map(|delay| delay.map(|ms| ms.max(25)));
        let i = match weights {
            [Some(_), Some(_)] => usize::from(self.assigned[1] < self.assigned[0]),
            [None, Some(_)] => 1,
            _ => 0,
        };
        let data = serde_json::to_vec(&Packet {
            id: new_nonce(*self.local_peer_id()),
            body: Payload::Room(wire.clone()),
        })?;
        let mut outcome = RoomPublishOutcome::NoSubscribers;
        for lane in [i, 1 - i] {
            match self.lane(lane).publish(topic.clone(), data.clone()) {
                Ok(_) => {
                    self.published_bytes[lane] += data.len() as u64;
                    if let [Some(a), Some(b)] = weights {
                        let cost = if a.abs_diff(b) <= 50 { 1 } else { [a, b][lane] };
                        self.assigned[lane] = self.assigned[lane]
                            .saturating_add(cost.saturating_mul(data.len() as u64));
                        let floor = self.assigned[0].min(self.assigned[1]);
                        self.assigned.iter_mut().for_each(|v| *v -= floor);
                    } else {
                        self.assigned = [0; 2];
                    }
                    return Ok(RoomPublishOutcome::Published);
                }
                Err(e) => match classify_room_publish_error(&e) {
                    RoomPublishPlan::Published => return Ok(RoomPublishOutcome::Published),
                    RoomPublishPlan::Failed => return Err(e.into()),
                    RoomPublishPlan::Congested => outcome = RoomPublishOutcome::Congested,
                    RoomPublishPlan::NoSubscribers => {}
                },
            }
        }
        Ok(outcome)
    }
}

fn p95(series: &VecDeque<(Instant, u64)>, freshness: Duration) -> Option<u64> {
    let mut values = series
        .iter()
        .filter(|(t, _)| t.elapsed() <= freshness)
        .map(|(_, v)| *v)
        .collect::<Vec<_>>();
    values.sort_unstable();
    values
        .get((values.len() * 95).div_ceil(100).saturating_sub(1))
        .copied()
}
