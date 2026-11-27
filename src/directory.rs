//! Signed, expiring presence shared over the authenticated directory protocol.
use libp2p::{Multiaddr, PeerId, identity, multiaddr::Protocol};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const ROUTE_PROBE: &str = "\0path";
pub const NAMESPACE: &str = "light-ear.network.v3";
pub const MAX_PEERS: usize = 512;
const TTL: u64 = 90;
const PAGE: usize = 8;
const MAX_ADDRESSES: usize = 8;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Link {
    pub peer: String,
    pub direct: bool,
    pub rtt_ms: u32,
    pub degraded: bool,
    pub lanes: u8,
    pub payload_bytes: [u64; 2],
    pub circuit_bytes: u64,
    pub media_rx: u64,
    pub media_relay_rx: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Presence {
    pub owner: String,
    pub sequence: u64,
    pub issued: u64,
    pub room: Option<String>,
    pub addresses: Vec<String>,
    pub links: Vec<Link>,
    pub transit_rooms: usize,
    pub upload_utilization: u8,
    pub media_p95_ms: Option<u64>,
    pub leases: [Vec<String>; 2],
    pub repairs: Vec<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Signed {
    pub record: Presence,
    key: Vec<u8>,
    signature: Vec<u8>,
}
fn signing_bytes(record: &Presence) -> Vec<u8> {
    let mut bytes = b"light-ear.presence.v3\0".to_vec();
    serde_json::to_writer(&mut bytes, record).expect("serializable presence");
    bytes
}
impl Signed {
    pub fn new(record: Presence, key: &identity::Keypair) -> Self {
        let signature = key.sign(&signing_bytes(&record)).expect("ed25519 signing");
        Self {
            record,
            key: key.public().encode_protobuf(),
            signature,
        }
    }
    fn valid(&self, now: u64) -> bool {
        let r = &self.record;
        r.issued <= now.saturating_add(5)
            && r.issued.saturating_add(TTL) > now
            && r.room.as_deref().is_none_or(valid_room)
            && r.addresses.len() <= MAX_ADDRESSES
            && r.addresses
                .iter()
                .all(|a| a.len() <= 512 && a.parse::<Multiaddr>().is_ok_and(|a| shareable(&a)))
            && r.repairs.len() <= MAX_PEERS
            && r.repairs.iter().all(|p| p.parse::<PeerId>().is_ok())
            && r.leases
                .iter()
                .all(|rooms| rooms.len() <= 9 && rooms.iter().all(|r| valid_room(r)))
            && r.links.len() <= MAX_PEERS
            && r.links.iter().all(|l| l.peer.parse::<PeerId>().is_ok())
            && identity::PublicKey::try_decode_protobuf(&self.key).is_ok_and(|key| {
                key.to_peer_id().to_string() == r.owner
                    && key.verify(&signing_bytes(r), &self.signature)
            })
    }
}
pub fn valid_room(room: &str) -> bool {
    !room.is_empty()
        && room.len() <= 128
        && room.trim() == room
        && !room.chars().any(char::is_control)
}
pub fn room_topic(room: &str) -> libp2p::gossipsub::IdentTopic {
    libp2p::gossipsub::IdentTopic::new(format!("light-ear.room.v3/{room}"))
}
pub fn presence_addresses<'a>(addrs: impl Iterator<Item = &'a Multiaddr>) -> Vec<String> {
    let (circuits, direct): (BTreeSet<_>, BTreeSet<_>) = addrs
        .filter(|a| shareable(a))
        .partition(|a| crate::connection_state::is_relay_address(a));
    // Keep one advertised reservation even on hosts with many public interfaces.
    circuits
        .iter()
        .take(1)
        .chain(&direct)
        .chain(circuits.iter().skip(1))
        .take(MAX_ADDRESSES)
        .map(ToString::to_string)
        .collect()
}
pub fn shareable(addr: &Multiaddr) -> bool {
    // LAN candidates stay in the local mDNS table. Gossip only public IP literals.
    let mut parts = addr.iter();
    let ip = parts.next();
    let transport = match parts.next() {
        Some(Protocol::Tcp(port)) => port != 0,
        Some(Protocol::Udp(port)) => port != 0 && parts.next() == Some(Protocol::QuicV1),
        _ => false,
    };
    if !transport {
        return false;
    }
    let suffix: Vec<_> = parts.collect();
    if !matches!(
        suffix.as_slice(),
        [] | [Protocol::P2p(_)]
            | [Protocol::P2p(_), Protocol::P2pCircuit]
            | [Protocol::P2p(_), Protocol::P2pCircuit, Protocol::P2p(_)]
    ) {
        return false;
    }
    match ip {
        Some(Protocol::Ip4(ip)) => {
            let [a, b, ..] = ip.octets();
            !(ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_multicast()
                || ip.is_documentation()
                || a == 0
                || a >= 224
                || (a == 100 && (64..128).contains(&b))
                || (a == 198 && (b == 18 || b == 19)))
        }
        Some(Protocol::Ip6(ip)) => {
            ip.segments()[0] & 0xe000 == 0x2000 && ip.segments()[..2] != [0x2001, 0xdb8]
        }
        _ => false,
    }
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Summary {
    pub known: BTreeMap<String, u64>,
    pub after: Option<String>,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Page {
    pub records: Vec<Signed>,
    pub next: Option<String>,
}

#[derive(Default)]
pub struct Directory {
    pub records: BTreeMap<String, Signed>,
}
impl Directory {
    pub fn merge(&mut self, entry: Signed, now: u64) -> bool {
        let id = &entry.record.owner;
        if self
            .records
            .get(id)
            .is_some_and(|old| old.record.sequence >= entry.record.sequence)
            || (!self.records.contains_key(id) && self.records.len() >= MAX_PEERS)
            || !entry.valid(now)
        {
            return false;
        }
        self.records.insert(id.clone(), entry);
        true
    }
    pub fn expire(&mut self, now: u64) {
        self.records
            .retain(|_, item| item.record.issued.saturating_add(TTL) > now);
    }
    pub fn summary(&self, after: Option<String>) -> Summary {
        Summary {
            known: self
                .records
                .iter()
                .map(|(k, v)| (k.clone(), v.record.sequence))
                .collect(),
            after,
        }
    }
    pub fn page(&self, q: &Summary) -> Page {
        if q.known.len() > MAX_PEERS {
            return Page::default();
        }
        let mut missing = self.records.iter().filter(|(k, v)| {
            q.after.as_ref().is_none_or(|a| *k > a)
                && q.known.get(*k).is_none_or(|seq| *seq < v.record.sequence)
        });
        let records: Vec<_> = missing
            .by_ref()
            .take(PAGE)
            .map(|(_, v)| v.clone())
            .collect();
        let next = missing
            .next()
            .and_then(|_| records.last().map(|v| v.record.owner.clone()));
        Page { records, next }
    }
    pub fn rooms(&self) -> BTreeMap<String, usize> {
        let mut rooms = BTreeMap::new();
        for name in self.records.values().filter_map(|v| v.record.room.as_ref()) {
            *rooms.entry(name.clone()).or_default() += 1
        }
        rooms
    }
    pub fn members(&self, room: &str) -> BTreeSet<String> {
        self.records
            .values()
            .filter(|v| v.record.room.as_deref() == Some(room))
            .map(|v| v.record.owner.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn entry(key: &identity::Keypair, seq: u64, room: Option<&str>) -> Signed {
        Signed::new(
            Presence {
                owner: key.public().to_peer_id().to_string(),
                sequence: seq,
                issued: 100,
                room: room.map(str::to_owned),
                addresses: vec![],
                links: vec![],
                transit_rooms: 8,
                upload_utilization: 0,
                media_p95_ms: None,
                leases: Default::default(),
                repairs: vec![],
            },
            key,
        )
    }
    #[test]
    fn signed_room_lease_cannot_be_extended_by_replay_or_tampering() {
        let key = identity::Keypair::generate_ed25519();
        let mut directory = Directory::default();
        let joined = entry(&key, 1, Some("music"));
        assert!(directory.merge(joined.clone(), 100));
        let mut forged = joined.clone();
        forged.record.room = None;
        forged.record.sequence = 2;
        assert!(!directory.merge(forged, 110));
        assert!(directory.merge(entry(&key, 2, None), 120));
        assert!(!directory.merge(joined.clone(), 189));
        assert!(directory.rooms().is_empty());
        directory.expire(190);
        assert!(!directory.merge(joined, 190));
        assert!(directory.records.is_empty());
    }
    #[test]
    fn paged_reconciliation_preserves_other_origins_and_skips_current_entries() {
        let mut first = Directory::default();
        let mut second = Directory::default();
        for _ in 0..19 {
            let r = entry(&identity::Keypair::generate_ed25519(), 1, Some("a"));
            assert!(first.merge(r, 100));
        }
        let mut cursor = None;
        let mut pages = 0;
        loop {
            let p = first.page(&second.summary(cursor));
            assert!(p.records.len() <= PAGE);
            pages += 1;
            for r in p.records {
                assert!(second.merge(r, 100));
            }
            cursor = p.next;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(pages, 3);
        assert_eq!(second.rooms()["a"], 19);
        assert!(first.page(&second.summary(None)).records.is_empty());
    }
    #[test]
    fn browser_presence_and_other_rooms_are_excluded_from_membership() {
        let mut d = Directory::default();
        let mut members = BTreeSet::new();
        for room in [Some("music"), Some("chat"), None, Some("music")] {
            let k = identity::Keypair::generate_ed25519();
            if room == Some("music") {
                members.insert(k.public().to_peer_id().to_string());
            }
            d.merge(entry(&k, 1, room), 100);
        }
        assert_eq!(d.members("music"), members);
    }
    #[test]
    fn directory_never_forwards_lan_candidates() {
        for a in [
            "/ip4/192.168.1.2/tcp/42",
            "/ip4/127.0.0.1/tcp/42",
            "/ip6/fd00::1/tcp/42",
            "/ip4/100.64.0.1/tcp/42",
        ] {
            assert!(!shareable(&a.parse().unwrap()));
        }
        assert!(shareable(&"/ip4/1.1.1.1/tcp/42".parse().unwrap()));
    }

    #[test]
    fn presence_keeps_a_public_reservation_among_many_direct_listeners() {
        let server = identity::Keypair::generate_ed25519().public().to_peer_id();
        let mut addrs: Vec<Multiaddr> = (1..=12)
            .map(|port| format!("/ip4/1.1.1.1/tcp/{port}").parse().unwrap())
            .collect();
        let public: Multiaddr = format!("/ip4/8.8.8.8/tcp/42/p2p/{server}/p2p-circuit")
            .parse()
            .unwrap();
        let private: Multiaddr = format!("/ip4/192.168.1.2/tcp/42/p2p/{server}/p2p-circuit")
            .parse()
            .unwrap();
        addrs.extend([public.clone(), public.clone(), private.clone()]);
        let advertised = presence_addresses(addrs.iter());
        assert_eq!(advertised.len(), MAX_ADDRESSES);
        assert_eq!(
            advertised
                .iter()
                .filter(|a| **a == public.to_string())
                .count(),
            1
        );
        assert!(!advertised.contains(&private.to_string()));
    }
}
