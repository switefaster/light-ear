use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error,
    io::{self, Read, Write},
    net::{Ipv4Addr, Ipv6Addr, SocketAddr, TcpListener, TcpStream},
    sync::{Arc, RwLock},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::Parser;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, SwarmBuilder, identify, identity, noise, ping, relay, rendezvous,
    swarm::{NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use serde::Serialize;
use tracing_subscriber::EnvFilter;

const EVENT_LOG_LIMIT: usize = 80;
const HTTP_READ_TIMEOUT: Duration = Duration::from_secs(2);
const SWARM_IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60 * 60);
const RELAY_MAX_CIRCUIT_DURATION: Duration = Duration::from_secs(60 * 60 * 6);
const RELAY_MAX_CIRCUIT_BYTES: u64 = 0;

#[derive(Debug, Parser)]
#[command(author, version, about = "Public light-ear relay and rendezvous node")]
struct Cli {
    #[arg(long, default_value_t = 4001)]
    port: u16,

    #[arg(long, default_value_t = 0)]
    secret_key_seed: u8,

    #[arg(long, value_parser = parse_multiaddr)]
    listen: Vec<Multiaddr>,

    #[arg(long, value_parser = parse_multiaddr)]
    external_addr: Vec<Multiaddr>,

    #[arg(long, default_value = "127.0.0.1:8080")]
    web_addr: SocketAddr,

    #[arg(long)]
    no_web: bool,
}

#[derive(NetworkBehaviour)]
struct RelayBehaviour {
    relay: relay::Behaviour,
    rendezvous: rendezvous::server::Behaviour,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
}

type SharedTopology = Arc<RwLock<TopologyState>>;

#[derive(Debug)]
struct TopologyState {
    local_peer_id: String,
    started_at_millis: u128,
    updated_at_millis: u128,
    listen_addresses: BTreeSet<String>,
    external_addresses: BTreeSet<String>,
    peers: BTreeMap<String, PeerState>,
    rendezvous_registrations: BTreeMap<String, RendezvousRegistrationState>,
    recent_events: VecDeque<TopologyEvent>,
}

#[derive(Debug, Default)]
struct PeerState {
    connections: usize,
    addresses: BTreeSet<String>,
    protocols: BTreeSet<String>,
    agent_version: Option<String>,
    last_seen_millis: u128,
}

#[derive(Debug)]
struct RendezvousRegistrationState {
    peer_id: String,
    namespace: String,
    addresses: Vec<String>,
    registered_at_millis: u128,
    refreshed_at_millis: u128,
}

#[derive(Debug, Clone, Serialize)]
struct TopologySnapshot {
    local_peer_id: String,
    started_at_millis: u128,
    updated_at_millis: u128,
    listen_addresses: Vec<String>,
    external_addresses: Vec<String>,
    connected_peers: Vec<TopologyPeer>,
    rendezvous_registrations: Vec<RendezvousRegistrationView>,
    recent_events: Vec<TopologyEvent>,
}

#[derive(Debug, Clone, Serialize)]
struct TopologyPeer {
    peer_id: String,
    connections: usize,
    addresses: Vec<String>,
    protocols: Vec<String>,
    agent_version: Option<String>,
    last_seen_millis: u128,
}

#[derive(Debug, Clone, Serialize)]
struct RendezvousRegistrationView {
    peer_id: String,
    namespace: String,
    addresses: Vec<String>,
    registered_at_millis: u128,
    refreshed_at_millis: u128,
}

#[derive(Debug, Clone, Serialize)]
struct TopologyEvent {
    at_millis: u128,
    kind: String,
    detail: String,
}

impl TopologyState {
    fn new(local_peer_id: PeerId) -> Self {
        let now = now_millis();
        Self {
            local_peer_id: local_peer_id.to_string(),
            started_at_millis: now,
            updated_at_millis: now,
            listen_addresses: BTreeSet::new(),
            external_addresses: BTreeSet::new(),
            peers: BTreeMap::new(),
            rendezvous_registrations: BTreeMap::new(),
            recent_events: VecDeque::new(),
        }
    }

    fn snapshot(&self) -> TopologySnapshot {
        TopologySnapshot {
            local_peer_id: self.local_peer_id.clone(),
            started_at_millis: self.started_at_millis,
            updated_at_millis: self.updated_at_millis,
            listen_addresses: self.listen_addresses.iter().cloned().collect(),
            external_addresses: self.external_addresses.iter().cloned().collect(),
            connected_peers: self
                .peers
                .iter()
                .map(|(peer_id, peer)| TopologyPeer {
                    peer_id: peer_id.clone(),
                    connections: peer.connections,
                    addresses: peer.addresses.iter().cloned().collect(),
                    protocols: peer.protocols.iter().cloned().collect(),
                    agent_version: peer.agent_version.clone(),
                    last_seen_millis: peer.last_seen_millis,
                })
                .collect(),
            rendezvous_registrations: self
                .rendezvous_registrations
                .values()
                .map(|registration| RendezvousRegistrationView {
                    peer_id: registration.peer_id.clone(),
                    namespace: registration.namespace.clone(),
                    addresses: registration.addresses.clone(),
                    registered_at_millis: registration.registered_at_millis,
                    refreshed_at_millis: registration.refreshed_at_millis,
                })
                .collect(),
            recent_events: self.recent_events.iter().cloned().collect(),
        }
    }

    fn add_listen_address(&mut self, address: Multiaddr) {
        self.listen_addresses.insert(address.to_string());
        self.push_event("listen", format!("listening on {address}"));
    }

    fn add_external_address(&mut self, address: Multiaddr) {
        self.external_addresses.insert(address.to_string());
        self.push_event("external", format!("advertising {address}"));
    }

    fn peer_connected(&mut self, peer_id: PeerId, address: Option<String>) {
        let now = now_millis();
        let connections = {
            let peer = self.peers.entry(peer_id.to_string()).or_default();
            peer.connections = peer.connections.saturating_add(1);
            peer.last_seen_millis = now;
            if let Some(address) = address {
                peer.addresses.insert(address);
            }
            peer.connections
        };
        self.touch();
        self.push_event(
            "peer",
            format!("{peer_id} connected ({connections} link(s))"),
        );
    }

    fn peer_disconnected(&mut self, peer_id: PeerId, remaining_connections: usize) {
        if remaining_connections == 0 {
            self.peers.remove(&peer_id.to_string());
        } else if let Some(peer) = self.peers.get_mut(&peer_id.to_string()) {
            peer.connections = remaining_connections;
            peer.last_seen_millis = now_millis();
        }
        self.touch();
        self.push_event(
            "peer",
            format!("{peer_id} disconnected ({remaining_connections} link(s) remain)"),
        );
    }

    fn update_identify(&mut self, peer_id: PeerId, info: &identify::Info) {
        let peer = self.peers.entry(peer_id.to_string()).or_default();
        peer.last_seen_millis = now_millis();
        peer.agent_version = Some(info.agent_version.clone());
        peer.addresses
            .extend(info.listen_addrs.iter().map(ToString::to_string));
        peer.protocols
            .extend(info.protocols.iter().map(ToString::to_string));
        self.touch();
    }

    fn register_rendezvous(&mut self, peer_id: PeerId, registration: &rendezvous::Registration) {
        let now = now_millis();
        let namespace = registration.namespace.to_string();
        let key = rendezvous_key(peer_id, &namespace);
        let addresses = registration
            .record
            .addresses()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let entry = self.rendezvous_registrations.entry(key).or_insert_with(|| {
            RendezvousRegistrationState {
                peer_id: peer_id.to_string(),
                namespace: namespace.clone(),
                addresses: Vec::new(),
                registered_at_millis: now,
                refreshed_at_millis: now,
            }
        });
        entry.addresses = addresses;
        entry.refreshed_at_millis = now;
        self.touch();
        self.push_event("rendezvous", format!("{peer_id} registered in {namespace}"));
    }

    fn unregister_rendezvous(&mut self, peer_id: PeerId, namespace: &str) {
        self.rendezvous_registrations
            .remove(&rendezvous_key(peer_id, namespace));
        self.touch();
        self.push_event(
            "rendezvous",
            format!("{peer_id} unregistered from {namespace}"),
        );
    }

    fn expire_rendezvous(&mut self, registration: &rendezvous::Registration) {
        let peer_id = registration.record.peer_id();
        let namespace = registration.namespace.to_string();
        self.rendezvous_registrations
            .remove(&rendezvous_key(peer_id, &namespace));
        self.touch();
        self.push_event(
            "rendezvous",
            format!("{peer_id} registration expired in {namespace}"),
        );
    }

    fn record_event(&mut self, kind: impl Into<String>, detail: impl Into<String>) {
        self.push_event(kind, detail);
    }

    fn touch(&mut self) {
        self.updated_at_millis = now_millis();
    }

    fn push_event(&mut self, kind: impl Into<String>, detail: impl Into<String>) {
        self.updated_at_millis = now_millis();
        self.recent_events.push_front(TopologyEvent {
            at_millis: self.updated_at_millis,
            kind: kind.into(),
            detail: detail.into(),
        });
        while self.recent_events.len() > EVENT_LOG_LIMIT {
            self.recent_events.pop_back();
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init();

    let cli = Cli::parse();
    let local_key = generate_ed25519(cli.secret_key_seed);
    let mut swarm = SwarmBuilder::with_existing_identity(local_key)
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_behaviour(|key| {
            let local_peer_id = key.public().to_peer_id();
            RelayBehaviour {
                relay: relay::Behaviour::new(
                    local_peer_id,
                    relay::Config {
                        // 0 disables the byte cap in libp2p-relay; duration still needs a
                        // positive timeout because 0 would fire the circuit timer immediately.
                        max_circuit_duration: RELAY_MAX_CIRCUIT_DURATION,
                        max_circuit_bytes: RELAY_MAX_CIRCUIT_BYTES,
                        ..Default::default()
                    },
                ),
                rendezvous: rendezvous::server::Behaviour::new(
                    rendezvous::server::Config::default(),
                ),
                identify: identify::Behaviour::new(identify::Config::new(
                    "/light-ear-relay/0.1.0".to_string(),
                    key.public(),
                )),
                ping: ping::Behaviour::default(),
            }
        })?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(SWARM_IDLE_CONNECTION_TIMEOUT))
        .build();

    let local_peer_id = *swarm.local_peer_id();
    println!("local peer id: {local_peer_id}");
    let topology = Arc::new(RwLock::new(TopologyState::new(local_peer_id)));
    let _dashboard = if cli.no_web {
        None
    } else {
        let (web_addr, handle) = start_dashboard(Arc::clone(&topology), cli.web_addr)?;
        println!("topology dashboard: http://{web_addr}/");
        Some(handle)
    };

    let listen_addrs = if cli.listen.is_empty() {
        default_listen_addrs(cli.port)?
    } else {
        cli.listen
    };

    for address in listen_addrs {
        swarm.listen_on(address.clone())?;
        println!("listening requested on {address}");
    }

    for address in cli.external_addr {
        swarm.add_external_address(address.clone());
        let display_address = ensure_peer_id(address, local_peer_id);
        update_topology(&topology, |topology| {
            topology.add_external_address(display_address.clone());
        });
        println!("external address: {display_address}");
    }

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => {
                let display_address = ensure_peer_id(address, local_peer_id);
                update_topology(&topology, |topology| {
                    topology.add_listen_address(display_address.clone());
                });
                println!("listening on {display_address}");
            }
            SwarmEvent::ConnectionEstablished {
                peer_id, endpoint, ..
            } => {
                let remote_address = endpoint.get_remote_address().to_string();
                update_topology(&topology, |topology| {
                    topology.peer_connected(peer_id, Some(remote_address.clone()));
                });
                println!("connected {peer_id} from {remote_address}");
            }
            SwarmEvent::ConnectionClosed {
                peer_id,
                num_established,
                ..
            } => {
                update_topology(&topology, |topology| {
                    topology.peer_disconnected(peer_id, num_established as usize);
                });
                println!("disconnected {peer_id}; {num_established} link(s) remain");
            }
            SwarmEvent::Behaviour(RelayBehaviourEvent::Identify(identify::Event::Received {
                peer_id,
                info,
                ..
            })) => {
                let observed_addr = info.observed_addr.clone();
                swarm.add_external_address(observed_addr.clone());
                let display_address = ensure_peer_id(observed_addr, local_peer_id);
                update_topology(&topology, |topology| {
                    topology.add_external_address(display_address.clone());
                    topology.update_identify(peer_id, &info);
                    topology.record_event("identify", format!("identified {peer_id}"));
                });
                println!("observed external address {display_address}");
            }
            SwarmEvent::Behaviour(RelayBehaviourEvent::Relay(event)) => {
                update_topology(&topology, |topology| {
                    topology.record_event("relay", format!("{event:?}"));
                });
                println!("relay event: {event:?}");
            }
            SwarmEvent::Behaviour(RelayBehaviourEvent::Rendezvous(event)) => match event {
                rendezvous::server::Event::PeerRegistered { peer, registration } => {
                    update_topology(&topology, |topology| {
                        topology.register_rendezvous(peer, &registration);
                    });
                    println!(
                        "rendezvous registered {peer} in {} with {} addr(s)",
                        registration.namespace,
                        registration.record.addresses().len()
                    );
                }
                rendezvous::server::Event::DiscoverServed {
                    enquirer,
                    registrations,
                } => {
                    update_topology(&topology, |topology| {
                        topology.record_event(
                            "rendezvous",
                            format!(
                                "served {enquirer} with {} registration(s)",
                                registrations.len()
                            ),
                        );
                    });
                    println!(
                        "rendezvous served {enquirer} with {} registration(s)",
                        registrations.len()
                    );
                }
                rendezvous::server::Event::PeerUnregistered { peer, namespace } => {
                    let namespace = namespace.to_string();
                    update_topology(&topology, |topology| {
                        topology.unregister_rendezvous(peer, &namespace);
                    });
                    println!("rendezvous unregistered {peer} from {namespace}");
                }
                rendezvous::server::Event::RegistrationExpired(registration) => {
                    update_topology(&topology, |topology| {
                        topology.expire_rendezvous(&registration);
                    });
                    println!(
                        "rendezvous registration expired for {} in {}",
                        registration.record.peer_id(),
                        registration.namespace
                    );
                }
                rendezvous::server::Event::PeerNotRegistered {
                    peer,
                    namespace,
                    error,
                } => {
                    update_topology(&topology, |topology| {
                        topology.record_event(
                            "rendezvous",
                            format!("declined {peer} in {namespace}: {error:?}"),
                        );
                    });
                    println!("rendezvous declined {peer} in {namespace}: {error:?}");
                }
                rendezvous::server::Event::DiscoverNotServed { enquirer, error } => {
                    update_topology(&topology, |topology| {
                        topology.record_event(
                            "rendezvous",
                            format!("declined discovery for {enquirer}: {error:?}"),
                        );
                    });
                    println!("rendezvous discovery declined {enquirer}: {error:?}");
                }
            },
            event => {
                println!("{event:?}");
            }
        }
    }
}

fn start_dashboard(
    topology: SharedTopology,
    address: SocketAddr,
) -> io::Result<(SocketAddr, thread::JoinHandle<()>)> {
    let listener = TcpListener::bind(address)?;
    let local_addr = listener.local_addr()?;
    let handle = thread::Builder::new()
        .name("light-ear-relay-dashboard".to_string())
        .spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let topology = Arc::clone(&topology);
                        if let Err(err) = thread::Builder::new()
                            .name("light-ear-relay-dashboard-client".to_string())
                            .spawn(move || handle_http_connection(stream, topology))
                        {
                            eprintln!("dashboard request thread failed: {err}");
                        }
                    }
                    Err(err) => eprintln!("dashboard accept failed: {err}"),
                }
            }
        })?;

    Ok((local_addr, handle))
}

fn handle_http_connection(mut stream: TcpStream, topology: SharedTopology) {
    let _ = stream.set_read_timeout(Some(HTTP_READ_TIMEOUT));
    let mut buffer = [0_u8; 4096];
    let read = match stream.read(&mut buffer) {
        Ok(read) => read,
        Err(err) => {
            eprintln!("dashboard request read failed: {err}");
            return;
        }
    };
    if read == 0 {
        return;
    }

    let request = String::from_utf8_lossy(&buffer[..read]);
    let path = request
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");

    match path {
        "/" | "/index.html" => {
            let _ = write_http_response(
                &mut stream,
                "200 OK",
                "text/html; charset=utf-8",
                DASHBOARD_HTML.as_bytes(),
            );
        }
        "/topology.json" => match topology_json(&topology) {
            Ok(json) => {
                let _ = write_http_response(
                    &mut stream,
                    "200 OK",
                    "application/json; charset=utf-8",
                    json.as_bytes(),
                );
            }
            Err(err) => {
                let body = format!("topology unavailable: {err}");
                let _ = write_http_response(
                    &mut stream,
                    "500 Internal Server Error",
                    "text/plain; charset=utf-8",
                    body.as_bytes(),
                );
            }
        },
        "/healthz" => {
            let _ =
                write_http_response(&mut stream, "200 OK", "text/plain; charset=utf-8", b"ok\n");
        }
        _ => {
            let _ = write_http_response(
                &mut stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                b"not found\n",
            );
        }
    }
}

fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &[u8],
) -> io::Result<()> {
    write!(
        stream,
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nCache-Control: no-store\r\nConnection: close\r\n\r\n",
        body.len()
    )?;
    stream.write_all(body)
}

fn topology_json(topology: &SharedTopology) -> io::Result<String> {
    let topology = topology
        .read()
        .map_err(|_| io::Error::other("topology state lock poisoned"))?;
    serde_json::to_string(&topology.snapshot()).map_err(io::Error::other)
}

fn update_topology(topology: &SharedTopology, update: impl FnOnce(&mut TopologyState)) {
    match topology.write() {
        Ok(mut topology) => update(&mut topology),
        Err(err) => eprintln!("topology state lock poisoned: {err}"),
    }
}

fn rendezvous_key(peer_id: PeerId, namespace: &str) -> String {
    format!("{namespace}|{peer_id}")
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn default_listen_addrs(port: u16) -> Result<Vec<Multiaddr>, Box<dyn Error>> {
    Ok(vec![
        Multiaddr::empty()
            .with(libp2p::multiaddr::Protocol::Ip6(Ipv6Addr::UNSPECIFIED))
            .with(libp2p::multiaddr::Protocol::Tcp(port)),
        Multiaddr::empty()
            .with(libp2p::multiaddr::Protocol::Ip6(Ipv6Addr::UNSPECIFIED))
            .with(libp2p::multiaddr::Protocol::Udp(port))
            .with(libp2p::multiaddr::Protocol::QuicV1),
        Multiaddr::empty()
            .with(libp2p::multiaddr::Protocol::Ip4(Ipv4Addr::UNSPECIFIED))
            .with(libp2p::multiaddr::Protocol::Tcp(port)),
        Multiaddr::empty()
            .with(libp2p::multiaddr::Protocol::Ip4(Ipv4Addr::UNSPECIFIED))
            .with(libp2p::multiaddr::Protocol::Udp(port))
            .with(libp2p::multiaddr::Protocol::QuicV1),
    ])
}

fn ensure_peer_id(address: Multiaddr, peer_id: PeerId) -> Multiaddr {
    if address
        .iter()
        .any(|protocol| matches!(protocol, libp2p::multiaddr::Protocol::P2p(_)))
    {
        address
    } else {
        address.with(libp2p::multiaddr::Protocol::P2p(peer_id))
    }
}

fn generate_ed25519(secret_key_seed: u8) -> identity::Keypair {
    let mut bytes = [0_u8; 32];
    bytes[0] = secret_key_seed;
    identity::Keypair::ed25519_from_bytes(bytes).expect("fixed-length ed25519 seed")
}

fn parse_multiaddr(value: &str) -> Result<Multiaddr, String> {
    value.parse().map_err(|err| format!("{err}"))
}

const DASHBOARD_HTML: &str = r###"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>light-ear relay topology</title>
  <style>
    :root {
      color-scheme: light;
      --paper: #f6f3ea;
      --ink: #1f2527;
      --muted: #687174;
      --line: #c9c0ad;
      --panel: #fffaf0;
      --accent: #146c72;
      --accent-2: #b65d2f;
      --relay: #1d4f5a;
      --peer: #d77933;
      --registered: #407a3c;
    }

    * {
      box-sizing: border-box;
    }

    body {
      margin: 0;
      min-height: 100vh;
      background:
        linear-gradient(var(--line) 1px, transparent 1px),
        linear-gradient(90deg, var(--line) 1px, transparent 1px),
        var(--paper);
      background-size: 28px 28px;
      color: var(--ink);
      font-family: "Cascadia Code", "IBM Plex Mono", ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
      letter-spacing: 0;
    }

    main {
      width: min(1280px, calc(100vw - 32px));
      margin: 0 auto;
      padding: 24px 0 32px;
    }

    header {
      display: flex;
      align-items: flex-end;
      justify-content: space-between;
      gap: 16px;
      padding-bottom: 18px;
      border-bottom: 2px solid var(--ink);
    }

    h1, h2 {
      margin: 0;
      font-weight: 800;
    }

    h1 {
      font-size: clamp(24px, 4vw, 42px);
      line-height: 1;
    }

    h2 {
      font-size: 15px;
      text-transform: uppercase;
    }

    .timestamp {
      color: var(--muted);
      font-size: 12px;
      text-align: right;
    }

    .stats {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin: 18px 0;
    }

    .stat, .panel {
      background: color-mix(in srgb, var(--panel) 92%, white);
      border: 2px solid var(--ink);
      box-shadow: 4px 4px 0 var(--ink);
    }

    .stat {
      min-height: 82px;
      padding: 12px;
    }

    .stat .label {
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
    }

    .stat .value {
      margin-top: 8px;
      font-size: 26px;
      font-weight: 800;
    }

    .grid {
      display: grid;
      grid-template-columns: minmax(0, 1.35fr) minmax(320px, 0.65fr);
      gap: 18px;
      align-items: start;
    }

    .panel {
      padding: 14px;
      min-width: 0;
    }

    .panel + .panel {
      margin-top: 18px;
    }

    .panel-head {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      align-items: center;
      margin-bottom: 12px;
    }

    .hint {
      color: var(--muted);
      font-size: 11px;
      text-align: right;
    }

    svg {
      display: block;
      width: 100%;
      height: auto;
      min-height: 360px;
      background: #fffdf6;
      border: 1px solid var(--line);
    }

    .list {
      display: grid;
      gap: 8px;
    }

    .row {
      display: grid;
      gap: 5px;
      padding: 9px;
      border: 1px solid var(--line);
      background: #fffdf6;
      overflow-wrap: anywhere;
    }

    .row strong {
      font-size: 12px;
    }

    .meta {
      color: var(--muted);
      font-size: 11px;
    }

    .chips {
      display: flex;
      flex-wrap: wrap;
      gap: 5px;
    }

    .chip {
      border: 1px solid var(--line);
      padding: 2px 5px;
      background: #f6efe0;
      color: var(--ink);
      font-size: 10px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      table-layout: fixed;
      font-size: 11px;
    }

    th, td {
      border-top: 1px solid var(--line);
      padding: 8px 6px;
      text-align: left;
      vertical-align: top;
      overflow-wrap: anywhere;
    }

    th {
      color: var(--muted);
      text-transform: uppercase;
      font-size: 10px;
    }

    .empty {
      color: var(--muted);
      padding: 18px 0;
      text-align: center;
      border-top: 1px solid var(--line);
    }

    @media (max-width: 900px) {
      main {
        width: min(100vw - 20px, 1280px);
      }

      header {
        align-items: flex-start;
        flex-direction: column;
      }

      .timestamp {
        text-align: left;
      }

      .stats {
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }

      .grid {
        grid-template-columns: 1fr;
      }
    }
  </style>
</head>
<body>
  <main>
    <header>
      <h1>light-ear relay topology</h1>
      <div class="timestamp">
        <div id="health">loading</div>
        <div id="updated">waiting for topology</div>
      </div>
    </header>

    <section class="stats" aria-label="topology counters">
      <div class="stat"><div class="label">Connected peers</div><div class="value" id="peerCount">0</div></div>
      <div class="stat"><div class="label">Rendezvous registrations</div><div class="value" id="registrationCount">0</div></div>
      <div class="stat"><div class="label">Namespaces</div><div class="value" id="namespaceCount">0</div></div>
      <div class="stat"><div class="label">Advertised addresses</div><div class="value" id="addressCount">0</div></div>
    </section>

    <section class="grid">
      <section class="panel">
        <div class="panel-head">
          <h2>Network map</h2>
          <div class="hint">Relay-observed view; direct peer links need client reports.</div>
        </div>
        <div id="graph"></div>
      </section>

      <section>
        <section class="panel">
          <div class="panel-head">
            <h2>Connected peers</h2>
            <div class="hint" id="localPeer"></div>
          </div>
          <div class="list" id="peerList"></div>
        </section>

        <section class="panel">
          <div class="panel-head">
            <h2>Rendezvous</h2>
            <div class="hint">by namespace</div>
          </div>
          <div id="registrationTable"></div>
        </section>

        <section class="panel">
          <div class="panel-head">
            <h2>Recent events</h2>
            <div class="hint">latest first</div>
          </div>
          <div class="list" id="eventList"></div>
        </section>
      </section>
    </section>
  </main>

  <script>
    const refreshMs = 2000;

    function shortPeer(peer) {
      if (!peer) return "";
      return peer.length > 14 ? peer.slice(0, 8) + "..." + peer.slice(-6) : peer;
    }

    function esc(value) {
      return String(value ?? "").replace(/[&<>"']/g, char => ({
        "&": "&amp;",
        "<": "&lt;",
        ">": "&gt;",
        '"': "&quot;",
        "'": "&#39;"
      })[char]);
    }

    function time(value) {
      if (!value) return "";
      return new Date(Number(value)).toLocaleTimeString();
    }

    function render(data) {
      document.getElementById("health").textContent = "online";
      document.getElementById("updated").textContent = "updated " + time(data.updated_at_millis);
      document.getElementById("peerCount").textContent = data.connected_peers.length;
      document.getElementById("registrationCount").textContent = data.rendezvous_registrations.length;
      document.getElementById("namespaceCount").textContent = new Set(data.rendezvous_registrations.map(reg => reg.namespace)).size;
      document.getElementById("addressCount").textContent = data.external_addresses.length;
      document.getElementById("localPeer").textContent = shortPeer(data.local_peer_id);
      renderGraph(data);
      renderPeers(data);
      renderRegistrations(data);
      renderEvents(data);
    }

    function renderGraph(data) {
      const peers = data.connected_peers;
      const registered = new Set(data.rendezvous_registrations.map(reg => reg.peer_id));
      const width = 1000;
      const height = 520;
      const cx = 500;
      const cy = 260;
      const radius = peers.length < 6 ? 160 : 205;
      let parts = [`<svg viewBox="0 0 ${width} ${height}" role="img" aria-label="Relay topology graph">`];
      parts.push(`<line x1="90" y1="${cy}" x2="910" y2="${cy}" stroke="#c9c0ad" stroke-width="1" stroke-dasharray="6 8"/>`);
      peers.forEach((peer, index) => {
        const angle = peers.length === 1 ? -Math.PI / 2 : (Math.PI * 2 * index / peers.length) - Math.PI / 2;
        const x = cx + Math.cos(angle) * radius;
        const y = cy + Math.sin(angle) * radius;
        const color = registered.has(peer.peer_id) ? "var(--registered)" : "var(--peer)";
        parts.push(`<line x1="${cx}" y1="${cy}" x2="${x}" y2="${y}" stroke="#1f2527" stroke-width="2"/>`);
        parts.push(`<circle cx="${x}" cy="${y}" r="24" fill="${color}" stroke="#1f2527" stroke-width="3"/>`);
        parts.push(`<text x="${x}" y="${y + 44}" text-anchor="middle" font-size="18" font-weight="800" fill="#1f2527">${esc(shortPeer(peer.peer_id))}</text>`);
        parts.push(`<text x="${x}" y="${y + 63}" text-anchor="middle" font-size="12" fill="#687174">${peer.connections} link(s)</text>`);
      });
      parts.push(`<circle cx="${cx}" cy="${cy}" r="46" fill="var(--relay)" stroke="#1f2527" stroke-width="4"/>`);
      parts.push(`<text x="${cx}" y="${cy + 6}" text-anchor="middle" font-size="18" font-weight="800" fill="#fffaf0">relay</text>`);
      if (!peers.length) {
        parts.push(`<text x="${cx}" y="${cy + 96}" text-anchor="middle" font-size="18" fill="#687174">waiting for peers</text>`);
      }
      parts.push(`</svg>`);
      document.getElementById("graph").innerHTML = parts.join("");
    }

    function renderPeers(data) {
      const target = document.getElementById("peerList");
      if (!data.connected_peers.length) {
        target.innerHTML = `<div class="empty">No active relay connections</div>`;
        return;
      }

      target.innerHTML = data.connected_peers.map(peer => {
        const addresses = peer.addresses.slice(0, 4).map(address => `<span class="chip">${esc(address)}</span>`).join("");
        const protocols = peer.protocols.slice(0, 4).map(protocol => `<span class="chip">${esc(protocol)}</span>`).join("");
        return `<div class="row">
          <strong>${esc(peer.peer_id)}</strong>
          <div class="meta">${peer.connections} link(s), last seen ${time(peer.last_seen_millis)}${peer.agent_version ? ", " + esc(peer.agent_version) : ""}</div>
          <div class="chips">${addresses || '<span class="chip">no advertised addresses</span>'}</div>
          <div class="chips">${protocols}</div>
        </div>`;
      }).join("");
    }

    function renderRegistrations(data) {
      const target = document.getElementById("registrationTable");
      if (!data.rendezvous_registrations.length) {
        target.innerHTML = `<div class="empty">No rendezvous registrations</div>`;
        return;
      }

      target.innerHTML = `<table>
        <thead><tr><th>Peer</th><th>Namespace</th><th>Addresses</th><th>Refreshed</th></tr></thead>
        <tbody>${data.rendezvous_registrations.map(reg => `<tr>
          <td>${esc(shortPeer(reg.peer_id))}</td>
          <td>${esc(reg.namespace)}</td>
          <td>${esc(reg.addresses.join(" "))}</td>
          <td>${time(reg.refreshed_at_millis)}</td>
        </tr>`).join("")}</tbody>
      </table>`;
    }

    function renderEvents(data) {
      const target = document.getElementById("eventList");
      if (!data.recent_events.length) {
        target.innerHTML = `<div class="empty">No events yet</div>`;
        return;
      }
      target.innerHTML = data.recent_events.slice(0, 24).map(event => `<div class="row">
        <strong>${esc(event.kind)} <span class="meta">${time(event.at_millis)}</span></strong>
        <div class="meta">${esc(event.detail)}</div>
      </div>`).join("");
    }

    async function load() {
      try {
        const response = await fetch("/topology.json", { cache: "no-store" });
        if (!response.ok) throw new Error(response.statusText);
        render(await response.json());
      } catch (error) {
        document.getElementById("health").textContent = "offline";
        document.getElementById("updated").textContent = String(error);
      }
    }

    load();
    setInterval(load, refreshMs);
  </script>
</body>
</html>
"###;
