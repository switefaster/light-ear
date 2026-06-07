use std::{
    collections::{HashMap, HashSet, VecDeque, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    io,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Result, anyhow};
use chrono::Local;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, StreamProtocol, SwarmBuilder, dcutr, gossipsub, identify, mdns, noise, ping,
    relay, rendezvous, request_response,
    swarm::{
        ConnectionId, NetworkBehaviour, SwarmEvent,
        behaviour::toggle::Toggle,
        dial_opts::{DialOpts, PeerCondition},
    },
    tcp, yamux,
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::mpsc,
    time::{self, Instant, MissedTickBehavior},
};

use crate::{
    bilibili,
    core::{
        ActiveVote, ChatRecord, FrontendEvent as UiEvent, MAX_MESSAGES, NetworkCommand,
        PeerNameClaim, PendingPlayback, PlaybackState, PlaybackView, QueueItem, QueueState,
        VoteAction, VoteProposal, VoteView, WireMessage, format_duration_ms,
        normalize_timestamp_micros,
    },
    player,
};

const HISTORY_SYNC_INTERVAL: Duration = Duration::from_secs(10);
const HISTORY_SYNC_BURST_TICK: Duration = Duration::from_millis(200);
const HISTORY_REQUEST_COOLDOWN: Duration = Duration::from_secs(5);
const QUEUE_REQUEST_COOLDOWN: Duration = Duration::from_secs(5);
const MUSIC_LOCAL_INTERVAL: Duration = Duration::from_millis(100);
const MUSIC_STATE_INTERVAL: Duration = Duration::from_secs(1);
const MUSIC_DRIFT_SEEK_THRESHOLD_MS: u64 = 700;
const MUSIC_PREPARE_TIMEOUT: Duration = Duration::from_secs(12);
const MUSIC_START_DELAY: Duration = Duration::from_millis(1500);
const VOTE_TIMEOUT: Duration = Duration::from_secs(20);
const DIRECT_PROMOTION_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const DIRECT_PROMOTION_MEDIUM_RETRY_INTERVAL: Duration = Duration::from_secs(120);
const DIRECT_PROMOTION_SLOW_RETRY_INTERVAL: Duration = Duration::from_secs(600);
const DIRECT_PROMOTION_MEDIUM_RETRY_FAILURES: u32 = 3;
const DIRECT_PROMOTION_SLOW_RETRY_FAILURES: u32 = 6;
const DIRECT_PROMOTION_MAX_FAILURES: u32 = 10;
const DIRECT_PROMOTION_FAILURE_DEDUP_WINDOW: Duration = Duration::from_secs(5);
const GOSSIP_WARMUP_TIMEOUT: Duration = Duration::from_secs(5);
const GOSSIP_WARMUP_CHECK_INTERVAL: Duration = Duration::from_millis(500);
const RENDEZVOUS_DISCOVER_INTERVAL: Duration = Duration::from_secs(30);
const RENDEZVOUS_REGISTER_INTERVAL: Duration = Duration::from_secs(30 * 60);
const RENDEZVOUS_TTL_SECONDS: u64 = 60 * 60 * 2;
const DIRECT_MESSAGE_PROTOCOL: &str = "/link-ear/direct-message/0.1.0";
static NONCE_SEQ: AtomicU64 = AtomicU64::new(0);

pub struct BackendConfig {
    pub name: String,
    pub topic: String,
    pub listen: Vec<Multiaddr>,
    pub peer: Vec<Multiaddr>,
    pub relay: Vec<Multiaddr>,
    pub no_mdns: bool,
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    gossipsub: gossipsub::Behaviour,
    direct_messages: request_response::json::Behaviour<DirectMessageRequest, DirectMessageResponse>,
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    relay: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
    rendezvous: rendezvous::client::Behaviour,
    mdns: Toggle<mdns::tokio::Behaviour>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DirectMessageRequest {
    topic: String,
    message: WireMessage,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DirectMessageResponse {
    accepted: bool,
}

#[derive(Default)]
struct PeerConnectionRoutes {
    direct: HashSet<ConnectionId>,
    relayed: HashSet<ConnectionId>,
}

impl PeerConnectionRoutes {
    fn add(&mut self, connection_id: ConnectionId, is_relayed: bool) {
        if is_relayed {
            self.relayed.insert(connection_id);
        } else {
            self.direct.insert(connection_id);
        }
    }

    fn remove(&mut self, connection_id: ConnectionId, was_relayed: bool) {
        if was_relayed {
            self.relayed.remove(&connection_id);
        } else {
            self.direct.remove(&connection_id);
        }
    }

    fn is_empty(&self) -> bool {
        self.direct.is_empty() && self.relayed.is_empty()
    }

    fn is_relay_only(&self) -> bool {
        self.direct.is_empty() && !self.relayed.is_empty()
    }

    fn has_direct(&self) -> bool {
        !self.direct.is_empty()
    }

    fn has_relayed(&self) -> bool {
        !self.relayed.is_empty()
    }

    fn relayed_connections(&self) -> Vec<ConnectionId> {
        self.relayed.iter().copied().collect()
    }
}

#[derive(Default)]
struct DirectPromotionBackoff {
    failures: u32,
    last_attempt: Option<Instant>,
    last_failure: Option<Instant>,
    in_flight: bool,
    suspended_reported: bool,
}

impl DirectPromotionBackoff {
    fn retry_interval(&self) -> Duration {
        match self.failures {
            0..DIRECT_PROMOTION_MEDIUM_RETRY_FAILURES => DIRECT_PROMOTION_RETRY_INTERVAL,
            DIRECT_PROMOTION_MEDIUM_RETRY_FAILURES..DIRECT_PROMOTION_SLOW_RETRY_FAILURES => {
                DIRECT_PROMOTION_MEDIUM_RETRY_INTERVAL
            }
            _ => DIRECT_PROMOTION_SLOW_RETRY_INTERVAL,
        }
    }

    fn retry_remaining(&self) -> Option<Duration> {
        let last_attempt = self.last_attempt?;
        self.retry_interval().checked_sub(last_attempt.elapsed())
    }

    fn should_attempt(&self) -> bool {
        !self.in_flight
            && self.failures < DIRECT_PROMOTION_MAX_FAILURES
            && self.retry_remaining().is_none()
    }

    fn mark_attempt(&mut self) {
        self.last_attempt = Some(Instant::now());
        self.in_flight = true;
    }

    fn mark_failure(&mut self) -> DirectPromotionFailureOutcome {
        let now = Instant::now();
        self.in_flight = false;
        if self.last_failure.is_some_and(|last_failure| {
            now.duration_since(last_failure) < DIRECT_PROMOTION_FAILURE_DEDUP_WINDOW
        }) {
            return DirectPromotionFailureOutcome::Duplicate;
        }

        self.last_failure = Some(now);
        self.failures = self.failures.saturating_add(1);
        self.suspended_reported = false;

        DirectPromotionFailureOutcome::Counted {
            failures: self.failures,
            retry_after: (self.failures < DIRECT_PROMOTION_MAX_FAILURES)
                .then(|| self.retry_interval()),
        }
    }
}

enum DirectPromotionFailureOutcome {
    Counted {
        failures: u32,
        retry_after: Option<Duration>,
    },
    Duplicate,
}

struct GossipsubWarmup {
    started_at: Instant,
}

impl GossipsubWarmup {
    fn new() -> Self {
        Self {
            started_at: Instant::now(),
        }
    }

    fn is_expired(&self) -> bool {
        self.started_at.elapsed() >= GOSSIP_WARMUP_TIMEOUT
    }
}

enum ChatPublishOutcome {
    Published,
    NoPeersSubscribed,
}

pub async fn run_network(
    config: BackendConfig,
    mut commands: mpsc::Receiver<NetworkCommand>,
    ui: mpsc::Sender<UiEvent>,
) -> Result<()> {
    let topic = gossipsub::IdentTopic::new(config.topic.clone());
    let mut explicit_peers = HashSet::new();
    let mut seen_messages = HashSet::new();
    let mut history = Vec::new();
    let mut message_seq = 0_u64;
    let mut peer_names = HashMap::new();
    let mut local_name_conflicts = HashSet::new();
    let mut history_request_times = HashMap::new();
    let mut queue_request_times = HashMap::new();
    let mut pending_sync_summaries = VecDeque::new();
    let mut peer_routes: HashMap<PeerId, PeerConnectionRoutes> = HashMap::new();
    let mut chat_subscribers = HashSet::new();
    let mut peer_direct_addresses: HashMap<PeerId, HashSet<Multiaddr>> = HashMap::new();
    let mut direct_promotion_backoffs = HashMap::new();
    let mut gossip_warmups = HashMap::new();
    let mut rendezvous_nodes = HashSet::new();
    let mut rendezvous_cookies = HashMap::new();
    let rendezvous_namespace = rendezvous::Namespace::new(config.topic.clone())
        .map_err(|err| anyhow!("invalid rendezvous namespace '{}': {err}", config.topic))?;
    let mut music_queue = VecDeque::new();
    let mut queue_version = 0_u64;
    let mut queue_updated_at = 0_i64;
    let mut active_vote: Option<ActiveVote> = None;
    let http_client = bilibili::client()?;
    let mut audio_player = match player::AudioPlayer::new() {
        Ok(player) => Some(player),
        Err(err) => {
            send_status(&ui, format!("audio output unavailable: {err}")).await;
            None
        }
    };
    let mut playback_state: Option<PlaybackState> = None;
    let mut pending_playback: Option<PendingPlayback> = None;
    let mut playback_version = 0_u64;
    let mut history_sync = time::interval(HISTORY_SYNC_INTERVAL);
    history_sync.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut history_sync_burst = time::interval(HISTORY_SYNC_BURST_TICK);
    history_sync_burst.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut music_local = time::interval(MUSIC_LOCAL_INTERVAL);
    music_local.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut music_sync = time::interval(MUSIC_STATE_INTERVAL);
    music_sync.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut direct_promotion = time::interval(DIRECT_PROMOTION_RETRY_INTERVAL);
    direct_promotion.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut gossip_warmup = time::interval(GOSSIP_WARMUP_CHECK_INTERVAL);
    gossip_warmup.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut rendezvous_discover = time::interval(RENDEZVOUS_DISCOVER_INTERVAL);
    rendezvous_discover.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut rendezvous_register = time::interval(RENDEZVOUS_REGISTER_INTERVAL);
    rendezvous_register.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut swarm = SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_quic()
        .with_dns()?
        .with_relay_client(noise::Config::new, yamux::Config::default)?
        .with_behaviour(
            |key, relay| -> Result<Behaviour, Box<dyn std::error::Error + Send + Sync>> {
                let peer_id = key.public().to_peer_id();
                let gossipsub = build_gossipsub(key)?;
                let direct_messages = request_response::json::Behaviour::new(
                    [(
                        StreamProtocol::new(DIRECT_MESSAGE_PROTOCOL),
                        request_response::ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                );
                let identify = identify::Behaviour::new(identify::Config::new(
                    "/link-ear/0.1.0".to_string(),
                    key.public(),
                ));
                let mdns = if config.no_mdns {
                    Toggle::from(None)
                } else {
                    Toggle::from(Some(mdns::tokio::Behaviour::new(
                        mdns::Config::default(),
                        peer_id,
                    )?))
                };

                Ok(Behaviour {
                    gossipsub,
                    direct_messages,
                    identify,
                    ping: ping::Behaviour::default(),
                    relay,
                    dcutr: dcutr::Behaviour::new(peer_id),
                    rendezvous: rendezvous::client::Behaviour::new(key.clone()),
                    mdns,
                })
            },
        )?
        .build();

    let local_peer_id = *swarm.local_peer_id();
    let local_joined_at = current_timestamp_micros();
    let _ = ui
        .send(UiEvent::LocalPeerId(local_peer_id.to_string()))
        .await;
    send_queue_view(
        &ui,
        queue_version,
        queue_updated_at,
        local_peer_id,
        &music_queue,
    )
    .await;
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    let listen_addrs = if config.listen.is_empty() {
        vec![
            "/ip6/::/tcp/0".parse()?,
            "/ip6/::/udp/0/quic-v1".parse()?,
            "/ip4/0.0.0.0/tcp/0".parse()?,
            "/ip4/0.0.0.0/udp/0/quic-v1".parse()?,
        ]
    } else {
        prioritize_multiaddrs(config.listen)
    };

    for addr in listen_addrs {
        match swarm.listen_on(addr.clone()) {
            Ok(_) => send_status(&ui, format!("listening requested on {addr}")).await,
            Err(err) => send_status(&ui, format!("listen failed on {addr}: {err}")).await,
        }
    }

    for relay_addr in prioritize_multiaddrs(config.relay) {
        let rendezvous_peer = peer_id_from_multiaddr(&relay_addr);
        if let Some(peer_id) = rendezvous_peer {
            rendezvous_nodes.insert(peer_id);
        } else {
            send_status(
                &ui,
                format!("relay address has no /p2p peer id; rendezvous disabled for {relay_addr}"),
            )
            .await;
        }

        let circuit_addr = relay_addr.with(libp2p::multiaddr::Protocol::P2pCircuit);
        match swarm.listen_on(circuit_addr.clone()) {
            Ok(_) => {
                send_status(
                    &ui,
                    format!("requesting relay reservation via {circuit_addr}"),
                )
                .await
            }
            Err(err) => {
                send_status(
                    &ui,
                    format!("relay reservation request failed {circuit_addr}: {err}"),
                )
                .await
            }
        }
    }

    for peer in prioritize_multiaddrs(config.peer) {
        match swarm.dial(peer.clone()) {
            Ok(_) => send_status(&ui, format!("dialing peer {peer}")).await,
            Err(err) => send_status(&ui, format!("peer dial failed {peer}: {err}")).await,
        }
    }

    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(NetworkCommand::Chat(text)) => {
                    let sent_at = current_timestamp_micros();
                    message_seq += 1;
                    let id = new_message_id(local_peer_id, sent_at, message_seq, &text);
                    let record = ChatRecord {
                        id: id.clone(),
                        peer_id: local_peer_id.to_string(),
                        joined_at: Some(local_joined_at),
                        author: config.name.clone(),
                        text,
                        sent_at,
                    };
                    if !local_name_conflicts.is_empty() {
                        send_status(
                            &ui,
                            format!(
                                "name '{}' belongs to an earlier peer; restart with a different --name",
                                config.name
                            ),
                        )
                        .await;
                        continue;
                    }

                    insert_record(&mut history, &mut seen_messages, record.clone());
                    send_history_snapshot(&ui, &history).await;

                    let msg = WireMessage::Chat {
                        id: Some(id),
                        peer_id: local_peer_id.to_string(),
                        joined_at: Some(local_joined_at),
                        name: record.author,
                        text: record.text,
                        sent_at,
                    };
                    match publish_chat_wire(&mut swarm, &topic, &msg) {
                        Ok(ChatPublishOutcome::Published) => {}
                        Ok(ChatPublishOutcome::NoPeersSubscribed) => {
                            let direct_count = send_direct_message_to_connected_peers(
                                &mut swarm,
                                &peer_routes,
                                &rendezvous_nodes,
                                &config.topic,
                                &msg,
                            );
                            if direct_count > 0 {
                                send_status(
                                    &ui,
                                    format!(
                                        "gossipsub has no chat subscribers; sent direct fallback to {direct_count} peer(s)"
                                    ),
                                )
                                .await;
                            } else {
                                send_status(&ui, "publish failed: NoPeersSubscribedToTopic".to_string()).await;
                            }
                        }
                        Err(err) => {
                            send_status(&ui, format!("publish failed: {err}")).await;
                        }
                    }
                }
                Some(NetworkCommand::EnqueueBilibili {
                    bvid,
                    part,
                    position,
                }) => {
                    send_status(&ui, format!("resolving bilibili {bvid} part {part}")).await;
                    match bilibili::resolve_track(&http_client, &bvid, part.saturating_sub(1)).await {
                        Ok(track) => {
                            let item = QueueItem {
                                item_id: new_queue_item_id(local_peer_id, &track.track_id),
                                track,
                                requested_by: local_peer_id.to_string(),
                                added_at_micros: current_timestamp_micros(),
                            };
                            let index = position
                                .map(|position| position.saturating_sub(1).min(music_queue.len()))
                                .unwrap_or(music_queue.len());
                            let title = item.track.title.clone();
                            music_queue.insert(index, item);
                            publish_queue_state(
                                &mut swarm,
                                &topic,
                                &mut queue_version,
                                &mut queue_updated_at,
                                local_peer_id,
                                &music_queue,
                            )?;
                            send_queue_view(
                                &ui,
                                queue_version,
                                queue_updated_at,
                                local_peer_id,
                                &music_queue,
                            )
                            .await;
                            send_status(&ui, format!("queued #{}, {title}", index + 1)).await;
                            start_next_if_idle(
                                &mut music_queue,
                                &mut queue_version,
                                &mut queue_updated_at,
                                &mut pending_playback,
                                &mut playback_state,
                                &mut playback_version,
                                &mut audio_player,
                                &http_client,
                                &mut swarm,
                                &topic,
                                local_peer_id,
                                &ui,
                            )
                            .await?;
                        }
                        Err(err) => {
                            send_status(&ui, format!("bilibili resolve failed: {err:#}")).await;
                        }
                    }
                }
                Some(NetworkCommand::ShowQueue) => {
                    send_queue_view(
                        &ui,
                        queue_version,
                        queue_updated_at,
                        local_peer_id,
                        &music_queue,
                    )
                    .await;
                    send_queue_status(&ui, playback_state.as_ref(), &music_queue).await;
                }
                Some(NetworkCommand::Pause) => {
                    if let Some(state) = playback_state.as_mut() {
                        let now = current_timestamp_micros();
                        if can_control_playback(state, local_peer_id) {
                            let position_ms = playback_position_ms(state, now);
                            playback_version += 1;
                            state.state_version = playback_version;
                            state.issued_at_micros = now;
                            state.playing = false;
                            state.position_ms = position_ms;
                            state.anchor_time_micros = now;
                            state.leader_peer_id = local_peer_id.to_string();
                            if let Some(player) = &mut audio_player {
                                player.set_playing(false, now)?;
                            }
                            send_playback_view(&ui, state).await;
                            publish_playback_state(&mut swarm, &topic, state)?;
                        } else {
                            propose_or_execute_vote(
                                VoteAction::Pause,
                                &mut active_vote,
                                &mut music_queue,
                                &mut queue_version,
                                &mut queue_updated_at,
                                &mut pending_playback,
                                &mut playback_state,
                                &mut playback_version,
                                &mut audio_player,
                                &http_client,
                                &mut swarm,
                                &topic,
                                local_peer_id,
                                &ui,
                            )
                            .await?;
                        }
                    }
                }
                Some(NetworkCommand::Resume) => {
                    if let Some(state) = playback_state.as_mut() {
                        let now = current_timestamp_micros();
                        if can_control_playback(state, local_peer_id) {
                            let position_ms = playback_position_ms(state, now);
                            let playing = can_play_at_position(state, position_ms);
                            playback_version += 1;
                            state.state_version = playback_version;
                            state.issued_at_micros = now;
                            state.playing = playing;
                            state.position_ms = position_ms;
                            state.anchor_time_micros = now;
                            state.leader_peer_id = local_peer_id.to_string();
                            if let Some(player) = &mut audio_player {
                                player.set_playing(playing, now)?;
                            }
                            send_playback_view(&ui, state).await;
                            publish_playback_state(&mut swarm, &topic, state)?;
                        } else {
                            propose_or_execute_vote(
                                VoteAction::Resume,
                                &mut active_vote,
                                &mut music_queue,
                                &mut queue_version,
                                &mut queue_updated_at,
                                &mut pending_playback,
                                &mut playback_state,
                                &mut playback_version,
                                &mut audio_player,
                                &http_client,
                                &mut swarm,
                                &topic,
                                local_peer_id,
                                &ui,
                            )
                            .await?;
                        }
                    }
                }
                Some(NetworkCommand::Seek(position_ms)) => {
                    if let Some(state) = playback_state.as_mut() {
                        let now = current_timestamp_micros();
                        if can_control_playback(state, local_peer_id) {
                            let position_ms = clamp_playback_position_ms(state, position_ms);
                            let playing = state.playing && can_play_at_position(state, position_ms);
                            playback_version += 1;
                            state.state_version = playback_version;
                            state.issued_at_micros = now;
                            state.playing = playing;
                            state.position_ms = position_ms;
                            state.anchor_time_micros = now;
                            state.leader_peer_id = local_peer_id.to_string();
                            if let Some(player) = &mut audio_player {
                                if let Err(err) = player.seek(position_ms, playing, now) {
                                    send_status(&ui, format!("audio seek failed: {err:#}")).await;
                                }
                            }
                            send_playback_view(&ui, state).await;
                            publish_playback_state(&mut swarm, &topic, state)?;
                        } else {
                            propose_or_execute_vote(
                                VoteAction::Seek { position_ms },
                                &mut active_vote,
                                &mut music_queue,
                                &mut queue_version,
                                &mut queue_updated_at,
                                &mut pending_playback,
                                &mut playback_state,
                                &mut playback_version,
                                &mut audio_player,
                                &http_client,
                                &mut swarm,
                                &topic,
                                local_peer_id,
                                &ui,
                            )
                            .await?;
                        }
                    }
                }
                Some(NetworkCommand::Skip) => {
                    if playback_state
                        .as_ref()
                        .is_some_and(|state| can_control_playback(state, local_peer_id))
                    {
                        skip_current_track(
                            &mut music_queue,
                            &mut queue_version,
                            &mut queue_updated_at,
                            &mut pending_playback,
                            &mut playback_state,
                            &mut playback_version,
                            &mut audio_player,
                            &http_client,
                            &mut swarm,
                            &topic,
                            local_peer_id,
                            &ui,
                        )
                        .await?;
                    } else {
                        propose_or_execute_vote(
                            VoteAction::Skip,
                            &mut active_vote,
                            &mut music_queue,
                            &mut queue_version,
                            &mut queue_updated_at,
                            &mut pending_playback,
                            &mut playback_state,
                            &mut playback_version,
                            &mut audio_player,
                            &http_client,
                            &mut swarm,
                            &topic,
                            local_peer_id,
                            &ui,
                        )
                        .await?;
                    }
                }
                Some(NetworkCommand::RemoveQueueItem(index)) => {
                    match queue_item_at(&music_queue, index) {
                        Some(item) if item.requested_by == local_peer_id.to_string() =>
                        {
                            let title = item.track.title.clone();
                            music_queue.remove(index - 1);
                            publish_queue_state(
                                &mut swarm,
                                &topic,
                                &mut queue_version,
                                &mut queue_updated_at,
                                local_peer_id,
                                &music_queue,
                            )?;
                            send_queue_view(
                                &ui,
                                queue_version,
                                queue_updated_at,
                                local_peer_id,
                                &music_queue,
                            )
                            .await;
                            send_status(&ui, format!("removed #{index}: {title}")).await;
                        }
                        Some(item) => {
                            propose_or_execute_vote(
                                VoteAction::Remove {
                                    item_id: item.item_id.clone(),
                                },
                                &mut active_vote,
                                &mut music_queue,
                                &mut queue_version,
                                &mut queue_updated_at,
                                &mut pending_playback,
                                &mut playback_state,
                                &mut playback_version,
                                &mut audio_player,
                                &http_client,
                                &mut swarm,
                                &topic,
                                local_peer_id,
                                &ui,
                            )
                            .await?;
                        }
                        None => send_status(&ui, format!("queue item #{index} does not exist")).await,
                    }
                }
                Some(NetworkCommand::MoveQueueItem { from, to }) => {
                    match queue_item_at(&music_queue, from) {
                        Some(item) => {
                            propose_or_execute_vote(
                                VoteAction::Move {
                                    item_id: item.item_id.clone(),
                                    to_index: to.saturating_sub(1),
                                },
                                &mut active_vote,
                                &mut music_queue,
                                &mut queue_version,
                                &mut queue_updated_at,
                                &mut pending_playback,
                                &mut playback_state,
                                &mut playback_version,
                                &mut audio_player,
                                &http_client,
                                &mut swarm,
                                &topic,
                                local_peer_id,
                                &ui,
                            )
                            .await?;
                        }
                        None => send_status(&ui, format!("queue item #{from} does not exist")).await,
                    }
                }
                Some(NetworkCommand::Vote(approve)) => {
                    cast_vote(
                        approve,
                        &mut active_vote,
                        &mut music_queue,
                        &mut queue_version,
                        &mut queue_updated_at,
                        &mut pending_playback,
                        &mut playback_state,
                        &mut playback_version,
                        &mut audio_player,
                        &http_client,
                        &mut swarm,
                        &topic,
                        local_peer_id,
                        &ui,
                    )
                    .await?;
                }
                None => break,
            },
            _ = music_local.tick() => {
                if active_vote.as_ref().is_some_and(|vote| Instant::now() >= vote.deadline) {
                    if let Some(vote) = active_vote.take() {
                        send_status(&ui, format!("vote {} timed out", vote.proposal.vote_id)).await;
                        send_vote_view(
                            &ui,
                            None,
                            &music_queue,
                            majority_threshold(swarm.connected_peers().count() + 1),
                        )
                        .await;
                    }
                }

                if let Err(err) = resolve_active_vote(
                    &mut active_vote,
                    &mut music_queue,
                    &mut queue_version,
                    &mut queue_updated_at,
                    &mut pending_playback,
                    &mut playback_state,
                    &mut playback_version,
                    &mut audio_player,
                    &http_client,
                    &mut swarm,
                    &topic,
                    local_peer_id,
                    &ui,
                )
                .await
                {
                    send_status(&ui, format!("vote execution failed: {err:#}")).await;
                }

                if let Err(err) = maybe_start_pending_playback(
                    &mut pending_playback,
                    &mut playback_state,
                    &mut playback_version,
                    &mut swarm,
                    &topic,
                    &ui,
                )
                .await
                {
                    send_status(&ui, format!("playback prepare failed: {err:#}")).await;
                }

                let mut finished_current = false;
                if let Some(state) = playback_state.as_mut() {
                    let now = current_timestamp_micros();
                    if let Err(err) = sync_loaded_player_to_state(&mut audio_player, state, now) {
                        send_status(&ui, format!("playback sync failed: {err:#}")).await;
                    }

                    finished_current = state.leader_peer_id == local_peer_id.to_string()
                        && state.track.is_some()
                        && state.playing
                        && now >= state.anchor_time_micros
                        && (!can_play_at_position(state, playback_position_ms(state, now))
                            || audio_player
                                .as_ref()
                                .is_some_and(|player| player.is_finished(now)));

                    if !finished_current {
                        send_playback_view(&ui, state).await;
                    }
                }

                if finished_current {
                    stop_current_playback(
                        &mut pending_playback,
                        &mut playback_state,
                        &mut playback_version,
                        &mut audio_player,
                        &mut swarm,
                        &topic,
                        local_peer_id,
                        "track finished",
                        &ui,
                    )
                    .await?;
                    start_next_if_idle(
                        &mut music_queue,
                        &mut queue_version,
                        &mut queue_updated_at,
                        &mut pending_playback,
                        &mut playback_state,
                        &mut playback_version,
                        &mut audio_player,
                        &http_client,
                        &mut swarm,
                        &topic,
                        local_peer_id,
                        &ui,
                    )
                    .await?;
                }
            },
            _ = history_sync.tick() => {
                if let Err(err) = publish_sync_summary(
                    &mut swarm,
                    &topic,
                    local_peer_id,
                    &config.name,
                    local_joined_at,
                    &history,
                    queue_version,
                    queue_updated_at,
                    &music_queue,
                ) {
                    send_status(&ui, format!("sync summary failed: {err}")).await;
                }
            },
            _ = history_sync_burst.tick() => {
                if let Err(err) = publish_pending_sync_summaries(
                    &mut pending_sync_summaries,
                    &mut swarm,
                    &topic,
                    local_peer_id,
                    &config.name,
                    local_joined_at,
                    &history,
                    queue_version,
                    queue_updated_at,
                    &music_queue,
                ) {
                    send_status(&ui, format!("sync summary failed: {err}")).await;
                }
            },
            _ = music_sync.tick() => {
                if pending_playback.is_none() {
                    if let Some(state) = playback_state.as_mut() {
                        if state.leader_peer_id == local_peer_id.to_string() && state.track.is_some() {
                            let now = current_timestamp_micros();
                            if !state.playing || now >= state.anchor_time_micros {
                                state.position_ms = playback_position_ms(state, now);
                                state.anchor_time_micros = now;
                            }
                            state.issued_at_micros = now;
                            publish_playback_state(&mut swarm, &topic, state)?;
                            send_playback_view(&ui, state).await;
                        }
                    }
                }
            },
            _ = direct_promotion.tick() => {
                retry_direct_promotions(
                    &mut swarm,
                    &peer_routes,
                    &peer_direct_addresses,
                    &mut direct_promotion_backoffs,
                    &chat_subscribers,
                    &mut gossip_warmups,
                    &ui,
                )
                .await;
            },
            _ = gossip_warmup.tick() => {
                retry_gossip_warmup_promotions(
                    &mut swarm,
                    &peer_routes,
                    &peer_direct_addresses,
                    &mut direct_promotion_backoffs,
                    &chat_subscribers,
                    &mut gossip_warmups,
                    &ui,
                )
                .await;
            },
            _ = rendezvous_register.tick() => {
                register_with_rendezvous_nodes(
                    &mut swarm,
                    &rendezvous_nodes,
                    &rendezvous_namespace,
                    &ui,
                )
                .await;
            },
            _ = rendezvous_discover.tick() => {
                discover_rendezvous_peers(
                    &mut swarm,
                    &rendezvous_nodes,
                    &rendezvous_namespace,
                    &rendezvous_cookies,
                    &ui,
                )
                .await;
            },
            event = swarm.select_next_some() => {
                let ctx = HistoryContext {
                    topic: &topic,
                    topic_name: &config.topic,
                    local_peer_id,
                    history: &mut history,
                    seen_messages: &mut seen_messages,
                    local_name: &config.name,
                    local_joined_at,
                    peer_names: &mut peer_names,
                    local_name_conflicts: &mut local_name_conflicts,
                    history_request_times: &mut history_request_times,
                    queue_request_times: &mut queue_request_times,
                    pending_sync_summaries: &mut pending_sync_summaries,
                    http_client: &http_client,
                    audio_player: &mut audio_player,
                    playback_state: &mut playback_state,
                    pending_playback: &mut pending_playback,
                    playback_version: &mut playback_version,
                    music_queue: &mut music_queue,
                    queue_version: &mut queue_version,
                    queue_updated_at: &mut queue_updated_at,
                    active_vote: &mut active_vote,
                };
                handle_swarm_event(
                    event,
                    &mut swarm,
                    &ui,
                    &mut explicit_peers,
                    &mut peer_routes,
                    &mut chat_subscribers,
                    &mut peer_direct_addresses,
                    &mut direct_promotion_backoffs,
                    &mut gossip_warmups,
                    &rendezvous_nodes,
                    &rendezvous_namespace,
                    &mut rendezvous_cookies,
                    ctx,
                )
                .await;
            }
        }
    }

    Ok(())
}

fn build_gossipsub(
    key: &libp2p::identity::Keypair,
) -> Result<gossipsub::Behaviour, Box<dyn std::error::Error + Send + Sync>> {
    let message_id_fn = |message: &gossipsub::Message| {
        let mut hasher = DefaultHasher::new();
        message.data.hash(&mut hasher);
        gossipsub::MessageId::from(hasher.finish().to_string())
    };

    let config = gossipsub::ConfigBuilder::default()
        .heartbeat_interval(Duration::from_secs(1))
        .validation_mode(gossipsub::ValidationMode::Strict)
        .message_id_fn(message_id_fn)
        .build()
        .map_err(|err| io::Error::other(err.to_string()))?;

    Ok(
        gossipsub::Behaviour::new(gossipsub::MessageAuthenticity::Signed(key.clone()), config)
            .map_err(io::Error::other)?,
    )
}

struct HistoryContext<'a> {
    topic: &'a gossipsub::IdentTopic,
    topic_name: &'a str,
    local_peer_id: PeerId,
    history: &'a mut Vec<ChatRecord>,
    seen_messages: &'a mut HashSet<String>,
    local_name: &'a str,
    local_joined_at: i64,
    peer_names: &'a mut HashMap<String, PeerNameClaim>,
    local_name_conflicts: &'a mut HashSet<String>,
    history_request_times: &'a mut HashMap<String, Instant>,
    queue_request_times: &'a mut HashMap<String, Instant>,
    pending_sync_summaries: &'a mut VecDeque<Instant>,
    http_client: &'a reqwest::Client,
    audio_player: &'a mut Option<player::AudioPlayer>,
    playback_state: &'a mut Option<PlaybackState>,
    pending_playback: &'a mut Option<PendingPlayback>,
    playback_version: &'a mut u64,
    music_queue: &'a mut VecDeque<QueueItem>,
    queue_version: &'a mut u64,
    queue_updated_at: &'a mut i64,
    active_vote: &'a mut Option<ActiveVote>,
}

async fn handle_swarm_event(
    event: SwarmEvent<BehaviourEvent>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    ui: &mpsc::Sender<UiEvent>,
    explicit_peers: &mut HashSet<PeerId>,
    peer_routes: &mut HashMap<PeerId, PeerConnectionRoutes>,
    chat_subscribers: &mut HashSet<PeerId>,
    peer_direct_addresses: &mut HashMap<PeerId, HashSet<Multiaddr>>,
    direct_promotion_backoffs: &mut HashMap<PeerId, DirectPromotionBackoff>,
    gossip_warmups: &mut HashMap<PeerId, GossipsubWarmup>,
    rendezvous_nodes: &HashSet<PeerId>,
    rendezvous_namespace: &rendezvous::Namespace,
    rendezvous_cookies: &mut HashMap<PeerId, rendezvous::Cookie>,
    mut ctx: HistoryContext<'_>,
) {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            send_status(ui, format!("listening on {address}")).await;
        }
        SwarmEvent::ExternalAddrConfirmed { address } => {
            send_status(ui, format!("confirmed external address {address}")).await;
            if is_relay_address(&address) {
                register_with_rendezvous_nodes(swarm, rendezvous_nodes, rendezvous_namespace, ui)
                    .await;
            }
        }
        SwarmEvent::ExternalAddrExpired { address } => {
            send_status(ui, format!("expired external address {address}")).await;
        }
        SwarmEvent::ConnectionEstablished {
            peer_id,
            connection_id,
            endpoint,
            ..
        } => {
            let is_relayed =
                endpoint.is_relayed() || is_relay_address(endpoint.get_remote_address());
            peer_routes
                .entry(peer_id)
                .or_default()
                .add(connection_id, is_relayed);

            if !rendezvous_nodes.contains(&peer_id) && explicit_peers.insert(peer_id) {
                swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                send_status(ui, format!("tracking {peer_id} as gossip peer")).await;
            }

            if is_relayed {
                send_status(ui, format!("connected {peer_id} via relay")).await;
                if !rendezvous_nodes.contains(&peer_id)
                    && !chat_subscribers.contains(&peer_id)
                    && start_gossip_warmup(gossip_warmups, peer_id)
                {
                    send_status(
                        ui,
                        format!(
                            "waiting up to {} for {peer_id} to subscribe to chat before direct promotion",
                            format_retry_duration(GOSSIP_WARMUP_TIMEOUT)
                        ),
                    )
                    .await;
                }
                maybe_promote_relayed_peer(
                    swarm,
                    peer_routes,
                    peer_direct_addresses,
                    direct_promotion_backoffs,
                    chat_subscribers,
                    gossip_warmups,
                    peer_id,
                    ui,
                )
                .await;
            } else {
                let has_relayed_route = peer_routes
                    .get(&peer_id)
                    .is_some_and(PeerConnectionRoutes::has_relayed);
                let promotion_allowed = if has_relayed_route && !chat_subscribers.contains(&peer_id)
                {
                    gossip_warmup_allows_promotion(peer_id, chat_subscribers, gossip_warmups, ui)
                        .await
                } else {
                    true
                };

                if promotion_allowed {
                    direct_promotion_backoffs.remove(&peer_id);
                    if chat_subscribers.contains(&peer_id) {
                        let closed_relays = close_relay_connections(swarm, peer_routes, peer_id);
                        if closed_relays > 0 {
                            send_status(
                                ui,
                                format!(
                                    "promoted {peer_id} to direct connection; closing {closed_relays} relay link(s)"
                                ),
                            )
                            .await;
                        } else {
                            send_status(ui, format!("connected {peer_id} directly")).await;
                        }
                    } else if has_relayed_route {
                        send_status(
                            ui,
                            format!(
                                "promoted {peer_id} to direct connection after gossip warmup timeout; keeping relay until chat subscription is ready"
                            ),
                        )
                        .await;
                    } else {
                        send_status(ui, format!("connected {peer_id} directly")).await;
                    }
                } else if swarm.close_connection(connection_id) {
                    send_status(
                        ui,
                        format!(
                            "closed early direct connection to {peer_id}; waiting for chat subscription or warmup timeout"
                        ),
                    )
                    .await;
                } else {
                    send_status(
                        ui,
                        format!(
                            "early direct connection to {peer_id} is waiting for chat subscription or warmup timeout"
                        ),
                    )
                    .await;
                }
            }

            if rendezvous_nodes.contains(&peer_id) {
                register_with_rendezvous_node(swarm, peer_id, rendezvous_namespace, ui).await;
                discover_rendezvous_node(
                    swarm,
                    peer_id,
                    rendezvous_namespace,
                    rendezvous_cookies.get(&peer_id),
                    ui,
                )
                .await;
            }

            let count = swarm.connected_peers().count();
            let _ = ui.send(UiEvent::PeerCount(count)).await;
            if let Err(err) = trigger_sync(swarm, &mut ctx) {
                send_status(ui, format!("sync summary failed: {err}")).await;
            }
            if let Err(err) = publish_music_snapshot(
                swarm,
                ctx.topic,
                ctx.local_peer_id,
                *ctx.queue_version,
                *ctx.queue_updated_at,
                ctx.music_queue,
                ctx.playback_state.as_ref(),
            ) {
                send_status(ui, format!("music snapshot failed: {err}")).await;
            }
        }
        SwarmEvent::ConnectionClosed {
            peer_id,
            connection_id,
            endpoint,
            num_established,
            ..
        } => {
            let was_relayed =
                endpoint.is_relayed() || is_relay_address(endpoint.get_remote_address());
            if let Some(routes) = peer_routes.get_mut(&peer_id) {
                routes.remove(connection_id, was_relayed);
                if routes.is_empty() {
                    peer_routes.remove(&peer_id);
                }
            }

            if num_established > 0 {
                let route = if was_relayed { "relay" } else { "direct" };
                send_status(
                    ui,
                    format!(
                        "{route} connection closed {peer_id}; {num_established} link(s) remain"
                    ),
                )
                .await;
            } else {
                direct_promotion_backoffs.remove(&peer_id);
                chat_subscribers.remove(&peer_id);
                gossip_warmups.remove(&peer_id);
                if explicit_peers.remove(&peer_id) {
                    swarm
                        .behaviour_mut()
                        .gossipsub
                        .remove_explicit_peer(&peer_id);
                }
                send_status(ui, format!("disconnected {peer_id}")).await;
                if forget_peer_name(
                    peer_id,
                    ctx.local_peer_id,
                    &mut *ctx.peer_names,
                    &mut *ctx.local_name_conflicts,
                    ctx.local_name,
                    ctx.local_joined_at,
                ) {
                    send_status(ui, format!("name '{}' is available again", ctx.local_name)).await;
                }
                let count = swarm.connected_peers().count();
                let _ = ui.send(UiEvent::PeerCount(count)).await;
                if let Some(pending) = ctx.pending_playback.as_mut() {
                    let peer_id = peer_id.to_string();
                    pending.expected_peers.remove(&peer_id);
                    pending.ready_peers.remove(&peer_id);
                }
                if let Err(err) = maybe_start_pending_playback(
                    &mut *ctx.pending_playback,
                    &mut *ctx.playback_state,
                    &mut *ctx.playback_version,
                    swarm,
                    ctx.topic,
                    ui,
                )
                .await
                {
                    send_status(ui, format!("playback start failed: {err:#}")).await;
                }
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
            let mut discovered = false;
            let mut direct_candidates = HashSet::new();
            for (peer_id, address) in list {
                discovered = true;
                if explicit_peers.insert(peer_id) {
                    swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                }
                if remember_direct_addresses(peer_direct_addresses, peer_id, [address]) > 0 {
                    direct_promotion_backoffs.remove(&peer_id);
                    direct_candidates.insert(peer_id);
                }
                send_status(ui, format!("mDNS discovered {peer_id}")).await;
            }
            for peer_id in direct_candidates {
                maybe_promote_relayed_peer(
                    swarm,
                    peer_routes,
                    peer_direct_addresses,
                    direct_promotion_backoffs,
                    chat_subscribers,
                    gossip_warmups,
                    peer_id,
                    ui,
                )
                .await;
            }
            if discovered {
                if let Err(err) = trigger_sync(swarm, &mut ctx) {
                    send_status(ui, format!("sync summary failed: {err}")).await;
                }
                if let Err(err) = publish_music_snapshot(
                    swarm,
                    ctx.topic,
                    ctx.local_peer_id,
                    *ctx.queue_version,
                    *ctx.queue_updated_at,
                    ctx.music_queue,
                    ctx.playback_state.as_ref(),
                ) {
                    send_status(ui, format!("music snapshot failed: {err}")).await;
                }
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
            for (peer_id, address) in list {
                forget_direct_address(peer_direct_addresses, peer_id, address);
                if forget_peer_name(
                    peer_id,
                    ctx.local_peer_id,
                    &mut *ctx.peer_names,
                    &mut *ctx.local_name_conflicts,
                    ctx.local_name,
                    ctx.local_joined_at,
                ) {
                    send_status(ui, format!("name '{}' is available again", ctx.local_name)).await;
                }
                if !is_peer_connected(swarm, peer_id) && explicit_peers.remove(&peer_id) {
                    swarm
                        .behaviour_mut()
                        .gossipsub
                        .remove_explicit_peer(&peer_id);
                }
                send_status(ui, format!("mDNS expired {peer_id}")).await;
            }
        }
        SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
            if remember_direct_addresses(peer_direct_addresses, peer_id, [address]) > 0 {
                direct_promotion_backoffs.remove(&peer_id);
                maybe_promote_relayed_peer(
                    swarm,
                    peer_routes,
                    peer_direct_addresses,
                    direct_promotion_backoffs,
                    chat_subscribers,
                    gossip_warmups,
                    peer_id,
                    ui,
                )
                .await;
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Identify(
            identify::Event::Received { peer_id, info, .. }
            | identify::Event::Pushed { peer_id, info, .. },
        )) => {
            if remember_direct_addresses(peer_direct_addresses, peer_id, info.listen_addrs) > 0 {
                direct_promotion_backoffs.remove(&peer_id);
                maybe_promote_relayed_peer(
                    swarm,
                    peer_routes,
                    peer_direct_addresses,
                    direct_promotion_backoffs,
                    chat_subscribers,
                    gossip_warmups,
                    peer_id,
                    ui,
                )
                .await;
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Identify(identify::Event::Error {
            peer_id,
            error,
            ..
        })) => {
            send_status(ui, format!("identify failed {peer_id}: {error}")).await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::Rendezvous(
            rendezvous::client::Event::Registered {
                rendezvous_node,
                namespace,
                ttl,
            },
        )) => {
            send_status(
                ui,
                format!("registered with rendezvous {rendezvous_node} in {namespace} for {ttl}s"),
            )
            .await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::Rendezvous(
            rendezvous::client::Event::RegisterFailed {
                rendezvous_node,
                namespace,
                error,
            },
        )) => {
            send_status(
                ui,
                format!("rendezvous register failed {rendezvous_node} in {namespace}: {error:?}"),
            )
            .await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::Rendezvous(
            rendezvous::client::Event::Discovered {
                rendezvous_node,
                registrations,
                cookie,
            },
        )) => {
            rendezvous_cookies.insert(rendezvous_node, cookie);
            let count = dial_rendezvous_registrations(
                swarm,
                explicit_peers,
                peer_direct_addresses,
                direct_promotion_backoffs,
                chat_subscribers,
                gossip_warmups,
                peer_routes,
                ctx.local_peer_id,
                registrations,
                ui,
            )
            .await;
            send_status(
                ui,
                format!("rendezvous {rendezvous_node} returned {count} peer address set(s)"),
            )
            .await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::Rendezvous(
            rendezvous::client::Event::DiscoverFailed {
                rendezvous_node,
                namespace,
                error,
            },
        )) => {
            let namespace = namespace
                .map(|namespace| namespace.to_string())
                .unwrap_or_else(|| "all namespaces".to_string());
            send_status(
                ui,
                format!("rendezvous discover failed {rendezvous_node} in {namespace}: {error:?}"),
            )
            .await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::Rendezvous(rendezvous::client::Event::Expired {
            peer,
        })) => {
            send_status(ui, format!("rendezvous registration expired for {peer}")).await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Subscribed {
            peer_id,
            topic,
        })) => {
            if topic == ctx.topic.hash() {
                chat_subscribers.insert(peer_id);
                gossip_warmups.remove(&peer_id);
                send_status(ui, format!("peer {peer_id} subscribed to chat")).await;
                if let Err(err) = trigger_sync(swarm, &mut ctx) {
                    send_status(ui, format!("sync summary failed: {err}")).await;
                }
                maybe_promote_relayed_peer(
                    swarm,
                    peer_routes,
                    peer_direct_addresses,
                    direct_promotion_backoffs,
                    chat_subscribers,
                    gossip_warmups,
                    peer_id,
                    ui,
                )
                .await;
                if peer_routes
                    .get(&peer_id)
                    .is_some_and(PeerConnectionRoutes::has_direct)
                {
                    let closed_relays = close_relay_connections(swarm, peer_routes, peer_id);
                    if closed_relays > 0 {
                        send_status(
                            ui,
                            format!(
                                "chat ready with {peer_id}; closing {closed_relays} relay link(s)"
                            ),
                        )
                        .await;
                    }
                }
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Unsubscribed {
            peer_id,
            topic,
        })) => {
            if topic == ctx.topic.hash() {
                chat_subscribers.remove(&peer_id);
                send_status(ui, format!("peer {peer_id} unsubscribed from chat")).await;
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(
            gossipsub::Event::GossipsubNotSupported { peer_id },
        )) => {
            send_status(ui, format!("peer {peer_id} does not support gossipsub")).await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::SlowPeer {
            peer_id,
            failed_messages,
        })) => {
            send_status(
                ui,
                format!("gossipsub slow peer {peer_id}: {failed_messages:?}"),
            )
            .await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::DirectMessages(
            request_response::Event::Message { peer, message, .. },
        )) => match message {
            request_response::Message::Request {
                request, channel, ..
            } => {
                let accepted =
                    apply_direct_wire_message(peer, request.topic, request.message, ui, &mut ctx)
                        .await;
                if swarm
                    .behaviour_mut()
                    .direct_messages
                    .send_response(channel, DirectMessageResponse { accepted })
                    .is_err()
                {
                    send_status(ui, format!("direct response failed {peer}: channel closed")).await;
                }
            }
            request_response::Message::Response { response, .. } => {
                if !response.accepted {
                    send_status(ui, format!("direct message ignored by {peer}")).await;
                }
            }
        },
        SwarmEvent::Behaviour(BehaviourEvent::DirectMessages(
            request_response::Event::OutboundFailure { peer, error, .. },
        )) => {
            send_status(ui, format!("direct message failed {peer}: {error}")).await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::DirectMessages(
            request_response::Event::InboundFailure { peer, error, .. },
        )) => {
            send_status(ui, format!("direct message inbound failed {peer}: {error}")).await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
            propagation_source,
            message,
            ..
        })) => match serde_json::from_slice::<WireMessage>(&message.data) {
            Ok(WireMessage::Chat {
                id,
                peer_id,
                joined_at,
                name,
                text,
                sent_at,
            }) => {
                let source_peer_id = message.source.unwrap_or(propagation_source);
                apply_chat_message(
                    &mut ctx,
                    ui,
                    id,
                    peer_id,
                    joined_at,
                    name,
                    text,
                    sent_at,
                    source_peer_id,
                    propagation_source,
                )
                .await;
            }
            Ok(WireMessage::NameClaim {
                peer_id,
                name,
                joined_at,
                ..
            }) => {
                if let Some(peer_id) = parse_peer_id(&peer_id) {
                    remember_peer_name(
                        peer_id,
                        &name,
                        ctx.local_peer_id,
                        ctx.local_name,
                        ctx.local_joined_at,
                        &mut *ctx.peer_names,
                        &mut *ctx.local_name_conflicts,
                        ui,
                        joined_at,
                    )
                    .await;
                }
            }
            Ok(WireMessage::HistorySummary { peer_id, count, .. }) => {
                let local_peer_id = ctx.local_peer_id.to_string();
                if peer_id != local_peer_id
                    && count > ctx.history.len()
                    && should_request_history(ctx.history_request_times, &peer_id)
                {
                    let request = WireMessage::HistoryRequest {
                        requester: local_peer_id,
                        target: peer_id.clone(),
                        known_count: ctx.history.len(),
                        nonce: new_nonce(ctx.local_peer_id),
                    };

                    match publish_history_wire(swarm, ctx.topic, &request) {
                        Ok(()) => {
                            ctx.history_request_times
                                .insert(peer_id.clone(), Instant::now());
                            send_status(ui, format!("requesting history from {peer_id}")).await;
                        }
                        Err(err) => {
                            send_status(ui, format!("history request failed: {err}")).await;
                        }
                    }
                }
            }
            Ok(WireMessage::HistoryRequest {
                requester,
                target,
                known_count,
                ..
            }) => {
                let local_peer_id = ctx.local_peer_id.to_string();
                if target == local_peer_id
                    && requester != local_peer_id
                    && ctx.history.len() > known_count
                {
                    let response = WireMessage::HistoryResponse {
                        target: None,
                        messages: ctx.history.clone(),
                        nonce: new_nonce(ctx.local_peer_id),
                    };

                    match publish_history_wire(swarm, ctx.topic, &response) {
                        Ok(()) => {
                            send_status(ui, format!("sent {} history messages", ctx.history.len()))
                                .await;
                        }
                        Err(err) => {
                            send_status(ui, format!("history response failed: {err}")).await;
                        }
                    }
                }
            }
            Ok(WireMessage::HistoryResponse {
                target, messages, ..
            }) => {
                let local_peer_id = ctx.local_peer_id.to_string();
                let is_for_me = match target.as_deref() {
                    Some(target) => target == local_peer_id,
                    None => true,
                };

                if is_for_me {
                    let mut added = 0;
                    for record in messages {
                        if let Some(peer_id) = parse_peer_id(&record.peer_id) {
                            remember_peer_name(
                                peer_id,
                                &record.author,
                                ctx.local_peer_id,
                                ctx.local_name,
                                ctx.local_joined_at,
                                &mut *ctx.peer_names,
                                &mut *ctx.local_name_conflicts,
                                ui,
                                record.joined_at,
                            )
                            .await;
                        }

                        if insert_record(ctx.history, ctx.seen_messages, record) {
                            added += 1;
                        }
                    }

                    if added > 0 {
                        send_history_snapshot(ui, ctx.history).await;
                        send_status(
                            ui,
                            format!("merged {added} history messages, now {}", ctx.history.len()),
                        )
                        .await;
                    }
                }
            }
            Ok(WireMessage::QueueSummary {
                peer_id,
                version,
                updated_at_micros,
                ..
            }) => {
                let local_peer_id = ctx.local_peer_id.to_string();
                if peer_id != local_peer_id
                    && is_queue_state_newer(
                        version,
                        updated_at_micros,
                        *ctx.queue_version,
                        *ctx.queue_updated_at,
                    )
                    && should_request_queue(ctx.queue_request_times, &peer_id)
                {
                    let request = WireMessage::QueueRequest {
                        requester: local_peer_id,
                        target: peer_id.clone(),
                        known_version: *ctx.queue_version,
                        known_updated_at_micros: *ctx.queue_updated_at,
                        nonce: new_nonce(ctx.local_peer_id),
                    };

                    match publish_history_wire(swarm, ctx.topic, &request) {
                        Ok(()) => {
                            ctx.queue_request_times
                                .insert(peer_id.clone(), Instant::now());
                            send_status(ui, format!("requesting queue from {peer_id}")).await;
                        }
                        Err(err) => {
                            send_status(ui, format!("queue request failed: {err}")).await;
                        }
                    }
                }
            }
            Ok(WireMessage::QueueRequest {
                requester,
                target,
                known_version,
                known_updated_at_micros,
                ..
            }) => {
                let local_peer_id = ctx.local_peer_id.to_string();
                if target == local_peer_id
                    && requester != local_peer_id
                    && is_queue_state_newer(
                        *ctx.queue_version,
                        *ctx.queue_updated_at,
                        known_version,
                        known_updated_at_micros,
                    )
                {
                    let response = WireMessage::QueueResponse {
                        target: requester.clone(),
                        state: build_queue_state(
                            *ctx.queue_version,
                            *ctx.queue_updated_at,
                            ctx.local_peer_id,
                            ctx.music_queue,
                        ),
                        nonce: new_nonce(ctx.local_peer_id),
                    };

                    match publish_history_wire(swarm, ctx.topic, &response) {
                        Ok(()) => {
                            send_status(
                                ui,
                                format!("sent {} queue item(s)", ctx.music_queue.len()),
                            )
                            .await;
                        }
                        Err(err) => {
                            send_status(ui, format!("queue response failed: {err}")).await;
                        }
                    }
                }
            }
            Ok(WireMessage::QueueResponse { target, state, .. }) => {
                if target == ctx.local_peer_id.to_string() {
                    apply_remote_queue_state(ui, &mut ctx, state, "synced queue").await;
                }
            }
            Ok(WireMessage::PlaybackState { state, .. }) => {
                let state = normalize_remote_playback_state(&state, current_timestamp_micros());
                if state.leader_peer_id != ctx.local_peer_id.to_string()
                    && should_apply_playback_state(ctx.playback_state.as_ref(), &state)
                {
                    cancel_local_pending_playback(
                        &mut *ctx.pending_playback,
                        swarm,
                        ctx.topic,
                        ctx.local_peer_id,
                        "superseded by remote playback",
                    );
                    match apply_remote_playback_state(
                        ctx.http_client,
                        &mut *ctx.audio_player,
                        &mut *ctx.playback_state,
                        &state,
                        ui,
                    )
                    .await
                    {
                        Ok(()) => {}
                        Err(err) => {
                            send_status(ui, format!("playback sync failed: {err:#}")).await;
                        }
                    }
                }
            }
            Ok(WireMessage::PlaybackPrepare {
                state,
                expected_peers,
                ..
            }) => {
                let state = normalize_remote_playback_state(&state, current_timestamp_micros());
                let is_expected = expected_peers.is_empty()
                    || expected_peers.contains(&ctx.local_peer_id.to_string());
                if state.leader_peer_id != ctx.local_peer_id.to_string()
                    && is_expected
                    && should_apply_playback_state(ctx.playback_state.as_ref(), &state)
                {
                    cancel_local_pending_playback(
                        &mut *ctx.pending_playback,
                        swarm,
                        ctx.topic,
                        ctx.local_peer_id,
                        "superseded by remote playback prepare",
                    );
                    match apply_playback_prepare(
                        ctx.http_client,
                        &mut *ctx.audio_player,
                        &mut *ctx.playback_state,
                        &state,
                        ui,
                    )
                    .await
                    {
                        Ok(ready) => {
                            if ready {
                                if let Err(err) = publish_playback_ready(
                                    swarm,
                                    ctx.topic,
                                    &state.session_id,
                                    ctx.local_peer_id,
                                ) {
                                    send_status(ui, format!("playback ready failed: {err}")).await;
                                }
                            }
                        }
                        Err(err) => {
                            send_status(ui, format!("playback prepare failed: {err:#}")).await;
                        }
                    }
                } else if !is_expected {
                    send_status(
                        ui,
                        "ignored playback prepare for another peer set".to_string(),
                    )
                    .await;
                }
            }
            Ok(WireMessage::PlaybackReady {
                session_id,
                peer_id,
                ..
            }) => {
                if let Some(pending) = ctx.pending_playback.as_mut() {
                    if pending.state.session_id == session_id
                        && pending.state.leader_peer_id == ctx.local_peer_id.to_string()
                        && pending.mark_ready(peer_id.clone())
                    {
                        send_status(
                            ui,
                            format!(
                                "peer {peer_id} ready ({}/{})",
                                pending.ready_count(),
                                pending.expected_count()
                            ),
                        )
                        .await;
                    }
                }

                if let Err(err) = maybe_start_pending_playback(
                    &mut *ctx.pending_playback,
                    &mut *ctx.playback_state,
                    &mut *ctx.playback_version,
                    swarm,
                    ctx.topic,
                    ui,
                )
                .await
                {
                    send_status(ui, format!("playback start failed: {err:#}")).await;
                }
            }
            Ok(WireMessage::PlaybackCancel {
                session_id,
                leader_peer_id,
                reason,
                ..
            }) => {
                if leader_peer_id != ctx.local_peer_id.to_string() {
                    apply_playback_cancel(
                        &mut *ctx.audio_player,
                        &mut *ctx.playback_state,
                        &session_id,
                        &reason,
                        ui,
                    )
                    .await;
                }
            }
            Ok(WireMessage::QueueState { state, .. }) => {
                apply_remote_queue_state(ui, &mut ctx, state, "queue updated").await;
            }
            Ok(WireMessage::VoteProposal { proposal, .. }) => {
                if proposal.proposer == ctx.local_peer_id.to_string() {
                    return;
                }
                if ctx.active_vote.is_some() {
                    send_status(
                        ui,
                        format!("ignored vote {}; another vote is active", proposal.vote_id),
                    )
                    .await;
                } else {
                    let mut vote = ActiveVote::new(proposal.clone(), Instant::now() + VOTE_TIMEOUT);
                    vote.vote(proposal.proposer.clone(), true);
                    *ctx.active_vote = Some(vote);
                    send_vote_view(
                        ui,
                        ctx.active_vote.as_ref(),
                        ctx.music_queue,
                        majority_threshold(swarm.connected_peers().count() + 1),
                    )
                    .await;
                    send_status(
                        ui,
                        format!(
                            "vote requested by {}: {} (/vote yes|no)",
                            short_peer(&proposal.proposer),
                            describe_vote_action(&proposal.action, ctx.music_queue)
                        ),
                    )
                    .await;
                }
            }
            Ok(WireMessage::VoteBallot {
                vote_id,
                peer_id,
                approve,
                ..
            }) => {
                let mut changed_vote = false;
                let threshold = majority_threshold(swarm.connected_peers().count() + 1);
                if let Some(vote) = ctx.active_vote.as_mut() {
                    if vote.proposal.vote_id == vote_id {
                        vote.vote(peer_id.clone(), approve);
                        changed_vote = true;
                        send_status(
                            ui,
                            format!(
                                "vote {vote_id}: {} from {} ({}/{})",
                                if approve { "yes" } else { "no" },
                                short_peer(&peer_id),
                                vote.approval_count(),
                                threshold
                            ),
                        )
                        .await;
                    }
                }
                if changed_vote {
                    send_vote_view(ui, ctx.active_vote.as_ref(), ctx.music_queue, threshold).await;
                }

                if let Err(err) = resolve_active_vote(
                    ctx.active_vote,
                    ctx.music_queue,
                    ctx.queue_version,
                    ctx.queue_updated_at,
                    ctx.pending_playback,
                    ctx.playback_state,
                    ctx.playback_version,
                    ctx.audio_player,
                    ctx.http_client,
                    swarm,
                    ctx.topic,
                    ctx.local_peer_id,
                    ui,
                )
                .await
                {
                    send_status(ui, format!("vote execution failed: {err:#}")).await;
                }
            }
            Err(err) => send_status(ui, format!("ignored invalid message: {err}")).await,
        },
        SwarmEvent::Behaviour(BehaviourEvent::Relay(event)) => {
            send_status(ui, format!("relay event: {event:?}")).await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::Dcutr(event)) => match event.result {
            Ok(connection_id) => {
                direct_promotion_backoffs.remove(&event.remote_peer_id);
                send_status(
                    ui,
                    format!(
                        "direct upgrade succeeded with {} on {connection_id:?}",
                        event.remote_peer_id
                    ),
                )
                .await;
            }
            Err(err) => {
                if peer_routes
                    .get(&event.remote_peer_id)
                    .is_some_and(PeerConnectionRoutes::is_relay_only)
                {
                    record_direct_promotion_failure(
                        direct_promotion_backoffs,
                        event.remote_peer_id,
                        format!("DCUtR failed: {err}"),
                        ui,
                    )
                    .await;
                } else {
                    send_status(
                        ui,
                        format!("direct upgrade failed with {}: {err}", event.remote_peer_id),
                    )
                    .await;
                }
            }
        },
        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            if let Some(peer_id) = peer_id {
                if direct_promotion_backoffs
                    .get(&peer_id)
                    .is_some_and(|backoff| backoff.in_flight)
                {
                    record_direct_promotion_failure(
                        direct_promotion_backoffs,
                        peer_id,
                        format!("outgoing direct dial failed: {error}"),
                        ui,
                    )
                    .await;
                } else {
                    send_status(ui, format!("outgoing connection error {peer_id}: {error}")).await;
                }
            } else {
                send_status(
                    ui,
                    format!("outgoing connection error unknown peer: {error}"),
                )
                .await;
            }
        }
        _ => {}
    }
}

async fn retry_direct_promotions(
    swarm: &mut libp2p::Swarm<Behaviour>,
    peer_routes: &HashMap<PeerId, PeerConnectionRoutes>,
    peer_direct_addresses: &HashMap<PeerId, HashSet<Multiaddr>>,
    direct_promotion_backoffs: &mut HashMap<PeerId, DirectPromotionBackoff>,
    chat_subscribers: &HashSet<PeerId>,
    gossip_warmups: &mut HashMap<PeerId, GossipsubWarmup>,
    ui: &mpsc::Sender<UiEvent>,
) {
    let peers = peer_routes.keys().copied().collect::<Vec<_>>();
    for peer_id in peers {
        maybe_promote_relayed_peer(
            swarm,
            peer_routes,
            peer_direct_addresses,
            direct_promotion_backoffs,
            chat_subscribers,
            gossip_warmups,
            peer_id,
            ui,
        )
        .await;
    }
}

async fn retry_gossip_warmup_promotions(
    swarm: &mut libp2p::Swarm<Behaviour>,
    peer_routes: &HashMap<PeerId, PeerConnectionRoutes>,
    peer_direct_addresses: &HashMap<PeerId, HashSet<Multiaddr>>,
    direct_promotion_backoffs: &mut HashMap<PeerId, DirectPromotionBackoff>,
    chat_subscribers: &HashSet<PeerId>,
    gossip_warmups: &mut HashMap<PeerId, GossipsubWarmup>,
    ui: &mpsc::Sender<UiEvent>,
) {
    let peers = gossip_warmups
        .iter()
        .filter_map(|(peer_id, warmup)| warmup.is_expired().then_some(*peer_id))
        .collect::<Vec<_>>();

    for peer_id in peers {
        maybe_promote_relayed_peer(
            swarm,
            peer_routes,
            peer_direct_addresses,
            direct_promotion_backoffs,
            chat_subscribers,
            gossip_warmups,
            peer_id,
            ui,
        )
        .await;
    }
}

async fn register_with_rendezvous_nodes(
    swarm: &mut libp2p::Swarm<Behaviour>,
    rendezvous_nodes: &HashSet<PeerId>,
    namespace: &rendezvous::Namespace,
    ui: &mpsc::Sender<UiEvent>,
) {
    for rendezvous_node in rendezvous_nodes {
        if is_peer_connected(swarm, *rendezvous_node) {
            register_with_rendezvous_node(swarm, *rendezvous_node, namespace, ui).await;
        }
    }
}

async fn register_with_rendezvous_node(
    swarm: &mut libp2p::Swarm<Behaviour>,
    rendezvous_node: PeerId,
    namespace: &rendezvous::Namespace,
    ui: &mpsc::Sender<UiEvent>,
) {
    if !has_external_addresses(swarm) {
        send_status(
            ui,
            format!(
                "rendezvous register deferred {rendezvous_node}: waiting for confirmed external address"
            ),
        )
        .await;
        return;
    }

    match swarm.behaviour_mut().rendezvous.register(
        namespace.clone(),
        rendezvous_node,
        Some(RENDEZVOUS_TTL_SECONDS),
    ) {
        Ok(()) => {
            send_status(
                ui,
                format!("registering with rendezvous {rendezvous_node} in {namespace}"),
            )
            .await;
        }
        Err(err) => {
            send_status(
                ui,
                format!("rendezvous register request failed {rendezvous_node}: {err:?}"),
            )
            .await;
        }
    }
}

async fn discover_rendezvous_peers(
    swarm: &mut libp2p::Swarm<Behaviour>,
    rendezvous_nodes: &HashSet<PeerId>,
    namespace: &rendezvous::Namespace,
    rendezvous_cookies: &HashMap<PeerId, rendezvous::Cookie>,
    ui: &mpsc::Sender<UiEvent>,
) {
    for rendezvous_node in rendezvous_nodes {
        if is_peer_connected(swarm, *rendezvous_node) {
            discover_rendezvous_node(
                swarm,
                *rendezvous_node,
                namespace,
                rendezvous_cookies.get(rendezvous_node),
                ui,
            )
            .await;
        }
    }
}

async fn discover_rendezvous_node(
    swarm: &mut libp2p::Swarm<Behaviour>,
    rendezvous_node: PeerId,
    namespace: &rendezvous::Namespace,
    cookie: Option<&rendezvous::Cookie>,
    ui: &mpsc::Sender<UiEvent>,
) {
    swarm.behaviour_mut().rendezvous.discover(
        Some(namespace.clone()),
        cookie.cloned(),
        None,
        rendezvous_node,
    );
    send_status(
        ui,
        format!("discovering peers via rendezvous {rendezvous_node} in {namespace}"),
    )
    .await;
}

async fn dial_rendezvous_registrations(
    swarm: &mut libp2p::Swarm<Behaviour>,
    explicit_peers: &mut HashSet<PeerId>,
    peer_direct_addresses: &mut HashMap<PeerId, HashSet<Multiaddr>>,
    direct_promotion_backoffs: &mut HashMap<PeerId, DirectPromotionBackoff>,
    chat_subscribers: &HashSet<PeerId>,
    gossip_warmups: &mut HashMap<PeerId, GossipsubWarmup>,
    peer_routes: &HashMap<PeerId, PeerConnectionRoutes>,
    local_peer_id: PeerId,
    registrations: Vec<rendezvous::Registration>,
    ui: &mpsc::Sender<UiEvent>,
) -> usize {
    let mut discovered = 0;
    for registration in registrations {
        let peer_id = registration.record.peer_id();
        if peer_id == local_peer_id {
            continue;
        }

        let addresses = registration
            .record
            .addresses()
            .iter()
            .filter_map(|address| normalize_peer_address(peer_id, address.clone()))
            .collect::<Vec<_>>();
        if addresses.is_empty() {
            continue;
        }
        discovered += 1;

        if explicit_peers.insert(peer_id) {
            swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
            send_status(ui, format!("tracking {peer_id} as rendezvous gossip peer")).await;
        }
        if remember_direct_addresses(peer_direct_addresses, peer_id, addresses.clone()) > 0 {
            direct_promotion_backoffs.remove(&peer_id);
        }

        if is_peer_connected(swarm, peer_id) {
            maybe_promote_relayed_peer(
                swarm,
                peer_routes,
                peer_direct_addresses,
                direct_promotion_backoffs,
                chat_subscribers,
                gossip_warmups,
                peer_id,
                ui,
            )
            .await;
            continue;
        }

        let address_count = addresses.len();
        let dial_opts = DialOpts::peer_id(peer_id)
            .addresses(prioritize_multiaddrs(addresses))
            .condition(PeerCondition::Disconnected)
            .build();
        match swarm.dial(dial_opts) {
            Ok(()) => {
                send_status(
                    ui,
                    format!(
                        "dialing discovered peer {peer_id} ({address_count} candidate address(es))"
                    ),
                )
                .await;
            }
            Err(err) => {
                send_status(
                    ui,
                    format!("rendezvous discovered peer dial failed {peer_id}: {err}"),
                )
                .await;
            }
        }
    }

    discovered
}

fn start_gossip_warmup(
    gossip_warmups: &mut HashMap<PeerId, GossipsubWarmup>,
    peer_id: PeerId,
) -> bool {
    if gossip_warmups.contains_key(&peer_id) {
        return false;
    }

    gossip_warmups.insert(peer_id, GossipsubWarmup::new());
    true
}

async fn gossip_warmup_allows_promotion(
    peer_id: PeerId,
    chat_subscribers: &HashSet<PeerId>,
    gossip_warmups: &mut HashMap<PeerId, GossipsubWarmup>,
    ui: &mpsc::Sender<UiEvent>,
) -> bool {
    if chat_subscribers.contains(&peer_id) {
        gossip_warmups.remove(&peer_id);
        return true;
    }

    let Some(warmup) = gossip_warmups.get(&peer_id) else {
        gossip_warmups.insert(peer_id, GossipsubWarmup::new());
        send_status(
            ui,
            format!(
                "waiting up to {} for {peer_id} to subscribe to chat before direct promotion",
                format_retry_duration(GOSSIP_WARMUP_TIMEOUT)
            ),
        )
        .await;
        return false;
    };
    if !warmup.is_expired() {
        return false;
    }

    gossip_warmups.remove(&peer_id);
    send_status(
        ui,
        format!("gossipsub warmup timed out for {peer_id}; trying direct promotion anyway"),
    )
    .await;
    true
}

async fn maybe_promote_relayed_peer(
    swarm: &mut libp2p::Swarm<Behaviour>,
    peer_routes: &HashMap<PeerId, PeerConnectionRoutes>,
    peer_direct_addresses: &HashMap<PeerId, HashSet<Multiaddr>>,
    direct_promotion_backoffs: &mut HashMap<PeerId, DirectPromotionBackoff>,
    chat_subscribers: &HashSet<PeerId>,
    gossip_warmups: &mut HashMap<PeerId, GossipsubWarmup>,
    peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) {
    if peer_id == *swarm.local_peer_id() {
        return;
    }
    if !peer_routes
        .get(&peer_id)
        .is_some_and(PeerConnectionRoutes::is_relay_only)
    {
        return;
    }

    let Some(addresses) = peer_direct_addresses.get(&peer_id) else {
        return;
    };
    if addresses.is_empty() {
        return;
    }

    if !gossip_warmup_allows_promotion(peer_id, chat_subscribers, gossip_warmups, ui).await {
        return;
    }

    let suspended_failures = {
        let backoff = direct_promotion_backoffs.entry(peer_id).or_default();
        if backoff.failures >= DIRECT_PROMOTION_MAX_FAILURES {
            if backoff.suspended_reported {
                return;
            }
            backoff.suspended_reported = true;
            Some(backoff.failures)
        } else {
            if !backoff.should_attempt() {
                return;
            }
            backoff.mark_attempt();
            None
        }
    };
    if let Some(failures) = suspended_failures {
        send_status(
            ui,
            format!(
                "direct promotion suspended for {peer_id} after {failures} failures; waiting for new direct addresses"
            ),
        )
        .await;
        return;
    }

    let addresses = prioritize_multiaddrs(addresses.iter().cloned().collect());
    let address_count = addresses.len();
    let dial_opts = DialOpts::peer_id(peer_id)
        .addresses(addresses)
        .condition(PeerCondition::Always)
        .build();

    match swarm.dial(dial_opts) {
        Ok(()) => {
            send_status(
                ui,
                format!(
                    "trying direct connection to {peer_id} ({address_count} candidate address(es))"
                ),
            )
            .await;
        }
        Err(err) => {
            record_direct_promotion_failure(
                direct_promotion_backoffs,
                peer_id,
                format!("dial request failed: {err}"),
                ui,
            )
            .await;
        }
    }
}

async fn record_direct_promotion_failure(
    direct_promotion_backoffs: &mut HashMap<PeerId, DirectPromotionBackoff>,
    peer_id: PeerId,
    reason: String,
    ui: &mpsc::Sender<UiEvent>,
) {
    let outcome = direct_promotion_backoffs
        .entry(peer_id)
        .or_default()
        .mark_failure();

    let DirectPromotionFailureOutcome::Counted {
        failures,
        retry_after,
    } = outcome
    else {
        return;
    };

    if let Some(retry_after) = retry_after {
        send_status(
            ui,
            format!(
                "direct promotion failed for {peer_id} ({failures}/{DIRECT_PROMOTION_MAX_FAILURES}): {reason}; retrying in {}",
                format_retry_duration(retry_after)
            ),
        )
        .await;
    } else {
        if let Some(backoff) = direct_promotion_backoffs.get_mut(&peer_id) {
            backoff.suspended_reported = true;
        }
        send_status(
            ui,
            format!(
                "direct promotion suspended for {peer_id} after {failures} failures: {reason}; waiting for new direct addresses"
            ),
        )
        .await;
    }
}

fn format_retry_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    if seconds < 60 {
        return format!("{seconds}s");
    }

    let minutes = seconds / 60;
    let seconds = seconds % 60;
    if seconds == 0 {
        format!("{minutes}m")
    } else {
        format!("{minutes}m {seconds}s")
    }
}

fn is_peer_connected(swarm: &libp2p::Swarm<Behaviour>, peer_id: PeerId) -> bool {
    swarm
        .connected_peers()
        .any(|connected| *connected == peer_id)
}

fn has_external_addresses(swarm: &libp2p::Swarm<Behaviour>) -> bool {
    swarm.external_addresses().next().is_some()
}

fn close_relay_connections(
    swarm: &mut libp2p::Swarm<Behaviour>,
    peer_routes: &HashMap<PeerId, PeerConnectionRoutes>,
    peer_id: PeerId,
) -> usize {
    let relay_connections = peer_routes
        .get(&peer_id)
        .map(PeerConnectionRoutes::relayed_connections)
        .unwrap_or_default();

    relay_connections
        .into_iter()
        .filter(|connection_id| swarm.close_connection(*connection_id))
        .count()
}

fn remember_direct_addresses<I>(
    peer_direct_addresses: &mut HashMap<PeerId, HashSet<Multiaddr>>,
    peer_id: PeerId,
    addresses: I,
) -> usize
where
    I: IntoIterator<Item = Multiaddr>,
{
    let addresses = addresses
        .into_iter()
        .filter_map(|address| normalize_direct_peer_address(peer_id, address))
        .collect::<Vec<_>>();
    if addresses.is_empty() {
        return 0;
    }

    let known_addresses = peer_direct_addresses.entry(peer_id).or_default();
    addresses
        .into_iter()
        .filter(|address| known_addresses.insert(address.clone()))
        .count()
}

fn forget_direct_address(
    peer_direct_addresses: &mut HashMap<PeerId, HashSet<Multiaddr>>,
    peer_id: PeerId,
    address: Multiaddr,
) {
    let Some(address) = normalize_direct_peer_address(peer_id, address) else {
        return;
    };
    let Some(known_addresses) = peer_direct_addresses.get_mut(&peer_id) else {
        return;
    };

    known_addresses.remove(&address);
    if known_addresses.is_empty() {
        peer_direct_addresses.remove(&peer_id);
    }
}

fn normalize_peer_address(peer_id: PeerId, address: Multiaddr) -> Option<Multiaddr> {
    if is_unspecified_ip_address(&address) {
        return None;
    }

    let last_peer_id = address
        .iter()
        .filter_map(|protocol| match protocol {
            libp2p::multiaddr::Protocol::P2p(address_peer_id) => Some(address_peer_id),
            _ => None,
        })
        .last();

    match last_peer_id {
        Some(address_peer_id) if address_peer_id == peer_id => Some(address),
        Some(_) => None,
        None => Some(address.with(libp2p::multiaddr::Protocol::P2p(peer_id))),
    }
}

fn normalize_direct_peer_address(peer_id: PeerId, address: Multiaddr) -> Option<Multiaddr> {
    if is_relay_address(&address) || is_unspecified_ip_address(&address) {
        return None;
    }

    let mut has_target_peer_id = false;
    for protocol in address.iter() {
        if let libp2p::multiaddr::Protocol::P2p(address_peer_id) = protocol {
            if address_peer_id != peer_id {
                return None;
            }
            has_target_peer_id = true;
        }
    }

    if has_target_peer_id {
        Some(address)
    } else {
        Some(address.with(libp2p::multiaddr::Protocol::P2p(peer_id)))
    }
}

fn is_relay_address(address: &Multiaddr) -> bool {
    address
        .iter()
        .any(|protocol| matches!(protocol, libp2p::multiaddr::Protocol::P2pCircuit))
}

fn is_unspecified_ip_address(address: &Multiaddr) -> bool {
    address.iter().any(|protocol| match protocol {
        libp2p::multiaddr::Protocol::Ip4(address) => address.is_unspecified(),
        libp2p::multiaddr::Protocol::Ip6(address) => address.is_unspecified(),
        _ => false,
    })
}

fn peer_id_from_multiaddr(address: &Multiaddr) -> Option<PeerId> {
    address
        .iter()
        .filter_map(|protocol| match protocol {
            libp2p::multiaddr::Protocol::P2p(peer_id) => Some(peer_id),
            _ => None,
        })
        .last()
}

fn publish_history_summary(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    history: &[ChatRecord],
) -> Result<()> {
    let summary = WireMessage::HistorySummary {
        peer_id: local_peer_id.to_string(),
        count: history.len(),
        newest_at: history
            .last()
            .map(|record| normalize_timestamp_micros(record.sent_at)),
        nonce: new_nonce(local_peer_id),
    };
    publish_history_wire(swarm, topic, &summary)
}

fn publish_name_claim(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    local_name: &str,
    local_joined_at: i64,
) -> Result<()> {
    let claim = WireMessage::NameClaim {
        peer_id: local_peer_id.to_string(),
        name: local_name.to_string(),
        joined_at: Some(local_joined_at),
        nonce: new_nonce(local_peer_id),
    };
    publish_history_wire(swarm, topic, &claim)
}

fn publish_presence_and_history(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    local_name: &str,
    local_joined_at: i64,
    history: &[ChatRecord],
) -> Result<()> {
    publish_name_claim(swarm, topic, local_peer_id, local_name, local_joined_at)?;
    publish_history_summary(swarm, topic, local_peer_id, history)
}

fn publish_queue_summary(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    queue_version: u64,
    queue_updated_at: i64,
    queue: &VecDeque<QueueItem>,
) -> Result<()> {
    if queue_version == 0 && queue.is_empty() {
        return Ok(());
    }

    let summary = WireMessage::QueueSummary {
        peer_id: local_peer_id.to_string(),
        version: queue_version,
        updated_at_micros: queue_updated_at,
        item_count: queue.len(),
        nonce: new_nonce(local_peer_id),
    };
    publish_history_wire(swarm, topic, &summary)
}

fn publish_sync_summary(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    local_name: &str,
    local_joined_at: i64,
    history: &[ChatRecord],
    queue_version: u64,
    queue_updated_at: i64,
    queue: &VecDeque<QueueItem>,
) -> Result<()> {
    publish_presence_and_history(
        swarm,
        topic,
        local_peer_id,
        local_name,
        local_joined_at,
        history,
    )?;
    publish_queue_summary(
        swarm,
        topic,
        local_peer_id,
        queue_version,
        queue_updated_at,
        queue,
    )
}

fn trigger_sync(swarm: &mut libp2p::Swarm<Behaviour>, ctx: &mut HistoryContext<'_>) -> Result<()> {
    publish_sync_summary(
        swarm,
        ctx.topic,
        ctx.local_peer_id,
        ctx.local_name,
        ctx.local_joined_at,
        ctx.history,
        *ctx.queue_version,
        *ctx.queue_updated_at,
        ctx.music_queue,
    )?;
    schedule_sync_burst(ctx.pending_sync_summaries);
    Ok(())
}

fn schedule_sync_burst(pending: &mut VecDeque<Instant>) {
    let now = Instant::now();
    for delay in [
        Duration::from_millis(300),
        Duration::from_millis(900),
        Duration::from_millis(1800),
    ] {
        pending.push_back(now + delay);
    }
}

fn publish_pending_sync_summaries(
    pending: &mut VecDeque<Instant>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    local_name: &str,
    local_joined_at: i64,
    history: &[ChatRecord],
    queue_version: u64,
    queue_updated_at: i64,
    queue: &VecDeque<QueueItem>,
) -> Result<()> {
    let now = Instant::now();
    while pending.front().is_some_and(|deadline| *deadline <= now) {
        pending.pop_front();
        publish_sync_summary(
            swarm,
            topic,
            local_peer_id,
            local_name,
            local_joined_at,
            history,
            queue_version,
            queue_updated_at,
            queue,
        )?;
    }
    Ok(())
}

async fn apply_direct_wire_message(
    source_peer_id: PeerId,
    topic_name: String,
    message: WireMessage,
    ui: &mpsc::Sender<UiEvent>,
    ctx: &mut HistoryContext<'_>,
) -> bool {
    if topic_name != ctx.topic_name {
        send_status(
            ui,
            format!("ignored direct message from {source_peer_id}: topic mismatch"),
        )
        .await;
        return false;
    }

    match message {
        WireMessage::Chat {
            id,
            peer_id,
            joined_at,
            name,
            text,
            sent_at,
        } => {
            apply_chat_message(
                ctx,
                ui,
                id,
                peer_id,
                joined_at,
                name,
                text,
                sent_at,
                source_peer_id,
                source_peer_id,
            )
            .await;
            true
        }
        WireMessage::NameClaim {
            peer_id,
            name,
            joined_at,
            ..
        } => {
            if let Some(peer_id) = parse_peer_id(&peer_id) {
                remember_peer_name(
                    peer_id,
                    &name,
                    ctx.local_peer_id,
                    ctx.local_name,
                    ctx.local_joined_at,
                    &mut *ctx.peer_names,
                    &mut *ctx.local_name_conflicts,
                    ui,
                    joined_at,
                )
                .await;
                true
            } else {
                false
            }
        }
        _ => false,
    }
}

async fn apply_chat_message(
    ctx: &mut HistoryContext<'_>,
    ui: &mpsc::Sender<UiEvent>,
    id: Option<String>,
    peer_id: String,
    joined_at: Option<i64>,
    name: String,
    text: String,
    sent_at: i64,
    source_peer_id: PeerId,
    id_peer_id: PeerId,
) -> bool {
    let claimed_peer_id = parse_peer_id(&peer_id).unwrap_or(source_peer_id);
    remember_peer_name(
        claimed_peer_id,
        &name,
        ctx.local_peer_id,
        ctx.local_name,
        ctx.local_joined_at,
        &mut *ctx.peer_names,
        &mut *ctx.local_name_conflicts,
        ui,
        joined_at,
    )
    .await;

    let id = id.unwrap_or_else(|| new_message_id(id_peer_id, sent_at, 0, &text));
    let record = ChatRecord {
        id,
        peer_id: claimed_peer_id.to_string(),
        joined_at,
        author: name,
        text,
        sent_at,
    };

    let inserted = insert_record(ctx.history, ctx.seen_messages, record);
    if inserted {
        send_history_snapshot(ui, ctx.history).await;
    }
    inserted
}

fn insert_record(
    history: &mut Vec<ChatRecord>,
    seen_messages: &mut HashSet<String>,
    record: ChatRecord,
) -> bool {
    if !seen_messages.insert(record.id.clone()) {
        return false;
    }

    history.push(record);
    history.sort_by(|left, right| {
        normalize_timestamp_micros(left.sent_at)
            .cmp(&normalize_timestamp_micros(right.sent_at))
            .then_with(|| left.id.cmp(&right.id))
    });

    if history.len() > MAX_MESSAGES {
        let overflow = history.len() - MAX_MESSAGES;
        for removed in history.drain(0..overflow) {
            seen_messages.remove(&removed.id);
        }
    }

    true
}

async fn send_history_snapshot(ui: &mpsc::Sender<UiEvent>, history: &[ChatRecord]) {
    let _ = ui.send(UiEvent::History(history.to_vec())).await;
}

async fn apply_remote_queue_state(
    ui: &mpsc::Sender<UiEvent>,
    ctx: &mut HistoryContext<'_>,
    state: QueueState,
    status_prefix: &str,
) -> bool {
    if !should_apply_queue_state(*ctx.queue_version, *ctx.queue_updated_at, &state) {
        return false;
    }

    *ctx.music_queue = VecDeque::from(state.items.clone());
    *ctx.queue_version = state.version;
    *ctx.queue_updated_at = state.updated_at_micros;
    let _ = ui.send(UiEvent::Queue(state.clone())).await;
    send_status(
        ui,
        format!("{status_prefix} by {}", short_peer(&state.updated_by)),
    )
    .await;
    send_queue_status(ui, ctx.playback_state.as_ref(), ctx.music_queue).await;
    true
}

async fn send_queue_view(
    ui: &mpsc::Sender<UiEvent>,
    queue_version: u64,
    queue_updated_at: i64,
    local_peer_id: PeerId,
    queue: &VecDeque<QueueItem>,
) {
    let _ = ui
        .send(UiEvent::Queue(QueueState {
            version: queue_version,
            updated_at_micros: queue_updated_at,
            updated_by: local_peer_id.to_string(),
            items: queue.iter().cloned().collect(),
        }))
        .await;
}

async fn send_vote_view(
    ui: &mpsc::Sender<UiEvent>,
    vote: Option<&ActiveVote>,
    queue: &VecDeque<QueueItem>,
    threshold: usize,
) {
    let payload = vote.map(|vote| VoteView {
        vote_id: vote.proposal.vote_id.clone(),
        proposer: vote.proposal.proposer.clone(),
        action_label: describe_vote_action(&vote.proposal.action, queue),
        approvals: vote.approvals.len(),
        rejections: vote.rejections.len(),
        threshold,
    });
    let _ = ui.send(UiEvent::Vote(payload)).await;
}

async fn remember_peer_name(
    peer_id: PeerId,
    name: &str,
    local_peer_id: PeerId,
    local_name: &str,
    local_joined_at: i64,
    peer_names: &mut HashMap<String, PeerNameClaim>,
    local_name_conflicts: &mut HashSet<String>,
    ui: &mpsc::Sender<UiEvent>,
    joined_at: Option<i64>,
) {
    if peer_id == local_peer_id {
        return;
    }

    let peer_id = peer_id.to_string();
    let was_blocked = !local_name_conflicts.is_empty();
    peer_names.insert(
        peer_id.clone(),
        PeerNameClaim {
            name: name.to_string(),
            joined_at,
        },
    );
    refresh_local_name_conflicts(
        local_peer_id,
        local_name,
        local_joined_at,
        peer_names,
        local_name_conflicts,
    );
    let is_blocked = !local_name_conflicts.is_empty();

    if !was_blocked && is_blocked {
        let winner = local_name_conflicts
            .iter()
            .next()
            .cloned()
            .unwrap_or(peer_id);
        send_status(
            ui,
            format!("name conflict: peer {winner} joined earlier with '{local_name}'"),
        )
        .await;
    } else if was_blocked && !is_blocked {
        send_status(ui, format!("name '{local_name}' is available again")).await;
    }
}

fn forget_peer_name(
    peer_id: PeerId,
    local_peer_id: PeerId,
    peer_names: &mut HashMap<String, PeerNameClaim>,
    local_name_conflicts: &mut HashSet<String>,
    local_name: &str,
    local_joined_at: i64,
) -> bool {
    let was_blocked = !local_name_conflicts.is_empty();
    let peer_id = peer_id.to_string();
    peer_names.remove(&peer_id);
    refresh_local_name_conflicts(
        local_peer_id,
        local_name,
        local_joined_at,
        peer_names,
        local_name_conflicts,
    );

    was_blocked && local_name_conflicts.is_empty()
}

fn refresh_local_name_conflicts(
    local_peer_id: PeerId,
    local_name: &str,
    local_joined_at: i64,
    peer_names: &HashMap<String, PeerNameClaim>,
    local_name_conflicts: &mut HashSet<String>,
) {
    local_name_conflicts.clear();

    for (peer_id, claim) in peer_names {
        let Some(remote_peer_id) = parse_peer_id(peer_id) else {
            continue;
        };

        if claim.name == local_name
            && remote_has_name_priority(
                remote_peer_id,
                claim.joined_at,
                local_peer_id,
                local_joined_at,
            )
        {
            local_name_conflicts.insert(peer_id.clone());
        }
    }
}

fn remote_has_name_priority(
    remote_peer_id: PeerId,
    remote_joined_at: Option<i64>,
    local_peer_id: PeerId,
    local_joined_at: i64,
) -> bool {
    let remote_priority = name_priority(remote_peer_id, remote_joined_at);
    let local_priority = name_priority(local_peer_id, Some(local_joined_at));
    remote_priority < local_priority
}

fn name_priority(peer_id: PeerId, joined_at: Option<i64>) -> (i64, String) {
    (
        joined_at
            .map(normalize_timestamp_micros)
            .unwrap_or(i64::MAX),
        peer_id.to_string(),
    )
}

fn parse_peer_id(value: &str) -> Option<PeerId> {
    if value.is_empty() {
        return None;
    }

    value.parse().ok()
}

fn should_request_history(history_request_times: &HashMap<String, Instant>, peer_id: &str) -> bool {
    history_request_times
        .get(peer_id)
        .is_none_or(|last_request| last_request.elapsed() >= HISTORY_REQUEST_COOLDOWN)
}

fn should_request_queue(queue_request_times: &HashMap<String, Instant>, peer_id: &str) -> bool {
    queue_request_times
        .get(peer_id)
        .is_none_or(|last_request| last_request.elapsed() >= QUEUE_REQUEST_COOLDOWN)
}

fn send_direct_message_to_connected_peers(
    swarm: &mut libp2p::Swarm<Behaviour>,
    peer_routes: &HashMap<PeerId, PeerConnectionRoutes>,
    rendezvous_nodes: &HashSet<PeerId>,
    topic_name: &str,
    message: &WireMessage,
) -> usize {
    let local_peer_id = *swarm.local_peer_id();
    let peer_ids = peer_routes
        .iter()
        .filter(|(peer_id, routes)| {
            **peer_id != local_peer_id
                && !rendezvous_nodes.contains(peer_id)
                && (routes.has_direct() || routes.has_relayed())
        })
        .map(|(peer_id, _)| *peer_id)
        .collect::<Vec<_>>();

    let count = peer_ids.len();
    for peer_id in peer_ids {
        swarm.behaviour_mut().direct_messages.send_request(
            &peer_id,
            DirectMessageRequest {
                topic: topic_name.to_string(),
                message: message.clone(),
            },
        );
    }

    count
}

fn publish_chat_wire(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    message: &WireMessage,
) -> Result<ChatPublishOutcome> {
    let data = serde_json::to_vec(message)?;
    match swarm.behaviour_mut().gossipsub.publish(topic.clone(), data) {
        Ok(_) | Err(gossipsub::PublishError::Duplicate) => Ok(ChatPublishOutcome::Published),
        Err(gossipsub::PublishError::NoPeersSubscribedToTopic) => {
            Ok(ChatPublishOutcome::NoPeersSubscribed)
        }
        Err(err) => Err(anyhow!(err)),
    }
}

fn publish_history_wire(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    message: &WireMessage,
) -> Result<()> {
    let data = serde_json::to_vec(message)?;
    match swarm.behaviour_mut().gossipsub.publish(topic.clone(), data) {
        Ok(_)
        | Err(gossipsub::PublishError::Duplicate)
        | Err(gossipsub::PublishError::NoPeersSubscribedToTopic) => Ok(()),
        Err(err) => Err(anyhow!(err)),
    }
}

fn publish_queue_state(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    queue_version: &mut u64,
    queue_updated_at: &mut i64,
    local_peer_id: PeerId,
    queue: &VecDeque<QueueItem>,
) -> Result<()> {
    *queue_version += 1;
    *queue_updated_at = current_timestamp_micros();
    publish_queue_snapshot(
        swarm,
        topic,
        *queue_version,
        *queue_updated_at,
        local_peer_id,
        queue,
    )
}

fn publish_queue_snapshot(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    queue_version: u64,
    queue_updated_at: i64,
    local_peer_id: PeerId,
    queue: &VecDeque<QueueItem>,
) -> Result<()> {
    if queue_version == 0 && queue.is_empty() {
        return Ok(());
    }

    let state = build_queue_state(queue_version, queue_updated_at, local_peer_id, queue);

    publish_history_wire(
        swarm,
        topic,
        &WireMessage::QueueState {
            state,
            nonce: new_nonce(local_peer_id),
        },
    )
}

fn publish_music_snapshot(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    queue_version: u64,
    queue_updated_at: i64,
    queue: &VecDeque<QueueItem>,
    playback_state: Option<&PlaybackState>,
) -> Result<()> {
    publish_queue_snapshot(
        swarm,
        topic,
        queue_version,
        queue_updated_at,
        local_peer_id,
        queue,
    )?;

    if let Some(state) = playback_state {
        if state.leader_peer_id == local_peer_id.to_string() {
            publish_playback_state(swarm, topic, state)?;
        }
    }

    Ok(())
}

fn build_queue_state(
    queue_version: u64,
    queue_updated_at: i64,
    local_peer_id: PeerId,
    queue: &VecDeque<QueueItem>,
) -> QueueState {
    QueueState {
        version: queue_version,
        updated_at_micros: queue_updated_at,
        updated_by: local_peer_id.to_string(),
        items: queue.iter().cloned().collect(),
    }
}

fn should_apply_queue_state(local_version: u64, local_updated_at: i64, state: &QueueState) -> bool {
    is_queue_state_newer(
        state.version,
        state.updated_at_micros,
        local_version,
        local_updated_at,
    )
}

fn is_queue_state_newer(
    candidate_version: u64,
    candidate_updated_at: i64,
    local_version: u64,
    local_updated_at: i64,
) -> bool {
    candidate_updated_at > local_updated_at
        || (candidate_updated_at == local_updated_at && candidate_version > local_version)
}

async fn start_next_if_idle(
    queue: &mut VecDeque<QueueItem>,
    queue_version: &mut u64,
    queue_updated_at: &mut i64,
    pending_playback: &mut Option<PendingPlayback>,
    playback_state: &mut Option<PlaybackState>,
    playback_version: &mut u64,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    if pending_playback.is_some() || playback_has_track(playback_state.as_ref()) {
        return Ok(());
    }

    let Some(item) = queue.pop_front() else {
        return Ok(());
    };
    publish_queue_state(
        swarm,
        topic,
        queue_version,
        queue_updated_at,
        local_peer_id,
        queue,
    )?;
    send_queue_view(ui, *queue_version, *queue_updated_at, local_peer_id, queue).await;
    begin_playback_prepare(
        item,
        pending_playback,
        playback_state,
        playback_version,
        audio_player,
        client,
        swarm,
        topic,
        local_peer_id,
        ui,
    )
    .await
}

async fn begin_playback_prepare(
    item: QueueItem,
    pending_playback: &mut Option<PendingPlayback>,
    playback_state: &mut Option<PlaybackState>,
    playback_version: &mut u64,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let title = item.track.title.clone();
    let now = current_timestamp_micros();
    if let Some(pending) = pending_playback.take() {
        publish_playback_cancel(
            swarm,
            topic,
            &pending.state.session_id,
            local_peer_id,
            "superseded by next queue item",
        )?;
    }

    *playback_version += 1;
    let state = PlaybackState {
        session_id: new_playback_session_id(local_peer_id, now, &item.track.track_id),
        leader_peer_id: local_peer_id.to_string(),
        track: Some(item.track),
        track_requested_by: Some(item.requested_by),
        state_version: *playback_version,
        issued_at_micros: now,
        playing: false,
        position_ms: 0,
        anchor_time_micros: now,
        rate: 1.0,
    };
    let expected_peers = expected_playback_peers(swarm, local_peer_id);
    *pending_playback = Some(PendingPlayback::new(
        state.clone(),
        expected_peers,
        Instant::now() + MUSIC_PREPARE_TIMEOUT,
    ));
    *playback_state = Some(state.clone());
    if let Some(player) = audio_player.as_mut() {
        player.set_playing(false, now)?;
    }
    send_playback_view(ui, &state).await;
    if let Some(pending) = pending_playback.as_ref() {
        publish_playback_prepare(swarm, topic, &state, &pending.expected_peers)?;
    }

    send_status(ui, format!("preparing {title}")).await;
    send_status(ui, format!("downloading {title}")).await;
    let Some(track) = state.track.as_ref() else {
        return Ok(());
    };
    let audio = match bilibili::download_audio(client, track).await {
        Ok(audio) => audio,
        Err(err) => {
            if let Some(pending) = pending_playback.take() {
                publish_playback_cancel(
                    swarm,
                    topic,
                    &pending.state.session_id,
                    local_peer_id,
                    "local audio download failed",
                )?;
            }
            return Err(err);
        }
    };

    let ready = if let Some(player) = audio_player.as_mut() {
        player
            .load(
                track.track_id.clone(),
                Arc::<[u8]>::from(audio.into_boxed_slice()),
                0,
                false,
                current_timestamp_micros(),
            )
            .is_ok()
    } else {
        true
    };

    if ready {
        if let Some(pending) = pending_playback.as_mut() {
            pending.mark_ready(local_peer_id.to_string());
        }
        send_status(ui, "local audio ready".to_string()).await;
        maybe_start_pending_playback(
            pending_playback,
            playback_state,
            playback_version,
            swarm,
            topic,
            ui,
        )
        .await?;
    } else if let Some(pending) = pending_playback.take() {
        publish_playback_cancel(
            swarm,
            topic,
            &pending.state.session_id,
            local_peer_id,
            "local audio failed to load",
        )?;
    }

    Ok(())
}

async fn skip_current_track(
    queue: &mut VecDeque<QueueItem>,
    queue_version: &mut u64,
    queue_updated_at: &mut i64,
    pending_playback: &mut Option<PendingPlayback>,
    playback_state: &mut Option<PlaybackState>,
    playback_version: &mut u64,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    stop_current_playback(
        pending_playback,
        playback_state,
        playback_version,
        audio_player,
        swarm,
        topic,
        local_peer_id,
        "skipped",
        ui,
    )
    .await?;
    start_next_if_idle(
        queue,
        queue_version,
        queue_updated_at,
        pending_playback,
        playback_state,
        playback_version,
        audio_player,
        client,
        swarm,
        topic,
        local_peer_id,
        ui,
    )
    .await
}

async fn stop_current_playback(
    pending_playback: &mut Option<PendingPlayback>,
    playback_state: &mut Option<PlaybackState>,
    playback_version: &mut u64,
    audio_player: &mut Option<player::AudioPlayer>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    reason: &str,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    if let Some(player) = audio_player.as_mut() {
        player.stop();
    }
    if let Some(pending) = pending_playback.take() {
        publish_playback_cancel(
            swarm,
            topic,
            &pending.state.session_id,
            local_peer_id,
            reason,
        )?;
    }

    let now = current_timestamp_micros();
    *playback_version += 1;
    let state = PlaybackState {
        session_id: new_playback_session_id(local_peer_id, now, "idle"),
        leader_peer_id: local_peer_id.to_string(),
        track: None,
        track_requested_by: None,
        state_version: *playback_version,
        issued_at_micros: now,
        playing: false,
        position_ms: 0,
        anchor_time_micros: now,
        rate: 1.0,
    };
    *playback_state = Some(state.clone());
    publish_playback_state(swarm, topic, &state)?;
    send_playback_view(ui, &state).await;
    send_status(ui, reason.to_string()).await;
    Ok(())
}

fn playback_has_track(state: Option<&PlaybackState>) -> bool {
    state.and_then(|state| state.track.as_ref()).is_some()
}

async fn propose_or_execute_vote(
    action: VoteAction,
    active_vote: &mut Option<ActiveVote>,
    queue: &mut VecDeque<QueueItem>,
    queue_version: &mut u64,
    queue_updated_at: &mut i64,
    pending_playback: &mut Option<PendingPlayback>,
    playback_state: &mut Option<PlaybackState>,
    playback_version: &mut u64,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    if active_vote.is_some() {
        send_status(ui, "another vote is already active".to_string()).await;
        return Ok(());
    }

    let now = current_timestamp_micros();
    let proposal = VoteProposal {
        vote_id: new_vote_id(local_peer_id, now),
        proposer: local_peer_id.to_string(),
        action,
        queue_version: *queue_version,
        playback_session_id: playback_state
            .as_ref()
            .map(|state| state.session_id.clone()),
        created_at_micros: now,
    };

    let mut vote = ActiveVote::new(proposal.clone(), Instant::now() + VOTE_TIMEOUT);
    vote.vote(local_peer_id.to_string(), true);
    publish_vote_proposal(swarm, topic, &proposal, local_peer_id)?;
    publish_vote_ballot(swarm, topic, &proposal.vote_id, local_peer_id, true)?;
    let approval_count = vote.approval_count();
    let threshold = majority_threshold(swarm.connected_peers().count() + 1);
    *active_vote = Some(vote);
    send_vote_view(ui, active_vote.as_ref(), queue, threshold).await;
    send_status(
        ui,
        format!(
            "started vote: {} ({}/{})",
            describe_vote_action(&proposal.action, queue),
            approval_count,
            threshold
        ),
    )
    .await;

    resolve_active_vote(
        active_vote,
        queue,
        queue_version,
        queue_updated_at,
        pending_playback,
        playback_state,
        playback_version,
        audio_player,
        client,
        swarm,
        topic,
        local_peer_id,
        ui,
    )
    .await
}

async fn cast_vote(
    approve: bool,
    active_vote: &mut Option<ActiveVote>,
    queue: &mut VecDeque<QueueItem>,
    queue_version: &mut u64,
    queue_updated_at: &mut i64,
    pending_playback: &mut Option<PendingPlayback>,
    playback_state: &mut Option<PlaybackState>,
    playback_version: &mut u64,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let Some(vote) = active_vote.as_mut() else {
        send_status(ui, "no active vote".to_string()).await;
        return Ok(());
    };

    vote.vote(local_peer_id.to_string(), approve);
    publish_vote_ballot(swarm, topic, &vote.proposal.vote_id, local_peer_id, approve)?;
    send_status(
        ui,
        format!(
            "voted {} on {}",
            if approve { "yes" } else { "no" },
            vote.proposal.vote_id
        ),
    )
    .await;
    send_vote_view(
        ui,
        active_vote.as_ref(),
        queue,
        majority_threshold(swarm.connected_peers().count() + 1),
    )
    .await;

    resolve_active_vote(
        active_vote,
        queue,
        queue_version,
        queue_updated_at,
        pending_playback,
        playback_state,
        playback_version,
        audio_player,
        client,
        swarm,
        topic,
        local_peer_id,
        ui,
    )
    .await
}

async fn resolve_active_vote(
    active_vote: &mut Option<ActiveVote>,
    queue: &mut VecDeque<QueueItem>,
    queue_version: &mut u64,
    queue_updated_at: &mut i64,
    pending_playback: &mut Option<PendingPlayback>,
    playback_state: &mut Option<PlaybackState>,
    playback_version: &mut u64,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let Some(vote) = active_vote.as_ref() else {
        return Ok(());
    };

    let threshold = majority_threshold(swarm.connected_peers().count() + 1);
    if vote.approval_count() < threshold {
        return Ok(());
    }

    let vote = active_vote
        .take()
        .ok_or_else(|| anyhow!("active vote disappeared"))?;
    send_vote_view(ui, None, queue, threshold).await;
    send_status(
        ui,
        format!(
            "vote passed: {}",
            describe_vote_action(&vote.proposal.action, queue)
        ),
    )
    .await;

    if let Some(reason) = stale_vote_reason(&vote.proposal, *queue_version, playback_state.as_ref())
    {
        send_status(ui, format!("vote discarded: {reason}")).await;
        return Ok(());
    }

    if should_execute_vote_locally(&vote.proposal, playback_state.as_ref(), local_peer_id) {
        execute_vote_action(
            vote.proposal.action,
            queue,
            queue_version,
            queue_updated_at,
            pending_playback,
            playback_state,
            playback_version,
            audio_player,
            client,
            swarm,
            topic,
            local_peer_id,
            ui,
        )
        .await?;
    }

    Ok(())
}

fn stale_vote_reason(
    proposal: &VoteProposal,
    queue_version: u64,
    playback_state: Option<&PlaybackState>,
) -> Option<&'static str> {
    match &proposal.action {
        VoteAction::Remove { .. } | VoteAction::Move { .. } => {
            (proposal.queue_version != queue_version).then_some("queue changed during vote")
        }
        VoteAction::Pause | VoteAction::Resume | VoteAction::Skip | VoteAction::Seek { .. } => {
            let current_session = playback_state
                .and_then(|state| state.track.as_ref().map(|_| state.session_id.as_str()));
            (proposal.playback_session_id.as_deref() != current_session)
                .then_some("playback changed during vote")
        }
    }
}

fn should_execute_vote_locally(
    proposal: &VoteProposal,
    playback_state: Option<&PlaybackState>,
    local_peer_id: PeerId,
) -> bool {
    match &proposal.action {
        VoteAction::Remove { .. } | VoteAction::Move { .. } => {
            proposal.proposer == local_peer_id.to_string()
        }
        VoteAction::Pause | VoteAction::Resume | VoteAction::Skip | VoteAction::Seek { .. } => {
            playback_state.is_some_and(|state| can_control_playback(state, local_peer_id))
        }
    }
}

async fn execute_vote_action(
    action: VoteAction,
    queue: &mut VecDeque<QueueItem>,
    queue_version: &mut u64,
    queue_updated_at: &mut i64,
    pending_playback: &mut Option<PendingPlayback>,
    playback_state: &mut Option<PlaybackState>,
    playback_version: &mut u64,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    match action {
        VoteAction::Pause => {
            if let Some(state) = playback_state.as_mut() {
                let now = current_timestamp_micros();
                let position_ms = playback_position_ms(state, now);
                *playback_version += 1;
                state.state_version = *playback_version;
                state.issued_at_micros = now;
                state.playing = false;
                state.position_ms = position_ms;
                state.anchor_time_micros = now;
                state.leader_peer_id = local_peer_id.to_string();
                if let Some(player) = audio_player.as_mut() {
                    player.set_playing(false, now)?;
                }
                publish_playback_state(swarm, topic, state)?;
                send_playback_view(ui, state).await;
            }
        }
        VoteAction::Resume => {
            if let Some(state) = playback_state.as_mut() {
                let now = current_timestamp_micros();
                let position_ms = playback_position_ms(state, now);
                let playing = can_play_at_position(state, position_ms);
                *playback_version += 1;
                state.state_version = *playback_version;
                state.issued_at_micros = now;
                state.playing = playing;
                state.position_ms = position_ms;
                state.anchor_time_micros = now;
                state.leader_peer_id = local_peer_id.to_string();
                if let Some(player) = audio_player.as_mut() {
                    player.set_playing(playing, now)?;
                }
                publish_playback_state(swarm, topic, state)?;
                send_playback_view(ui, state).await;
            }
        }
        VoteAction::Skip => {
            skip_current_track(
                queue,
                queue_version,
                queue_updated_at,
                pending_playback,
                playback_state,
                playback_version,
                audio_player,
                client,
                swarm,
                topic,
                local_peer_id,
                ui,
            )
            .await?;
        }
        VoteAction::Seek { position_ms } => {
            if let Some(state) = playback_state.as_mut() {
                let now = current_timestamp_micros();
                let position_ms = clamp_playback_position_ms(state, position_ms);
                let playing = state.playing && can_play_at_position(state, position_ms);
                *playback_version += 1;
                state.state_version = *playback_version;
                state.issued_at_micros = now;
                state.playing = playing;
                state.position_ms = position_ms;
                state.anchor_time_micros = now;
                state.leader_peer_id = local_peer_id.to_string();
                if let Some(player) = audio_player.as_mut() {
                    player.seek(position_ms, playing, now)?;
                }
                publish_playback_state(swarm, topic, state)?;
                send_playback_view(ui, state).await;
            }
        }
        VoteAction::Remove { item_id } => {
            if let Some(index) = queue.iter().position(|item| item.item_id == item_id) {
                let removed = queue.remove(index);
                publish_queue_state(
                    swarm,
                    topic,
                    queue_version,
                    queue_updated_at,
                    local_peer_id,
                    queue,
                )?;
                send_queue_view(ui, *queue_version, *queue_updated_at, local_peer_id, queue).await;
                if let Some(item) = removed {
                    send_status(ui, format!("removed {}", item.track.title)).await;
                }
            }
        }
        VoteAction::Move { item_id, to_index } => {
            if let Some(index) = queue.iter().position(|item| item.item_id == item_id) {
                if let Some(item) = queue.remove(index) {
                    let to_index = to_index.min(queue.len());
                    queue.insert(to_index, item);
                    publish_queue_state(
                        swarm,
                        topic,
                        queue_version,
                        queue_updated_at,
                        local_peer_id,
                        queue,
                    )?;
                    send_queue_view(ui, *queue_version, *queue_updated_at, local_peer_id, queue)
                        .await;
                    send_status(ui, format!("moved queue item to #{}", to_index + 1)).await;
                }
            }
        }
    }

    Ok(())
}

fn publish_vote_proposal(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    proposal: &VoteProposal,
    local_peer_id: PeerId,
) -> Result<()> {
    publish_history_wire(
        swarm,
        topic,
        &WireMessage::VoteProposal {
            proposal: proposal.clone(),
            nonce: new_nonce(local_peer_id),
        },
    )
}

fn publish_vote_ballot(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    vote_id: &str,
    local_peer_id: PeerId,
    approve: bool,
) -> Result<()> {
    publish_history_wire(
        swarm,
        topic,
        &WireMessage::VoteBallot {
            vote_id: vote_id.to_string(),
            peer_id: local_peer_id.to_string(),
            approve,
            nonce: new_nonce(local_peer_id),
        },
    )
}

fn majority_threshold(total_peers: usize) -> usize {
    total_peers / 2 + 1
}

fn can_control_playback(state: &PlaybackState, local_peer_id: PeerId) -> bool {
    state
        .track_requested_by
        .as_ref()
        .is_some_and(|requester| requester == &local_peer_id.to_string())
}

fn queue_item_at(queue: &VecDeque<QueueItem>, index: usize) -> Option<&QueueItem> {
    index.checked_sub(1).and_then(|index| queue.get(index))
}

async fn send_queue_status(
    ui: &mpsc::Sender<UiEvent>,
    playback_state: Option<&PlaybackState>,
    queue: &VecDeque<QueueItem>,
) {
    if let Some(track) = playback_state.and_then(|state| state.track.as_ref()) {
        send_status(ui, format!("now: {}", track.title)).await;
    } else {
        send_status(ui, "now: idle".to_string()).await;
    }

    if queue.is_empty() {
        send_status(ui, "queue is empty".to_string()).await;
        return;
    }

    for (index, item) in queue.iter().take(3).enumerate() {
        send_status(
            ui,
            format!(
                "#{} {} ({})",
                index + 1,
                item.track.title,
                short_peer(&item.requested_by)
            ),
        )
        .await;
    }
    if queue.len() > 3 {
        send_status(ui, format!("... and {} more", queue.len() - 3)).await;
    }
}

fn describe_vote_action(action: &VoteAction, queue: &VecDeque<QueueItem>) -> String {
    match action {
        VoteAction::Pause => "pause playback".to_string(),
        VoteAction::Resume => "resume playback".to_string(),
        VoteAction::Skip => "skip current track".to_string(),
        VoteAction::Seek { position_ms } => {
            format!("seek to {}", format_duration_ms(*position_ms))
        }
        VoteAction::Remove { item_id } => queue
            .iter()
            .position(|item| item.item_id == *item_id)
            .map(|index| format!("remove queue item #{}", index + 1))
            .unwrap_or_else(|| "remove queue item".to_string()),
        VoteAction::Move { item_id, to_index } => queue
            .iter()
            .position(|item| item.item_id == *item_id)
            .map(|index| format!("move queue item #{} to #{}", index + 1, to_index + 1))
            .unwrap_or_else(|| format!("move queue item to #{}", to_index + 1)),
    }
}

fn short_peer(peer_id: &str) -> String {
    peer_id.chars().take(8).collect()
}

fn publish_playback_state(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    state: &PlaybackState,
) -> Result<()> {
    publish_history_wire(
        swarm,
        topic,
        &WireMessage::PlaybackState {
            state: state.clone(),
            nonce: new_nonce(
                state
                    .leader_peer_id
                    .parse()
                    .unwrap_or_else(|_| *swarm.local_peer_id()),
            ),
        },
    )
}

fn publish_playback_prepare(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    state: &PlaybackState,
    expected_peers: &HashSet<String>,
) -> Result<()> {
    publish_history_wire(
        swarm,
        topic,
        &WireMessage::PlaybackPrepare {
            state: state.clone(),
            expected_peers: expected_peers.iter().cloned().collect(),
            nonce: new_nonce(
                state
                    .leader_peer_id
                    .parse()
                    .unwrap_or_else(|_| *swarm.local_peer_id()),
            ),
        },
    )
}

fn publish_playback_ready(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    session_id: &str,
    local_peer_id: PeerId,
) -> Result<()> {
    publish_history_wire(
        swarm,
        topic,
        &WireMessage::PlaybackReady {
            session_id: session_id.to_string(),
            peer_id: local_peer_id.to_string(),
            nonce: new_nonce(local_peer_id),
        },
    )
}

fn publish_playback_cancel(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    session_id: &str,
    local_peer_id: PeerId,
    reason: &str,
) -> Result<()> {
    publish_history_wire(
        swarm,
        topic,
        &WireMessage::PlaybackCancel {
            session_id: session_id.to_string(),
            leader_peer_id: local_peer_id.to_string(),
            reason: reason.to_string(),
            nonce: new_nonce(local_peer_id),
        },
    )
}

fn cancel_local_pending_playback(
    pending_playback: &mut Option<PendingPlayback>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    local_peer_id: PeerId,
    reason: &str,
) {
    if let Some(pending) = pending_playback.take() {
        let _ = publish_playback_cancel(
            swarm,
            topic,
            &pending.state.session_id,
            local_peer_id,
            reason,
        );
    }
}

async fn maybe_start_pending_playback(
    pending_playback: &mut Option<PendingPlayback>,
    playback_state: &mut Option<PlaybackState>,
    playback_version: &mut u64,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let Some(pending) = pending_playback.as_ref() else {
        return Ok(());
    };

    let timed_out = Instant::now() >= pending.deadline;
    if !pending.is_ready() && !timed_out {
        return Ok(());
    }

    let pending = pending_playback
        .take()
        .ok_or_else(|| anyhow!("pending playback disappeared"))?;
    let now = current_timestamp_micros();
    *playback_version += 1;

    let mut state = pending.state.clone();
    state.state_version = *playback_version;
    state.issued_at_micros = now;
    state.playing = true;
    state.position_ms = 0;
    state.anchor_time_micros = now + duration_micros(MUSIC_START_DELAY);

    *playback_state = Some(state.clone());
    publish_playback_state(swarm, topic, &state)?;
    send_playback_view(ui, &state).await;

    let reason = if pending.is_ready() {
        "all peers ready"
    } else {
        "ready wait timed out"
    };
    send_status(
        ui,
        format!(
            "starting playback in {:.1}s ({reason}, {}/{})",
            MUSIC_START_DELAY.as_secs_f32(),
            pending.ready_count(),
            pending.expected_count()
        ),
    )
    .await;

    Ok(())
}

async fn apply_playback_prepare(
    client: &reqwest::Client,
    audio_player: &mut Option<player::AudioPlayer>,
    current_state: &mut Option<PlaybackState>,
    state: &PlaybackState,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<bool> {
    let now = current_timestamp_micros();
    if let Some(player) = audio_player.as_mut() {
        player.set_playing(false, now)?;
    }

    *current_state = Some(state.clone());
    send_playback_view(ui, state).await;

    let Some(track) = &state.track else {
        return Ok(true);
    };

    send_status(ui, format!("preparing {}", track.title)).await;
    let Some(player) = audio_player.as_mut() else {
        send_status(
            ui,
            "audio output unavailable; confirming prepare".to_string(),
        )
        .await;
        return Ok(true);
    };

    send_status(ui, format!("downloading {}", track.title)).await;
    let audio = bilibili::download_audio(client, track).await?;
    player.load(
        track.track_id.clone(),
        Arc::<[u8]>::from(audio.into_boxed_slice()),
        0,
        false,
        current_timestamp_micros(),
    )?;
    send_status(ui, "local audio ready".to_string()).await;
    Ok(true)
}

async fn apply_playback_cancel(
    audio_player: &mut Option<player::AudioPlayer>,
    playback_state: &mut Option<PlaybackState>,
    session_id: &str,
    reason: &str,
    ui: &mpsc::Sender<UiEvent>,
) {
    if playback_state
        .as_ref()
        .is_some_and(|state| state.session_id == session_id)
    {
        if let Some(player) = audio_player.as_mut() {
            player.stop();
        }
        *playback_state = None;
        let _ = ui.send(UiEvent::Playback(None)).await;
        send_status(ui, format!("playback canceled: {reason}")).await;
    }
}

fn sync_loaded_player_to_state(
    audio_player: &mut Option<player::AudioPlayer>,
    state: &PlaybackState,
    now_micros: i64,
) -> Result<()> {
    let Some(track) = &state.track else {
        return Ok(());
    };
    let Some(player) = audio_player.as_mut() else {
        return Ok(());
    };
    if player.current_track_id() != Some(track.track_id.as_str()) {
        return Ok(());
    }

    let desired_position = playback_position_ms(state, now_micros);
    let should_play = playback_should_be_audible(state, now_micros);
    let current_position = player.position_ms(now_micros);
    let drift = current_position.abs_diff(desired_position);

    if drift > MUSIC_DRIFT_SEEK_THRESHOLD_MS || (should_play && !player.is_playing()) {
        player.seek(desired_position, should_play, now_micros)?;
    } else {
        player.set_playing(should_play, now_micros)?;
    }

    Ok(())
}

fn expected_playback_peers(
    swarm: &libp2p::Swarm<Behaviour>,
    local_peer_id: PeerId,
) -> HashSet<String> {
    let mut peers = swarm
        .connected_peers()
        .map(|peer_id| peer_id.to_string())
        .collect::<HashSet<_>>();
    peers.insert(local_peer_id.to_string());
    peers
}

fn new_playback_session_id(local_peer_id: PeerId, issued_at_micros: i64, track_id: &str) -> String {
    format!("{local_peer_id}:{issued_at_micros}:{track_id}")
}

fn should_apply_playback_state(current: Option<&PlaybackState>, next: &PlaybackState) -> bool {
    let Some(current) = current else {
        return true;
    };

    let current_key = playback_order_key(current);
    let next_key = playback_order_key(next);
    if next_key != current_key {
        return next_key > current_key;
    }

    current.track.as_ref().map(|track| &track.track_id)
        == next.track.as_ref().map(|track| &track.track_id)
        && current.leader_peer_id == next.leader_peer_id
        && (current.anchor_time_micros < next.anchor_time_micros
            || current.position_ms != next.position_ms
            || current.playing != next.playing)
}

fn normalize_remote_playback_state(
    state: &PlaybackState,
    received_at_micros: i64,
) -> PlaybackState {
    let mut normalized = state.clone();
    let start_delay_micros = if state.playing && state.anchor_time_micros > state.issued_at_micros {
        state
            .anchor_time_micros
            .saturating_sub(state.issued_at_micros)
    } else {
        0
    };

    normalized.anchor_time_micros = received_at_micros.saturating_add(start_delay_micros);
    normalized
}

async fn apply_remote_playback_state(
    client: &reqwest::Client,
    audio_player: &mut Option<player::AudioPlayer>,
    current_state: &mut Option<PlaybackState>,
    state: &PlaybackState,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let now = current_timestamp_micros();
    let desired_position = playback_position_ms(state, now);
    let should_play = playback_should_be_audible(state, now);

    if state.track.is_none() {
        if let Some(player) = audio_player.as_mut() {
            player.stop();
        }
        *current_state = Some(state.clone());
        send_playback_view(ui, state).await;
        return Ok(());
    }

    if let Some(track) = &state.track {
        if let Some(player) = audio_player.as_mut() {
            if player.current_track_id() != Some(track.track_id.as_str()) {
                send_status(ui, format!("downloading {}", track.title)).await;
                let audio = bilibili::download_audio(client, track).await?;
                player.load(
                    track.track_id.clone(),
                    Arc::<[u8]>::from(audio.into_boxed_slice()),
                    desired_position,
                    should_play,
                    now,
                )?;
            } else {
                let current_position = player.position_ms(now);
                let drift = current_position.abs_diff(desired_position);
                if drift > MUSIC_DRIFT_SEEK_THRESHOLD_MS || (should_play && !player.is_playing()) {
                    player.seek(desired_position, should_play, now)?;
                } else {
                    player.set_playing(should_play, now)?;
                }
            }
        }
    }

    *current_state = Some(state.clone());
    send_playback_view(ui, state).await;
    Ok(())
}

fn playback_position_ms(state: &PlaybackState, now_micros: i64) -> u64 {
    let position_ms = if !state.playing {
        state.position_ms
    } else {
        let elapsed_micros = now_micros.saturating_sub(state.anchor_time_micros).max(0) as f64;
        let elapsed_ms = (elapsed_micros / 1000.0 * state.rate.max(0.0) as f64) as u64;
        state.position_ms.saturating_add(elapsed_ms)
    };

    clamp_playback_position_ms(state, position_ms)
}

fn playback_order_key(state: &PlaybackState) -> (i64, u64, &str) {
    (
        state.issued_at_micros,
        state.state_version,
        state.leader_peer_id.as_str(),
    )
}

fn playback_duration_ms(state: &PlaybackState) -> Option<u64> {
    state.track.as_ref().map(|track| track.duration_ms)
}

fn clamp_playback_position_ms(state: &PlaybackState, position_ms: u64) -> u64 {
    playback_duration_ms(state).map_or(position_ms, |duration| position_ms.min(duration))
}

fn can_play_at_position(state: &PlaybackState, position_ms: u64) -> bool {
    playback_duration_ms(state).is_none_or(|duration| position_ms < duration)
}

fn playback_should_be_audible(state: &PlaybackState, now_micros: i64) -> bool {
    state.playing
        && now_micros >= state.anchor_time_micros
        && can_play_at_position(state, playback_position_ms(state, now_micros))
}

fn duration_micros(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

async fn send_playback_view(ui: &mpsc::Sender<UiEvent>, state: &PlaybackState) {
    let now = current_timestamp_micros();
    let playback = state.track.as_ref().map(|track| PlaybackView {
        title: track.title.clone(),
        playing: playback_should_be_audible(state, now),
        position_ms: playback_position_ms(state, now),
        duration_ms: track.duration_ms,
        leader_peer_id: state.leader_peer_id.clone(),
    });
    let _ = ui.send(UiEvent::Playback(playback)).await;
}

fn current_timestamp_micros() -> i64 {
    Local::now().timestamp_micros()
}

fn new_nonce(peer_id: PeerId) -> u64 {
    let mut hasher = DefaultHasher::new();
    let sequence = NONCE_SEQ.fetch_add(1, Ordering::Relaxed);
    peer_id.hash(&mut hasher);
    current_timestamp_micros().hash(&mut hasher);
    sequence.hash(&mut hasher);
    hasher.finish()
}

fn new_message_id(peer_id: PeerId, sent_at: i64, sequence: u64, text: &str) -> String {
    let mut hasher = DefaultHasher::new();
    peer_id.hash(&mut hasher);
    sent_at.hash(&mut hasher);
    sequence.hash(&mut hasher);
    text.hash(&mut hasher);
    format!("{peer_id}-{sent_at}-{:x}", hasher.finish())
}

fn new_queue_item_id(peer_id: PeerId, track_id: &str) -> String {
    let now = current_timestamp_micros();
    let mut hasher = DefaultHasher::new();
    peer_id.hash(&mut hasher);
    now.hash(&mut hasher);
    track_id.hash(&mut hasher);
    format!("q-{peer_id}-{now}-{:x}", hasher.finish())
}

fn new_vote_id(peer_id: PeerId, created_at_micros: i64) -> String {
    let mut hasher = DefaultHasher::new();
    peer_id.hash(&mut hasher);
    created_at_micros.hash(&mut hasher);
    NONCE_SEQ.fetch_add(1, Ordering::Relaxed).hash(&mut hasher);
    format!("v-{peer_id}-{created_at_micros}-{:x}", hasher.finish())
}

fn prioritize_multiaddrs(addrs: Vec<Multiaddr>) -> Vec<Multiaddr> {
    let mut addrs = addrs;
    addrs.sort_by(|a, b| ipv6_preference_score(b).cmp(&ipv6_preference_score(a)));
    addrs
}

fn ipv6_preference_score(addr: &Multiaddr) -> u8 {
    let text = addr.to_string();
    if text.contains("/ip6/") || text.contains("/dns6/") {
        2
    } else if text.contains("/ip4/") || text.contains("/dns4/") {
        1
    } else {
        0
    }
}

async fn send_status(ui: &mpsc::Sender<UiEvent>, status: String) {
    let _ = ui.send(UiEvent::Status(status)).await;
}
