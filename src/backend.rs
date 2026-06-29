use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
    io,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use anyhow::{Context, Result, anyhow};
use chrono::Local;
use futures::StreamExt;
use libp2p::{
    Multiaddr, PeerId, SwarmBuilder, dcutr, gossipsub, identify, mdns, noise, ping, relay,
    rendezvous,
    swarm::{
        NetworkBehaviour, SwarmEvent,
        behaviour::toggle::Toggle,
        dial_opts::{DialOpts, PeerCondition},
    },
    tcp, yamux,
};
use tokio::{
    sync::mpsc,
    time::{self, Instant, MissedTickBehavior},
};

use crate::{
    bilibili,
    buffer_state::{
        BufferCoordinator, BufferOperation, BufferPeerStatus, BufferQuorum, BufferStatusOutcome,
    },
    connection_state::{
        ConnectionEffect, ConnectionState, DIRECT_PROMOTION_RETRY_INTERVAL,
        GOSSIP_WARMUP_CHECK_INTERVAL, RelayCloseReason, is_relay_address, normalize_peer_address,
        peer_id_from_multiaddr, prioritize_multiaddrs,
    },
    core::{
        ChatRecord, FrontendEvent as UiEvent, MAX_MESSAGES, NetworkCommand, PeerNameClaim,
        PeerNameView, PlaybackBufferOperationKind, PlaybackBufferStatusKind, PlaybackCacheStatus,
        PlaybackCacheView, PlaybackState, PlaybackTrack, PlaybackView, QueueItem, QueueState,
        VoteAction, VoteProposal, WireMessage, normalize_timestamp_micros,
    },
    media_cache,
    music_state::{
        MusicState, PlaybackReadyOutcome, VoteCastOutcome, VoteResolution, can_control_playback,
        can_play_at_position, describe_vote_action, duration_micros, is_queue_state_newer,
        majority_threshold, normalize_remote_playback_state, playback_position_ms,
        playback_should_be_audible, queue_item_at, should_apply_playback_state,
    },
    playback_health::{PlaybackHealth, PlaybackHealthDecision},
    player,
};

const HISTORY_SYNC_INTERVAL: Duration = Duration::from_secs(10);
const HISTORY_SYNC_BURST_TICK: Duration = Duration::from_millis(200);
const SYNC_IMMEDIATE_MIN_INTERVAL: Duration = Duration::from_secs(1);
const MUSIC_SNAPSHOT_MIN_INTERVAL: Duration = Duration::from_secs(1);
const HISTORY_REQUEST_COOLDOWN: Duration = Duration::from_secs(5);
const QUEUE_REQUEST_COOLDOWN: Duration = Duration::from_secs(5);
const MUSIC_LOCAL_INTERVAL: Duration = Duration::from_millis(100);
const MUSIC_STATE_INTERVAL: Duration = Duration::from_secs(1);
const MUSIC_DRIFT_SEEK_THRESHOLD_MS: u64 = 700;
const MUSIC_START_DELAY: Duration = Duration::from_millis(1500);
const BUFFER_OPERATION_TIMEOUT: Duration = Duration::from_secs(20);
const BUFFER_PREPARE_REPUBLISH_INTERVAL: Duration = Duration::from_secs(2);
const BUFFER_READY_WINDOW_MS: u64 = media_cache::READY_WINDOW_MS;
const BUFFER_HEALTH_LOSS_GRACE: Duration = Duration::from_secs(3);
const BUFFER_HEALTH_STALE_AFTER: Duration = Duration::from_secs(5);
const BUFFER_HEALTH_PUBLISH_MIN_INTERVAL: Duration = Duration::from_secs(1);
const BUFFER_HEALTH_PUBLISH_MIN_ADVANCE_MS: u64 = media_cache::LOW_WATERMARK_MS;
const AUDIO_OUTPUT_RETRY_INTERVAL: Duration = Duration::from_secs(5);
const VOTE_TIMEOUT: Duration = Duration::from_secs(20);
const RENDEZVOUS_DISCOVER_INTERVAL: Duration = Duration::from_secs(30);
const RENDEZVOUS_REGISTER_INTERVAL: Duration = Duration::from_secs(60 * 60);
const RENDEZVOUS_TTL_SECONDS: u64 = 60 * 60 * 2;
const RENDEZVOUS_UNREGISTER_GRACE: Duration = Duration::from_millis(500);
const ZERO_PEER_RECOVERY_TICK: Duration = Duration::from_secs(1);
const ZERO_PEER_RECOVERY_DISCOVER_DELAYS: [Duration; 5] = [
    Duration::from_secs(0),
    Duration::from_secs(5),
    Duration::from_secs(10),
    Duration::from_secs(20),
    Duration::from_secs(30),
];
const SWARM_IDLE_CONNECTION_TIMEOUT: Duration = Duration::from_secs(60 * 60);
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
    identify: identify::Behaviour,
    ping: ping::Behaviour,
    relay: relay::client::Behaviour,
    dcutr: dcutr::Behaviour,
    rendezvous: rendezvous::client::Behaviour,
    mdns: Toggle<mdns::tokio::Behaviour>,
}

struct PublishTargets<'a> {
    rendezvous_nodes: &'a HashSet<PeerId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoomPublishOutcome {
    Published,
    NoSubscribers,
    Congested,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RoomPublishPlan {
    Published,
    NoSubscribers,
    Congested,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FinishedPlaybackRole {
    Leader,
    Follower,
}

#[derive(Default)]
struct LocalMediaResolver {
    bilibili_tracks: HashMap<String, PlaybackTrack>,
}

impl LocalMediaResolver {
    fn remember(&mut self, track: &PlaybackTrack) {
        if let Some(key) = local_media_key(track) {
            self.bilibili_tracks.insert(key, track.clone());
        }
    }

    fn invalidate(&mut self, track: &PlaybackTrack) {
        if let Some(key) = local_media_key(track) {
            self.bilibili_tracks.remove(&key);
        }
    }

    async fn playable_track(
        &mut self,
        client: &reqwest::Client,
        track: &PlaybackTrack,
    ) -> Result<PlaybackTrack> {
        let Some(key) = local_media_key(track) else {
            return Ok(track.clone());
        };

        if let Some(local) = self.bilibili_tracks.get(&key) {
            return Ok(merge_local_media(track, local));
        }

        let resolved = bilibili::resolve_track(client, &track.bvid, track.part.saturating_sub(1))
            .await
            .with_context(|| {
                format!(
                    "failed to resolve local bilibili media {} part {}",
                    track.bvid, track.part
                )
            })?;
        let playable = merge_local_media(track, &resolved);
        self.bilibili_tracks.insert(key, resolved);
        Ok(playable)
    }
}

fn local_media_key(track: &PlaybackTrack) -> Option<String> {
    (track.source_kind == "bilibili" && !track.bvid.is_empty() && track.part > 0)
        .then(|| format!("bilibili:{}:{}", track.bvid, track.part))
}

fn merge_local_media(shared: &PlaybackTrack, local: &PlaybackTrack) -> PlaybackTrack {
    let mut playable = shared.clone();
    playable.audio_url = local.audio_url.clone();
    playable.referer = local.referer.clone();
    playable.media_init_range = local.media_init_range;
    playable.media_index_range = local.media_index_range;
    playable
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RendezvousDiscoverMode {
    Incremental,
    Full,
}

#[derive(Debug, Default)]
struct ZeroPeerRecovery {
    active: bool,
    discover_deadlines: VecDeque<Instant>,
}

#[derive(Debug, Default)]
struct SyncAnnouncementState {
    pending: VecDeque<Instant>,
    last_immediate: Option<Instant>,
    last_music_snapshot: Option<Instant>,
}

impl ZeroPeerRecovery {
    fn start(&mut self, now: Instant) -> bool {
        if self.active {
            return false;
        }

        self.active = true;
        self.discover_deadlines.clear();
        for delay in ZERO_PEER_RECOVERY_DISCOVER_DELAYS {
            self.discover_deadlines.push_back(now + delay);
        }
        true
    }

    fn finish(&mut self) -> bool {
        if !self.active {
            return false;
        }

        self.active = false;
        self.discover_deadlines.clear();
        true
    }

    fn pop_due_discovery(&mut self, now: Instant) -> bool {
        if !self.active {
            return false;
        }

        let mut due = false;
        while self
            .discover_deadlines
            .front()
            .is_some_and(|deadline| *deadline <= now)
        {
            self.discover_deadlines.pop_front();
            due = true;
        }

        if due && self.active && self.discover_deadlines.is_empty() {
            self.discover_deadlines
                .push_back(now + RENDEZVOUS_DISCOVER_INTERVAL);
        }

        due
    }
}

pub async fn run_network(
    config: BackendConfig,
    mut commands: mpsc::Receiver<NetworkCommand>,
    ui: mpsc::Sender<UiEvent>,
) -> Result<()> {
    let topic = gossipsub::IdentTopic::new(config.topic.clone());
    let mut seen_messages = HashSet::new();
    let mut history = Vec::new();
    let mut message_seq = 0_u64;
    let mut peer_names = HashMap::new();
    let mut history_request_times = HashMap::new();
    let mut queue_request_times = HashMap::new();
    let mut sync_announcements = SyncAnnouncementState::default();
    let mut rendezvous_nodes = HashSet::new();
    let mut rendezvous_cookies = HashMap::new();
    let relay_addrs = prioritize_multiaddrs(config.relay.clone());
    let mut zero_peer_recovery = ZeroPeerRecovery::default();
    let rendezvous_namespace = rendezvous::Namespace::new(config.topic.clone())
        .map_err(|err| anyhow!("invalid rendezvous namespace '{}': {err}", config.topic))?;
    let mut music = MusicState::new();
    let http_client = bilibili::client()?;
    let mut local_media = LocalMediaResolver::default();
    let mut failed_audio_sessions = HashSet::new();
    let mut buffer_coordinator = BufferCoordinator::new();
    let mut playback_health = PlaybackHealth::new();
    let mut audio_player = match player::AudioPlayer::new() {
        Ok(player) => Some(player),
        Err(err) => {
            send_status(&ui, format!("audio output unavailable: {err:#}")).await;
            None
        }
    };
    let mut next_audio_output_retry = Instant::now() + AUDIO_OUTPUT_RETRY_INTERVAL;
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
    let mut zero_peer_recovery_tick = time::interval(ZERO_PEER_RECOVERY_TICK);
    zero_peer_recovery_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);

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
                let identify = identify::Behaviour::new(identify::Config::new(
                    "/light-ear/0.1.0".to_string(),
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
                    identify,
                    ping: ping::Behaviour::default(),
                    relay,
                    dcutr: dcutr::Behaviour::new(peer_id),
                    rendezvous: rendezvous::client::Behaviour::new(key.clone()),
                    mdns,
                })
            },
        )?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(SWARM_IDLE_CONNECTION_TIMEOUT))
        .build();

    let local_peer_id = *swarm.local_peer_id();
    let mut connections = ConnectionState::new(local_peer_id);
    let local_joined_at = current_timestamp_micros();
    let _ = ui
        .send(UiEvent::LocalPeerId(local_peer_id.to_string()))
        .await;
    send_queue_view(&ui, local_peer_id, &music).await;
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

    for relay_addr in relay_addrs.iter().cloned() {
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
                    let causal_frontier = history_frontier(&history);
                    message_seq += 1;
                    let id = new_message_id(local_peer_id, sent_at, message_seq, &text);
                    let record = ChatRecord {
                        id: id.clone(),
                        peer_id: local_peer_id.to_string(),
                        joined_at: Some(local_joined_at),
                        seq: message_seq,
                        causal_frontier: causal_frontier.clone(),
                        author: config.name.clone(),
                        text,
                        sent_at,
                    };
                    insert_record(&mut history, &mut seen_messages, record.clone());
                    send_history_snapshot(&ui, &history).await;

                    let msg = WireMessage::Chat {
                        id: Some(id),
                        peer_id: local_peer_id.to_string(),
                        joined_at: Some(local_joined_at),
                        seq: message_seq,
                        causal_frontier,
                        name: record.author,
                        text: record.text,
                        sent_at,
                    };
                    let targets = PublishTargets {
                        rendezvous_nodes: &rendezvous_nodes,
                    };
                    match publish_chat_wire(&mut swarm, &topic, &targets, &msg) {
                        Ok(RoomPublishOutcome::Published) => {}
                        Ok(RoomPublishOutcome::NoSubscribers) => {
                            send_status(&ui, "gossipsub publish failed: no peers subscribed to the room topic".to_string()).await;
                        }
                        Ok(RoomPublishOutcome::Congested) => {
                            send_status(&ui, "gossipsub publish delayed: peer queues are full".to_string()).await;
                        }
                        Err(err) => {
                            send_status(&ui, format!("publish failed: {err}")).await;
                        }
                    }
                }
                Some(NetworkCommand::EnqueueBilibili {
                    bvid,
                    part,
                    position: _,
                }) => {
                    send_status(&ui, format!("resolving bilibili {bvid} part {part}")).await;
                    match bilibili::resolve_track(&http_client, &bvid, part.saturating_sub(1)).await {
                        Ok(track) => {
                            local_media.remember(&track);
                            let item = QueueItem {
                                item_id: new_queue_item_id(local_peer_id, &track.track_id),
                                track,
                                requested_by: local_peer_id.to_string(),
                                added_at_micros: current_timestamp_micros(),
                            };
                            let title = item.track.title.clone();
                            let index = music.append_queue_item(item);
                            let targets = PublishTargets {
                                rendezvous_nodes: &rendezvous_nodes,
                            };
                            publish_queue_state(
                                &mut swarm,
                                &topic,
                                &targets,
                                &mut music,
                                local_peer_id,
                            )?;
                            send_queue_view(&ui, local_peer_id, &music).await;
                            send_status(&ui, format!("queued #{}, {title}", index + 1)).await;
                            start_next_if_idle(
                                &mut music,
                                &mut audio_player,
                                &http_client,
                                &mut local_media,
                                &mut failed_audio_sessions,
                                &mut buffer_coordinator,
                                &mut swarm,
                                &topic,
                                &targets,
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
                    send_queue_view(&ui, local_peer_id, &music).await;
                    send_queue_status(&ui, music.playback_state(), &music.queue).await;
                }
                Some(NetworkCommand::Pause) => {
                    if !music.has_track() {
                        send_status(&ui, "no active playback".to_string()).await;
                    } else {
                        let targets = PublishTargets {
                            rendezvous_nodes: &rendezvous_nodes,
                        };
                        let room_peer_total =
                            room_peer_count(&swarm, &rendezvous_nodes, local_peer_id);
                        propose_or_execute_vote(
                            VoteAction::Pause,
                            &mut music,
                            &mut audio_player,
                            &http_client,
                            &mut local_media,
                            &mut failed_audio_sessions,
                            &mut buffer_coordinator,
                            &mut swarm,
                            &topic,
                            &targets,
                            local_peer_id,
                            room_peer_total,
                            &mut queue_request_times,
                            &ui,
                        )
                        .await?;
                    }
                }
                Some(NetworkCommand::Resume) => {
                    if !music.has_track() {
                        send_status(&ui, "no active playback".to_string()).await;
                    } else {
                        let targets = PublishTargets {
                            rendezvous_nodes: &rendezvous_nodes,
                        };
                        let room_peer_total =
                            room_peer_count(&swarm, &rendezvous_nodes, local_peer_id);
                        propose_or_execute_vote(
                            VoteAction::Resume,
                            &mut music,
                            &mut audio_player,
                            &http_client,
                            &mut local_media,
                            &mut failed_audio_sessions,
                            &mut buffer_coordinator,
                            &mut swarm,
                            &topic,
                            &targets,
                            local_peer_id,
                            room_peer_total,
                            &mut queue_request_times,
                            &ui,
                        )
                        .await?;
                    }
                }
                Some(NetworkCommand::Seek(position_ms)) => {
                    if !music.has_track() {
                        send_status(&ui, "no active playback".to_string()).await;
                    } else {
                        let now = current_timestamp_micros();
                        let targets = PublishTargets {
                            rendezvous_nodes: &rendezvous_nodes,
                        };
                        let room_peer_total =
                            room_peer_count(&swarm, &rendezvous_nodes, local_peer_id);
                        if music
                            .playback_state()
                            .is_some_and(|state| can_control_playback(state, local_peer_id))
                        {
                            let Some(current) = music.playback_state().cloned() else {
                                continue;
                            };
                            let Some(item) = queue_item_from_playback_state(&current) else {
                                continue;
                            };
                            let position_ms = position_ms.min(item.track.duration_ms);
                            if let Some(state) = music.pause_playback_for_vote(local_peer_id, now) {
                                if let Some(player) = &mut audio_player {
                                    player.set_playing(false, now)?;
                                }
                                send_playback_view(&ui, &state).await;
                                publish_playback_state(&mut swarm, &topic, &targets, &state)?;
                            }
                            begin_buffer_operation(
                                &mut buffer_coordinator,
                                &mut music,
                                item,
                                PlaybackBufferOperationKind::Seek,
                                position_ms,
                                &mut audio_player,
                                &http_client,
                                &mut local_media,
                                &mut failed_audio_sessions,
                                &mut swarm,
                                &topic,
                                &targets,
                                local_peer_id,
                                &ui,
                            )
                            .await?;
                        } else {
                            propose_or_execute_vote(
                                VoteAction::Seek { position_ms },
                                &mut music,
                                &mut audio_player,
                                &http_client,
                                &mut local_media,
                                &mut failed_audio_sessions,
                                &mut buffer_coordinator,
                                &mut swarm,
                                &topic,
                                &targets,
                                local_peer_id,
                                room_peer_total,
                                &mut queue_request_times,
                                &ui,
                            )
                            .await?;
                        }
                    }
                }
                Some(NetworkCommand::SetVolume(percent)) => {
                    if let Some(player) = audio_player.as_mut() {
                        player.set_volume(percent, current_timestamp_micros())?;
                    }
                    send_status(&ui, format!("local volume set to {percent}%")).await;
                }
                Some(NetworkCommand::Skip) => {
                    if !music.has_track() {
                        send_status(&ui, "no active playback".to_string()).await;
                    } else {
                        let targets = PublishTargets {
                            rendezvous_nodes: &rendezvous_nodes,
                        };
                        let room_peer_total =
                            room_peer_count(&swarm, &rendezvous_nodes, local_peer_id);
                        propose_or_execute_vote(
                            VoteAction::Skip,
                            &mut music,
                            &mut audio_player,
                            &http_client,
                            &mut local_media,
                            &mut failed_audio_sessions,
                            &mut buffer_coordinator,
                            &mut swarm,
                            &topic,
                            &targets,
                            local_peer_id,
                            room_peer_total,
                            &mut queue_request_times,
                            &ui,
                        )
                        .await?;
                    }
                }
                Some(NetworkCommand::RemoveQueueItem(index)) => {
                    match queue_item_at(&music.queue, index) {
                        Some(item) if item.requested_by == local_peer_id.to_string() =>
                        {
                            let title = item.track.title.clone();
                            music.remove_queue_index(index);
                            let targets = PublishTargets {
                                rendezvous_nodes: &rendezvous_nodes,
                            };
                            publish_queue_state(
                                &mut swarm,
                                &topic,
                                &targets,
                                &mut music,
                                local_peer_id,
                            )?;
                            send_queue_view(&ui, local_peer_id, &music).await;
                            send_status(&ui, format!("removed #{index}: {title}")).await;
                        }
                        Some(item) => {
                            let targets = PublishTargets {
                                rendezvous_nodes: &rendezvous_nodes,
                            };
                            let room_peer_total =
                                room_peer_count(&swarm, &rendezvous_nodes, local_peer_id);
                            propose_or_execute_vote(
                                VoteAction::Remove {
                                    item_id: item.item_id.clone(),
                                },
                                &mut music,
                                &mut audio_player,
                                &http_client,
                                &mut local_media,
                                &mut failed_audio_sessions,
                                &mut buffer_coordinator,
                                &mut swarm,
                                &topic,
                                &targets,
                                local_peer_id,
                                room_peer_total,
                                &mut queue_request_times,
                                &ui,
                            )
                            .await?;
                        }
                        None => send_status(&ui, format!("queue item #{index} does not exist")).await,
                    }
                }
                Some(NetworkCommand::MoveQueueItem { from, to }) => {
                    match queue_item_at(&music.queue, from) {
                        Some(item) => {
                            let targets = PublishTargets {
                                rendezvous_nodes: &rendezvous_nodes,
                            };
                            let room_peer_total =
                                room_peer_count(&swarm, &rendezvous_nodes, local_peer_id);
                            propose_or_execute_vote(
                                VoteAction::Move {
                                    item_id: item.item_id.clone(),
                                    to_index: to.saturating_sub(1),
                                },
                                &mut music,
                                &mut audio_player,
                                &http_client,
                                &mut local_media,
                                &mut failed_audio_sessions,
                                &mut buffer_coordinator,
                                &mut swarm,
                                &topic,
                                &targets,
                                local_peer_id,
                                room_peer_total,
                                &mut queue_request_times,
                                &ui,
                            )
                            .await?;
                        }
                        None => send_status(&ui, format!("queue item #{from} does not exist")).await,
                    }
                }
                Some(NetworkCommand::Vote(approve)) => {
                    let targets = PublishTargets {
                        rendezvous_nodes: &rendezvous_nodes,
                    };
                    let room_peer_total = room_peer_count(&swarm, &rendezvous_nodes, local_peer_id);
                    cast_vote(
                        approve,
                        &mut music,
                        &mut audio_player,
                        &http_client,
                        &mut local_media,
                        &mut failed_audio_sessions,
                        &mut buffer_coordinator,
                        &mut swarm,
                        &topic,
                        &targets,
                        local_peer_id,
                        room_peer_total,
                        &mut queue_request_times,
                        &ui,
                    )
                    .await?;
                }
                Some(NetworkCommand::Shutdown) => {
                    unregister_from_rendezvous_nodes(
                        &mut swarm,
                        &rendezvous_nodes,
                        &rendezvous_namespace,
                        &ui,
                    )
                    .await;
                    break;
                }
                None => {
                    unregister_from_rendezvous_nodes(
                        &mut swarm,
                        &rendezvous_nodes,
                        &rendezvous_namespace,
                        &ui,
                    )
                    .await;
                    break;
                }
            },
            _ = music_local.tick() => {
                if audio_player.is_none()
                    && (music.has_track() || buffer_coordinator.is_active())
                    && Instant::now() >= next_audio_output_retry
                {
                    next_audio_output_retry = Instant::now() + AUDIO_OUTPUT_RETRY_INTERVAL;
                    if try_restore_audio_output(&mut audio_player, &ui, "scheduled retry").await {
                        if let Some(state) = music.playback_state().cloned().filter(|state| state.track.is_some()) {
                            if let Err(err) = apply_remote_playback_state(
                                &http_client,
                                &mut local_media,
                                &mut audio_player,
                                &mut music,
                                &state,
                                local_peer_id,
                                &ui,
                            ).await {
                                if handle_local_audio_output_error(
                                    &mut audio_player,
                                    &ui,
                                    "playback sync after audio output recovery",
                                    &err,
                                )
                                .await
                                {
                                    continue;
                                }
                                send_status(&ui, format!("playback sync failed after audio output recovery: {err:#}")).await;
                            }
                        }
                    }
                }

                let targets = PublishTargets {
                    rendezvous_nodes: &rendezvous_nodes,
                };
                let room_peer_total = room_peer_count(&swarm, &rendezvous_nodes, local_peer_id);
                let player_events = audio_player
                    .as_mut()
                    .map_or_else(Vec::new, player::AudioPlayer::drain_events);
                if let Err(err) = handle_audio_player_events(
                    player_events,
                    &mut music,
                    &mut audio_player,
                    &http_client,
                    &mut local_media,
                    &mut failed_audio_sessions,
                    &mut buffer_coordinator,
                    &mut playback_health,
                    &mut swarm,
                    &topic,
                    &targets,
                    local_peer_id,
                    &ui,
                )
                .await
                {
                    send_status(&ui, format!("audio stream event failed: {err:#}")).await;
                }

                if let Some(vote) = music.take_timed_out_vote(Instant::now()) {
                    send_status(&ui, format!("vote {} timed out", vote.proposal.vote_id)).await;
                    send_vote_view(
                        &ui,
                        &music,
                        majority_threshold(room_peer_total),
                        room_peer_total,
                        local_peer_id,
                    )
                    .await;
                }

                if let Err(err) = resolve_active_vote(
                    &mut music,
                    &mut audio_player,
                    &http_client,
                    &mut local_media,
                    &mut failed_audio_sessions,
                    &mut buffer_coordinator,
                    &mut swarm,
                    &topic,
                    &targets,
                    local_peer_id,
                    room_peer_total,
                    &mut queue_request_times,
                    &ui,
                )
                .await
                {
                    send_status(&ui, format!("vote execution failed: {err:#}")).await;
                }

                if let Err(err) = maybe_start_pending_playback(
                    &mut music,
                    &mut swarm,
                    &topic,
                    &targets,
                    local_peer_id,
                    &ui,
                )
                .await
                {
                    send_status(&ui, format!("playback prepare failed: {err:#}")).await;
                }

                if let Err(err) = resolve_buffer_operation(
                    &mut buffer_coordinator,
                    &mut music,
                    &mut audio_player,
                    &http_client,
                    &mut local_media,
                    &mut failed_audio_sessions,
                    &mut swarm,
                    &topic,
                    &targets,
                    local_peer_id,
                    &ui,
                )
                .await
                {
                    send_status(&ui, format!("buffer operation failed: {err:#}")).await;
                }

                if let Err(err) = start_next_if_idle(
                    &mut music,
                    &mut audio_player,
                    &http_client,
                    &mut local_media,
                    &mut failed_audio_sessions,
                    &mut buffer_coordinator,
                    &mut swarm,
                    &topic,
                    &targets,
                    local_peer_id,
                    &ui,
                )
                .await
                {
                    send_status(&ui, format!("buffer prepare failed: {err:#}")).await;
                }

                if let Err(err) = maybe_pause_for_buffer_health_loss(
                    &mut music,
                    &mut audio_player,
                    &http_client,
                    &mut local_media,
                    &mut failed_audio_sessions,
                    &mut buffer_coordinator,
                    &mut playback_health,
                    &mut swarm,
                    &topic,
                    &targets,
                    &rendezvous_nodes,
                    local_peer_id,
                    &ui,
                )
                .await
                {
                    send_status(&ui, format!("buffer health recovery failed: {err:#}")).await;
                }

                let mut finished_current = None;
                if let Some(state) = music.playback_state().cloned() {
                    let now = current_timestamp_micros();
                    if !should_sync_player_to_playback_state(&buffer_coordinator, &state) {
                        continue;
                    }
                    if let Err(err) = sync_loaded_player_to_state(&mut audio_player, &state, now) {
                        if handle_local_audio_output_error(
                            &mut audio_player,
                            &ui,
                            "playback sync",
                            &err,
                        )
                        .await
                        {
                            continue;
                        }
                        failed_audio_sessions.insert(state.session_id.clone());
                        if state.leader_peer_id == local_peer_id.to_string() {
                            send_status(&ui, format!("local playback failed: {err:#}")).await;
                            stop_current_playback(
                                &mut music,
                                &mut audio_player,
                                &mut swarm,
                                &topic,
                                &targets,
                                local_peer_id,
                                "local audio playback failed",
                                &ui,
                            )
                            .await?;
                            start_next_if_idle(
                                &mut music,
                                &mut audio_player,
                                &http_client,
                                &mut local_media,
                                &mut failed_audio_sessions,
                                &mut buffer_coordinator,
                                &mut swarm,
                                &topic,
                                &targets,
                                local_peer_id,
                                &ui,
                            )
                            .await?;
                            continue;
                        }

                        mark_local_audio_session_failed(
                            &mut audio_player,
                            &mut music,
                            &mut failed_audio_sessions,
                            &state,
                            format!("playback sync failed: {err:#}"),
                            &ui,
                        )
                        .await;
                        continue;
                    }

                    let local_audio_finished = audio_player
                        .as_ref()
                        .is_some_and(|player| player.is_finished(now));
                    finished_current =
                        finished_playback_role(&state, local_peer_id, now, local_audio_finished);

                    if finished_current.is_none() {
                        send_playback_view(&ui, &state).await;
                    }
                }

                if let Some(finished_current) = finished_current {
                    match finished_current {
                        FinishedPlaybackRole::Leader => {
                            stop_current_playback(
                                &mut music,
                                &mut audio_player,
                                &mut swarm,
                                &topic,
                                &targets,
                                local_peer_id,
                                "track finished",
                                &ui,
                            )
                            .await?;
                            start_next_if_idle(
                                &mut music,
                                &mut audio_player,
                                &http_client,
                                &mut local_media,
                                &mut failed_audio_sessions,
                                &mut buffer_coordinator,
                                &mut swarm,
                                &topic,
                                &targets,
                                local_peer_id,
                                &ui,
                            )
                            .await?;
                        }
                        FinishedPlaybackRole::Follower => {
                            if let Some(player) = audio_player.as_mut() {
                                player.stop();
                            }
                            let state =
                                music.stop_current_playback(local_peer_id, current_timestamp_micros());
                            send_playback_view(&ui, &state).await;
                            send_status(
                                &ui,
                                "track finished locally; waiting for leader".to_string(),
                            )
                            .await;
                        }
                    }
                }
            },
            _ = history_sync.tick() => {
                let targets = PublishTargets {
                    rendezvous_nodes: &rendezvous_nodes,
                };
                if let Err(err) = publish_sync_summary(
                    &mut swarm,
                    &topic,
                    &targets,
                    local_peer_id,
                    &config.name,
                    local_joined_at,
                    &history,
                    music.queue_version,
                    music.queue_updated_at,
                    &music.queue,
                ) {
                    send_status(&ui, format!("sync summary failed: {err}")).await;
                }
            },
            _ = history_sync_burst.tick() => {
                let targets = PublishTargets {
                    rendezvous_nodes: &rendezvous_nodes,
                };
                if let Err(err) = publish_pending_sync_summaries(
                    &mut sync_announcements,
                    &mut swarm,
                    &topic,
                    &targets,
                    local_peer_id,
                    &config.name,
                    local_joined_at,
                    &history,
                    music.queue_version,
                    music.queue_updated_at,
                    &music.queue,
                ) {
                    send_status(&ui, format!("sync summary failed: {err}")).await;
                }
            },
            _ = music_sync.tick() => {
                if !music.has_pending_playback() {
                    let mut state_to_publish = None;
                    if let Some(state) = music.playback_state_mut() {
                        if state.leader_peer_id == local_peer_id.to_string() && state.track.is_some() {
                            let now = current_timestamp_micros();
                            if !state.playing || now >= state.anchor_time_micros {
                                state.position_ms = playback_position_ms(state, now);
                                state.anchor_time_micros = now;
                            }
                            state.issued_at_micros = now;
                            state_to_publish = Some(state.clone());
                        }
                    }
                    if let Some(state) = state_to_publish {
                        let targets = PublishTargets {
                            rendezvous_nodes: &rendezvous_nodes,
                        };
                        publish_playback_state(&mut swarm, &topic, &targets, &state)?;
                        send_playback_view(&ui, &state).await;
                    }
                }
            },
            _ = direct_promotion.tick() => {
                retry_direct_promotions(
                    &mut swarm,
                    &mut connections,
                    &ui,
                    &rendezvous_nodes,
                )
                .await;
            },
            _ = gossip_warmup.tick() => {
                retry_gossip_warmup_promotions(
                    &mut swarm,
                    &mut connections,
                    &ui,
                    &rendezvous_nodes,
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
                    RendezvousDiscoverMode::Incremental,
                    &ui,
                )
                .await;
            },
            _ = zero_peer_recovery_tick.tick() => {
                if zero_peer_recovery.pop_due_discovery(Instant::now()) {
                    run_zero_peer_recovery(
                        &mut swarm,
                        &relay_addrs,
                        &rendezvous_nodes,
                        &rendezvous_namespace,
                        &rendezvous_cookies,
                        &mut sync_announcements,
                        &ui,
                    )
                    .await;
                }
            },
            event = swarm.select_next_some() => {
                let ctx = HistoryContext {
                    topic: &topic,
                    local_peer_id,
                    history: &mut history,
                    seen_messages: &mut seen_messages,
                    local_name: &config.name,
                    local_joined_at,
                    peer_names: &mut peer_names,
                    history_request_times: &mut history_request_times,
                    queue_request_times: &mut queue_request_times,
                    sync_announcements: &mut sync_announcements,
                    http_client: &http_client,
                    local_media: &mut local_media,
                    failed_audio_sessions: &mut failed_audio_sessions,
                    buffer_coordinator: &mut buffer_coordinator,
                    playback_health: &mut playback_health,
                    audio_player: &mut audio_player,
                    music: &mut music,
                };
                handle_swarm_event(
                    event,
                    &mut swarm,
                    &ui,
                    &mut connections,
                    &rendezvous_nodes,
                    &rendezvous_namespace,
                    &mut rendezvous_cookies,
                    &mut zero_peer_recovery,
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
    local_peer_id: PeerId,
    history: &'a mut Vec<ChatRecord>,
    seen_messages: &'a mut HashSet<String>,
    local_name: &'a str,
    local_joined_at: i64,
    peer_names: &'a mut HashMap<String, PeerNameClaim>,
    history_request_times: &'a mut HashMap<String, Instant>,
    queue_request_times: &'a mut HashMap<String, Instant>,
    sync_announcements: &'a mut SyncAnnouncementState,
    http_client: &'a reqwest::Client,
    local_media: &'a mut LocalMediaResolver,
    failed_audio_sessions: &'a mut HashSet<String>,
    buffer_coordinator: &'a mut BufferCoordinator,
    playback_health: &'a mut PlaybackHealth,
    audio_player: &'a mut Option<player::AudioPlayer>,
    music: &'a mut MusicState,
}

async fn apply_connection_effects(
    swarm: &mut libp2p::Swarm<Behaviour>,
    connections: &mut ConnectionState,
    ui: &mpsc::Sender<UiEvent>,
    rendezvous_nodes: &HashSet<PeerId>,
    effects: Vec<ConnectionEffect>,
) {
    let should_send_peer_views = !effects.is_empty();
    let mut pending = VecDeque::from(effects);
    while let Some(effect) = pending.pop_front() {
        match effect {
            ConnectionEffect::Status(status) => send_status(ui, status).await,
            ConnectionEffect::TrackGossipPeer(peer_id) => {
                swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
            }
            ConnectionEffect::UntrackGossipPeer(peer_id) => {
                swarm
                    .behaviour_mut()
                    .gossipsub
                    .remove_explicit_peer(&peer_id);
            }
            ConnectionEffect::ResetBackoff(_) => {}
            ConnectionEffect::DialDirect { peer_id, addresses } => {
                let dial_opts = DialOpts::peer_id(peer_id)
                    .addresses(addresses)
                    .condition(PeerCondition::Always)
                    .build();
                if let Err(err) = swarm.dial(dial_opts) {
                    pending.extend(connections.record_direct_promotion_failure(
                        peer_id,
                        format!("dial request failed: {err}"),
                        Instant::now(),
                    ));
                }
            }
            ConnectionEffect::CloseRelayConnections {
                peer_id,
                connection_ids,
                reason,
            } => {
                let closed_relays = connection_ids
                    .into_iter()
                    .filter(|connection_id| swarm.close_connection(*connection_id))
                    .count();
                if closed_relays > 0 {
                    let status = match reason {
                        RelayCloseReason::HandoffSettled => {
                            format!(
                                "direct connection settled with {peer_id}; closing {closed_relays} relay link(s)"
                            )
                        }
                    };
                    send_status(ui, status).await;
                }
            }
            ConnectionEffect::CloseEarlyDirectConnection {
                peer_id,
                connection_id,
            } => {
                if swarm.close_connection(connection_id) {
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
        }
    }
    if should_send_peer_views {
        send_peer_views(ui, connections, rendezvous_nodes).await;
    }
}

async fn handle_swarm_event(
    event: SwarmEvent<BehaviourEvent>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    ui: &mpsc::Sender<UiEvent>,
    connections: &mut ConnectionState,
    rendezvous_nodes: &HashSet<PeerId>,
    rendezvous_namespace: &rendezvous::Namespace,
    rendezvous_cookies: &mut HashMap<PeerId, rendezvous::Cookie>,
    zero_peer_recovery: &mut ZeroPeerRecovery,
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
            let effects = connections.connection_established(
                peer_id,
                connection_id,
                is_relayed,
                rendezvous_nodes.contains(&peer_id),
                Instant::now(),
            );
            apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;

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

            let targets = PublishTargets { rendezvous_nodes };
            let count = connected_room_peer_count(swarm, rendezvous_nodes);
            let _ = ui.send(UiEvent::PeerCount(count)).await;
            if count > 0 && !rendezvous_nodes.contains(&peer_id) && zero_peer_recovery.finish() {
                schedule_sync_burst(ctx.sync_announcements);
                send_status(
                    ui,
                    format!("room peer {peer_id} reconnected; refreshing sync"),
                )
                .await;
            }
            if let Err(err) = trigger_sync(swarm, &targets, &mut ctx) {
                send_status(ui, format!("sync summary failed: {err}")).await;
            }
            if let Err(err) = publish_music_snapshot_if_due(
                ctx.sync_announcements,
                swarm,
                ctx.topic,
                &targets,
                ctx.local_peer_id,
                ctx.music,
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
            let effects =
                connections.connection_closed(peer_id, connection_id, was_relayed, num_established);
            apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;

            if num_established == 0 {
                if forget_peer_name(peer_id, &mut *ctx.peer_names) {
                    send_peer_names(ui, &*ctx.peer_names).await;
                }
                let count = connected_room_peer_count(swarm, rendezvous_nodes);
                let _ = ui.send(UiEvent::PeerCount(count)).await;
                if count == 0
                    && !rendezvous_nodes.contains(&peer_id)
                    && zero_peer_recovery.start(Instant::now())
                {
                    send_status(
                        ui,
                        "all room peers disconnected; starting rendezvous recovery".to_string(),
                    )
                    .await;
                }
                let peer_id = peer_id.to_string();
                ctx.music.remove_pending_peer(&peer_id);
                let targets = PublishTargets { rendezvous_nodes };
                if ctx
                    .buffer_coordinator
                    .remove_expected_peer(&peer_id, Instant::now())
                    .is_some()
                {
                    if let Err(err) = resolve_buffer_operation(
                        ctx.buffer_coordinator,
                        ctx.music,
                        &mut *ctx.audio_player,
                        ctx.http_client,
                        ctx.local_media,
                        ctx.failed_audio_sessions,
                        swarm,
                        ctx.topic,
                        &targets,
                        ctx.local_peer_id,
                        ui,
                    )
                    .await
                    {
                        send_status(ui, format!("buffer operation failed: {err:#}")).await;
                    }
                    send_buffer_view(ui, ctx.buffer_coordinator, ctx.local_peer_id).await;
                }
                if let Err(err) = maybe_start_pending_playback(
                    ctx.music,
                    swarm,
                    ctx.topic,
                    &targets,
                    ctx.local_peer_id,
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
            for (peer_id, address) in list {
                discovered = true;
                let mut effects = connections.track_gossip_peer(peer_id, None);
                effects.extend(connections.learn_direct_addresses(
                    peer_id,
                    [address],
                    Instant::now(),
                ));
                apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;
                send_status(ui, format!("mDNS discovered {peer_id}")).await;
            }
            if discovered {
                let targets = PublishTargets { rendezvous_nodes };
                if let Err(err) = trigger_sync(swarm, &targets, &mut ctx) {
                    send_status(ui, format!("sync summary failed: {err}")).await;
                }
                if let Err(err) = publish_music_snapshot_if_due(
                    ctx.sync_announcements,
                    swarm,
                    ctx.topic,
                    &targets,
                    ctx.local_peer_id,
                    ctx.music,
                ) {
                    send_status(ui, format!("music snapshot failed: {err}")).await;
                }
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Expired(list))) => {
            for (peer_id, address) in list {
                if connections.forget_direct_address(peer_id, address) {
                    send_peer_views(ui, connections, rendezvous_nodes).await;
                }
                if forget_peer_name(peer_id, &mut *ctx.peer_names) {
                    send_peer_names(ui, &*ctx.peer_names).await;
                }
                if !is_peer_connected(swarm, peer_id) {
                    let effects = connections.untrack_gossip_peer(peer_id);
                    apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects)
                        .await;
                }
                send_status(ui, format!("mDNS expired {peer_id}")).await;
            }
        }
        SwarmEvent::NewExternalAddrOfPeer { peer_id, address } => {
            let effects = connections.learn_direct_addresses(peer_id, [address], Instant::now());
            apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::Identify(
            identify::Event::Received { peer_id, info, .. }
            | identify::Event::Pushed { peer_id, info, .. },
        )) => {
            let effects =
                connections.learn_direct_addresses(peer_id, info.listen_addrs, Instant::now());
            apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;
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
                connections,
                ctx.local_peer_id,
                registrations,
                ui,
                rendezvous_nodes,
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
                let effects = connections.chat_subscribed(peer_id, Instant::now());
                apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;
                let targets = PublishTargets { rendezvous_nodes };
                if let Err(err) = trigger_sync(swarm, &targets, &mut ctx) {
                    send_status(ui, format!("sync summary failed: {err}")).await;
                }
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Unsubscribed {
            peer_id,
            topic,
        })) => {
            if topic == ctx.topic.hash() {
                let effects = connections.chat_unsubscribed(peer_id);
                apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(
            gossipsub::Event::GossipsubNotSupported { peer_id },
        )) => {
            send_status(
                ui,
                format!("peer {peer_id} cannot receive room messages: gossipsub is not supported"),
            )
            .await;
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
            let targets = PublishTargets { rendezvous_nodes };
            if ctx
                .buffer_coordinator
                .remove_expected_peer(&peer_id.to_string(), Instant::now())
                .is_some()
            {
                send_status(
                    ui,
                    format!("removed slow peer {peer_id} from current buffer quorum"),
                )
                .await;
                if let Err(err) = resolve_buffer_operation(
                    ctx.buffer_coordinator,
                    ctx.music,
                    &mut *ctx.audio_player,
                    ctx.http_client,
                    ctx.local_media,
                    ctx.failed_audio_sessions,
                    swarm,
                    ctx.topic,
                    &targets,
                    ctx.local_peer_id,
                    ui,
                )
                .await
                {
                    send_status(ui, format!("buffer operation failed: {err:#}")).await;
                }
                send_buffer_view(ui, ctx.buffer_coordinator, ctx.local_peer_id).await;
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
            propagation_source,
            message,
            ..
        })) => match serde_json::from_slice::<WireMessage>(&message.data) {
            Ok(wire) => {
                let source_peer_id = message.source.unwrap_or(propagation_source);
                apply_wire_message(source_peer_id, wire, swarm, rendezvous_nodes, ui, &mut ctx)
                    .await;
            }
            Err(err) => send_status(ui, format!("ignored invalid message: {err}")).await,
        },
        SwarmEvent::Behaviour(BehaviourEvent::Relay(event)) => {
            send_status(ui, format!("relay event: {event:?}")).await;
        }
        SwarmEvent::Behaviour(BehaviourEvent::Dcutr(event)) => match event.result {
            Ok(connection_id) => {
                let mut effects = connections.dcutr_succeeded(event.remote_peer_id);
                effects.push(ConnectionEffect::Status(format!(
                    "direct upgrade succeeded with {} on {connection_id:?}",
                    event.remote_peer_id
                )));
                apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;
            }
            Err(err) => {
                let effects =
                    connections.dcutr_failed(event.remote_peer_id, err.to_string(), Instant::now());
                apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;
            }
        },
        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
            if let Some(peer_id) = peer_id {
                let effects = connections.outgoing_connection_error(
                    peer_id,
                    format!("outgoing direct dial failed: {error}"),
                    Instant::now(),
                );
                apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;
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
    connections: &mut ConnectionState,
    ui: &mpsc::Sender<UiEvent>,
    rendezvous_nodes: &HashSet<PeerId>,
) {
    let effects = connections.promotion_tick(Instant::now());
    apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;
}

async fn retry_gossip_warmup_promotions(
    swarm: &mut libp2p::Swarm<Behaviour>,
    connections: &mut ConnectionState,
    ui: &mpsc::Sender<UiEvent>,
    rendezvous_nodes: &HashSet<PeerId>,
) {
    let effects = connections.warmup_tick(Instant::now());
    apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;
}

async fn run_zero_peer_recovery(
    swarm: &mut libp2p::Swarm<Behaviour>,
    relay_addrs: &[Multiaddr],
    rendezvous_nodes: &HashSet<PeerId>,
    namespace: &rendezvous::Namespace,
    rendezvous_cookies: &HashMap<PeerId, rendezvous::Cookie>,
    sync_announcements: &mut SyncAnnouncementState,
    ui: &mpsc::Sender<UiEvent>,
) {
    ensure_rendezvous_connections(swarm, relay_addrs, rendezvous_nodes, ui).await;
    register_with_rendezvous_nodes(swarm, rendezvous_nodes, namespace, ui).await;
    discover_rendezvous_peers(
        swarm,
        rendezvous_nodes,
        namespace,
        rendezvous_cookies,
        RendezvousDiscoverMode::Full,
        ui,
    )
    .await;
    schedule_sync_burst(sync_announcements);
}

async fn ensure_rendezvous_connections(
    swarm: &mut libp2p::Swarm<Behaviour>,
    relay_addrs: &[Multiaddr],
    rendezvous_nodes: &HashSet<PeerId>,
    ui: &mpsc::Sender<UiEvent>,
) {
    for relay_addr in relay_addrs {
        let Some(peer_id) = peer_id_from_multiaddr(relay_addr) else {
            continue;
        };
        if !rendezvous_nodes.contains(&peer_id) || is_peer_connected(swarm, peer_id) {
            continue;
        }

        let dial_opts = DialOpts::peer_id(peer_id)
            .addresses(vec![relay_addr.clone()])
            .condition(PeerCondition::Disconnected)
            .build();
        match swarm.dial(dial_opts) {
            Ok(()) => {
                send_status(ui, format!("reconnecting rendezvous {peer_id}")).await;
            }
            Err(err) => {
                send_status(
                    ui,
                    format!("rendezvous reconnect dial failed {peer_id}: {err}"),
                )
                .await;
            }
        }
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

async fn unregister_from_rendezvous_nodes(
    swarm: &mut libp2p::Swarm<Behaviour>,
    rendezvous_nodes: &HashSet<PeerId>,
    namespace: &rendezvous::Namespace,
    ui: &mpsc::Sender<UiEvent>,
) {
    let connected_nodes = rendezvous_nodes
        .iter()
        .copied()
        .filter(|peer_id| is_peer_connected(swarm, *peer_id))
        .collect::<Vec<_>>();
    if connected_nodes.is_empty() {
        return;
    }

    for rendezvous_node in connected_nodes {
        swarm
            .behaviour_mut()
            .rendezvous
            .unregister(namespace.clone(), rendezvous_node);
        send_status(
            ui,
            format!("unregistering from rendezvous {rendezvous_node} in {namespace}"),
        )
        .await;
    }

    let grace = time::sleep(RENDEZVOUS_UNREGISTER_GRACE);
    tokio::pin!(grace);
    loop {
        tokio::select! {
            _ = &mut grace => break,
            event = swarm.select_next_some() => {
                if let SwarmEvent::Behaviour(BehaviourEvent::Rendezvous(event)) = event {
                    send_status(ui, format!("rendezvous shutdown event: {event:?}")).await;
                }
            }
        }
    }
}

async fn discover_rendezvous_peers(
    swarm: &mut libp2p::Swarm<Behaviour>,
    rendezvous_nodes: &HashSet<PeerId>,
    namespace: &rendezvous::Namespace,
    rendezvous_cookies: &HashMap<PeerId, rendezvous::Cookie>,
    mode: RendezvousDiscoverMode,
    ui: &mpsc::Sender<UiEvent>,
) {
    for rendezvous_node in rendezvous_nodes {
        if is_peer_connected(swarm, *rendezvous_node) {
            discover_rendezvous_node(
                swarm,
                *rendezvous_node,
                namespace,
                rendezvous_cookie_for_mode(mode, rendezvous_cookies, rendezvous_node),
                ui,
            )
            .await;
        }
    }
}

fn rendezvous_cookie_for_mode<'a>(
    mode: RendezvousDiscoverMode,
    cookies: &'a HashMap<PeerId, rendezvous::Cookie>,
    rendezvous_node: &PeerId,
) -> Option<&'a rendezvous::Cookie> {
    match mode {
        RendezvousDiscoverMode::Incremental => cookies.get(rendezvous_node),
        RendezvousDiscoverMode::Full => None,
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
    connections: &mut ConnectionState,
    local_peer_id: PeerId,
    registrations: Vec<rendezvous::Registration>,
    ui: &mpsc::Sender<UiEvent>,
    rendezvous_nodes: &HashSet<PeerId>,
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

        if is_peer_connected(swarm, peer_id) {
            let mut effects = connections.track_gossip_peer(
                peer_id,
                Some(format!("tracking {peer_id} as rendezvous gossip peer")),
            );
            effects.extend(connections.learn_direct_addresses(peer_id, addresses, Instant::now()));
            apply_connection_effects(swarm, connections, ui, rendezvous_nodes, effects).await;
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

fn is_peer_connected(swarm: &libp2p::Swarm<Behaviour>, peer_id: PeerId) -> bool {
    swarm
        .connected_peers()
        .any(|connected| *connected == peer_id)
}

fn room_peer_ids(
    swarm: &libp2p::Swarm<Behaviour>,
    rendezvous_nodes: &HashSet<PeerId>,
    local_peer_id: PeerId,
) -> HashSet<PeerId> {
    room_peer_ids_from_connected(
        local_peer_id,
        swarm.connected_peers().copied(),
        rendezvous_nodes,
    )
}

fn room_peer_count(
    swarm: &libp2p::Swarm<Behaviour>,
    rendezvous_nodes: &HashSet<PeerId>,
    local_peer_id: PeerId,
) -> usize {
    room_peer_ids(swarm, rendezvous_nodes, local_peer_id).len()
}

fn connected_room_peer_count(
    swarm: &libp2p::Swarm<Behaviour>,
    rendezvous_nodes: &HashSet<PeerId>,
) -> usize {
    swarm
        .connected_peers()
        .filter(|peer_id| !rendezvous_nodes.contains(peer_id))
        .count()
}

fn room_peer_ids_from_connected<I>(
    local_peer_id: PeerId,
    connected_peers: I,
    rendezvous_nodes: &HashSet<PeerId>,
) -> HashSet<PeerId>
where
    I: IntoIterator<Item = PeerId>,
{
    let mut peers = connected_peers
        .into_iter()
        .filter(|peer_id| !rendezvous_nodes.contains(peer_id))
        .collect::<HashSet<_>>();
    peers.insert(local_peer_id);
    peers
}

fn has_external_addresses(swarm: &libp2p::Swarm<Behaviour>) -> bool {
    swarm.external_addresses().next().is_some()
}

fn publish_history_summary(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
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
    publish_room_wire(swarm, topic, targets, &summary)
}

fn publish_name_claim(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
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
    publish_room_wire(swarm, topic, targets, &claim)
}

fn publish_presence_and_history(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    local_name: &str,
    local_joined_at: i64,
    history: &[ChatRecord],
) -> Result<()> {
    publish_name_claim(
        swarm,
        topic,
        targets,
        local_peer_id,
        local_name,
        local_joined_at,
    )?;
    publish_history_summary(swarm, topic, targets, local_peer_id, history)
}

fn publish_queue_summary(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
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
    publish_room_wire(swarm, topic, targets, &summary)
}

fn publish_sync_summary(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
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
        targets,
        local_peer_id,
        local_name,
        local_joined_at,
        history,
    )?;
    publish_queue_summary(
        swarm,
        topic,
        targets,
        local_peer_id,
        queue_version,
        queue_updated_at,
        queue,
    )
}

fn trigger_sync(
    swarm: &mut libp2p::Swarm<Behaviour>,
    targets: &PublishTargets<'_>,
    ctx: &mut HistoryContext<'_>,
) -> Result<()> {
    let now = Instant::now();
    if should_publish_immediate_sync(ctx.sync_announcements, now) {
        publish_sync_summary(
            swarm,
            ctx.topic,
            targets,
            ctx.local_peer_id,
            ctx.local_name,
            ctx.local_joined_at,
            ctx.history,
            ctx.music.queue_version,
            ctx.music.queue_updated_at,
            &ctx.music.queue,
        )?;
        ctx.sync_announcements.last_immediate = Some(now);
    }
    schedule_sync_burst(ctx.sync_announcements);
    Ok(())
}

fn should_publish_immediate_sync(state: &SyncAnnouncementState, now: Instant) -> bool {
    state
        .last_immediate
        .is_none_or(|last| now.saturating_duration_since(last) >= SYNC_IMMEDIATE_MIN_INTERVAL)
}

fn schedule_sync_burst(state: &mut SyncAnnouncementState) {
    if !state.pending.is_empty() {
        return;
    }
    let now = Instant::now();
    for delay in [
        Duration::from_millis(300),
        Duration::from_millis(900),
        Duration::from_millis(1800),
    ] {
        state.pending.push_back(now + delay);
    }
}

fn publish_pending_sync_summaries(
    state: &mut SyncAnnouncementState,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    local_name: &str,
    local_joined_at: i64,
    history: &[ChatRecord],
    queue_version: u64,
    queue_updated_at: i64,
    queue: &VecDeque<QueueItem>,
) -> Result<()> {
    let now = Instant::now();
    while state
        .pending
        .front()
        .is_some_and(|deadline| *deadline <= now)
    {
        state.pending.pop_front();
        publish_sync_summary(
            swarm,
            topic,
            targets,
            local_peer_id,
            local_name,
            local_joined_at,
            history,
            queue_version,
            queue_updated_at,
            queue,
        )?;
        state.last_immediate = Some(now);
    }
    Ok(())
}

async fn apply_wire_message(
    source_peer_id: PeerId,
    message: WireMessage,
    swarm: &mut libp2p::Swarm<Behaviour>,
    rendezvous_nodes: &HashSet<PeerId>,
    ui: &mpsc::Sender<UiEvent>,
    ctx: &mut HistoryContext<'_>,
) -> bool {
    if let Err(reason) = validate_wire_source(&message, source_peer_id) {
        send_status(
            ui,
            format!("ignored message from {source_peer_id}: {reason}"),
        )
        .await;
        return false;
    }

    let targets = PublishTargets { rendezvous_nodes };

    match message {
        WireMessage::Chat {
            id,
            peer_id,
            joined_at,
            seq,
            causal_frontier,
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
                seq,
                causal_frontier,
                name,
                text,
                sent_at,
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
                if remember_peer_name(
                    peer_id,
                    &name,
                    ctx.local_peer_id,
                    &mut *ctx.peer_names,
                    joined_at,
                )
                .await
                {
                    send_peer_names(ui, &*ctx.peer_names).await;
                }
                true
            } else {
                false
            }
        }
        WireMessage::HistorySummary {
            peer_id,
            count,
            newest_at,
            ..
        } => {
            let local_peer_id = ctx.local_peer_id.to_string();
            if peer_id != local_peer_id
                && history_summary_is_newer(ctx.history, count, newest_at)
                && should_request_history(ctx.history_request_times, &peer_id)
            {
                let request = WireMessage::HistoryRequest {
                    requester: local_peer_id,
                    target: peer_id.clone(),
                    known_count: ctx.history.len(),
                    nonce: new_nonce(ctx.local_peer_id),
                };

                match publish_room_wire_with_outcome(swarm, ctx.topic, &request) {
                    Ok(RoomPublishOutcome::Published) => {
                        ctx.history_request_times
                            .insert(peer_id.clone(), Instant::now());
                        send_status(ui, format!("requesting history from {peer_id}")).await;
                    }
                    Ok(RoomPublishOutcome::NoSubscribers) => {
                        send_status(
                            ui,
                            format!(
                                "history request for {peer_id} not sent: no gossipsub subscribers"
                            ),
                        )
                        .await;
                    }
                    Ok(RoomPublishOutcome::Congested) => {
                        send_status(
                            ui,
                            format!(
                                "history request for {peer_id} delayed: gossipsub queues are full"
                            ),
                        )
                        .await;
                    }
                    Err(err) => {
                        send_status(ui, format!("history request failed: {err}")).await;
                    }
                }
            }
            true
        }
        WireMessage::HistoryRequest {
            requester,
            target,
            known_count,
            ..
        } => {
            let local_peer_id = ctx.local_peer_id.to_string();
            if target == local_peer_id
                && requester != local_peer_id
                && ctx.history.len() > known_count
            {
                let response = WireMessage::HistoryResponse {
                    target: Some(requester.clone()),
                    messages: ctx.history.clone(),
                    nonce: new_nonce(ctx.local_peer_id),
                };

                match publish_room_wire_with_outcome(swarm, ctx.topic, &response) {
                    Ok(RoomPublishOutcome::Published) => {
                        send_status(ui, format!("sent {} history messages", ctx.history.len()))
                            .await;
                    }
                    Ok(RoomPublishOutcome::NoSubscribers) => {
                        send_status(
                            ui,
                            format!(
                                "history response to {requester} not sent: no gossipsub subscribers"
                            ),
                        )
                        .await;
                    }
                    Ok(RoomPublishOutcome::Congested) => {
                        send_status(
                            ui,
                            format!(
                                "history response to {requester} delayed: gossipsub queues are full"
                            ),
                        )
                        .await;
                    }
                    Err(err) => {
                        send_status(ui, format!("history response failed: {err}")).await;
                    }
                }
            }
            true
        }
        WireMessage::HistoryResponse {
            target, messages, ..
        } => {
            let local_peer_id = ctx.local_peer_id.to_string();
            let is_for_me = match target.as_deref() {
                Some(target) => target == local_peer_id,
                None => true,
            };

            if is_for_me {
                let mut added = 0;
                let mut names_changed = false;
                for record in messages {
                    if let Some(peer_id) = parse_peer_id(&record.peer_id) {
                        names_changed |= remember_peer_name(
                            peer_id,
                            &record.author,
                            ctx.local_peer_id,
                            &mut *ctx.peer_names,
                            record.joined_at,
                        )
                        .await;
                    }

                    if insert_record(ctx.history, ctx.seen_messages, record) {
                        added += 1;
                    }
                }

                if names_changed {
                    send_peer_names(ui, &*ctx.peer_names).await;
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
            true
        }
        WireMessage::QueueSummary {
            peer_id,
            version,
            updated_at_micros,
            ..
        } => {
            let local_peer_id = ctx.local_peer_id.to_string();
            if peer_id != local_peer_id
                && is_queue_state_newer(
                    version,
                    updated_at_micros,
                    ctx.music.queue_version,
                    ctx.music.queue_updated_at,
                )
                && should_request_queue(ctx.queue_request_times, &peer_id)
            {
                let request = WireMessage::QueueRequest {
                    requester: local_peer_id,
                    target: peer_id.clone(),
                    known_version: ctx.music.queue_version,
                    known_updated_at_micros: ctx.music.queue_updated_at,
                    nonce: new_nonce(ctx.local_peer_id),
                };

                match publish_room_wire_with_outcome(swarm, ctx.topic, &request) {
                    Ok(RoomPublishOutcome::Published) => {
                        ctx.queue_request_times
                            .insert(peer_id.clone(), Instant::now());
                        send_status(ui, format!("requesting queue from {peer_id}")).await;
                    }
                    Ok(RoomPublishOutcome::NoSubscribers) => {
                        send_status(
                            ui,
                            format!(
                                "queue request for {peer_id} not sent: no gossipsub subscribers"
                            ),
                        )
                        .await;
                    }
                    Ok(RoomPublishOutcome::Congested) => {
                        send_status(
                            ui,
                            format!(
                                "queue request for {peer_id} delayed: gossipsub queues are full"
                            ),
                        )
                        .await;
                    }
                    Err(err) => {
                        send_status(ui, format!("queue request failed: {err}")).await;
                    }
                }
            }
            true
        }
        WireMessage::QueueRequest {
            requester,
            target,
            known_version,
            known_updated_at_micros,
            ..
        } => {
            let local_peer_id = ctx.local_peer_id.to_string();
            if target == local_peer_id
                && requester != local_peer_id
                && is_queue_state_newer(
                    ctx.music.queue_version,
                    ctx.music.queue_updated_at,
                    known_version,
                    known_updated_at_micros,
                )
            {
                let response = WireMessage::QueueResponse {
                    target: requester.clone(),
                    state: build_queue_state(ctx.local_peer_id, ctx.music),
                    nonce: new_nonce(ctx.local_peer_id),
                };

                match publish_room_wire_with_outcome(swarm, ctx.topic, &response) {
                    Ok(RoomPublishOutcome::Published) => {
                        send_status(ui, format!("sent {} queue item(s)", ctx.music.queue.len()))
                            .await;
                    }
                    Ok(RoomPublishOutcome::NoSubscribers) => {
                        send_status(
                            ui,
                            format!(
                                "queue response to {requester} not sent: no gossipsub subscribers"
                            ),
                        )
                        .await;
                    }
                    Ok(RoomPublishOutcome::Congested) => {
                        send_status(
                            ui,
                            format!(
                                "queue response to {requester} delayed: gossipsub queues are full"
                            ),
                        )
                        .await;
                    }
                    Err(err) => {
                        send_status(ui, format!("queue response failed: {err}")).await;
                    }
                }
            }
            true
        }
        WireMessage::QueueResponse { target, state, .. } => {
            if target == ctx.local_peer_id.to_string() {
                if apply_remote_queue_state(ui, ctx, state, "synced queue").await {
                    if let Err(err) = resolve_active_vote_after_queue_apply(
                        ctx,
                        swarm,
                        rendezvous_nodes,
                        &targets,
                        ui,
                    )
                    .await
                    {
                        send_status(ui, format!("vote execution failed: {err:#}")).await;
                    }
                }
            }
            true
        }
        WireMessage::PlaybackState { state, .. } => {
            let state = normalize_remote_playback_state(&state, current_timestamp_micros());
            if state.track.is_none() {
                ctx.failed_audio_sessions.clear();
            }
            if playback_state_uses_failed_audio_session(ctx.failed_audio_sessions, &state) {
                return true;
            }

            if state.leader_peer_id != ctx.local_peer_id.to_string()
                && should_apply_playback_state(ctx.music.playback_state(), &state)
            {
                abandon_local_buffer_operation(
                    ctx.buffer_coordinator,
                    swarm,
                    ctx.topic,
                    &targets,
                    ctx.local_peer_id,
                    "superseded by remote playback",
                    ui,
                )
                .await;
                cancel_local_pending_playback(
                    ctx.music,
                    swarm,
                    ctx.topic,
                    &targets,
                    ctx.local_peer_id,
                    "superseded by remote playback",
                );
                match apply_remote_playback_state(
                    ctx.http_client,
                    ctx.local_media,
                    &mut *ctx.audio_player,
                    ctx.music,
                    &state,
                    ctx.local_peer_id,
                    ui,
                )
                .await
                {
                    Ok(()) => {
                        if ctx
                            .buffer_coordinator
                            .clear_matching_playback_state(&state)
                            .is_some()
                        {
                            let _ = ui.send(UiEvent::PlaybackBuffer(None)).await;
                        }
                    }
                    Err(err) => {
                        if handle_local_audio_output_error(
                            &mut *ctx.audio_player,
                            ui,
                            "playback sync",
                            &err,
                        )
                        .await
                        {
                            return true;
                        }
                        send_status(ui, format!("playback sync failed: {err:#}")).await;
                        mark_local_audio_session_failed(
                            &mut *ctx.audio_player,
                            ctx.music,
                            ctx.failed_audio_sessions,
                            &state,
                            format!("playback sync failed: {err:#}"),
                            ui,
                        )
                        .await;
                    }
                }
            }
            true
        }
        WireMessage::PlaybackPrepare {
            state,
            expected_peers,
            ..
        } => {
            let state = normalize_remote_playback_state(&state, current_timestamp_micros());
            let is_expected = expected_peers.is_empty()
                || expected_peers.contains(&ctx.local_peer_id.to_string());
            if playback_state_uses_failed_audio_session(ctx.failed_audio_sessions, &state) {
                return true;
            }

            if state.leader_peer_id != ctx.local_peer_id.to_string()
                && is_expected
                && should_apply_playback_state(ctx.music.playback_state(), &state)
            {
                abandon_local_buffer_operation(
                    ctx.buffer_coordinator,
                    swarm,
                    ctx.topic,
                    &targets,
                    ctx.local_peer_id,
                    "superseded by remote playback prepare",
                    ui,
                )
                .await;
                cancel_local_pending_playback(
                    ctx.music,
                    swarm,
                    ctx.topic,
                    &targets,
                    ctx.local_peer_id,
                    "superseded by remote playback prepare",
                );
                match apply_playback_prepare(
                    ctx.http_client,
                    ctx.local_media,
                    &mut *ctx.audio_player,
                    ctx.music,
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
                                &targets,
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
            true
        }
        WireMessage::PlaybackReady {
            session_id,
            peer_id,
            ..
        } => {
            match ctx
                .music
                .mark_playback_ready(&session_id, &peer_id, ctx.local_peer_id)
            {
                PlaybackReadyOutcome::Marked { ready, expected } => {
                    send_status(ui, format!("peer {peer_id} ready ({ready}/{expected})")).await;
                }
                PlaybackReadyOutcome::Ignored => {}
            }

            if let Err(err) = maybe_start_pending_playback(
                ctx.music,
                swarm,
                ctx.topic,
                &targets,
                ctx.local_peer_id,
                ui,
            )
            .await
            {
                send_status(ui, format!("playback start failed: {err:#}")).await;
            }
            true
        }
        WireMessage::PlaybackCancel {
            session_id,
            leader_peer_id,
            reason,
            ..
        } => {
            if leader_peer_id != ctx.local_peer_id.to_string() {
                apply_playback_cancel(&mut *ctx.audio_player, ctx.music, &session_id, &reason, ui)
                    .await;
            }
            true
        }
        WireMessage::PlaybackBufferPrepare {
            operation_id,
            session_id,
            track_id,
            position_ms,
            kind,
            expected_peers,
            leader_peer_id,
            ..
        } => {
            if leader_peer_id == ctx.local_peer_id.to_string() {
                return true;
            }
            let expected = expected_peers.into_iter().collect::<HashSet<_>>();
            if !expected.is_empty() && !expected.contains(&ctx.local_peer_id.to_string()) {
                return true;
            }

            if ctx.failed_audio_sessions.contains(&session_id) {
                publish_playback_buffer_status(
                    swarm,
                    ctx.topic,
                    &targets,
                    &operation_id,
                    &session_id,
                    ctx.local_peer_id,
                    PlaybackBufferStatusKind::Failed,
                    None,
                    Some("local audio already failed for this session".to_string()),
                )
                .ok();
                let _ = ui.send(UiEvent::PlaybackBuffer(None)).await;
                return true;
            }

            if matches!(kind, PlaybackBufferOperationKind::Start)
                && playback_state_resolves_buffer_prepare(
                    ctx.music.playback_state(),
                    &session_id,
                    &track_id,
                    &leader_peer_id,
                )
            {
                return true;
            }

            if let Some(local_status) = ctx.buffer_coordinator.status_for(
                &operation_id,
                &session_id,
                &ctx.local_peer_id.to_string(),
            ) {
                publish_playback_buffer_status(
                    swarm,
                    ctx.topic,
                    &targets,
                    &operation_id,
                    &session_id,
                    ctx.local_peer_id,
                    local_status.status,
                    local_status.buffered_until_ms,
                    local_status.error,
                )
                .ok();
                return true;
            }

            let Some(track) = find_buffer_track(ctx.music, &track_id).cloned() else {
                publish_playback_buffer_status(
                    swarm,
                    ctx.topic,
                    &targets,
                    &operation_id,
                    &session_id,
                    ctx.local_peer_id,
                    PlaybackBufferStatusKind::Failed,
                    None,
                    Some("track is not available locally".to_string()),
                )
                .ok();
                return true;
            };
            abandon_local_buffer_operation(
                ctx.buffer_coordinator,
                swarm,
                ctx.topic,
                &targets,
                ctx.local_peer_id,
                "superseded by remote buffer prepare",
                ui,
            )
            .await;
            let state = PlaybackState {
                session_id: session_id.clone(),
                leader_peer_id: leader_peer_id.clone(),
                track: Some(track.clone()),
                track_requested_by: Some(leader_peer_id.clone()),
                state_version: ctx.music.playback_version.saturating_add(1),
                issued_at_micros: current_timestamp_micros(),
                playing: false,
                position_ms,
                anchor_time_micros: current_timestamp_micros(),
                rate: 1.0,
            };
            ctx.buffer_coordinator.receive_remote_operation(
                operation_id.clone(),
                state.clone(),
                kind,
                expected,
                BUFFER_OPERATION_TIMEOUT,
                Instant::now(),
            );
            publish_playback_buffer_status(
                swarm,
                ctx.topic,
                &targets,
                &operation_id,
                &session_id,
                ctx.local_peer_id,
                PlaybackBufferStatusKind::Buffering,
                Some(position_ms),
                None,
            )
            .ok();
            let _ = ctx.buffer_coordinator.mark_status(
                &operation_id,
                &session_id,
                &ctx.local_peer_id.to_string(),
                PlaybackBufferStatusKind::Buffering,
                Some(position_ms),
                None,
                Instant::now(),
            );
            send_buffer_view(ui, ctx.buffer_coordinator, ctx.local_peer_id).await;
            let audio_available =
                ensure_audio_output(&mut *ctx.audio_player, ui, "buffer prepare").await;
            if let Some(player) = ctx.audio_player.as_mut() {
                let prepare_result = match ctx
                    .local_media
                    .playable_track(ctx.http_client, &track)
                    .await
                {
                    Ok(playable_track) => player.prepare_stream(
                        ctx.http_client,
                        Some(operation_id.clone()),
                        session_id.clone(),
                        playable_track,
                        position_ms,
                    ),
                    Err(err) => Err(err),
                };
                if let Err(err) = prepare_result {
                    let _ = ctx.buffer_coordinator.mark_status(
                        &operation_id,
                        &session_id,
                        &ctx.local_peer_id.to_string(),
                        PlaybackBufferStatusKind::Failed,
                        None,
                        Some(format!("stream prepare failed: {err:#}")),
                        Instant::now(),
                    );
                    publish_playback_buffer_status(
                        swarm,
                        ctx.topic,
                        &targets,
                        &operation_id,
                        &session_id,
                        ctx.local_peer_id,
                        PlaybackBufferStatusKind::Failed,
                        None,
                        Some(format!("stream prepare failed: {err:#}")),
                    )
                    .ok();
                    send_buffer_view(ui, ctx.buffer_coordinator, ctx.local_peer_id).await;
                }
            } else {
                let buffered_until = Some(
                    position_ms
                        .saturating_add(BUFFER_READY_WINDOW_MS)
                        .min(track.duration_ms),
                );
                let _ = ctx.buffer_coordinator.mark_status(
                    &operation_id,
                    &session_id,
                    &ctx.local_peer_id.to_string(),
                    PlaybackBufferStatusKind::Ready,
                    buffered_until,
                    None,
                    Instant::now(),
                );
                publish_playback_buffer_status(
                    swarm,
                    ctx.topic,
                    &targets,
                    &operation_id,
                    &session_id,
                    ctx.local_peer_id,
                    PlaybackBufferStatusKind::Ready,
                    buffered_until,
                    None,
                )
                .ok();
                if !audio_available {
                    send_status(
                        ui,
                        "audio output unavailable locally; reporting buffer ready so the room can continue"
                            .to_string(),
                    )
                    .await;
                }
                send_buffer_view(ui, ctx.buffer_coordinator, ctx.local_peer_id).await;
            }
            true
        }
        WireMessage::PlaybackBufferStatus {
            operation_id,
            session_id,
            peer_id,
            status,
            buffered_until_ms,
            error,
            ..
        } => {
            let peer_status = BufferPeerStatus {
                status: status.clone(),
                buffered_until_ms,
                error: error.clone(),
            };
            let duplicate_status = ctx.buffer_coordinator.status_matches(
                &operation_id,
                &session_id,
                &peer_id,
                &peer_status,
            );
            let status_outcome = ctx.buffer_coordinator.mark_status(
                &operation_id,
                &session_id,
                &peer_id,
                status,
                buffered_until_ms,
                error,
                Instant::now(),
            );
            let is_local_leader = ctx.buffer_coordinator.active().is_some_and(|operation| {
                operation.state.leader_peer_id == ctx.local_peer_id.to_string()
            });
            if is_local_leader {
                if let Err(err) = resolve_buffer_operation(
                    ctx.buffer_coordinator,
                    ctx.music,
                    &mut *ctx.audio_player,
                    ctx.http_client,
                    ctx.local_media,
                    ctx.failed_audio_sessions,
                    swarm,
                    ctx.topic,
                    &targets,
                    ctx.local_peer_id,
                    ui,
                )
                .await
                {
                    send_status(ui, format!("buffer operation failed: {err:#}")).await;
                }
            }
            if !duplicate_status && !matches!(status_outcome, BufferStatusOutcome::Ignored) {
                send_buffer_view(ui, ctx.buffer_coordinator, ctx.local_peer_id).await;
            }
            true
        }
        WireMessage::PlaybackBufferHealth {
            session_id,
            peer_id,
            status,
            buffered_until_ms,
            ..
        } => {
            ctx.playback_health.mark_status(
                &session_id,
                &peer_id,
                status,
                buffered_until_ms,
                Instant::now(),
            );
            true
        }
        WireMessage::PlaybackBufferCancel {
            operation_id,
            reason,
            ..
        } => {
            if ctx.buffer_coordinator.cancel(&operation_id).is_some() {
                if let Some(player) = ctx.audio_player.as_mut() {
                    player.stop();
                }
                let _ = ui.send(UiEvent::PlaybackBuffer(None)).await;
                send_status(ui, format!("buffer canceled: {reason}")).await;
            }
            true
        }
        WireMessage::QueueState { state, .. } => {
            if apply_remote_queue_state(ui, ctx, state, "queue updated").await {
                if let Err(err) = resolve_active_vote_after_queue_apply(
                    ctx,
                    swarm,
                    rendezvous_nodes,
                    &targets,
                    ui,
                )
                .await
                {
                    send_status(ui, format!("vote execution failed: {err:#}")).await;
                }
            }
            true
        }
        WireMessage::VoteProposal { proposal, .. } => {
            if proposal.proposer == ctx.local_peer_id.to_string() {
                return true;
            }
            match ctx
                .music
                .receive_vote_proposal(proposal.clone(), Instant::now() + VOTE_TIMEOUT)
            {
                Ok(()) => {
                    let known_version = ctx.music.queue_version;
                    let known_updated_at_micros = ctx.music.queue_updated_at;
                    let room_peer_total =
                        room_peer_count(swarm, rendezvous_nodes, ctx.local_peer_id);
                    send_vote_view(
                        ui,
                        ctx.music,
                        majority_threshold(room_peer_total),
                        room_peer_total,
                        ctx.local_peer_id,
                    )
                    .await;
                    send_status(
                        ui,
                        format!(
                            "vote requested by {}: {} (/vote yes|no)",
                            short_peer(&proposal.proposer),
                            describe_vote_action(&proposal.action, &ctx.music.queue)
                        ),
                    )
                    .await;
                    request_queue_for_vote_context(
                        &proposal,
                        known_version,
                        known_updated_at_micros,
                        ctx.queue_request_times,
                        swarm,
                        ctx.topic,
                        &targets,
                        ctx.local_peer_id,
                        ui,
                    )
                    .await;
                }
                Err("another vote is active") => {
                    send_status(
                        ui,
                        format!("ignored vote {}; another vote is active", proposal.vote_id),
                    )
                    .await;
                }
                Err(reason) => {
                    send_status(
                        ui,
                        format!("ignored stale vote {}: {reason}", proposal.vote_id),
                    )
                    .await;
                }
            }
            true
        }
        WireMessage::VoteBallot {
            vote_id,
            peer_id,
            approve,
            ..
        } => {
            let room_peer_total = room_peer_count(swarm, rendezvous_nodes, ctx.local_peer_id);
            let threshold = majority_threshold(room_peer_total);
            let mut changed_vote = false;
            if ctx.music.cast_vote_for(&vote_id, peer_id.clone(), approve) {
                changed_vote = true;
                if let Some(vote) = ctx.music.active_vote.as_ref() {
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
                send_vote_view(ui, ctx.music, threshold, room_peer_total, ctx.local_peer_id).await;
            }

            if let Err(err) = resolve_active_vote(
                ctx.music,
                ctx.audio_player,
                ctx.http_client,
                ctx.local_media,
                ctx.failed_audio_sessions,
                ctx.buffer_coordinator,
                swarm,
                ctx.topic,
                &targets,
                ctx.local_peer_id,
                room_peer_total,
                ctx.queue_request_times,
                ui,
            )
            .await
            {
                send_status(ui, format!("vote execution failed: {err:#}")).await;
            }
            true
        }
    }
}

async fn apply_chat_message(
    ctx: &mut HistoryContext<'_>,
    ui: &mpsc::Sender<UiEvent>,
    id: Option<String>,
    peer_id: String,
    joined_at: Option<i64>,
    seq: u64,
    causal_frontier: BTreeMap<String, u64>,
    name: String,
    text: String,
    sent_at: i64,
    source_peer_id: PeerId,
) -> bool {
    let claimed_peer_id = parse_peer_id(&peer_id).unwrap_or(source_peer_id);
    if remember_peer_name(
        claimed_peer_id,
        &name,
        ctx.local_peer_id,
        &mut *ctx.peer_names,
        joined_at,
    )
    .await
    {
        send_peer_names(ui, &*ctx.peer_names).await;
    }

    let id = id.unwrap_or_else(|| new_message_id(source_peer_id, sent_at, 0, &text));
    let record = ChatRecord {
        id,
        peer_id: claimed_peer_id.to_string(),
        joined_at,
        seq,
        causal_frontier,
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
    sort_history_causally(history);

    if history.len() > MAX_MESSAGES {
        let overflow = history.len() - MAX_MESSAGES;
        for removed in history.drain(0..overflow) {
            seen_messages.remove(&removed.id);
        }
    }

    true
}

fn sort_history_causally(history: &mut Vec<ChatRecord>) {
    let mut remaining = (0..history.len()).collect::<Vec<_>>();
    let mut ordered = Vec::with_capacity(history.len());

    while !remaining.is_empty() {
        let mut ready = remaining
            .iter()
            .copied()
            .filter(|candidate| {
                !remaining.iter().copied().any(|other| {
                    candidate != &other && chat_depends_on(&history[*candidate], &history[other])
                })
            })
            .collect::<Vec<_>>();

        if ready.is_empty() {
            let fallback = remaining
                .iter()
                .copied()
                .min_by(|left, right| chat_fallback_order(&history[*left], &history[*right]))
                .expect("remaining is not empty");
            ready.push(fallback);
        }

        ready.sort_by(|left, right| chat_fallback_order(&history[*left], &history[*right]));
        for index in ready {
            ordered.push(history[index].clone());
            remaining.retain(|remaining| *remaining != index);
        }
    }

    *history = ordered;
}

fn chat_depends_on(record: &ChatRecord, candidate_dependency: &ChatRecord) -> bool {
    if record.id == candidate_dependency.id {
        return false;
    }
    if candidate_dependency.seq == 0 || candidate_dependency.peer_id.is_empty() {
        return false;
    }
    let Some(candidate_key) = chat_record_actor_key(candidate_dependency) else {
        return false;
    };
    let record_key = chat_record_actor_key(record);
    if record_key.as_ref() == Some(&candidate_key)
        && record.seq > 0
        && candidate_dependency.seq < record.seq
    {
        return true;
    }

    record
        .causal_frontier
        .get(&candidate_key)
        .is_some_and(|latest_seen| candidate_dependency.seq <= *latest_seen)
}

fn chat_fallback_order(left: &ChatRecord, right: &ChatRecord) -> std::cmp::Ordering {
    normalize_timestamp_micros(left.sent_at)
        .cmp(&normalize_timestamp_micros(right.sent_at))
        .then_with(|| left.id.cmp(&right.id))
}

fn history_frontier(history: &[ChatRecord]) -> BTreeMap<String, u64> {
    let mut frontier = BTreeMap::new();
    for record in history {
        let Some(actor_key) = chat_record_actor_key(record) else {
            continue;
        };

        frontier
            .entry(actor_key)
            .and_modify(|seq: &mut u64| *seq = (*seq).max(record.seq))
            .or_insert(record.seq);
    }
    frontier
}

fn chat_record_actor_key(record: &ChatRecord) -> Option<String> {
    if record.seq == 0 || record.peer_id.is_empty() {
        return None;
    }
    Some(chat_actor_key(&record.peer_id, record.joined_at))
}

fn chat_actor_key(peer_id: &str, joined_at: Option<i64>) -> String {
    joined_at.map_or_else(
        || peer_id.to_string(),
        |joined_at| format!("{peer_id}@{joined_at}"),
    )
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
    let Some(outcome) = ctx.music.apply_remote_queue_state(state) else {
        return false;
    };

    let _ = ui.send(UiEvent::Queue(outcome.state.clone())).await;
    if let Some(invalidated) = outcome.invalidated_vote {
        let _ = ui.send(UiEvent::Vote(None)).await;
        send_status(ui, format!("vote discarded: {}", invalidated.reason)).await;
    }
    send_status(
        ui,
        format!(
            "{status_prefix} by {}",
            short_peer(&outcome.state.updated_by)
        ),
    )
    .await;
    send_queue_status(ui, ctx.music.playback_state(), &ctx.music.queue).await;
    true
}

async fn resolve_active_vote_after_queue_apply(
    ctx: &mut HistoryContext<'_>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    rendezvous_nodes: &HashSet<PeerId>,
    targets: &PublishTargets<'_>,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let room_peer_total = room_peer_count(swarm, rendezvous_nodes, ctx.local_peer_id);
    resolve_active_vote(
        ctx.music,
        ctx.audio_player,
        ctx.http_client,
        ctx.local_media,
        ctx.failed_audio_sessions,
        ctx.buffer_coordinator,
        swarm,
        ctx.topic,
        targets,
        ctx.local_peer_id,
        room_peer_total,
        ctx.queue_request_times,
        ui,
    )
    .await
}

async fn send_queue_view(ui: &mpsc::Sender<UiEvent>, local_peer_id: PeerId, music: &MusicState) {
    let _ = ui
        .send(UiEvent::Queue(music.queue_state(local_peer_id)))
        .await;
}

async fn send_vote_view(
    ui: &mpsc::Sender<UiEvent>,
    music: &MusicState,
    threshold: usize,
    eligible_peers: usize,
    local_peer_id: PeerId,
) {
    let _ = ui
        .send(UiEvent::Vote(music.vote_view(
            threshold,
            eligible_peers,
            &local_peer_id.to_string(),
        )))
        .await;
}

async fn remember_peer_name(
    peer_id: PeerId,
    name: &str,
    local_peer_id: PeerId,
    peer_names: &mut HashMap<String, PeerNameClaim>,
    joined_at: Option<i64>,
) -> bool {
    if peer_id == local_peer_id {
        return false;
    }

    let peer_id = peer_id.to_string();
    let next = PeerNameClaim {
        name: name.to_string(),
        joined_at,
    };
    let changed = peer_names
        .get(&peer_id)
        .is_none_or(|current| current.name != next.name || current.joined_at != next.joined_at);
    peer_names.insert(peer_id, next);
    changed
}

fn forget_peer_name(peer_id: PeerId, peer_names: &mut HashMap<String, PeerNameClaim>) -> bool {
    let peer_id = peer_id.to_string();
    peer_names.remove(&peer_id).is_some()
}

async fn send_peer_names(ui: &mpsc::Sender<UiEvent>, peer_names: &HashMap<String, PeerNameClaim>) {
    let mut names = peer_names
        .iter()
        .map(|(peer_id, claim)| PeerNameView {
            peer_id: peer_id.clone(),
            name: claim.name.clone(),
            joined_at: claim.joined_at,
        })
        .collect::<Vec<_>>();
    names.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.peer_id.cmp(&right.peer_id))
    });
    let _ = ui.send(UiEvent::PeerNames(names)).await;
}

fn parse_peer_id(value: &str) -> Option<PeerId> {
    if value.is_empty() {
        return None;
    }

    value.parse().ok()
}

fn peer_field_matches_source(value: &str, source_peer_id: PeerId) -> bool {
    parse_peer_id(value) == Some(source_peer_id)
}

fn queue_vote_needs_newer_state(proposal: &VoteProposal, local_queue_version: u64) -> bool {
    matches!(
        proposal.action,
        VoteAction::Remove { .. } | VoteAction::Move { .. }
    ) && proposal.queue_version > local_queue_version
}

async fn request_queue_for_vote_context(
    proposal: &VoteProposal,
    known_version: u64,
    known_updated_at_micros: i64,
    queue_request_times: &mut HashMap<String, Instant>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    _targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) {
    if !queue_vote_needs_newer_state(proposal, known_version)
        || !should_request_queue(queue_request_times, &proposal.proposer)
    {
        return;
    }

    let request = WireMessage::QueueRequest {
        requester: local_peer_id.to_string(),
        target: proposal.proposer.clone(),
        known_version,
        known_updated_at_micros,
        nonce: new_nonce(local_peer_id),
    };

    match publish_room_wire_with_outcome(swarm, topic, &request) {
        Ok(RoomPublishOutcome::Published) => {
            queue_request_times.insert(proposal.proposer.clone(), Instant::now());
            send_status(
                ui,
                format!(
                    "requesting queue from {} for vote context",
                    short_peer(&proposal.proposer)
                ),
            )
            .await;
        }
        Ok(RoomPublishOutcome::NoSubscribers) => {
            send_status(
                ui,
                format!(
                    "queue request for vote {} not sent: no gossipsub subscribers",
                    proposal.vote_id
                ),
            )
            .await;
        }
        Ok(RoomPublishOutcome::Congested) => {
            send_status(
                ui,
                format!(
                    "queue request for vote {} delayed: gossipsub queues are full",
                    proposal.vote_id
                ),
            )
            .await;
        }
        Err(err) => {
            send_status(ui, format!("queue request for vote context failed: {err}")).await;
        }
    }
}

fn validate_wire_source(message: &WireMessage, source_peer_id: PeerId) -> Result<(), &'static str> {
    match message {
        WireMessage::Chat { peer_id, .. }
            if !peer_id.is_empty() && !peer_field_matches_source(peer_id, source_peer_id) =>
        {
            Err("chat peer_id does not match source")
        }
        WireMessage::NameClaim { peer_id, .. }
            if !peer_field_matches_source(peer_id, source_peer_id) =>
        {
            Err("name claim peer_id does not match source")
        }
        WireMessage::HistorySummary { peer_id, .. }
            if !peer_field_matches_source(peer_id, source_peer_id) =>
        {
            Err("history summary peer_id does not match source")
        }
        WireMessage::HistoryRequest { requester, .. }
            if !peer_field_matches_source(requester, source_peer_id) =>
        {
            Err("history request requester does not match source")
        }
        WireMessage::QueueSummary { peer_id, .. }
            if !peer_field_matches_source(peer_id, source_peer_id) =>
        {
            Err("queue summary peer_id does not match source")
        }
        WireMessage::QueueRequest { requester, .. }
            if !peer_field_matches_source(requester, source_peer_id) =>
        {
            Err("queue request requester does not match source")
        }
        WireMessage::QueueState { state, .. }
            if !peer_field_matches_source(&state.updated_by, source_peer_id) =>
        {
            Err("queue state updated_by does not match source")
        }
        WireMessage::QueueResponse { state, .. }
            if !peer_field_matches_source(&state.updated_by, source_peer_id) =>
        {
            Err("queue response updated_by does not match source")
        }
        WireMessage::PlaybackState { state, .. } | WireMessage::PlaybackPrepare { state, .. }
            if !peer_field_matches_source(&state.leader_peer_id, source_peer_id) =>
        {
            Err("playback leader does not match source")
        }
        WireMessage::PlaybackReady { peer_id, .. }
            if !peer_field_matches_source(peer_id, source_peer_id) =>
        {
            Err("playback ready peer does not match source")
        }
        WireMessage::PlaybackCancel { leader_peer_id, .. }
            if !peer_field_matches_source(leader_peer_id, source_peer_id) =>
        {
            Err("playback cancel leader does not match source")
        }
        WireMessage::PlaybackBufferPrepare { leader_peer_id, .. }
        | WireMessage::PlaybackBufferCancel { leader_peer_id, .. }
            if !peer_field_matches_source(leader_peer_id, source_peer_id) =>
        {
            Err("playback buffer leader does not match source")
        }
        WireMessage::PlaybackBufferStatus { peer_id, .. }
            if !peer_field_matches_source(peer_id, source_peer_id) =>
        {
            Err("playback buffer status peer does not match source")
        }
        WireMessage::PlaybackBufferHealth { peer_id, .. }
            if !peer_field_matches_source(peer_id, source_peer_id) =>
        {
            Err("playback buffer health peer does not match source")
        }
        WireMessage::VoteProposal { proposal, .. }
            if !peer_field_matches_source(&proposal.proposer, source_peer_id) =>
        {
            Err("vote proposer does not match source")
        }
        WireMessage::VoteBallot { peer_id, .. }
            if !peer_field_matches_source(peer_id, source_peer_id) =>
        {
            Err("vote ballot peer does not match source")
        }
        _ => Ok(()),
    }
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

fn history_summary_is_newer(
    history: &[ChatRecord],
    remote_count: usize,
    remote_newest_at: Option<i64>,
) -> bool {
    if remote_count > history.len() {
        return true;
    }

    let Some(remote_newest_at) = remote_newest_at.map(normalize_timestamp_micros) else {
        return false;
    };
    let Some(local_newest_at) = history_newest_at(history) else {
        return remote_count > 0;
    };

    remote_newest_at > local_newest_at
}

fn history_newest_at(history: &[ChatRecord]) -> Option<i64> {
    history
        .iter()
        .map(|record| normalize_timestamp_micros(record.sent_at))
        .max()
}

fn publish_chat_wire(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    _targets: &PublishTargets<'_>,
    message: &WireMessage,
) -> Result<RoomPublishOutcome> {
    publish_room_wire_with_outcome(swarm, topic, message)
}

fn classify_room_publish_error(error: &gossipsub::PublishError) -> RoomPublishPlan {
    match error {
        gossipsub::PublishError::Duplicate => RoomPublishPlan::Published,
        gossipsub::PublishError::NoPeersSubscribedToTopic => RoomPublishPlan::NoSubscribers,
        gossipsub::PublishError::AllQueuesFull(_) => RoomPublishPlan::Congested,
        _ => RoomPublishPlan::Failed,
    }
}

fn publish_room_wire_with_outcome(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    message: &WireMessage,
) -> Result<RoomPublishOutcome> {
    let data = serde_json::to_vec(message)?;
    match swarm.behaviour_mut().gossipsub.publish(topic.clone(), data) {
        Ok(_) => Ok(RoomPublishOutcome::Published),
        Err(err) => match classify_room_publish_error(&err) {
            RoomPublishPlan::Published => Ok(RoomPublishOutcome::Published),
            RoomPublishPlan::NoSubscribers => Ok(RoomPublishOutcome::NoSubscribers),
            RoomPublishPlan::Congested => Ok(RoomPublishOutcome::Congested),
            RoomPublishPlan::Failed => Err(anyhow!(err)),
        },
    }
}

fn publish_room_wire(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    _targets: &PublishTargets<'_>,
    message: &WireMessage,
) -> Result<()> {
    publish_room_wire_with_outcome(swarm, topic, message).map(|_| ())
}

fn publish_queue_state(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    music: &mut MusicState,
    local_peer_id: PeerId,
) -> Result<()> {
    music.mark_queue_changed(current_timestamp_micros());
    publish_queue_snapshot(swarm, topic, targets, local_peer_id, music)
}

fn publish_queue_snapshot(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    music: &MusicState,
) -> Result<()> {
    if music.queue_version == 0 && music.queue.is_empty() {
        return Ok(());
    }

    let state = build_queue_state(local_peer_id, music);

    publish_room_wire(
        swarm,
        topic,
        targets,
        &WireMessage::QueueState {
            state,
            nonce: new_nonce(local_peer_id),
        },
    )
}

fn publish_music_snapshot(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    music: &MusicState,
) -> Result<()> {
    publish_queue_snapshot(swarm, topic, targets, local_peer_id, music)?;

    if let Some(state) = music.playback_state() {
        if state.leader_peer_id == local_peer_id.to_string() {
            publish_playback_state(swarm, topic, targets, state)?;
        }
    }

    Ok(())
}

fn publish_music_snapshot_if_due(
    sync: &mut SyncAnnouncementState,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    music: &MusicState,
) -> Result<()> {
    let now = Instant::now();
    if !should_publish_music_snapshot(sync, now) {
        return Ok(());
    }
    publish_music_snapshot(swarm, topic, targets, local_peer_id, music)?;
    sync.last_music_snapshot = Some(now);
    Ok(())
}

fn should_publish_music_snapshot(sync: &SyncAnnouncementState, now: Instant) -> bool {
    sync.last_music_snapshot
        .is_none_or(|last| now.saturating_duration_since(last) >= MUSIC_SNAPSHOT_MIN_INTERVAL)
}

fn build_queue_state(local_peer_id: PeerId, music: &MusicState) -> QueueState {
    music.queue_state(local_peer_id)
}

async fn ensure_audio_output(
    audio_player: &mut Option<player::AudioPlayer>,
    ui: &mpsc::Sender<UiEvent>,
    context: &str,
) -> bool {
    if audio_player.is_some() {
        return true;
    }

    try_restore_audio_output(audio_player, ui, context).await
}

async fn try_restore_audio_output(
    audio_player: &mut Option<player::AudioPlayer>,
    ui: &mpsc::Sender<UiEvent>,
    context: &str,
) -> bool {
    match player::AudioPlayer::new() {
        Ok(player) => {
            *audio_player = Some(player);
            send_status(ui, format!("audio output recovered during {context}")).await;
            true
        }
        Err(err) => {
            send_status(
                ui,
                format!("audio output unavailable during {context}: {err:#}"),
            )
            .await;
            false
        }
    }
}

async fn handle_local_audio_output_error(
    audio_player: &mut Option<player::AudioPlayer>,
    ui: &mpsc::Sender<UiEvent>,
    context: &str,
    error: &anyhow::Error,
) -> bool {
    if !is_audio_output_error(error) {
        return false;
    }

    *audio_player = None;
    send_status(
        ui,
        format!("audio output unavailable during {context}: {error:#}; will retry locally"),
    )
    .await;
    true
}

fn is_audio_output_error(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    [
        "audio output",
        "output device",
        "default output",
        "audio sink",
        "sink after reopening",
        "failed to configure default",
        "failed to open default",
    ]
    .iter()
    .any(|needle| message.contains(needle))
}

async fn handle_audio_player_events(
    events: Vec<player::AudioPlayerEvent>,
    music: &mut MusicState,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    failed_audio_sessions: &mut HashSet<String>,
    buffer: &mut BufferCoordinator,
    playback_health: &mut PlaybackHealth,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    for event in events {
        match event {
            player::AudioPlayerEvent::Prepared {
                operation_id,
                session_id,
                track_id,
                buffered_until_ms,
            } => {
                if let Some(operation_id) = operation_id {
                    if active_buffer_operation_matches(
                        buffer,
                        &operation_id,
                        &session_id,
                        &track_id,
                    ) {
                        let _ = publish_local_buffer_health(
                            playback_health,
                            swarm,
                            topic,
                            targets,
                            local_peer_id,
                            &session_id,
                            PlaybackBufferStatusKind::Ready,
                            Some(buffered_until_ms),
                        );
                        let _ = buffer.mark_status(
                            &operation_id,
                            &session_id,
                            &local_peer_id.to_string(),
                            PlaybackBufferStatusKind::Ready,
                            Some(buffered_until_ms),
                            None,
                            Instant::now(),
                        );
                        if let Some(status) = describe_buffer_publish_result(
                            "buffer ready status",
                            publish_playback_buffer_status(
                                swarm,
                                topic,
                                targets,
                                &operation_id,
                                &session_id,
                                local_peer_id,
                                PlaybackBufferStatusKind::Ready,
                                Some(buffered_until_ms),
                                None,
                            ),
                        ) {
                            send_status(ui, status).await;
                        }
                        resolve_buffer_operation(
                            buffer,
                            music,
                            audio_player,
                            client,
                            local_media,
                            failed_audio_sessions,
                            swarm,
                            topic,
                            targets,
                            local_peer_id,
                            ui,
                        )
                        .await?;
                        send_buffer_view(ui, buffer, local_peer_id).await;
                    }
                } else if stream_event_matches_active_operation(
                    buffer,
                    &session_id,
                    &track_id,
                    None,
                ) {
                    continue;
                } else if music.has_pending_playback() {
                    if !music_stream_event_matches(music, &session_id, &track_id) {
                        continue;
                    }
                    let outcome = music.mark_playback_ready(
                        &session_id,
                        &local_peer_id.to_string(),
                        local_peer_id,
                    );
                    if matches!(outcome, PlaybackReadyOutcome::Marked { .. }) {
                        let _ = publish_local_buffer_health(
                            playback_health,
                            swarm,
                            topic,
                            targets,
                            local_peer_id,
                            &session_id,
                            PlaybackBufferStatusKind::Ready,
                            Some(buffered_until_ms),
                        );
                        maybe_start_pending_playback(
                            music,
                            swarm,
                            topic,
                            targets,
                            local_peer_id,
                            ui,
                        )
                        .await?;
                        publish_playback_ready(swarm, topic, targets, &session_id, local_peer_id)?;
                    }
                } else if music.playback_state().is_some_and(|state| {
                    playback_state_matches_track(state, &session_id, &track_id)
                }) {
                    let _ = publish_local_buffer_health(
                        playback_health,
                        swarm,
                        topic,
                        targets,
                        local_peer_id,
                        &session_id,
                        PlaybackBufferStatusKind::Ready,
                        Some(buffered_until_ms),
                    );
                    if let Some(state) = music.playback_state().cloned() {
                        sync_loaded_player_to_state(
                            audio_player,
                            &state,
                            current_timestamp_micros(),
                        )?;
                    }
                }
            }
            player::AudioPlayerEvent::Cache { operation_id, view } => {
                if !stream_event_matches_current(
                    buffer,
                    music,
                    operation_id.as_deref(),
                    &view.session_id,
                    &view.track_id,
                ) {
                    continue;
                }
                let status = match view.status {
                    PlaybackCacheStatus::Ready => PlaybackBufferStatusKind::Ready,
                    PlaybackCacheStatus::Preparing | PlaybackCacheStatus::Buffering => {
                        PlaybackBufferStatusKind::Buffering
                    }
                    PlaybackCacheStatus::Failed => PlaybackBufferStatusKind::Failed,
                };
                let _ = publish_local_buffer_health(
                    playback_health,
                    swarm,
                    topic,
                    targets,
                    local_peer_id,
                    &view.session_id,
                    status,
                    Some(view.buffered_until_ms),
                );
                let _ = ui.send(UiEvent::PlaybackCache(Some(view))).await;
            }
            player::AudioPlayerEvent::Buffering {
                operation_id,
                session_id,
                track_id,
                buffered_from_ms,
                buffered_until_ms,
            } => {
                if !stream_event_matches_current(
                    buffer,
                    music,
                    operation_id.as_deref(),
                    &session_id,
                    &track_id,
                ) {
                    continue;
                }
                let duration_ms = track_duration_for_session(music, &session_id, &track_id)
                    .unwrap_or(buffered_until_ms);
                let view = PlaybackCacheView {
                    session_id: session_id.clone(),
                    track_id,
                    status: PlaybackCacheStatus::Buffering,
                    buffered_from_ms,
                    buffered_until_ms,
                    duration_ms,
                    error: None,
                };
                let _ = publish_local_buffer_health(
                    playback_health,
                    swarm,
                    topic,
                    targets,
                    local_peer_id,
                    &session_id,
                    PlaybackBufferStatusKind::Buffering,
                    Some(buffered_until_ms),
                );
                let _ = ui.send(UiEvent::PlaybackCache(Some(view))).await;
            }
            player::AudioPlayerEvent::DownloadStatus {
                operation_id,
                session_id,
                track_id,
                message,
            } => {
                if !stream_event_matches_current(
                    buffer,
                    music,
                    operation_id.as_deref(),
                    &session_id,
                    &track_id,
                ) {
                    continue;
                }
                send_status(ui, message).await;
            }
            player::AudioPlayerEvent::Failed {
                operation_id,
                session_id,
                track_id,
                title,
                error,
            } => {
                let matches_current = operation_id.as_deref().map_or_else(
                    || stream_event_matches_current(buffer, music, None, &session_id, &track_id),
                    |operation_id| {
                        active_buffer_operation_matches(
                            buffer,
                            operation_id,
                            &session_id,
                            &track_id,
                        )
                    },
                );
                if !matches_current {
                    continue;
                }
                if let Some(track) = buffer
                    .active()
                    .filter(|operation| {
                        operation.state.session_id == session_id
                            && operation
                                .state
                                .track
                                .as_ref()
                                .is_some_and(|track| track.track_id == track_id)
                    })
                    .and_then(|operation| operation.state.track.as_ref())
                    .or_else(|| {
                        music
                            .playback_state()
                            .filter(|state| {
                                playback_state_matches_track(state, &session_id, &track_id)
                            })
                            .and_then(|state| state.track.as_ref())
                    })
                {
                    local_media.invalidate(track);
                }
                let _ = publish_local_buffer_health(
                    playback_health,
                    swarm,
                    topic,
                    targets,
                    local_peer_id,
                    &session_id,
                    PlaybackBufferStatusKind::Failed,
                    None,
                );
                let view = PlaybackCacheView {
                    session_id: session_id.clone(),
                    track_id: track_id.clone(),
                    status: PlaybackCacheStatus::Failed,
                    buffered_from_ms: 0,
                    buffered_until_ms: 0,
                    duration_ms: track_duration_for_session(music, &session_id, &track_id)
                        .unwrap_or(0),
                    error: Some(error.clone()),
                };
                let _ = ui.send(UiEvent::PlaybackCache(Some(view))).await;
                send_status(ui, format!("audio stream failed for {title}: {error}")).await;

                if let Some(operation_id) = operation_id {
                    if let Some(state) = buffer
                        .active()
                        .filter(|operation| {
                            operation.operation_id == operation_id
                                && operation.state.session_id == session_id
                        })
                        .map(|operation| operation.state.clone())
                    {
                        handle_buffer_prepare_failure(
                            buffer,
                            audio_player,
                            music,
                            failed_audio_sessions,
                            swarm,
                            topic,
                            targets,
                            local_peer_id,
                            &state,
                            format!("audio stream failed: {error}"),
                            ui,
                        )
                        .await?;
                    }
                } else if let Some(state) = music
                    .playback_state()
                    .filter(|state| playback_state_matches_track(state, &session_id, &track_id))
                    .cloned()
                {
                    if state.leader_peer_id == local_peer_id.to_string() {
                        handle_local_leader_audio_failure(
                            music,
                            audio_player,
                            failed_audio_sessions,
                            swarm,
                            topic,
                            targets,
                            local_peer_id,
                            &state,
                            "local audio stream failed",
                            ui,
                        )
                        .await?;
                        start_next_if_idle(
                            music,
                            audio_player,
                            client,
                            local_media,
                            failed_audio_sessions,
                            buffer,
                            swarm,
                            topic,
                            targets,
                            local_peer_id,
                            ui,
                        )
                        .await?;
                    } else {
                        mark_local_audio_session_failed(
                            audio_player,
                            music,
                            failed_audio_sessions,
                            &state,
                            format!("audio stream failed: {error}"),
                            ui,
                        )
                        .await;
                    }
                }
            }
            player::AudioPlayerEvent::OutputDeviceError { error } => {
                if audio_player
                    .as_ref()
                    .and_then(|player| player.current_track_id())
                    .is_none()
                {
                    *audio_player = None;
                    send_status(
                        ui,
                        format!("audio output error while idle: {error}; will reopen on playback"),
                    )
                    .await;
                    continue;
                }
                send_status(
                    ui,
                    format!("audio output error: {error}; reopening default output"),
                )
                .await;
                match audio_player
                    .as_mut()
                    .map(|player| player.recover_output_device(current_timestamp_micros()))
                {
                    Some(Ok(true)) => {
                        send_status(
                            ui,
                            "audio output reopened and playback reattached".to_string(),
                        )
                        .await;
                    }
                    Some(Ok(false)) => {
                        send_status(ui, "audio output reopened".to_string()).await;
                    }
                    Some(Err(err)) => {
                        send_status(ui, format!("audio output recovery failed: {err:#}")).await;
                    }
                    None => {
                        send_status(ui, "audio output unavailable".to_string()).await;
                    }
                }
            }
            player::AudioPlayerEvent::OutputDeviceChanged { previous, current } => {
                if audio_player
                    .as_ref()
                    .and_then(|player| player.current_track_id())
                    .is_none()
                {
                    *audio_player = None;
                    send_status(
                        ui,
                        format!(
                            "audio output device changed from {} to {} while idle; will reopen on playback",
                            previous.as_deref().unwrap_or("none"),
                            current.as_deref().unwrap_or("none")
                        ),
                    )
                    .await;
                    continue;
                }
                send_status(
                    ui,
                    format!(
                        "audio output device changed from {} to {}; reopening output",
                        previous.as_deref().unwrap_or("none"),
                        current.as_deref().unwrap_or("none")
                    ),
                )
                .await;
                match audio_player
                    .as_mut()
                    .map(|player| player.recover_output_device(current_timestamp_micros()))
                {
                    Some(Ok(true)) => {
                        send_status(
                            ui,
                            "audio output reopened and playback reattached".to_string(),
                        )
                        .await;
                    }
                    Some(Ok(false)) => {
                        send_status(ui, "audio output reopened".to_string()).await;
                    }
                    Some(Err(err)) => {
                        send_status(ui, format!("audio output recovery failed: {err:#}")).await;
                    }
                    None => {
                        send_status(ui, "audio output unavailable".to_string()).await;
                    }
                }
            }
            player::AudioPlayerEvent::Ended {
                session_id,
                track_id,
            } => {
                if music.playback_state().is_some_and(|state| {
                    playback_state_matches_track(state, &session_id, &track_id)
                }) {
                    send_status(ui, format!("local stream ended for {track_id}")).await;
                }
            }
        }
    }

    Ok(())
}

fn stream_event_matches_current(
    buffer: &BufferCoordinator,
    music: &MusicState,
    operation_id: Option<&str>,
    session_id: &str,
    track_id: &str,
) -> bool {
    if let Some(operation) = buffer.active().filter(|operation| {
        operation.state.session_id == session_id
            && operation
                .state
                .track
                .as_ref()
                .is_some_and(|track| track.track_id == track_id)
    }) {
        return operation_id == Some(operation.operation_id.as_str());
    }

    music_stream_event_matches(music, session_id, track_id)
}

fn stream_event_matches_active_operation(
    buffer: &BufferCoordinator,
    session_id: &str,
    track_id: &str,
    operation_id: Option<&str>,
) -> bool {
    buffer.active().is_some_and(|operation| {
        operation.state.session_id == session_id
            && operation
                .state
                .track
                .as_ref()
                .is_some_and(|track| track.track_id == track_id)
            && operation_id
                .is_none_or(|operation_id| operation_id == operation.operation_id.as_str())
    })
}

fn active_buffer_operation_matches(
    buffer: &BufferCoordinator,
    operation_id: &str,
    session_id: &str,
    track_id: &str,
) -> bool {
    buffer.active().is_some_and(|operation| {
        operation.operation_id == operation_id
            && operation.state.session_id == session_id
            && operation
                .state
                .track
                .as_ref()
                .is_some_and(|track| track.track_id == track_id)
    })
}

fn should_sync_player_to_playback_state(buffer: &BufferCoordinator, state: &PlaybackState) -> bool {
    let Some(track) = state.track.as_ref() else {
        return true;
    };

    !buffer.active().is_some_and(|operation| {
        operation.state.session_id == state.session_id
            && operation
                .state
                .track
                .as_ref()
                .is_some_and(|operation_track| operation_track.track_id == track.track_id)
    })
}

fn music_stream_event_matches(music: &MusicState, session_id: &str, track_id: &str) -> bool {
    music
        .playback_state()
        .is_some_and(|state| playback_state_matches_track(state, session_id, track_id))
}

fn playback_state_matches_track(state: &PlaybackState, session_id: &str, track_id: &str) -> bool {
    state.session_id == session_id
        && state
            .track
            .as_ref()
            .is_some_and(|track| track.track_id == track_id)
}

async fn maybe_pause_for_buffer_health_loss(
    music: &mut MusicState,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    failed_audio_sessions: &mut HashSet<String>,
    buffer: &mut BufferCoordinator,
    playback_health: &mut PlaybackHealth,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    rendezvous_nodes: &HashSet<PeerId>,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    if buffer.is_active() {
        return Ok(());
    }

    let Some(current) = music.playback_state().cloned() else {
        playback_health.clear();
        let _ = ui.send(UiEvent::PlaybackCache(None)).await;
        return Ok(());
    };
    if current.leader_peer_id != local_peer_id.to_string() || !current.playing {
        return Ok(());
    }

    let expected = expected_playback_peers(swarm, rendezvous_nodes, local_peer_id);
    let position_ms = playback_position_ms(&current, current_timestamp_micros());
    match playback_health.evaluate(
        &current.session_id,
        &expected,
        position_ms.saturating_add(media_cache::LOW_WATERMARK_MS),
        Instant::now(),
        BUFFER_HEALTH_LOSS_GRACE,
        BUFFER_HEALTH_STALE_AFTER,
    ) {
        PlaybackHealthDecision::MajorityLost { healthy, threshold } => {
            let Some(item) = queue_item_from_playback_state(&current) else {
                return Ok(());
            };
            let now = current_timestamp_micros();
            if let Some(state) = music.pause_playback_for_vote(local_peer_id, now) {
                if let Some(player) = audio_player.as_mut() {
                    player.set_playing(false, now)?;
                }
                publish_playback_state(swarm, topic, targets, &state)?;
                send_playback_view(ui, &state).await;
            }
            send_status(
                ui,
                format!("buffer majority lost ({healthy}/{threshold}); waiting to resume"),
            )
            .await;
            begin_buffer_operation(
                buffer,
                music,
                item,
                PlaybackBufferOperationKind::Resume,
                position_ms,
                audio_player,
                client,
                local_media,
                failed_audio_sessions,
                swarm,
                topic,
                targets,
                local_peer_id,
                ui,
            )
            .await?;
        }
        PlaybackHealthDecision::Stable { .. } => {}
    }

    Ok(())
}

async fn handle_local_leader_audio_failure(
    music: &mut MusicState,
    audio_player: &mut Option<player::AudioPlayer>,
    failed_audio_sessions: &mut HashSet<String>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    state: &PlaybackState,
    reason: &str,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    failed_audio_sessions.insert(state.session_id.clone());
    if let Some(player) = audio_player.as_mut() {
        player.stop();
    }

    if let Some(cancel) = music.take_local_pending_cancel(local_peer_id, reason) {
        publish_playback_cancel(
            swarm,
            topic,
            targets,
            &cancel.session_id,
            local_peer_id,
            &cancel.reason,
        )?;
        let _ = ui.send(UiEvent::Playback(None)).await;
        send_status(ui, format!("playback canceled: {reason}")).await;
        return Ok(());
    }

    if music
        .playback_state()
        .is_some_and(|current| current.session_id == state.session_id)
    {
        let idle = music.stop_current_playback(local_peer_id, current_timestamp_micros());
        publish_playback_state(swarm, topic, targets, &idle)?;
        send_playback_view(ui, &idle).await;
    }

    send_status(ui, reason.to_string()).await;
    Ok(())
}

async fn handle_buffer_prepare_failure(
    buffer: &mut BufferCoordinator,
    audio_player: &mut Option<player::AudioPlayer>,
    music: &mut MusicState,
    failed_audio_sessions: &mut HashSet<String>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    state: &PlaybackState,
    reason: String,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    failed_audio_sessions.insert(state.session_id.clone());
    if let Some(player) = audio_player.as_mut() {
        player.stop();
    }

    let Some(operation_id) = buffer
        .active()
        .filter(|operation| operation.state.session_id == state.session_id)
        .map(|operation| operation.operation_id.clone())
    else {
        mark_local_audio_session_failed(
            audio_player,
            music,
            failed_audio_sessions,
            state,
            reason,
            ui,
        )
        .await;
        return Ok(());
    };

    let is_leader = state.leader_peer_id == local_peer_id.to_string();
    if is_leader {
        if let Some(operation) = buffer.clear() {
            publish_playback_buffer_cancel(
                swarm,
                topic,
                targets,
                &operation.operation_id,
                &operation.state.session_id,
                local_peer_id,
                &reason,
            )?;
            let _ = ui.send(UiEvent::PlaybackBuffer(None)).await;
            send_status(ui, format!("buffer canceled: {reason}")).await;
            if matches!(operation.kind, PlaybackBufferOperationKind::Start) {
                remove_buffer_queue_item(music, &operation);
                publish_queue_state(swarm, topic, targets, music, local_peer_id)?;
                send_queue_view(ui, local_peer_id, music).await;
            }
        }
    } else {
        let _ = buffer.mark_status(
            &operation_id,
            &state.session_id,
            &local_peer_id.to_string(),
            PlaybackBufferStatusKind::Failed,
            None,
            Some(reason.clone()),
            Instant::now(),
        );
        if let Some(status) = describe_buffer_publish_result(
            "buffer failure status",
            publish_playback_buffer_status(
                swarm,
                topic,
                targets,
                &operation_id,
                &state.session_id,
                local_peer_id,
                PlaybackBufferStatusKind::Failed,
                None,
                Some(reason.clone()),
            ),
        ) {
            send_status(ui, status).await;
        }
        mark_local_audio_session_failed(
            audio_player,
            music,
            failed_audio_sessions,
            state,
            reason,
            ui,
        )
        .await;
        let _ = buffer.clear();
        let _ = ui.send(UiEvent::PlaybackBuffer(None)).await;
    }
    send_buffer_view(ui, buffer, local_peer_id).await;
    Ok(())
}

async fn resolve_buffer_operation(
    buffer: &mut BufferCoordinator,
    music: &mut MusicState,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    failed_audio_sessions: &mut HashSet<String>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let now = Instant::now();
    let Some(operation) = buffer.active() else {
        return Ok(());
    };
    if operation.state.leader_peer_id != local_peer_id.to_string() {
        // Followers report local readiness, but only the operation leader decides
        // quorum timeout/cancel/commit. Clearing here makes the UI pill blink
        // whenever the leader later republishes the still-active prepare.
        return Ok(());
    }

    let Some(quorum) = buffer.quorum(now) else {
        return Ok(());
    };

    match quorum {
        BufferQuorum::Ready { ready, threshold } => {
            let Some(operation) = buffer.active().cloned() else {
                return Ok(());
            };
            let local_peer = local_peer_id.to_string();
            let local_status = operation.statuses.get(&local_peer);
            let local_ready =
                local_status.is_some_and(|status| status.status == PlaybackBufferStatusKind::Ready);
            if audio_player.is_some() && !local_ready {
                let already_buffering = local_status
                    .is_some_and(|status| status.status == PlaybackBufferStatusKind::Buffering);
                if !already_buffering {
                    let _ = buffer.mark_status(
                        &operation.operation_id,
                        &operation.state.session_id,
                        &local_peer,
                        PlaybackBufferStatusKind::Buffering,
                        Some(operation.state.position_ms),
                        None,
                        Instant::now(),
                    );
                    if let Some(status) = describe_buffer_publish_result(
                        "buffer local status",
                        publish_playback_buffer_status(
                            swarm,
                            topic,
                            targets,
                            &operation.operation_id,
                            &operation.state.session_id,
                            local_peer_id,
                            PlaybackBufferStatusKind::Buffering,
                            Some(operation.state.position_ms),
                            None,
                        ),
                    ) {
                        send_status(ui, status).await;
                    }
                    send_status(
                        ui,
                        "buffer quorum ready; waiting for local prepared audio".to_string(),
                    )
                    .await;
                }
                let _ = buffer.extend_active_deadline(now + BUFFER_OPERATION_TIMEOUT);
                send_buffer_view(ui, buffer, local_peer_id).await;
                return Ok(());
            }
            if !commit_buffer_operation(
                &operation,
                music,
                audio_player,
                client,
                local_media,
                swarm,
                topic,
                targets,
                local_peer_id,
                ui,
            )
            .await?
            {
                let already_buffering = operation
                    .statuses
                    .get(&local_peer)
                    .is_some_and(|status| status.status == PlaybackBufferStatusKind::Buffering);
                let _ = buffer.mark_status(
                    &operation.operation_id,
                    &operation.state.session_id,
                    &local_peer,
                    PlaybackBufferStatusKind::Buffering,
                    Some(operation.state.position_ms),
                    None,
                    Instant::now(),
                );
                if !already_buffering {
                    if let Some(status) = describe_buffer_publish_result(
                        "buffer local status",
                        publish_playback_buffer_status(
                            swarm,
                            topic,
                            targets,
                            &operation.operation_id,
                            &operation.state.session_id,
                            local_peer_id,
                            PlaybackBufferStatusKind::Buffering,
                            Some(operation.state.position_ms),
                            None,
                        ),
                    ) {
                        send_status(ui, status).await;
                    }
                    send_status(
                        ui,
                        "buffer quorum ready; waiting for local prepared audio".to_string(),
                    )
                    .await;
                }
                let _ = buffer.extend_active_deadline(now + BUFFER_OPERATION_TIMEOUT);
                send_buffer_view(ui, buffer, local_peer_id).await;
                return Ok(());
            }
            let _ = buffer.cancel(&operation.operation_id);
            let _ = ui.send(UiEvent::PlaybackBuffer(None)).await;
            send_status(ui, format!("buffer quorum ready ({ready}/{threshold})")).await;
        }
        BufferQuorum::Impossible {
            ready,
            failed,
            threshold,
        } => {
            let Some(operation) = buffer.clear() else {
                return Ok(());
            };
            publish_playback_buffer_cancel(
                swarm,
                topic,
                targets,
                &operation.operation_id,
                &operation.state.session_id,
                local_peer_id,
                "buffer quorum impossible",
            )?;
            let _ = ui.send(UiEvent::PlaybackBuffer(None)).await;
            send_status(
                ui,
                format!("buffer quorum impossible ({ready}/{threshold}, failed {failed})"),
            )
            .await;
            if operation
                .statuses
                .get(&local_peer_id.to_string())
                .is_some_and(|status| status.status == PlaybackBufferStatusKind::Failed)
            {
                failed_audio_sessions.insert(operation.state.session_id.clone());
            }
            if matches!(operation.kind, PlaybackBufferOperationKind::Start) {
                let title = operation
                    .state
                    .track
                    .as_ref()
                    .map(|track| track.title.clone());
                remove_buffer_queue_item(music, &operation);
                publish_queue_state(swarm, topic, targets, music, local_peer_id)?;
                send_queue_view(ui, local_peer_id, music).await;
                if let Some(title) = title {
                    send_status(ui, format!("skipped {title}: buffer quorum impossible")).await;
                }
            }
        }
        BufferQuorum::TimedOut { ready, threshold } => {
            let local_peer = local_peer_id.to_string();
            let local_status = buffer
                .active()
                .and_then(|operation| operation.statuses.get(&local_peer).cloned());
            if local_status
                .as_ref()
                .is_some_and(|status| status.status == PlaybackBufferStatusKind::Buffering)
            {
                if let Some(operation) =
                    buffer.extend_active_deadline(now + BUFFER_OPERATION_TIMEOUT)
                {
                    if let Some(status) = describe_buffer_publish_result(
                        "buffer prepare republish",
                        publish_playback_buffer_prepare(
                            swarm,
                            topic,
                            targets,
                            &operation,
                            local_peer_id,
                        ),
                    ) {
                        send_status(ui, status).await;
                    }
                    let _ = buffer.mark_prepare_published(&operation.operation_id, now);
                    if let Some(status) = local_status {
                        if let Some(status_message) = describe_buffer_publish_result(
                            "buffer local status republish",
                            publish_playback_buffer_status(
                                swarm,
                                topic,
                                targets,
                                &operation.operation_id,
                                &operation.state.session_id,
                                local_peer_id,
                                status.status,
                                status.buffered_until_ms,
                                status.error,
                            ),
                        ) {
                            send_status(ui, status_message).await;
                        }
                    }
                    send_buffer_view(ui, buffer, local_peer_id).await;
                    send_status(
                        ui,
                        format!("local buffer still loading; extended wait ({ready}/{threshold})"),
                    )
                    .await;
                    return Ok(());
                }
            }
            let Some(operation) = buffer.clear() else {
                return Ok(());
            };
            publish_playback_buffer_cancel(
                swarm,
                topic,
                targets,
                &operation.operation_id,
                &operation.state.session_id,
                local_peer_id,
                "buffer quorum timed out",
            )?;
            let _ = ui.send(UiEvent::PlaybackBuffer(None)).await;
            send_status(ui, format!("buffer quorum timed out ({ready}/{threshold})")).await;
            if matches!(operation.kind, PlaybackBufferOperationKind::Start) {
                let title = operation
                    .state
                    .track
                    .as_ref()
                    .map(|track| track.title.clone());
                remove_buffer_queue_item(music, &operation);
                publish_queue_state(swarm, topic, targets, music, local_peer_id)?;
                send_queue_view(ui, local_peer_id, music).await;
                if let Some(title) = title {
                    send_status(ui, format!("skipped {title}: buffer quorum timed out")).await;
                }
            }
        }
        BufferQuorum::Waiting { .. } => {
            if let Some(operation) =
                buffer.prepare_republish_due(now, BUFFER_PREPARE_REPUBLISH_INTERVAL)
            {
                if let Some(status) = describe_buffer_publish_result(
                    "buffer prepare republish",
                    publish_playback_buffer_prepare(
                        swarm,
                        topic,
                        targets,
                        &operation,
                        local_peer_id,
                    ),
                ) {
                    send_status(ui, status).await;
                }
            }
            send_buffer_view(ui, buffer, local_peer_id).await;
        }
    }

    Ok(())
}

async fn commit_buffer_operation(
    operation: &BufferOperation,
    music: &mut MusicState,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<bool> {
    let mut state = operation.state.clone();
    if let (Some(player), Some(track)) = (audio_player.as_mut(), state.track.as_ref()) {
        if !player.start_prepared_stream(
            &state.session_id,
            &track.track_id,
            state.position_ms,
            false,
        )? {
            let playable_track = local_media.playable_track(client, track).await?;
            player.prepare_stream(
                client,
                Some(operation.operation_id.clone()),
                state.session_id.clone(),
                playable_track,
                state.position_ms,
            )?;
            return Ok(false);
        }
    }

    let now = current_timestamp_micros();
    music.playback_version = music.playback_version.saturating_add(1);
    state.state_version = music.playback_version;
    state.issued_at_micros = now;
    state.playing = true;
    state.anchor_time_micros = now + duration_micros(MUSIC_START_DELAY);

    if matches!(operation.kind, PlaybackBufferOperationKind::Start) {
        remove_buffer_queue_item(music, operation);
        publish_queue_state(swarm, topic, targets, music, local_peer_id)?;
        send_queue_view(ui, local_peer_id, music).await;
    }

    let _ = music.set_playback_state(state.clone());
    publish_playback_state(swarm, topic, targets, &state)?;
    send_playback_view(ui, &state).await;
    Ok(true)
}

fn remove_buffer_queue_item(music: &mut MusicState, operation: &BufferOperation) {
    let Some(item_id) = operation.queue_item_id.as_ref() else {
        return;
    };
    if let Some(index) = music.queue.iter().position(|item| &item.item_id == item_id) {
        music.queue.remove(index);
        music.mark_queue_changed(current_timestamp_micros());
    }
}

fn remove_started_playback_queue_item(music: &mut MusicState, state: &PlaybackState) -> bool {
    let Some(track) = state.track.as_ref() else {
        return false;
    };

    let Some(index) = music
        .queue
        .iter()
        .position(|item| queue_item_matches_started_playback(item, state, &track.track_id))
    else {
        return false;
    };

    music.queue.remove(index);
    true
}

fn queue_item_matches_started_playback(
    item: &QueueItem,
    state: &PlaybackState,
    track_id: &str,
) -> bool {
    if item.track.track_id != track_id {
        return false;
    }
    if state
        .track_requested_by
        .as_ref()
        .is_some_and(|requester| item.requested_by != *requester)
    {
        return false;
    }

    item.added_at_micros <= state.issued_at_micros
}

fn playback_state_resolves_buffer_prepare(
    current: Option<&PlaybackState>,
    session_id: &str,
    track_id: &str,
    leader_peer_id: &str,
) -> bool {
    let Some(current) = current else {
        return false;
    };
    if current.session_id != session_id || current.leader_peer_id != leader_peer_id {
        return false;
    }
    let Some(track) = current.track.as_ref() else {
        return false;
    };

    track.track_id == track_id
}

fn find_buffer_track<'a>(
    music: &'a MusicState,
    track_id: &str,
) -> Option<&'a crate::core::PlaybackTrack> {
    music
        .playback_state()
        .and_then(|state| state.track.as_ref())
        .filter(|track| track.track_id == track_id)
        .or_else(|| {
            music
                .queue
                .iter()
                .find(|item| item.track.track_id == track_id)
                .map(|item| &item.track)
        })
}

fn queue_item_from_playback_state(state: &PlaybackState) -> Option<QueueItem> {
    let track = state.track.clone()?;
    Some(QueueItem {
        item_id: format!("playback-{}", state.session_id),
        requested_by: state
            .track_requested_by
            .clone()
            .unwrap_or_else(|| state.leader_peer_id.clone()),
        added_at_micros: state.issued_at_micros,
        track,
    })
}

fn track_duration_for_session(music: &MusicState, session_id: &str, track_id: &str) -> Option<u64> {
    music
        .playback_state()
        .filter(|state| state.session_id == session_id)
        .and_then(|state| state.track.as_ref())
        .filter(|track| track.track_id == track_id)
        .map(|track| track.duration_ms)
        .or_else(|| {
            music
                .queue
                .iter()
                .find(|item| item.track.track_id == track_id)
                .map(|item| item.track.duration_ms)
        })
}

async fn mark_local_audio_session_failed(
    audio_player: &mut Option<player::AudioPlayer>,
    music: &mut MusicState,
    failed_audio_sessions: &mut HashSet<String>,
    state: &PlaybackState,
    reason: String,
    ui: &mpsc::Sender<UiEvent>,
) {
    failed_audio_sessions.insert(state.session_id.clone());
    if let Some(player) = audio_player.as_mut() {
        player.stop();
    }

    if music
        .playback_state()
        .is_some_and(|current| current.session_id == state.session_id)
    {
        let _ = ui.send(UiEvent::Playback(None)).await;
    }

    send_status(
        ui,
        format!(
            "local audio unavailable for {}: {reason}",
            playback_state_title(state)
        ),
    )
    .await;
}

fn playback_state_uses_failed_audio_session(
    failed_audio_sessions: &HashSet<String>,
    state: &PlaybackState,
) -> bool {
    state.track.is_some() && failed_audio_sessions.contains(&state.session_id)
}

fn playback_state_title(state: &PlaybackState) -> &str {
    state
        .track
        .as_ref()
        .map_or("playback", |track| track.title.as_str())
}

async fn start_next_if_idle(
    music: &mut MusicState,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    failed_audio_sessions: &mut HashSet<String>,
    buffer: &mut BufferCoordinator,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    if !music.can_start_next() {
        return Ok(());
    }
    if buffer.is_active() {
        return Ok(());
    }

    let Some(item) = music.queue.front().cloned() else {
        return Ok(());
    };
    begin_buffer_operation(
        buffer,
        music,
        item,
        PlaybackBufferOperationKind::Start,
        0,
        audio_player,
        client,
        local_media,
        failed_audio_sessions,
        swarm,
        topic,
        targets,
        local_peer_id,
        ui,
    )
    .await
}

async fn begin_buffer_operation(
    buffer: &mut BufferCoordinator,
    music: &mut MusicState,
    item: QueueItem,
    kind: PlaybackBufferOperationKind,
    position_ms: u64,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    failed_audio_sessions: &mut HashSet<String>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let title = item.track.title.clone();
    let now = current_timestamp_micros();
    let operation_id = format!("{local_peer_id}:{now}:buffer:{:?}", kind);
    let session_id = match kind {
        PlaybackBufferOperationKind::Start => {
            format!("{local_peer_id}:{now}:{}", item.track.track_id)
        }
        PlaybackBufferOperationKind::Seek => music
            .playback_state()
            .map(|state| state.session_id.clone())
            .unwrap_or_else(|| format!("{local_peer_id}:{now}:{}", item.track.track_id)),
        PlaybackBufferOperationKind::Resume => music
            .playback_state()
            .map(|state| state.session_id.clone())
            .unwrap_or_else(|| format!("{local_peer_id}:{now}:{}", item.track.track_id)),
    };
    let expected_peers = expected_playback_peers(swarm, targets.rendezvous_nodes, local_peer_id);
    let state = PlaybackState {
        session_id,
        leader_peer_id: local_peer_id.to_string(),
        track: Some(item.track.clone()),
        track_requested_by: Some(item.requested_by.clone()),
        state_version: music.playback_version.saturating_add(1),
        issued_at_micros: now,
        playing: false,
        position_ms,
        anchor_time_micros: now,
        rate: 1.0,
    };
    let queue_item_id = matches!(kind, PlaybackBufferOperationKind::Start).then_some(item.item_id);
    let operation = buffer
        .start_leader_operation(
            operation_id.clone(),
            state.clone(),
            kind,
            queue_item_id,
            expected_peers,
            Instant::now() + BUFFER_OPERATION_TIMEOUT,
        )
        .clone();

    if let Some(player) = audio_player.as_mut() {
        player.set_playing(false, now)?;
    }
    if let Some(status) = describe_buffer_publish_result(
        "buffer prepare",
        publish_playback_buffer_prepare(swarm, topic, targets, &operation, local_peer_id),
    ) {
        send_status(ui, status).await;
    }
    let _ = buffer.mark_prepare_published(&operation.operation_id, Instant::now());
    let _ = buffer.mark_status(
        &operation.operation_id,
        &operation.state.session_id,
        &local_peer_id.to_string(),
        PlaybackBufferStatusKind::Buffering,
        Some(position_ms),
        None,
        Instant::now(),
    );
    if let Some(status) = describe_buffer_publish_result(
        "buffer local status",
        publish_playback_buffer_status(
            swarm,
            topic,
            targets,
            &operation.operation_id,
            &operation.state.session_id,
            local_peer_id,
            PlaybackBufferStatusKind::Buffering,
            Some(position_ms),
            None,
        ),
    ) {
        send_status(ui, status).await;
    }
    send_buffer_view(ui, buffer, local_peer_id).await;
    send_status(ui, format!("buffering {title}")).await;

    let Some(track) = state.track.as_ref() else {
        return Ok(());
    };
    if let Some(player) = audio_player.as_mut() {
        let playable_track = local_media.playable_track(client, track).await?;
        player.prepare_stream(
            client,
            Some(operation.operation_id.clone()),
            operation.state.session_id.clone(),
            playable_track,
            position_ms,
        )?;
    } else {
        let buffered_until = Some(
            position_ms
                .saturating_add(BUFFER_READY_WINDOW_MS)
                .min(track.duration_ms),
        );
        let _ = buffer.mark_status(
            &operation.operation_id,
            &operation.state.session_id,
            &local_peer_id.to_string(),
            PlaybackBufferStatusKind::Ready,
            buffered_until,
            None,
            Instant::now(),
        );
        resolve_buffer_operation(
            buffer,
            music,
            audio_player,
            client,
            local_media,
            failed_audio_sessions,
            swarm,
            topic,
            targets,
            local_peer_id,
            ui,
        )
        .await?;
    }

    Ok(())
}

async fn stop_current_playback(
    music: &mut MusicState,
    audio_player: &mut Option<player::AudioPlayer>,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    reason: &str,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    if let Some(player) = audio_player.as_mut() {
        player.stop();
    }
    if let Some(cancel) = music.take_local_pending_cancel(local_peer_id, reason) {
        publish_playback_cancel(
            swarm,
            topic,
            targets,
            &cancel.session_id,
            local_peer_id,
            &cancel.reason,
        )?;
    }

    let state = music.stop_current_playback(local_peer_id, current_timestamp_micros());
    publish_playback_state(swarm, topic, targets, &state)?;
    send_playback_view(ui, &state).await;
    send_status(ui, reason.to_string()).await;
    Ok(())
}

async fn propose_or_execute_vote(
    action: VoteAction,
    music: &mut MusicState,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    failed_audio_sessions: &mut HashSet<String>,
    buffer_coordinator: &mut BufferCoordinator,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    room_peer_count: usize,
    queue_request_times: &mut HashMap<String, Instant>,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    if music.active_vote.is_some() {
        send_status(ui, "another vote is already active".to_string()).await;
        return Ok(());
    }

    let now = current_timestamp_micros();
    let proposal = VoteProposal {
        vote_id: new_vote_id(local_peer_id, now),
        proposer: local_peer_id.to_string(),
        action,
        queue_version: music.queue_version,
        playback_session_id: music.playback_state().map(|state| state.session_id.clone()),
        created_at_micros: now,
    };
    if let Some(reason) = music.stale_vote_reason(&proposal) {
        send_status(ui, format!("vote not started: {reason}")).await;
        return Ok(());
    }

    music.start_vote(proposal.clone(), Instant::now() + VOTE_TIMEOUT);
    publish_vote_proposal(swarm, topic, targets, &proposal, local_peer_id)?;
    publish_vote_ballot(
        swarm,
        topic,
        targets,
        &proposal.vote_id,
        local_peer_id,
        true,
    )?;
    let approval_count = music
        .active_vote
        .as_ref()
        .map_or(0, |vote| vote.approval_count());
    let threshold = majority_threshold(room_peer_count);
    send_vote_view(ui, music, threshold, room_peer_count, local_peer_id).await;
    send_status(
        ui,
        format!(
            "started vote: {} ({}/{})",
            describe_vote_action(&proposal.action, &music.queue),
            approval_count,
            threshold
        ),
    )
    .await;

    resolve_active_vote(
        music,
        audio_player,
        client,
        local_media,
        failed_audio_sessions,
        buffer_coordinator,
        swarm,
        topic,
        targets,
        local_peer_id,
        room_peer_count,
        queue_request_times,
        ui,
    )
    .await
}

async fn cast_vote(
    approve: bool,
    music: &mut MusicState,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    failed_audio_sessions: &mut HashSet<String>,
    buffer_coordinator: &mut BufferCoordinator,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    room_peer_count: usize,
    queue_request_times: &mut HashMap<String, Instant>,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let Some(vote_outcome) = music.cast_vote(local_peer_id.to_string(), approve) else {
        send_status(ui, "no active vote".to_string()).await;
        return Ok(());
    };
    let vote_id = match vote_outcome {
        VoteCastOutcome::Accepted { vote_id } => vote_id,
        VoteCastOutcome::Duplicate { vote_id } => {
            send_status(ui, format!("already voted on {vote_id}")).await;
            return Ok(());
        }
    };

    publish_vote_ballot(swarm, topic, targets, &vote_id, local_peer_id, approve)?;
    send_status(
        ui,
        format!("voted {} on {vote_id}", if approve { "yes" } else { "no" }),
    )
    .await;
    send_vote_view(
        ui,
        music,
        majority_threshold(room_peer_count),
        room_peer_count,
        local_peer_id,
    )
    .await;

    resolve_active_vote(
        music,
        audio_player,
        client,
        local_media,
        failed_audio_sessions,
        buffer_coordinator,
        swarm,
        topic,
        targets,
        local_peer_id,
        room_peer_count,
        queue_request_times,
        ui,
    )
    .await
}

async fn resolve_active_vote(
    music: &mut MusicState,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    failed_audio_sessions: &mut HashSet<String>,
    buffer_coordinator: &mut BufferCoordinator,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    room_peer_count: usize,
    queue_request_times: &mut HashMap<String, Instant>,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let threshold = majority_threshold(room_peer_count);
    if let Some(proposal) = music.ready_vote_waiting_for_queue(threshold) {
        request_queue_for_vote_context(
            &proposal,
            music.queue_version,
            music.queue_updated_at,
            queue_request_times,
            swarm,
            topic,
            targets,
            local_peer_id,
            ui,
        )
        .await;
        return Ok(());
    }

    let Some(resolution) = music.resolve_vote(threshold, room_peer_count) else {
        return Ok(());
    };

    send_vote_view(ui, music, threshold, room_peer_count, local_peer_id).await;
    let proposal = match resolution {
        VoteResolution::Passed(proposal) => {
            send_status(
                ui,
                format!(
                    "vote passed: {}",
                    describe_vote_action(&proposal.action, &music.queue)
                ),
            )
            .await;
            proposal
        }
        VoteResolution::Rejected(proposal) => {
            send_status(
                ui,
                format!(
                    "vote rejected: {}",
                    describe_vote_action(&proposal.action, &music.queue)
                ),
            )
            .await;
            return Ok(());
        }
    };

    if let Some(reason) = music.stale_vote_reason(&proposal) {
        send_status(ui, format!("vote discarded: {reason}")).await;
        return Ok(());
    }

    if music.should_execute_vote_locally(&proposal, local_peer_id) {
        execute_vote_action(
            proposal,
            music,
            audio_player,
            client,
            local_media,
            failed_audio_sessions,
            buffer_coordinator,
            swarm,
            topic,
            targets,
            local_peer_id,
            ui,
        )
        .await?;
    }

    Ok(())
}

async fn execute_vote_action(
    proposal: VoteProposal,
    music: &mut MusicState,
    audio_player: &mut Option<player::AudioPlayer>,
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    failed_audio_sessions: &mut HashSet<String>,
    buffer_coordinator: &mut BufferCoordinator,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let actor_peer_id = parse_peer_id(&proposal.proposer).unwrap_or(local_peer_id);
    match proposal.action {
        VoteAction::Pause => {
            let now = proposal.created_at_micros;
            if let Some(state) = music.pause_playback_for_vote(actor_peer_id, now) {
                if let Some(player) = audio_player.as_mut() {
                    player.set_playing(false, now)?;
                }
                publish_playback_state_if_local_source(
                    swarm,
                    topic,
                    targets,
                    &state,
                    local_peer_id,
                )?;
                send_playback_view(ui, &state).await;
            }
        }
        VoteAction::Resume => {
            if proposal.proposer != local_peer_id.to_string() {
                return Ok(());
            }
            let now = proposal.created_at_micros;
            let Some(current) = music.playback_state().cloned() else {
                return Ok(());
            };
            let Some(item) = queue_item_from_playback_state(&current) else {
                return Ok(());
            };
            let position_ms = playback_position_ms(&current, now);
            if let Some(state) = music.pause_playback_for_vote(actor_peer_id, now) {
                if let Some(player) = audio_player.as_mut() {
                    player.set_playing(false, now)?;
                }
                publish_playback_state_if_local_source(
                    swarm,
                    topic,
                    targets,
                    &state,
                    local_peer_id,
                )?;
                send_playback_view(ui, &state).await;
            }
            begin_buffer_operation(
                buffer_coordinator,
                music,
                item,
                PlaybackBufferOperationKind::Resume,
                position_ms,
                audio_player,
                client,
                local_media,
                failed_audio_sessions,
                swarm,
                topic,
                targets,
                local_peer_id,
                ui,
            )
            .await?;
        }
        VoteAction::Skip => {
            if let Some(player) = audio_player.as_mut() {
                player.stop();
            }
            let state =
                music.stop_current_playback_for_vote(actor_peer_id, proposal.created_at_micros);
            publish_playback_state_if_local_source(swarm, topic, targets, &state, local_peer_id)?;
            send_playback_view(ui, &state).await;
            send_status(ui, "skipped".to_string()).await;
            if proposal.proposer == local_peer_id.to_string() {
                start_next_if_idle(
                    music,
                    audio_player,
                    client,
                    local_media,
                    failed_audio_sessions,
                    buffer_coordinator,
                    swarm,
                    topic,
                    targets,
                    local_peer_id,
                    ui,
                )
                .await?;
            }
        }
        VoteAction::Seek { position_ms } => {
            if proposal.proposer != local_peer_id.to_string() {
                return Ok(());
            }
            let now = proposal.created_at_micros;
            let Some(current) = music.playback_state().cloned() else {
                return Ok(());
            };
            let Some(item) = queue_item_from_playback_state(&current) else {
                return Ok(());
            };
            let position_ms = position_ms.min(item.track.duration_ms);
            if let Some(state) = music.pause_playback_for_vote(actor_peer_id, now) {
                if let Some(player) = audio_player.as_mut() {
                    player.set_playing(false, now)?;
                }
                publish_playback_state_if_local_source(
                    swarm,
                    topic,
                    targets,
                    &state,
                    local_peer_id,
                )?;
                send_playback_view(ui, &state).await;
            }
            begin_buffer_operation(
                buffer_coordinator,
                music,
                item,
                PlaybackBufferOperationKind::Seek,
                position_ms,
                audio_player,
                client,
                local_media,
                failed_audio_sessions,
                swarm,
                topic,
                targets,
                local_peer_id,
                ui,
            )
            .await?;
        }
        VoteAction::Remove { item_id } => {
            if let Some(index) = music.queue.iter().position(|item| item.item_id == item_id) {
                let removed = music.queue.remove(index);
                music.mark_queue_vote_applied(proposal.created_at_micros);
                publish_queue_snapshot(swarm, topic, targets, local_peer_id, music)?;
                send_queue_view(ui, local_peer_id, music).await;
                if let Some(item) = removed {
                    send_status(ui, format!("removed {}", item.track.title)).await;
                }
            } else {
                send_status(
                    ui,
                    "vote discarded: queue item is no longer available".to_string(),
                )
                .await;
            }
        }
        VoteAction::Move { item_id, to_index } => {
            if let Some(index) = music.queue.iter().position(|item| item.item_id == item_id) {
                if let Some(item) = music.queue.remove(index) {
                    let to_index = to_index.min(music.queue.len());
                    music.queue.insert(to_index, item);
                    music.mark_queue_vote_applied(proposal.created_at_micros);
                    publish_queue_snapshot(swarm, topic, targets, local_peer_id, music)?;
                    send_queue_view(ui, local_peer_id, music).await;
                    send_status(ui, format!("moved queue item to #{}", to_index + 1)).await;
                }
            } else {
                send_status(
                    ui,
                    "vote discarded: queue item is no longer available".to_string(),
                )
                .await;
            }
        }
    }

    Ok(())
}

fn publish_vote_proposal(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    proposal: &VoteProposal,
    local_peer_id: PeerId,
) -> Result<()> {
    publish_room_wire(
        swarm,
        topic,
        targets,
        &WireMessage::VoteProposal {
            proposal: proposal.clone(),
            nonce: new_nonce(local_peer_id),
        },
    )
}

fn publish_vote_ballot(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    vote_id: &str,
    local_peer_id: PeerId,
    approve: bool,
) -> Result<()> {
    publish_room_wire(
        swarm,
        topic,
        targets,
        &WireMessage::VoteBallot {
            vote_id: vote_id.to_string(),
            peer_id: local_peer_id.to_string(),
            approve,
            nonce: new_nonce(local_peer_id),
        },
    )
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

fn short_peer(peer_id: &str) -> String {
    peer_id.chars().take(8).collect()
}

fn publish_playback_state(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    state: &PlaybackState,
) -> Result<()> {
    publish_room_wire(
        swarm,
        topic,
        targets,
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

fn publish_playback_state_if_local_source(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    state: &PlaybackState,
    local_peer_id: PeerId,
) -> Result<()> {
    if state.leader_peer_id == local_peer_id.to_string() {
        publish_playback_state(swarm, topic, targets, state)?;
    }
    Ok(())
}

fn publish_playback_ready(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    session_id: &str,
    local_peer_id: PeerId,
) -> Result<()> {
    publish_room_wire(
        swarm,
        topic,
        targets,
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
    targets: &PublishTargets<'_>,
    session_id: &str,
    local_peer_id: PeerId,
    reason: &str,
) -> Result<()> {
    publish_room_wire(
        swarm,
        topic,
        targets,
        &WireMessage::PlaybackCancel {
            session_id: session_id.to_string(),
            leader_peer_id: local_peer_id.to_string(),
            reason: reason.to_string(),
            nonce: new_nonce(local_peer_id),
        },
    )
}

fn publish_playback_buffer_prepare(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    _targets: &PublishTargets<'_>,
    operation: &BufferOperation,
    local_peer_id: PeerId,
) -> Result<RoomPublishOutcome> {
    let track_id = operation
        .state
        .track
        .as_ref()
        .map_or_else(String::new, |track| track.track_id.clone());
    publish_room_wire_with_outcome(
        swarm,
        topic,
        &WireMessage::PlaybackBufferPrepare {
            operation_id: operation.operation_id.clone(),
            session_id: operation.state.session_id.clone(),
            track_id,
            position_ms: operation.state.position_ms,
            kind: operation.kind.clone(),
            expected_peers: operation.expected_peers.iter().cloned().collect(),
            leader_peer_id: local_peer_id.to_string(),
            nonce: new_nonce(local_peer_id),
        },
    )
}

fn publish_playback_buffer_status(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    _targets: &PublishTargets<'_>,
    operation_id: &str,
    session_id: &str,
    local_peer_id: PeerId,
    status: PlaybackBufferStatusKind,
    buffered_until_ms: Option<u64>,
    error: Option<String>,
) -> Result<RoomPublishOutcome> {
    publish_room_wire_with_outcome(
        swarm,
        topic,
        &WireMessage::PlaybackBufferStatus {
            operation_id: operation_id.to_string(),
            session_id: session_id.to_string(),
            peer_id: local_peer_id.to_string(),
            status,
            buffered_until_ms,
            error,
            nonce: new_nonce(local_peer_id),
        },
    )
}

fn describe_buffer_publish_result(
    action: &str,
    result: Result<RoomPublishOutcome>,
) -> Option<String> {
    match result {
        Ok(RoomPublishOutcome::Published) => None,
        Ok(RoomPublishOutcome::NoSubscribers) => {
            Some(format!("{action} not sent: no gossipsub subscribers"))
        }
        Ok(RoomPublishOutcome::Congested) => {
            Some(format!("{action} delayed: gossipsub peer queues are full"))
        }
        Err(err) => Some(format!("{action} failed: {err:#}")),
    }
}

fn publish_local_buffer_health(
    playback_health: &mut PlaybackHealth,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    session_id: &str,
    status: PlaybackBufferStatusKind,
    buffered_until_ms: Option<u64>,
) -> Result<()> {
    let should_publish = playback_health.mark_local_status_for_publish(
        session_id,
        &local_peer_id.to_string(),
        status.clone(),
        buffered_until_ms,
        Instant::now(),
        BUFFER_HEALTH_PUBLISH_MIN_INTERVAL,
        BUFFER_HEALTH_PUBLISH_MIN_ADVANCE_MS,
    );
    if !should_publish {
        return Ok(());
    }
    publish_playback_buffer_health(
        swarm,
        topic,
        targets,
        session_id,
        local_peer_id,
        status,
        buffered_until_ms,
    )
}

fn publish_playback_buffer_health(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    session_id: &str,
    local_peer_id: PeerId,
    status: PlaybackBufferStatusKind,
    buffered_until_ms: Option<u64>,
) -> Result<()> {
    publish_room_wire(
        swarm,
        topic,
        targets,
        &WireMessage::PlaybackBufferHealth {
            session_id: session_id.to_string(),
            peer_id: local_peer_id.to_string(),
            status,
            buffered_until_ms,
            nonce: new_nonce(local_peer_id),
        },
    )
}

fn publish_playback_buffer_cancel(
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    operation_id: &str,
    session_id: &str,
    local_peer_id: PeerId,
    reason: &str,
) -> Result<()> {
    publish_room_wire(
        swarm,
        topic,
        targets,
        &WireMessage::PlaybackBufferCancel {
            operation_id: operation_id.to_string(),
            session_id: session_id.to_string(),
            leader_peer_id: local_peer_id.to_string(),
            reason: reason.to_string(),
            nonce: new_nonce(local_peer_id),
        },
    )
}

async fn abandon_local_buffer_operation(
    buffer: &mut BufferCoordinator,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    reason: &str,
    ui: &mpsc::Sender<UiEvent>,
) {
    let Some(operation) = buffer.clear_if_local_leader(&local_peer_id.to_string()) else {
        return;
    };

    if let Err(err) = publish_playback_buffer_cancel(
        swarm,
        topic,
        targets,
        &operation.operation_id,
        &operation.state.session_id,
        local_peer_id,
        reason,
    ) {
        send_status(ui, format!("buffer cancel not sent: {err:#}")).await;
    }
    let _ = ui.send(UiEvent::PlaybackBuffer(None)).await;
    send_status(ui, format!("buffer canceled: {reason}")).await;
}

fn cancel_local_pending_playback(
    music: &mut MusicState,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    reason: &str,
) {
    if let Some(cancel) = music.take_local_pending_cancel(local_peer_id, reason) {
        let _ = publish_playback_cancel(
            swarm,
            topic,
            targets,
            &cancel.session_id,
            local_peer_id,
            &cancel.reason,
        );
    }
}

async fn maybe_start_pending_playback(
    music: &mut MusicState,
    swarm: &mut libp2p::Swarm<Behaviour>,
    topic: &gossipsub::IdentTopic,
    targets: &PublishTargets<'_>,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    if let Some(start) = music.maybe_start_pending_playback(
        local_peer_id,
        Instant::now(),
        current_timestamp_micros(),
        MUSIC_START_DELAY,
    ) {
        publish_playback_state(swarm, topic, targets, &start.state)?;
        send_playback_view(ui, &start.state).await;
        send_status(
            ui,
            format!(
                "starting playback in {:.1}s ({}, {}/{})",
                MUSIC_START_DELAY.as_secs_f32(),
                start.reason,
                start.ready,
                start.expected
            ),
        )
        .await;
    }

    Ok(())
}

async fn apply_playback_prepare(
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    audio_player: &mut Option<player::AudioPlayer>,
    music: &mut MusicState,
    state: &PlaybackState,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<bool> {
    let now = current_timestamp_micros();
    let invalidated_vote = music.set_remote_playback_prepare(state.clone());
    if let Some(player) = audio_player.as_mut() {
        player.set_playing(false, now)?;
    }

    send_playback_view(ui, state).await;
    if let Some(invalidated_vote) = invalidated_vote {
        let _ = ui.send(UiEvent::Vote(None)).await;
        send_status(ui, invalidated_vote.reason.to_string()).await;
    }

    let Some(track) = &state.track else {
        return Ok(true);
    };

    send_status(ui, format!("preparing {}", track.title)).await;
    if !ensure_audio_output(audio_player, ui, "playback prepare").await {
        send_status(
            ui,
            "audio output unavailable locally; confirming prepare so the room can continue"
                .to_string(),
        )
        .await;
        return Ok(true);
    }

    send_status(ui, format!("streaming {}", track.title)).await;
    if let Some(player) = audio_player.as_mut() {
        let playable_track = local_media.playable_track(client, track).await?;
        player.prepare_stream(
            client,
            None,
            state.session_id.clone(),
            playable_track,
            state.position_ms,
        )?;
    }
    Ok(false)
}

async fn apply_playback_cancel(
    audio_player: &mut Option<player::AudioPlayer>,
    music: &mut MusicState,
    session_id: &str,
    reason: &str,
    ui: &mpsc::Sender<UiEvent>,
) {
    let matches_session = music
        .playback_state()
        .is_some_and(|state| state.session_id == session_id);
    if !matches_session {
        return;
    }

    if let Some(player) = audio_player.as_mut() {
        player.stop();
    }
    let invalidated_vote = music.cancel_playback(session_id);
    let _ = ui.send(UiEvent::Playback(None)).await;
    if let Some(invalidated_vote) = invalidated_vote {
        let _ = ui.send(UiEvent::Vote(None)).await;
        send_status(ui, invalidated_vote.reason.to_string()).await;
    }
    send_status(ui, format!("playback canceled: {reason}")).await;
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
    if player.current_session_id().is_some()
        && player.current_session_id() != Some(state.session_id.as_str())
    {
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

fn finished_playback_role(
    state: &PlaybackState,
    local_peer_id: PeerId,
    now_micros: i64,
    local_audio_finished: bool,
) -> Option<FinishedPlaybackRole> {
    if state.track.is_none() || !state.playing || now_micros < state.anchor_time_micros {
        return None;
    }

    let finished = !can_play_at_position(state, playback_position_ms(state, now_micros))
        || local_audio_finished;
    if !finished {
        return None;
    }

    if state.leader_peer_id == local_peer_id.to_string() {
        Some(FinishedPlaybackRole::Leader)
    } else {
        Some(FinishedPlaybackRole::Follower)
    }
}

fn expected_playback_peers(
    swarm: &libp2p::Swarm<Behaviour>,
    rendezvous_nodes: &HashSet<PeerId>,
    local_peer_id: PeerId,
) -> HashSet<String> {
    room_peer_ids(swarm, rendezvous_nodes, local_peer_id)
        .into_iter()
        .map(|peer_id| peer_id.to_string())
        .collect()
}

async fn apply_remote_playback_state(
    client: &reqwest::Client,
    local_media: &mut LocalMediaResolver,
    audio_player: &mut Option<player::AudioPlayer>,
    music: &mut MusicState,
    state: &PlaybackState,
    local_peer_id: PeerId,
    ui: &mpsc::Sender<UiEvent>,
) -> Result<()> {
    let now = current_timestamp_micros();
    let desired_position = playback_position_ms(state, now);
    let should_play = playback_should_be_audible(state, now);

    if state.track.is_none() {
        if let Some(player) = audio_player.as_mut() {
            player.stop();
        }
        let invalidated_vote = music.set_playback_state(state.clone());
        send_playback_view(ui, state).await;
        if let Some(invalidated_vote) = invalidated_vote {
            let _ = ui.send(UiEvent::Vote(None)).await;
            send_status(ui, invalidated_vote.reason.to_string()).await;
        }
        return Ok(());
    }

    let invalidated_vote = music.set_playback_state(state.clone());
    send_playback_view(ui, state).await;
    if let Some(invalidated_vote) = invalidated_vote {
        let _ = ui.send(UiEvent::Vote(None)).await;
        send_status(ui, invalidated_vote.reason.to_string()).await;
    }
    if remove_started_playback_queue_item(music, state) {
        send_queue_view(ui, local_peer_id, music).await;
    }

    if let Some(track) = &state.track {
        if let Some(player) = audio_player.as_mut() {
            if player.current_track_id() != Some(track.track_id.as_str())
                || player.current_session_id() != Some(state.session_id.as_str())
            {
                send_status(ui, format!("streaming {}", track.title)).await;
                let playable_track = local_media.playable_track(client, track).await?;
                player.prepare_stream(
                    client,
                    None,
                    state.session_id.clone(),
                    playable_track,
                    desired_position,
                )?;
            } else {
                let current_position = player.position_ms(now);
                let drift = current_position.abs_diff(desired_position);
                if drift > MUSIC_DRIFT_SEEK_THRESHOLD_MS || (should_play && !player.is_playing()) {
                    if !player.start_prepared_stream(
                        &state.session_id,
                        &track.track_id,
                        desired_position,
                        should_play,
                    )? {
                        let playable_track = local_media.playable_track(client, track).await?;
                        player.prepare_stream(
                            client,
                            None,
                            state.session_id.clone(),
                            playable_track,
                            desired_position,
                        )?;
                    }
                } else {
                    player.set_playing(should_play, now)?;
                }
            }
        }
    }

    Ok(())
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

async fn send_buffer_view(
    ui: &mpsc::Sender<UiEvent>,
    buffer: &BufferCoordinator,
    local_peer_id: PeerId,
) {
    let view = buffer
        .active()
        .map(|operation| operation.view(&local_peer_id.to_string(), Instant::now()));
    let _ = ui.send(UiEvent::PlaybackBuffer(view)).await;
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

async fn send_status(ui: &mpsc::Sender<UiEvent>, status: String) {
    let _ = ui.send(UiEvent::Status(status)).await;
}

async fn send_peer_views(
    ui: &mpsc::Sender<UiEvent>,
    connections: &ConnectionState,
    rendezvous_nodes: &HashSet<PeerId>,
) {
    let _ = ui
        .send(UiEvent::Peers(connections.peer_views(rendezvous_nodes)))
        .await;
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use libp2p::identity;
    use tokio::task::JoinHandle;

    use crate::connection_state::{
        DIRECT_PROMOTION_FAILURE_DEDUP_WINDOW, DIRECT_PROMOTION_MAX_FAILURES,
        DIRECT_PROMOTION_MEDIUM_RETRY_FAILURES, DIRECT_PROMOTION_MEDIUM_RETRY_INTERVAL,
        DIRECT_PROMOTION_RETRY_INTERVAL, DIRECT_PROMOTION_SLOW_RETRY_FAILURES,
        DIRECT_PROMOTION_SLOW_RETRY_INTERVAL, DirectPromotionBackoff,
        DirectPromotionFailureOutcome, normalize_direct_peer_address,
    };

    use super::*;

    fn peer_id() -> PeerId {
        identity::Keypair::generate_ed25519().public().to_peer_id()
    }

    #[test]
    fn zero_peer_recovery_schedules_full_discovery_burst_and_repeats() {
        let now = Instant::now();
        let mut recovery = ZeroPeerRecovery::default();

        assert!(recovery.start(now));
        assert!(!recovery.start(now));
        assert!(recovery.active);
        assert_eq!(recovery.discover_deadlines.len(), 5);

        assert!(recovery.pop_due_discovery(now));
        assert!(!recovery.pop_due_discovery(now + Duration::from_secs(4)));
        assert!(recovery.pop_due_discovery(now + Duration::from_secs(5)));
        assert!(recovery.pop_due_discovery(now + Duration::from_secs(10)));
        assert!(recovery.pop_due_discovery(now + Duration::from_secs(20)));
        assert!(recovery.pop_due_discovery(now + Duration::from_secs(30)));

        assert_eq!(
            recovery.discover_deadlines.front().copied(),
            Some(now + Duration::from_secs(60))
        );
        assert!(!recovery.pop_due_discovery(now + Duration::from_secs(59)));
        assert!(recovery.pop_due_discovery(now + Duration::from_secs(60)));
    }

    #[test]
    fn zero_peer_recovery_clears_when_a_room_peer_returns() {
        let now = Instant::now();
        let mut recovery = ZeroPeerRecovery::default();

        assert!(recovery.start(now));
        assert!(recovery.finish());

        assert!(!recovery.active);
        assert!(recovery.discover_deadlines.is_empty());
        assert!(!recovery.pop_due_discovery(now + Duration::from_secs(60)));
        assert!(!recovery.finish());

        let later = now + Duration::from_secs(90);
        assert!(recovery.start(later));
        assert_eq!(recovery.discover_deadlines.front().copied(), Some(later));
    }

    #[test]
    fn sync_burst_scheduling_is_coalesced() {
        let mut sync = SyncAnnouncementState::default();

        schedule_sync_burst(&mut sync);
        assert_eq!(sync.pending.len(), 3);
        let first = sync.pending.clone();

        schedule_sync_burst(&mut sync);
        assert_eq!(sync.pending, first);
    }

    #[test]
    fn immediate_sync_and_music_snapshot_are_rate_limited() {
        let now = Instant::now();
        let mut sync = SyncAnnouncementState::default();

        assert!(should_publish_immediate_sync(&sync, now));
        sync.last_immediate = Some(now);
        assert!(!should_publish_immediate_sync(
            &sync,
            now + Duration::from_millis(999)
        ));
        assert!(should_publish_immediate_sync(
            &sync,
            now + SYNC_IMMEDIATE_MIN_INTERVAL
        ));

        assert!(should_publish_music_snapshot(&sync, now));
        sync.last_music_snapshot = Some(now);
        assert!(!should_publish_music_snapshot(
            &sync,
            now + Duration::from_millis(999)
        ));
        assert!(should_publish_music_snapshot(
            &sync,
            now + MUSIC_SNAPSHOT_MIN_INTERVAL
        ));
    }

    fn record(id: impl Into<String>, sent_at: i64) -> ChatRecord {
        ChatRecord {
            id: id.into(),
            peer_id: "peer".to_string(),
            joined_at: Some(1_700_000_000_000_000),
            seq: 0,
            causal_frontier: BTreeMap::new(),
            author: "alice".to_string(),
            text: "hello".to_string(),
            sent_at,
        }
    }

    fn track(id: &str, duration_ms: u64) -> crate::core::PlaybackTrack {
        crate::core::PlaybackTrack {
            track_id: id.to_string(),
            title: id.to_string(),
            source_kind: "bilibili".to_string(),
            bvid: "BV1A4411N7".to_string(),
            part: 1,
            duration_ms,
            audio_url: "https://example.test/audio.m4a".to_string(),
            referer: "https://www.bilibili.com/video/BV1A4411N7".to_string(),
            media_init_range: None,
            media_index_range: None,
        }
    }

    #[test]
    fn local_media_key_uses_bilibili_identity() {
        let media = track("track", 120_000);

        assert_eq!(
            local_media_key(&media),
            Some("bilibili:BV1A4411N7:1".to_string())
        );
    }

    #[test]
    fn local_media_key_ignores_non_bilibili_or_incomplete_identity() {
        let mut media = track("track", 120_000);
        media.source_kind = "http".to_string();
        assert!(local_media_key(&media).is_none());

        media = track("track", 120_000);
        media.bvid.clear();
        assert!(local_media_key(&media).is_none());

        media = track("track", 120_000);
        media.part = 0;
        assert!(local_media_key(&media).is_none());
    }

    #[test]
    fn local_media_merge_preserves_shared_metadata_and_uses_local_media() {
        let mut shared = track("shared-track", 120_000);
        shared.audio_url = "https://remote.example/audio.m4a".to_string();
        shared.referer = "https://remote.example/referer".to_string();

        let mut local = shared.clone();
        local.track_id = "local-track".to_string();
        local.title = "local title".to_string();
        local.duration_ms = 240_000;
        local.audio_url = "https://local.example/audio.m4a".to_string();
        local.referer = "https://local.example/referer".to_string();
        local.media_init_range = Some(crate::core::PlaybackByteRange {
            start: 10,
            end_inclusive: 20,
        });
        local.media_index_range = Some(crate::core::PlaybackByteRange {
            start: 30,
            end_inclusive: 40,
        });

        let playable = merge_local_media(&shared, &local);

        assert_eq!(playable.track_id, shared.track_id);
        assert_eq!(playable.title, shared.title);
        assert_eq!(playable.duration_ms, shared.duration_ms);
        assert_eq!(playable.source_kind, shared.source_kind);
        assert_eq!(playable.bvid, shared.bvid);
        assert_eq!(playable.part, shared.part);
        assert_eq!(playable.audio_url, local.audio_url);
        assert_eq!(playable.referer, local.referer);
        assert_eq!(playable.media_init_range, local.media_init_range);
        assert_eq!(playable.media_index_range, local.media_index_range);
    }

    fn playback_state(
        leader: PeerId,
        playing: bool,
        position_ms: u64,
        anchor_time_micros: i64,
        duration_ms: u64,
    ) -> PlaybackState {
        PlaybackState {
            session_id: "session".to_string(),
            leader_peer_id: leader.to_string(),
            track: Some(track("track", duration_ms)),
            track_requested_by: Some(leader.to_string()),
            state_version: 1,
            issued_at_micros: anchor_time_micros,
            playing,
            position_ms,
            anchor_time_micros,
            rate: 1.0,
        }
    }

    struct TestBackend {
        commands: mpsc::Sender<NetworkCommand>,
        events: mpsc::Receiver<UiEvent>,
        task: JoinHandle<Result<()>>,
    }

    impl TestBackend {
        async fn recv_matching<T>(
            &mut self,
            timeout: Duration,
            mut matcher: impl FnMut(UiEvent) -> Option<T>,
        ) -> T {
            time::timeout(timeout, async {
                loop {
                    let event = self
                        .events
                        .recv()
                        .await
                        .expect("backend event channel open");
                    if let Some(value) = matcher(event) {
                        return value;
                    }
                }
            })
            .await
            .expect("timed out waiting for backend event")
        }

        fn abort(self) {
            self.task.abort();
        }
    }

    fn spawn_test_backend(
        name: &str,
        topic: &str,
        listen: Vec<Multiaddr>,
        peer: Vec<Multiaddr>,
    ) -> TestBackend {
        let (commands, command_rx) = mpsc::channel(32);
        let (ui_tx, events) = mpsc::channel(128);
        let config = BackendConfig {
            name: name.to_string(),
            topic: topic.to_string(),
            listen,
            peer,
            relay: Vec::new(),
            no_mdns: true,
        };
        let task = tokio::spawn(run_network(config, command_rx, ui_tx));
        TestBackend {
            commands,
            events,
            task,
        }
    }

    async fn wait_for_local_peer(backend: &mut TestBackend) -> PeerId {
        backend
            .recv_matching(Duration::from_secs(5), |event| match event {
                UiEvent::LocalPeerId(peer_id) => peer_id.parse().ok(),
                _ => None,
            })
            .await
    }

    async fn wait_for_tcp_listen_addr(backend: &mut TestBackend) -> Multiaddr {
        backend
            .recv_matching(Duration::from_secs(5), |event| match event {
                UiEvent::Status(status) => status
                    .strip_prefix("listening on ")
                    .and_then(|address| address.parse::<Multiaddr>().ok())
                    .filter(|address| {
                        address
                            .iter()
                            .any(|protocol| matches!(protocol, libp2p::multiaddr::Protocol::Tcp(_)))
                    }),
                _ => None,
            })
            .await
    }

    async fn wait_for_peer_count(backend: &mut TestBackend, expected: usize) {
        backend
            .recv_matching(Duration::from_secs(8), |event| match event {
                UiEvent::PeerCount(count) if count >= expected => Some(()),
                _ => None,
            })
            .await
    }

    async fn wait_for_history_text(backend: &mut TestBackend, text: &str) {
        backend
            .recv_matching(Duration::from_secs(8), |event| match event {
                UiEvent::History(records) if records.iter().any(|record| record.text == text) => {
                    Some(())
                }
                _ => None,
            })
            .await
    }

    #[test]
    fn insert_record_dedups_and_orders_by_normalized_timestamp() {
        let mut history = Vec::new();
        let mut seen_messages = HashSet::new();

        assert!(insert_record(
            &mut history,
            &mut seen_messages,
            record("later-seconds", 1_700_000_001)
        ));
        assert!(insert_record(
            &mut history,
            &mut seen_messages,
            record("older-micros", 1_700_000_000_250_000)
        ));
        assert!(insert_record(
            &mut history,
            &mut seen_messages,
            record("middle-millis", 1_700_000_000_500)
        ));
        assert!(!insert_record(
            &mut history,
            &mut seen_messages,
            record("middle-millis", 1_700_000_000_500)
        ));

        let ids = history
            .iter()
            .map(|record| record.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["older-micros", "middle-millis", "later-seconds"]);
        assert_eq!(seen_messages.len(), 3);
    }

    #[test]
    fn insert_record_orders_causal_dependencies_before_clock_time() {
        let mut history = Vec::new();
        let mut seen_messages = HashSet::new();
        let mut frontier = BTreeMap::new();
        let joined_at = Some(1_700_000_000_000_000);
        frontier.insert(chat_actor_key("alice", joined_at), 1);

        let alice = ChatRecord {
            id: "alice-1".to_string(),
            peer_id: "alice".to_string(),
            joined_at,
            seq: 1,
            causal_frontier: BTreeMap::new(),
            author: "alice".to_string(),
            text: "question".to_string(),
            sent_at: 1_700_000_000_200_000,
        };
        let bob = ChatRecord {
            id: "bob-1".to_string(),
            peer_id: "bob".to_string(),
            joined_at,
            seq: 1,
            causal_frontier: frontier,
            author: "bob".to_string(),
            text: "answer".to_string(),
            sent_at: 1_700_000_000_100_000,
        };

        assert!(insert_record(&mut history, &mut seen_messages, bob));
        assert!(insert_record(&mut history, &mut seen_messages, alice));

        let ids = history
            .iter()
            .map(|record| record.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(ids, ["alice-1", "bob-1"]);
    }

    #[test]
    fn insert_record_trims_old_history_and_seen_messages() {
        let mut history = Vec::new();
        let mut seen_messages = HashSet::new();
        let base = 1_700_000_000_000_000_i64;

        for index in 0..MAX_MESSAGES + 2 {
            assert!(insert_record(
                &mut history,
                &mut seen_messages,
                record(format!("id-{index}"), base + index as i64)
            ));
        }

        assert_eq!(history.len(), MAX_MESSAGES);
        assert_eq!(
            history.first().map(|record| record.id.as_str()),
            Some("id-2")
        );
        assert!(!seen_messages.contains("id-0"));
        assert!(!seen_messages.contains("id-1"));
        assert!(seen_messages.contains(&format!("id-{}", MAX_MESSAGES + 1)));
    }

    #[test]
    fn peer_address_normalization_appends_or_rejects_peer_ids() {
        let peer = peer_id();
        let other_peer = peer_id();

        let base: Multiaddr = "/ip4/192.0.2.10/tcp/4001".parse().unwrap();
        let normalized = normalize_peer_address(peer, base.clone()).unwrap();
        assert_eq!(peer_id_from_multiaddr(&normalized), Some(peer));

        let wrong_peer: Multiaddr = format!("/ip4/192.0.2.10/tcp/4001/p2p/{other_peer}")
            .parse()
            .unwrap();
        assert!(normalize_peer_address(peer, wrong_peer.clone()).is_none());

        let unspecified: Multiaddr = "/ip4/0.0.0.0/tcp/4001".parse().unwrap();
        assert!(normalize_peer_address(peer, unspecified).is_none());

        let direct = normalize_direct_peer_address(peer, base).unwrap();
        assert_eq!(peer_id_from_multiaddr(&direct), Some(peer));
        assert!(normalize_direct_peer_address(peer, wrong_peer).is_none());

        let relay_addr: Multiaddr =
            format!("/ip4/192.0.2.20/tcp/4001/p2p/{other_peer}/p2p-circuit/p2p/{peer}")
                .parse()
                .unwrap();
        assert!(normalize_direct_peer_address(peer, relay_addr).is_none());
    }

    #[test]
    fn room_peer_set_excludes_rendezvous_nodes_and_includes_local() {
        let local = peer_id();
        let room_peer = peer_id();
        let rendezvous = peer_id();
        let rendezvous_nodes = HashSet::from([rendezvous]);

        let peers = room_peer_ids_from_connected(local, [room_peer, rendezvous], &rendezvous_nodes);

        assert!(peers.contains(&local));
        assert!(peers.contains(&room_peer));
        assert!(!peers.contains(&rendezvous));
        assert_eq!(majority_threshold(peers.len()), 2);
    }

    #[tokio::test]
    async fn peer_names_allow_duplicate_display_aliases() {
        let local = peer_id();
        let alice_one = peer_id();
        let alice_two = peer_id();
        let mut peer_names = HashMap::new();

        remember_peer_name(alice_one, "alice", local, &mut peer_names, Some(1)).await;
        remember_peer_name(alice_two, "alice", local, &mut peer_names, Some(2)).await;

        assert_eq!(peer_names.len(), 2);
        assert_eq!(
            peer_names
                .get(&alice_one.to_string())
                .map(|claim| claim.name.as_str()),
            Some("alice")
        );
        assert_eq!(
            peer_names
                .get(&alice_two.to_string())
                .map(|claim| claim.name.as_str()),
            Some("alice")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_loopback_smoke_syncs_chat_history_between_two_backends() {
        let topic = format!("light-ear.test.{}", current_timestamp_micros());
        let mut alice = spawn_test_backend(
            "alice",
            &topic,
            vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            Vec::new(),
        );
        let alice_peer_id = wait_for_local_peer(&mut alice).await;
        let alice_addr = wait_for_tcp_listen_addr(&mut alice)
            .await
            .with(libp2p::multiaddr::Protocol::P2p(alice_peer_id));

        let mut bob = spawn_test_backend(
            "bob",
            &topic,
            vec!["/ip4/127.0.0.1/tcp/0".parse().unwrap()],
            vec![alice_addr],
        );
        let _bob_peer_id = wait_for_local_peer(&mut bob).await;

        wait_for_peer_count(&mut alice, 1).await;
        wait_for_peer_count(&mut bob, 1).await;

        let text = "hello from loopback smoke";
        alice
            .commands
            .send(NetworkCommand::Chat(text.to_string()))
            .await
            .expect("alice command channel open");

        wait_for_history_text(&mut alice, text).await;
        wait_for_history_text(&mut bob, text).await;

        alice.abort();
        bob.abort();
    }

    #[test]
    fn finished_playback_role_distinguishes_leader_and_follower() {
        let leader = peer_id();
        let follower = peer_id();
        let state = playback_state(leader, true, 0, 1_000_000, 1_000);

        assert_eq!(
            finished_playback_role(&state, leader, 2_000_000, false),
            Some(FinishedPlaybackRole::Leader)
        );
        assert_eq!(
            finished_playback_role(&state, follower, 2_000_000, false),
            Some(FinishedPlaybackRole::Follower)
        );
    }

    #[test]
    fn finished_playback_role_ignores_unfinished_or_inaudible_states() {
        let leader = peer_id();
        let not_done = playback_state(leader, true, 0, 1_000_000, 2_000);
        let future_anchor = playback_state(leader, true, 0, 3_000_000, 1_000);
        let paused_at_end = playback_state(leader, false, 1_000, 1_000_000, 1_000);

        assert_eq!(
            finished_playback_role(&not_done, leader, 2_000_000, false),
            None
        );
        assert_eq!(
            finished_playback_role(&future_anchor, leader, 2_000_000, true),
            None
        );
        assert_eq!(
            finished_playback_role(&paused_at_end, leader, 2_000_000, true),
            None
        );
        assert_eq!(
            finished_playback_role(&not_done, leader, 1_500_000, true),
            Some(FinishedPlaybackRole::Leader)
        );
    }

    #[test]
    fn failed_audio_session_blocks_track_state_but_not_idle_state() {
        let leader = peer_id();
        let mut failed_audio_sessions = HashSet::new();
        failed_audio_sessions.insert("session".to_string());

        let active = playback_state(leader, true, 0, 1_000_000, 1_000);
        assert!(playback_state_uses_failed_audio_session(
            &failed_audio_sessions,
            &active
        ));

        let mut idle = active.clone();
        idle.track = None;
        idle.track_requested_by = None;
        assert!(!playback_state_uses_failed_audio_session(
            &failed_audio_sessions,
            &idle
        ));

        let mut other_session = active;
        other_session.session_id = "other-session".to_string();
        assert!(!playback_state_uses_failed_audio_session(
            &failed_audio_sessions,
            &other_session
        ));
    }

    #[test]
    fn stream_event_matching_requires_current_session_and_track() {
        let leader = peer_id();
        let state = playback_state(leader, true, 0, 1_000_000, 10_000);
        let mut music = MusicState::default();
        let buffer = BufferCoordinator::new();

        music.set_playback_state(state.clone());

        assert!(music_stream_event_matches(&music, "session", "track"));
        assert!(stream_event_matches_current(
            &buffer, &music, None, "session", "track"
        ));
        assert!(!music_stream_event_matches(
            &music,
            "session",
            "other-track"
        ));
        assert!(!music_stream_event_matches(
            &music,
            "other-session",
            "track"
        ));

        let mut idle = state;
        idle.track = None;
        music.set_playback_state(idle);
        assert!(!music_stream_event_matches(&music, "session", "track"));
    }

    #[test]
    fn stream_event_matching_accepts_only_active_buffer_operation() {
        let leader = peer_id();
        let mut buffer = BufferCoordinator::new();
        let music = MusicState::default();
        let state = playback_state(leader, false, 2_000, 1_000_000, 10_000);

        buffer.start_leader_operation(
            "op".to_string(),
            state,
            PlaybackBufferOperationKind::Seek,
            None,
            HashSet::from([leader.to_string()]),
            Instant::now() + Duration::from_secs(10),
        );

        assert!(active_buffer_operation_matches(
            &buffer, "op", "session", "track"
        ));
        assert!(stream_event_matches_current(
            &buffer,
            &music,
            Some("op"),
            "session",
            "track"
        ));
        assert!(!stream_event_matches_current(
            &buffer, &music, None, "session", "track"
        ));
        assert!(!stream_event_matches_current(
            &buffer,
            &music,
            Some("other-op"),
            "session",
            "track"
        ));
        assert!(!active_buffer_operation_matches(
            &buffer, "other-op", "session", "track"
        ));
        assert!(!active_buffer_operation_matches(
            &buffer,
            "op",
            "other-session",
            "track"
        ));
        assert!(!active_buffer_operation_matches(
            &buffer,
            "op",
            "session",
            "other-track"
        ));
        assert!(!stream_event_matches_current(
            &buffer,
            &music,
            Some("op"),
            "other-session",
            "track"
        ));
    }

    #[test]
    fn player_sync_waits_while_same_track_buffer_operation_is_active() {
        let leader = peer_id();
        let mut buffer = BufferCoordinator::new();
        let playback = playback_state(leader, false, 2_000, 1_000_000, 120_000);
        let mut seek_operation = playback.clone();
        seek_operation.position_ms = 60_000;

        buffer.start_leader_operation(
            "op".to_string(),
            seek_operation,
            PlaybackBufferOperationKind::Seek,
            None,
            HashSet::from([leader.to_string()]),
            Instant::now() + Duration::from_secs(10),
        );

        assert!(!should_sync_player_to_playback_state(&buffer, &playback));

        let mut other_track = playback.clone();
        other_track.track = Some(track("other-track", 120_000));
        assert!(should_sync_player_to_playback_state(&buffer, &other_track));

        let mut idle = playback;
        idle.track = None;
        assert!(should_sync_player_to_playback_state(&buffer, &idle));
    }

    #[test]
    fn active_playback_resolves_stale_start_buffer_prepare() {
        let leader = peer_id();
        let playback = playback_state(leader, true, 0, 1_000_000, 120_000);

        assert!(playback_state_resolves_buffer_prepare(
            Some(&playback),
            "session",
            "track",
            &leader.to_string()
        ));
        assert!(!playback_state_resolves_buffer_prepare(
            Some(&playback),
            "other-session",
            "track",
            &leader.to_string()
        ));

        let mut paused = playback.clone();
        paused.playing = false;
        assert!(playback_state_resolves_buffer_prepare(
            Some(&paused),
            "session",
            "track",
            &leader.to_string()
        ));

        let other_leader = peer_id();
        assert!(!playback_state_resolves_buffer_prepare(
            Some(&playback),
            "session",
            "track",
            &other_leader.to_string()
        ));
    }

    #[test]
    fn started_remote_playback_removes_matching_local_queue_item_without_version_bump() {
        let leader = peer_id();
        let requester = leader.to_string();
        let mut music = MusicState::default();
        music.queue_version = 7;
        music.queue_updated_at = 42;

        let mut playback = playback_state(leader, true, 0, 2_000_000, 120_000);
        playback.track_requested_by = Some(requester.clone());
        music.append_queue_item(QueueItem {
            item_id: "played".to_string(),
            requested_by: requester.clone(),
            added_at_micros: 1_000_000,
            track: track("track", 120_000),
        });
        music.append_queue_item(QueueItem {
            item_id: "later-duplicate".to_string(),
            requested_by: requester,
            added_at_micros: 3_000_000,
            track: track("track", 120_000),
        });
        music.append_queue_item(QueueItem {
            item_id: "other".to_string(),
            requested_by: leader.to_string(),
            added_at_micros: 1_500_000,
            track: track("other-track", 120_000),
        });

        assert!(remove_started_playback_queue_item(&mut music, &playback));

        let remaining = music
            .queue
            .iter()
            .map(|item| item.item_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(remaining, vec!["later-duplicate", "other"]);
        assert_eq!(music.queue_version, 7);
        assert_eq!(music.queue_updated_at, 42);
    }

    #[test]
    fn audio_output_error_classification_is_narrow() {
        assert!(is_audio_output_error(&anyhow!(
            "failed to create audio sink after reopening output device"
        )));
        assert!(is_audio_output_error(&anyhow!(
            "failed to configure default audio output"
        )));
        assert!(!is_audio_output_error(&anyhow!(
            "failed to decode streaming audio packet"
        )));
        assert!(!is_audio_output_error(&anyhow!(
            "media range request failed"
        )));
    }

    #[tokio::test]
    async fn local_audio_failure_keeps_room_playback_state_for_convergence() {
        let leader = peer_id();
        let state = playback_state(leader, false, 0, 1_000_000, 1_000);
        let mut music = MusicState::default();
        music.set_remote_playback_prepare(state.clone());
        let mut failed_audio_sessions = HashSet::new();
        let (ui_tx, mut ui_rx) = mpsc::channel(4);

        mark_local_audio_session_failed(
            &mut None,
            &mut music,
            &mut failed_audio_sessions,
            &state,
            "decode failed".to_string(),
            &ui_tx,
        )
        .await;

        assert!(failed_audio_sessions.contains(&state.session_id));
        assert_eq!(
            music
                .playback_state()
                .map(|state| state.session_id.as_str()),
            Some(state.session_id.as_str())
        );
        assert!(!music.can_start_next());
        assert!(matches!(ui_rx.recv().await, Some(UiEvent::Playback(None))));
        assert!(
            matches!(ui_rx.recv().await, Some(UiEvent::Status(status)) if status.contains("decode failed"))
        );
    }

    #[test]
    fn room_publish_error_classification_reports_no_subscribers() {
        assert_eq!(
            classify_room_publish_error(&gossipsub::PublishError::Duplicate),
            RoomPublishPlan::Published
        );
        assert_eq!(
            classify_room_publish_error(&gossipsub::PublishError::NoPeersSubscribedToTopic),
            RoomPublishPlan::NoSubscribers
        );
        assert_eq!(
            classify_room_publish_error(&gossipsub::PublishError::AllQueuesFull(1)),
            RoomPublishPlan::Congested
        );
    }

    #[test]
    fn history_summary_newer_uses_count_or_newest_timestamp() {
        let mut history = vec![
            record("newer", 1_700_000_000_500_000),
            record("older", 1_700_000_000_000_000),
        ];

        assert!(history_summary_is_newer(&history, 3, None));
        assert!(history_summary_is_newer(
            &history,
            2,
            Some(1_700_000_001_000_000)
        ));
        assert!(!history_summary_is_newer(
            &history,
            2,
            Some(1_700_000_000_250_000)
        ));
        assert!(!history_summary_is_newer(&history, 2, None));

        history.clear();
        assert!(history_summary_is_newer(
            &history,
            1,
            Some(1_700_000_000_000_000)
        ));
        assert!(!history_summary_is_newer(
            &history,
            0,
            Some(1_700_000_000_000_000)
        ));
    }

    #[test]
    fn queue_vote_sync_is_needed_only_for_future_queue_versions() {
        let proposer = peer_id();
        let future = VoteProposal {
            vote_id: "vote".to_string(),
            proposer: proposer.to_string(),
            action: VoteAction::Move {
                item_id: "item".to_string(),
                to_index: 0,
            },
            queue_version: 3,
            playback_session_id: None,
            created_at_micros: 1_700_000_000_000_000,
        };
        let current = VoteProposal {
            queue_version: 2,
            ..future.clone()
        };
        let playback = VoteProposal {
            action: VoteAction::Pause,
            queue_version: 4,
            playback_session_id: Some("session".to_string()),
            ..future.clone()
        };

        assert!(queue_vote_needs_newer_state(&future, 2));
        assert!(!queue_vote_needs_newer_state(&current, 2));
        assert!(!queue_vote_needs_newer_state(&playback, 2));
    }

    #[test]
    fn wire_source_validation_accepts_matching_actor_messages() {
        let source = peer_id();
        let proposal = VoteProposal {
            vote_id: "vote".to_string(),
            proposer: source.to_string(),
            action: VoteAction::Skip,
            queue_version: 1,
            playback_session_id: Some("session".to_string()),
            created_at_micros: 1_700_000_000_000_000,
        };

        assert!(
            validate_wire_source(
                &WireMessage::Chat {
                    id: None,
                    peer_id: source.to_string(),
                    joined_at: None,
                    seq: 1,
                    causal_frontier: BTreeMap::new(),
                    name: "alice".to_string(),
                    text: "hello".to_string(),
                    sent_at: 1_700_000_000_000_000,
                },
                source
            )
            .is_ok()
        );
        assert!(
            validate_wire_source(
                &WireMessage::Chat {
                    id: None,
                    peer_id: String::new(),
                    joined_at: None,
                    seq: 1,
                    causal_frontier: BTreeMap::new(),
                    name: "alice".to_string(),
                    text: "hello".to_string(),
                    sent_at: 1_700_000_000_000_000,
                },
                source
            )
            .is_ok()
        );
        assert!(
            validate_wire_source(&WireMessage::VoteProposal { proposal, nonce: 1 }, source).is_ok()
        );
        assert!(
            validate_wire_source(
                &WireMessage::VoteBallot {
                    vote_id: "vote".to_string(),
                    peer_id: source.to_string(),
                    approve: true,
                    nonce: 1,
                },
                source
            )
            .is_ok()
        );
        assert!(
            validate_wire_source(
                &WireMessage::PlaybackBufferPrepare {
                    operation_id: "op".to_string(),
                    session_id: "session".to_string(),
                    track_id: "track".to_string(),
                    position_ms: 0,
                    kind: PlaybackBufferOperationKind::Start,
                    expected_peers: vec![source.to_string()],
                    leader_peer_id: source.to_string(),
                    nonce: 1,
                },
                source,
            )
            .is_ok()
        );
        assert!(
            validate_wire_source(
                &WireMessage::PlaybackBufferStatus {
                    operation_id: "op".to_string(),
                    session_id: "session".to_string(),
                    peer_id: source.to_string(),
                    status: PlaybackBufferStatusKind::Ready,
                    buffered_until_ms: Some(12_000),
                    error: None,
                    nonce: 1,
                },
                source,
            )
            .is_ok()
        );
        assert!(
            validate_wire_source(
                &WireMessage::PlaybackBufferHealth {
                    session_id: "session".to_string(),
                    peer_id: source.to_string(),
                    status: PlaybackBufferStatusKind::Ready,
                    buffered_until_ms: Some(12_000),
                    nonce: 1,
                },
                source,
            )
            .is_ok()
        );
    }

    #[test]
    fn wire_source_validation_rejects_mismatched_actor_messages() {
        let source = peer_id();
        let other = peer_id();
        let proposal = VoteProposal {
            vote_id: "vote".to_string(),
            proposer: other.to_string(),
            action: VoteAction::Skip,
            queue_version: 1,
            playback_session_id: Some("session".to_string()),
            created_at_micros: 1_700_000_000_000_000,
        };
        let playback = PlaybackState {
            session_id: "session".to_string(),
            leader_peer_id: other.to_string(),
            track: None,
            track_requested_by: None,
            state_version: 1,
            issued_at_micros: 1_700_000_000_000_000,
            playing: false,
            position_ms: 0,
            anchor_time_micros: 1_700_000_000_000_000,
            rate: 1.0,
        };
        let queue = QueueState {
            version: 1,
            updated_at_micros: 1_700_000_000_000_000,
            updated_by: other.to_string(),
            items: Vec::new(),
        };

        assert_eq!(
            validate_wire_source(
                &WireMessage::NameClaim {
                    peer_id: other.to_string(),
                    name: "mallory".to_string(),
                    joined_at: None,
                    nonce: 1,
                },
                source,
            ),
            Err("name claim peer_id does not match source")
        );
        assert_eq!(
            validate_wire_source(&WireMessage::VoteProposal { proposal, nonce: 1 }, source),
            Err("vote proposer does not match source")
        );
        assert_eq!(
            validate_wire_source(
                &WireMessage::PlaybackReady {
                    session_id: "session".to_string(),
                    peer_id: other.to_string(),
                    nonce: 1,
                },
                source,
            ),
            Err("playback ready peer does not match source")
        );
        assert_eq!(
            validate_wire_source(
                &WireMessage::PlaybackState {
                    state: playback,
                    nonce: 1,
                },
                source,
            ),
            Err("playback leader does not match source")
        );
        assert_eq!(
            validate_wire_source(
                &WireMessage::QueueState {
                    state: queue,
                    nonce: 1
                },
                source
            ),
            Err("queue state updated_by does not match source")
        );
        assert_eq!(
            validate_wire_source(
                &WireMessage::PlaybackBufferPrepare {
                    operation_id: "op".to_string(),
                    session_id: "session".to_string(),
                    track_id: "track".to_string(),
                    position_ms: 0,
                    kind: PlaybackBufferOperationKind::Seek,
                    expected_peers: vec![source.to_string()],
                    leader_peer_id: other.to_string(),
                    nonce: 1,
                },
                source,
            ),
            Err("playback buffer leader does not match source")
        );
        assert_eq!(
            validate_wire_source(
                &WireMessage::PlaybackBufferStatus {
                    operation_id: "op".to_string(),
                    session_id: "session".to_string(),
                    peer_id: other.to_string(),
                    status: PlaybackBufferStatusKind::Ready,
                    buffered_until_ms: Some(12_000),
                    error: None,
                    nonce: 1,
                },
                source,
            ),
            Err("playback buffer status peer does not match source")
        );
        assert_eq!(
            validate_wire_source(
                &WireMessage::PlaybackBufferHealth {
                    session_id: "session".to_string(),
                    peer_id: other.to_string(),
                    status: PlaybackBufferStatusKind::Ready,
                    buffered_until_ms: Some(12_000),
                    nonce: 1,
                },
                source,
            ),
            Err("playback buffer health peer does not match source")
        );
    }

    #[test]
    fn wire_source_validation_allows_history_response_records_from_other_peers() {
        let source = peer_id();
        let other = peer_id();
        let mut record = record("from-other", 1_700_000_000_000_000);
        record.peer_id = other.to_string();

        assert!(
            validate_wire_source(
                &WireMessage::HistoryResponse {
                    target: Some(source.to_string()),
                    messages: vec![record],
                    nonce: 1,
                },
                source,
            )
            .is_ok()
        );
    }

    #[test]
    fn direct_promotion_backoff_tiers_and_suspends() {
        let mut backoff = DirectPromotionBackoff::default();
        let mut now = Instant::now();

        assert!(matches!(
            backoff.mark_failure(now),
            DirectPromotionFailureOutcome::Counted {
                failures: 1,
                retry_after: Some(DIRECT_PROMOTION_RETRY_INTERVAL),
            }
        ));
        assert!(matches!(
            backoff.mark_failure(now + Duration::from_secs(1)),
            DirectPromotionFailureOutcome::Duplicate
        ));

        for expected_failures in 2..=DIRECT_PROMOTION_MAX_FAILURES {
            now += DIRECT_PROMOTION_FAILURE_DEDUP_WINDOW + Duration::from_secs(1);
            let outcome = backoff.mark_failure(now);
            let expected_retry_after = match expected_failures {
                DIRECT_PROMOTION_MAX_FAILURES => None,
                0..DIRECT_PROMOTION_MEDIUM_RETRY_FAILURES => Some(DIRECT_PROMOTION_RETRY_INTERVAL),
                DIRECT_PROMOTION_MEDIUM_RETRY_FAILURES..DIRECT_PROMOTION_SLOW_RETRY_FAILURES => {
                    Some(DIRECT_PROMOTION_MEDIUM_RETRY_INTERVAL)
                }
                _ => Some(DIRECT_PROMOTION_SLOW_RETRY_INTERVAL),
            };

            assert!(matches!(
                outcome,
                DirectPromotionFailureOutcome::Counted {
                    failures,
                    retry_after,
                } if failures == expected_failures && retry_after == expected_retry_after
            ));
        }

        assert_eq!(backoff.failures, DIRECT_PROMOTION_MAX_FAILURES);
        assert!(!backoff.should_attempt(now));
    }
}
