use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use tokio::time::Instant;

pub const MAX_MESSAGES: usize = 300;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatRecord {
    pub id: String,
    #[serde(default)]
    pub peer_id: String,
    #[serde(default)]
    pub joined_at: Option<i64>,
    pub author: String,
    pub text: String,
    pub sent_at: i64,
}

#[derive(Debug, Clone)]
pub struct PeerNameClaim {
    pub name: String,
    pub joined_at: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaybackTrack {
    pub track_id: String,
    pub title: String,
    pub source_kind: String,
    pub bvid: String,
    pub part: usize,
    pub duration_ms: u64,
    pub audio_url: String,
    pub referer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueItem {
    pub item_id: String,
    pub track: PlaybackTrack,
    pub requested_by: String,
    pub added_at_micros: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueState {
    pub version: u64,
    pub updated_at_micros: i64,
    pub updated_by: String,
    pub items: Vec<QueueItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaybackState {
    #[serde(default)]
    pub session_id: String,
    pub leader_peer_id: String,
    pub track: Option<PlaybackTrack>,
    #[serde(default)]
    pub track_requested_by: Option<String>,
    pub state_version: u64,
    #[serde(default)]
    pub issued_at_micros: i64,
    pub playing: bool,
    pub position_ms: u64,
    pub anchor_time_micros: i64,
    pub rate: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum VoteAction {
    Pause,
    Resume,
    Skip,
    Seek { position_ms: u64 },
    Remove { item_id: String },
    Move { item_id: String, to_index: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteProposal {
    pub vote_id: String,
    pub proposer: String,
    pub action: VoteAction,
    pub queue_version: u64,
    pub playback_session_id: Option<String>,
    pub created_at_micros: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaybackView {
    pub title: String,
    pub playing: bool,
    pub position_ms: u64,
    pub duration_ms: u64,
    pub leader_peer_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteView {
    pub vote_id: String,
    pub proposer: String,
    pub action_label: String,
    pub approvals: usize,
    pub rejections: usize,
    pub threshold: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum FrontendEvent {
    Status(String),
    PeerCount(usize),
    LocalPeerId(String),
    History(Vec<ChatRecord>),
    Playback(Option<PlaybackView>),
    Queue(QueueState),
    Vote(Option<VoteView>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WireMessage {
    Chat {
        #[serde(default)]
        id: Option<String>,
        #[serde(default)]
        peer_id: String,
        #[serde(default)]
        joined_at: Option<i64>,
        name: String,
        text: String,
        sent_at: i64,
    },
    NameClaim {
        peer_id: String,
        name: String,
        #[serde(default)]
        joined_at: Option<i64>,
        #[serde(default)]
        nonce: u64,
    },
    HistorySummary {
        peer_id: String,
        count: usize,
        newest_at: Option<i64>,
        #[serde(default)]
        nonce: u64,
    },
    HistoryRequest {
        requester: String,
        target: String,
        known_count: usize,
        #[serde(default)]
        nonce: u64,
    },
    HistoryResponse {
        target: Option<String>,
        messages: Vec<ChatRecord>,
        #[serde(default)]
        nonce: u64,
    },
    PlaybackState {
        state: PlaybackState,
        #[serde(default)]
        nonce: u64,
    },
    PlaybackPrepare {
        state: PlaybackState,
        expected_peers: Vec<String>,
        #[serde(default)]
        nonce: u64,
    },
    PlaybackReady {
        session_id: String,
        peer_id: String,
        #[serde(default)]
        nonce: u64,
    },
    PlaybackCancel {
        session_id: String,
        leader_peer_id: String,
        reason: String,
        #[serde(default)]
        nonce: u64,
    },
    QueueState {
        state: QueueState,
        #[serde(default)]
        nonce: u64,
    },
    QueueSummary {
        peer_id: String,
        version: u64,
        updated_at_micros: i64,
        item_count: usize,
        #[serde(default)]
        nonce: u64,
    },
    QueueRequest {
        requester: String,
        target: String,
        known_version: u64,
        known_updated_at_micros: i64,
        #[serde(default)]
        nonce: u64,
    },
    QueueResponse {
        target: String,
        state: QueueState,
        #[serde(default)]
        nonce: u64,
    },
    VoteProposal {
        proposal: VoteProposal,
        #[serde(default)]
        nonce: u64,
    },
    VoteBallot {
        vote_id: String,
        peer_id: String,
        approve: bool,
        #[serde(default)]
        nonce: u64,
    },
}

#[derive(Debug)]
pub enum NetworkCommand {
    Chat(String),
    EnqueueBilibili {
        bvid: String,
        part: usize,
        position: Option<usize>,
    },
    ShowQueue,
    Pause,
    Resume,
    Seek(u64),
    Skip,
    RemoveQueueItem(usize),
    MoveQueueItem {
        from: usize,
        to: usize,
    },
    Vote(bool),
}

pub struct PendingPlayback {
    pub state: PlaybackState,
    pub expected_peers: HashSet<String>,
    pub ready_peers: HashSet<String>,
    pub deadline: Instant,
}

pub struct ActiveVote {
    pub proposal: VoteProposal,
    pub approvals: HashSet<String>,
    pub rejections: HashSet<String>,
    pub deadline: Instant,
}

impl PendingPlayback {
    pub fn new(state: PlaybackState, expected_peers: HashSet<String>, deadline: Instant) -> Self {
        Self {
            state,
            expected_peers,
            ready_peers: HashSet::new(),
            deadline,
        }
    }

    pub fn mark_ready(&mut self, peer_id: String) -> bool {
        if self.expected_peers.contains(&peer_id) {
            self.ready_peers.insert(peer_id)
        } else {
            false
        }
    }

    pub fn ready_count(&self) -> usize {
        self.ready_peers.len()
    }

    pub fn expected_count(&self) -> usize {
        self.expected_peers.len()
    }

    pub fn is_ready(&self) -> bool {
        self.expected_peers.is_subset(&self.ready_peers)
    }
}

impl ActiveVote {
    pub fn new(proposal: VoteProposal, deadline: Instant) -> Self {
        Self {
            proposal,
            approvals: HashSet::new(),
            rejections: HashSet::new(),
            deadline,
        }
    }

    pub fn vote(&mut self, peer_id: String, approve: bool) {
        self.approvals.remove(&peer_id);
        self.rejections.remove(&peer_id);
        if approve {
            self.approvals.insert(peer_id);
        } else {
            self.rejections.insert(peer_id);
        }
    }

    pub fn approval_count(&self) -> usize {
        self.approvals.len()
    }
}

pub fn normalize_timestamp_micros(timestamp: i64) -> i64 {
    let abs = timestamp.saturating_abs();
    if abs < TIMESTAMP_SECONDS_CUTOFF {
        timestamp.saturating_mul(1_000_000)
    } else if abs < TIMESTAMP_MILLIS_CUTOFF {
        timestamp.saturating_mul(1_000)
    } else {
        timestamp
    }
}

pub fn format_duration_ms(duration_ms: u64) -> String {
    let total_seconds = duration_ms / 1000;
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    format!("{minutes:02}:{seconds:02}")
}

const TIMESTAMP_SECONDS_CUTOFF: i64 = 10_000_000_000;
const TIMESTAMP_MILLIS_CUTOFF: i64 = 10_000_000_000_000;
