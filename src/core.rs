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
pub struct PeerNameView {
    pub peer_id: String,
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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackCacheStatus {
    Preparing,
    Ready,
    Buffering,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaybackCacheView {
    pub session_id: String,
    pub track_id: String,
    pub status: PlaybackCacheStatus,
    pub buffered_until_ms: u64,
    pub duration_ms: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackBufferOperationKind {
    Start,
    Seek,
    Resume,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PlaybackBufferStatusKind {
    Ready,
    Buffering,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaybackBufferView {
    pub operation_id: String,
    pub session_id: String,
    pub track_id: String,
    pub kind: PlaybackBufferOperationKind,
    pub local_status: PlaybackBufferStatusKind,
    pub ready: usize,
    pub buffering: usize,
    pub failed: usize,
    pub threshold: usize,
    pub eligible_peers: usize,
    pub position_ms: u64,
    pub buffered_until_ms: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteView {
    pub vote_id: String,
    pub proposer: String,
    pub action_label: String,
    pub approvals: usize,
    pub rejections: usize,
    pub threshold: usize,
    pub eligible_peers: usize,
    pub pending: usize,
    pub local_vote: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerConnectionView {
    pub peer_id: String,
    pub kind: String,
    pub route: String,
    pub direct_connections: usize,
    pub relayed_connections: usize,
    pub direct_address_count: usize,
    pub chat_subscribed: bool,
    pub direct_promotion_attempts: u32,
    pub direct_promotion_failures: u32,
    pub direct_promotion_in_flight: bool,
    pub direct_promotion_suspended: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum FrontendEvent {
    Status(String),
    PeerCount(usize),
    LocalPeerId(String),
    History(Vec<ChatRecord>),
    Playback(Option<PlaybackView>),
    PlaybackCache(Option<PlaybackCacheView>),
    PlaybackBuffer(Option<PlaybackBufferView>),
    Queue(QueueState),
    Vote(Option<VoteView>),
    Peers(Vec<PeerConnectionView>),
    PeerNames(Vec<PeerNameView>),
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
    PlaybackBufferPrepare {
        operation_id: String,
        session_id: String,
        track_id: String,
        position_ms: u64,
        kind: PlaybackBufferOperationKind,
        expected_peers: Vec<String>,
        leader_peer_id: String,
        #[serde(default)]
        nonce: u64,
    },
    PlaybackBufferStatus {
        operation_id: String,
        session_id: String,
        peer_id: String,
        status: PlaybackBufferStatusKind,
        buffered_until_ms: Option<u64>,
        error: Option<String>,
        #[serde(default)]
        nonce: u64,
    },
    PlaybackBufferHealth {
        session_id: String,
        peer_id: String,
        status: PlaybackBufferStatusKind,
        buffered_until_ms: Option<u64>,
        #[serde(default)]
        nonce: u64,
    },
    PlaybackBufferCancel {
        operation_id: String,
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
    SetVolume(u8),
    Skip,
    RemoveQueueItem(usize),
    MoveQueueItem {
        from: usize,
        to: usize,
    },
    Vote(bool),
    Shutdown,
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum VoteTerminalOutcome {
    Pending,
    Passed,
    Rejected,
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

    pub fn vote(&mut self, peer_id: String, approve: bool) -> bool {
        if self.approvals.contains(&peer_id) || self.rejections.contains(&peer_id) {
            return false;
        }

        if approve {
            self.approvals.insert(peer_id)
        } else {
            self.rejections.insert(peer_id)
        }
    }

    pub fn approval_count(&self) -> usize {
        self.approvals.len()
    }

    pub fn rejection_count(&self) -> usize {
        self.rejections.len()
    }

    pub fn local_vote(&self, peer_id: &str) -> Option<bool> {
        if self.approvals.contains(peer_id) {
            Some(true)
        } else if self.rejections.contains(peer_id) {
            Some(false)
        } else {
            None
        }
    }

    pub fn pending_count(&self, eligible_peers: usize) -> usize {
        eligible_peers.saturating_sub(self.approvals.len() + self.rejections.len())
    }

    pub fn terminal_outcome(&self, threshold: usize, eligible_peers: usize) -> VoteTerminalOutcome {
        if self.approvals.len() >= threshold {
            return VoteTerminalOutcome::Passed;
        }

        if self
            .approvals
            .len()
            .saturating_add(self.pending_count(eligible_peers))
            < threshold
        {
            return VoteTerminalOutcome::Rejected;
        }

        VoteTerminalOutcome::Pending
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tokio::time::{Duration, Instant};

    use super::*;

    fn playback_state() -> PlaybackState {
        PlaybackState {
            session_id: "session".to_string(),
            leader_peer_id: "leader".to_string(),
            track: None,
            track_requested_by: None,
            state_version: 1,
            issued_at_micros: 1_700_000_000_000_000,
            playing: false,
            position_ms: 0,
            anchor_time_micros: 1_700_000_000_000_000,
            rate: 1.0,
        }
    }

    #[test]
    fn normalize_timestamp_micros_handles_seconds_millis_and_micros() {
        assert_eq!(
            normalize_timestamp_micros(1_700_000_000),
            1_700_000_000_000_000
        );
        assert_eq!(
            normalize_timestamp_micros(1_700_000_000_123),
            1_700_000_000_123_000
        );
        assert_eq!(
            normalize_timestamp_micros(1_700_000_000_123_456),
            1_700_000_000_123_456
        );
    }

    #[test]
    fn format_duration_ms_uses_total_minutes() {
        assert_eq!(format_duration_ms(65_000), "01:05");
        assert_eq!(format_duration_ms(3_665_000), "61:05");
    }

    #[test]
    fn pending_playback_tracks_expected_ready_subset() {
        let expected = HashSet::from(["alice".to_string(), "bob".to_string()]);
        let mut pending = PendingPlayback::new(
            playback_state(),
            expected,
            Instant::now() + Duration::from_secs(1),
        );

        assert_eq!(pending.expected_count(), 2);
        assert_eq!(pending.ready_count(), 0);
        assert!(!pending.is_ready());
        assert!(!pending.mark_ready("carol".to_string()));

        assert!(pending.mark_ready("alice".to_string()));
        assert_eq!(pending.ready_count(), 1);
        assert!(!pending.is_ready());

        assert!(pending.mark_ready("bob".to_string()));
        assert_eq!(pending.ready_count(), 2);
        assert!(pending.is_ready());
    }

    #[test]
    fn active_vote_accepts_only_one_ballot_per_peer() {
        let proposal = VoteProposal {
            vote_id: "vote-1".to_string(),
            proposer: "alice".to_string(),
            action: VoteAction::Pause,
            queue_version: 1,
            playback_session_id: Some("session".to_string()),
            created_at_micros: 1_700_000_000_000_000,
        };
        let mut vote = ActiveVote::new(proposal, Instant::now() + Duration::from_secs(1));

        assert!(vote.vote("bob".to_string(), true));
        assert_eq!(vote.approval_count(), 1);
        assert_eq!(vote.rejections.len(), 0);

        assert!(!vote.vote("bob".to_string(), false));
        assert_eq!(vote.approval_count(), 1);
        assert_eq!(vote.rejections.len(), 0);

        assert!(vote.vote("carol".to_string(), false));
        assert_eq!(vote.approval_count(), 1);
        assert_eq!(vote.rejections.len(), 1);
        assert_eq!(vote.local_vote("bob"), Some(true));
        assert_eq!(vote.local_vote("carol"), Some(false));
        assert_eq!(vote.local_vote("dave"), None);
    }

    #[test]
    fn active_vote_terminal_outcome_detects_pass_and_impossible_pass() {
        let proposal = VoteProposal {
            vote_id: "vote-1".to_string(),
            proposer: "alice".to_string(),
            action: VoteAction::Pause,
            queue_version: 1,
            playback_session_id: Some("session".to_string()),
            created_at_micros: 1_700_000_000_000_000,
        };
        let mut vote = ActiveVote::new(proposal, Instant::now() + Duration::from_secs(1));

        assert!(vote.vote("alice".to_string(), true));
        assert_eq!(vote.terminal_outcome(2, 3), VoteTerminalOutcome::Pending);

        assert!(vote.vote("bob".to_string(), false));
        assert_eq!(vote.terminal_outcome(2, 3), VoteTerminalOutcome::Pending);

        assert!(vote.vote("carol".to_string(), false));
        assert_eq!(vote.terminal_outcome(2, 3), VoteTerminalOutcome::Rejected);

        let proposal = VoteProposal {
            vote_id: "vote-2".to_string(),
            proposer: "alice".to_string(),
            action: VoteAction::Pause,
            queue_version: 1,
            playback_session_id: Some("session".to_string()),
            created_at_micros: 1_700_000_000_000_000,
        };
        let mut vote = ActiveVote::new(proposal, Instant::now() + Duration::from_secs(1));
        assert!(vote.vote("alice".to_string(), true));
        assert!(vote.vote("bob".to_string(), true));
        assert_eq!(vote.terminal_outcome(2, 3), VoteTerminalOutcome::Passed);
    }
}
