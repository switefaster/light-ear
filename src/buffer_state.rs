use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Duration;

use tokio::time::Instant;

use crate::core::{
    PlaybackBufferOperationKind, PlaybackBufferStatusKind, PlaybackBufferView, PlaybackState,
};
use crate::music_state::majority_threshold;

#[derive(Debug, Clone)]
pub(crate) struct BufferOperation {
    pub(crate) operation_id: String,
    pub(crate) state: PlaybackState,
    pub(crate) kind: PlaybackBufferOperationKind,
    pub(crate) queue_item_id: Option<String>,
    pub(crate) expected_peers: HashSet<String>,
    pub(crate) statuses: HashMap<String, BufferPeerStatus>,
    pub(crate) deadline: Instant,
    prepare_published_at: Option<Instant>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BufferPeerStatus {
    pub(crate) status: PlaybackBufferStatusKind,
    pub(crate) buffered_until_ms: Option<u64>,
    pub(crate) error: Option<String>,
}

#[derive(Debug, Default)]
pub(crate) struct BufferCoordinator {
    active: Option<BufferOperation>,
    retired: VecDeque<(String, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum BufferStatusOutcome {
    Accepted(BufferQuorum),
    Ignored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BufferQuorum {
    Waiting {
        ready: usize,
        buffering: usize,
        failed: usize,
        threshold: usize,
    },
    Ready {
        ready: usize,
        threshold: usize,
    },
    Impossible {
        ready: usize,
        failed: usize,
        threshold: usize,
    },
    TimedOut {
        ready: usize,
        threshold: usize,
    },
}

impl BufferCoordinator {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn active(&self) -> Option<&BufferOperation> {
        self.active.as_ref()
    }

    pub(crate) fn is_retired(&self, leader: &str, id: &str) -> bool {
        self.retired
            .iter()
            .any(|(peer, op)| peer == leader && op == id)
    }

    pub(crate) fn retire(&mut self, leader: String, id: String) {
        if !self.is_retired(&leader, &id) {
            if self.retired.len() == 64 {
                self.retired.pop_front();
            }
            self.retired.push_back((leader, id));
        }
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active.is_some()
    }

    pub(crate) fn start_leader_operation(
        &mut self,
        operation_id: String,
        state: PlaybackState,
        kind: PlaybackBufferOperationKind,
        queue_item_id: Option<String>,
        expected_peers: HashSet<String>,
        deadline: Instant,
    ) -> &BufferOperation {
        self.clear();
        self.active = Some(BufferOperation {
            operation_id,
            state,
            kind,
            queue_item_id,
            expected_peers,
            statuses: HashMap::new(),
            deadline,
            prepare_published_at: None,
        });
        self.active.as_ref().expect("operation was just inserted")
    }

    pub(crate) fn receive_remote_operation(
        &mut self,
        operation_id: String,
        queue_item_id: Option<String>,
        state: PlaybackState,
        kind: PlaybackBufferOperationKind,
        expected_peers: HashSet<String>,
        timeout: Duration,
        now: Instant,
    ) -> &BufferOperation {
        if self.active.as_ref().is_some_and(|operation| {
            operation.operation_id == operation_id && operation.state.session_id == state.session_id
        }) {
            return self.active.as_ref().expect("operation exists");
        }

        self.clear();
        self.active = Some(BufferOperation {
            operation_id,
            state,
            kind,
            queue_item_id,
            expected_peers,
            statuses: HashMap::new(),
            deadline: now + timeout,
            prepare_published_at: None,
        });
        self.active.as_ref().expect("operation was just inserted")
    }

    pub(crate) fn status_for(
        &self,
        operation_id: &str,
        session_id: &str,
        peer_id: &str,
    ) -> Option<BufferPeerStatus> {
        let operation = self.active.as_ref()?;
        (operation.operation_id == operation_id && operation.state.session_id == session_id)
            .then(|| operation.statuses.get(peer_id).cloned())
            .flatten()
    }

    pub(crate) fn status_matches(
        &self,
        operation_id: &str,
        session_id: &str,
        peer_id: &str,
        status: &BufferPeerStatus,
    ) -> bool {
        let Some(operation) = self.active.as_ref() else {
            return false;
        };
        if operation.operation_id != operation_id || operation.state.session_id != session_id {
            return false;
        }

        operation.statuses.get(peer_id) == Some(status)
    }

    pub(crate) fn remove_expected_peer(
        &mut self,
        peer_id: &str,
        now: Instant,
    ) -> Option<BufferQuorum> {
        let operation = self.active.as_mut()?;
        let removed_expected = operation.expected_peers.remove(peer_id);
        let removed_status = operation.statuses.remove(peer_id).is_some();
        if removed_expected || removed_status {
            Some(operation.quorum(now))
        } else {
            None
        }
    }

    pub(crate) fn mark_prepare_published(&mut self, operation_id: &str, now: Instant) -> bool {
        let Some(operation) = self.active.as_mut() else {
            return false;
        };
        if operation.operation_id != operation_id {
            return false;
        }

        operation.prepare_published_at = Some(now);
        true
    }

    pub(crate) fn prepare_republish_due(
        &mut self,
        now: Instant,
        interval: Duration,
    ) -> Option<BufferOperation> {
        let operation = self.active.as_mut()?;
        let due = operation
            .prepare_published_at
            .is_none_or(|last| now.saturating_duration_since(last) >= interval);
        if !due {
            return None;
        }

        operation.prepare_published_at = Some(now);
        Some(operation.clone())
    }

    pub(crate) fn mark_status(
        &mut self,
        operation_id: &str,
        session_id: &str,
        peer_id: &str,
        status: PlaybackBufferStatusKind,
        buffered_until_ms: Option<u64>,
        error: Option<String>,
        now: Instant,
    ) -> BufferStatusOutcome {
        let Some(operation) = self.active.as_mut() else {
            return BufferStatusOutcome::Ignored;
        };
        if operation.operation_id != operation_id || operation.state.session_id != session_id {
            return BufferStatusOutcome::Ignored;
        }
        if !operation.expected_peers.contains(peer_id) {
            return BufferStatusOutcome::Ignored;
        }

        operation.statuses.insert(
            peer_id.to_string(),
            BufferPeerStatus {
                status,
                buffered_until_ms,
                error,
            },
        );
        BufferStatusOutcome::Accepted(operation.quorum(now))
    }

    pub(crate) fn quorum(&self, now: Instant) -> Option<BufferQuorum> {
        self.active.as_ref().map(|operation| operation.quorum(now))
    }

    pub(crate) fn cancel(&mut self, operation_id: &str) -> Option<BufferOperation> {
        if self
            .active
            .as_ref()
            .is_some_and(|operation| operation.operation_id == operation_id)
        {
            self.clear()
        } else {
            None
        }
    }

    pub(crate) fn clear(&mut self) -> Option<BufferOperation> {
        let op = self.active.take()?;
        self.retire(op.state.leader_peer_id.clone(), op.operation_id.clone());
        Some(op)
    }

    pub(crate) fn clear_if_local_leader(&mut self, local_peer_id: &str) -> Option<BufferOperation> {
        if self
            .active
            .as_ref()
            .is_some_and(|operation| operation.state.leader_peer_id == local_peer_id)
        {
            self.clear()
        } else {
            None
        }
    }

    pub(crate) fn clear_matching_playback_state(
        &mut self,
        state: &PlaybackState,
    ) -> Option<BufferOperation> {
        if self
            .active
            .as_ref()
            .is_some_and(|operation| operation.is_resolved_by_playback_state(state))
        {
            self.clear()
        } else {
            None
        }
    }
}

impl BufferOperation {
    /// Both room trees can deliver the pre-seek pause after the new prepare.
    pub(crate) fn precedes_commit(&self, state: &PlaybackState) -> bool {
        self.state.session_id == state.session_id
            && self.state.leader_peer_id == state.leader_peer_id
            && self
                .state
                .track
                .as_ref()
                .zip(state.track.as_ref())
                .is_some_and(|(a, b)| a.track_id == b.track_id)
            && if self.state.state_version == 0 {
                // Older senders omitted the planned version.
                !state.playing
            } else {
                state.state_version < self.state.state_version
            }
    }

    fn is_resolved_by_playback_state(&self, state: &PlaybackState) -> bool {
        if self.state.session_id != state.session_id || self.precedes_commit(state) {
            return false;
        }
        let Some(track) = state.track.as_ref() else {
            return true;
        };
        if !state.playing {
            return false;
        }

        self.state
            .track
            .as_ref()
            .is_some_and(|operation_track| operation_track.track_id == track.track_id)
    }

    pub(crate) fn quorum(&self, now: Instant) -> BufferQuorum {
        let ready = self.count(PlaybackBufferStatusKind::Ready);
        let buffering = self.count(PlaybackBufferStatusKind::Buffering);
        let failed = self.count(PlaybackBufferStatusKind::Failed);
        let threshold = majority_threshold(self.expected_peers.len());

        if now >= self.deadline {
            return BufferQuorum::TimedOut { ready, threshold };
        }
        if ready >= threshold {
            return BufferQuorum::Ready { ready, threshold };
        }

        let remaining = self.expected_peers.len().saturating_sub(ready + failed);
        if ready + remaining < threshold {
            return BufferQuorum::Impossible {
                ready,
                failed,
                threshold,
            };
        }

        BufferQuorum::Waiting {
            ready,
            buffering,
            failed,
            threshold,
        }
    }

    pub(crate) fn view(&self, local_peer_id: &str, now: Instant) -> PlaybackBufferView {
        let ready = self.count(PlaybackBufferStatusKind::Ready);
        let buffering = self.count(PlaybackBufferStatusKind::Buffering);
        let failed = self.count(PlaybackBufferStatusKind::Failed);
        let local = self.statuses.get(local_peer_id);
        let threshold = match self.quorum(now) {
            BufferQuorum::Waiting { threshold, .. }
            | BufferQuorum::Ready { threshold, .. }
            | BufferQuorum::Impossible { threshold, .. }
            | BufferQuorum::TimedOut { threshold, .. } => threshold,
        };

        PlaybackBufferView {
            queue_item_id: self.queue_item_id.clone(),
            title: self
                .state
                .track
                .as_ref()
                .map(|track| track.title.clone())
                .unwrap_or_default(),
            operation_id: self.operation_id.clone(),
            session_id: self.state.session_id.clone(),
            track_id: self
                .state
                .track
                .as_ref()
                .map_or_else(String::new, |track| track.track_id.clone()),
            kind: self.kind.clone(),
            local_status: local
                .map(|status| status.status.clone())
                .unwrap_or(PlaybackBufferStatusKind::Buffering),
            ready,
            buffering,
            failed,
            threshold,
            eligible_peers: self.expected_peers.len(),
            position_ms: self.state.position_ms,
            buffered_until_ms: local.and_then(|status| status.buffered_until_ms),
            error: local.and_then(|status| status.error.clone()),
        }
    }

    fn count(&self, status: PlaybackBufferStatusKind) -> usize {
        self.statuses
            .values()
            .filter(|peer_status| peer_status.status == status)
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state(session_id: &str) -> PlaybackState {
        PlaybackState {
            session_id: session_id.to_string(),
            leader_peer_id: "leader".to_string(),
            track: None,
            track_requested_by: None,
            state_version: 1,
            issued_at_micros: 1,
            playing: false,
            position_ms: 42,
            anchor_time_micros: 1,
            rate: 1.0,
        }
    }

    fn track_state(session_id: &str, playing: bool) -> PlaybackState {
        PlaybackState {
            track: Some(crate::core::PlaybackTrack {
                track_id: "track".to_string(),
                title: "Track".to_string(),
                source_kind: "bilibili".to_string(),
                bvid: "BV1234567890".to_string(),
                part: 1,
                duration_ms: 120_000,
                audio_url: "https://example.test/audio.m4a".to_string(),
                referer: "https://www.bilibili.com/video/BV1234567890".to_string(),
                media_init_range: None,
                media_index_range: None,
                media_is_progressive: false,
                local_audio: None,
            }),
            playing,
            ..state(session_id)
        }
    }

    fn peers<const N: usize>(ids: [&str; N]) -> HashSet<String> {
        ids.into_iter().map(str::to_string).collect()
    }

    #[test]
    fn clear_if_local_leader_does_not_clear_remote_operation() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        let mut local_state = state("local-session");
        local_state.leader_peer_id = "local".to_string();
        coordinator.start_leader_operation(
            "local-op".to_string(),
            local_state,
            PlaybackBufferOperationKind::Start,
            Some("queue-item".to_string()),
            peers(["local", "remote"]),
            now + Duration::from_secs(10),
        );

        let cleared = coordinator.clear_if_local_leader("local").unwrap();
        assert_eq!(cleared.operation_id, "local-op");
        assert!(coordinator.is_retired("local", "local-op"));
        assert!(!coordinator.is_retired("other", "local-op"));
        assert!(!coordinator.is_active());

        coordinator.receive_remote_operation(
            "remote-op".to_string(),
            None,
            state("remote-session"),
            PlaybackBufferOperationKind::Resume,
            peers(["local", "remote"]),
            Duration::from_secs(10),
            now,
        );

        assert!(coordinator.clear_if_local_leader("local").is_none());
        assert!(coordinator.is_active());
    }

    #[test]
    fn quorum_ready_requires_strict_majority() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        coordinator.start_leader_operation(
            "op".to_string(),
            state("session"),
            PlaybackBufferOperationKind::Start,
            None,
            peers(["a", "b", "c"]),
            now + Duration::from_secs(10),
        );

        assert!(matches!(
            coordinator.mark_status(
                "op",
                "session",
                "a",
                PlaybackBufferStatusKind::Ready,
                Some(12_000),
                None,
                now
            ),
            BufferStatusOutcome::Accepted(BufferQuorum::Waiting {
                ready: 1,
                threshold: 2,
                ..
            })
        ));
        assert!(matches!(
            coordinator.mark_status(
                "op",
                "session",
                "b",
                PlaybackBufferStatusKind::Ready,
                Some(12_000),
                None,
                now
            ),
            BufferStatusOutcome::Accepted(BufferQuorum::Ready {
                ready: 2,
                threshold: 2
            })
        ));
    }

    #[test]
    fn wrong_or_stale_status_is_ignored() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        coordinator.start_leader_operation(
            "op".to_string(),
            state("session"),
            PlaybackBufferOperationKind::Seek,
            None,
            peers(["a", "b"]),
            now + Duration::from_secs(10),
        );

        assert_eq!(
            coordinator.mark_status(
                "other-op",
                "session",
                "a",
                PlaybackBufferStatusKind::Ready,
                None,
                None,
                now,
            ),
            BufferStatusOutcome::Ignored
        );
        assert_eq!(
            coordinator.mark_status(
                "op",
                "other-session",
                "a",
                PlaybackBufferStatusKind::Ready,
                None,
                None,
                now,
            ),
            BufferStatusOutcome::Ignored
        );
        assert_eq!(
            coordinator.mark_status(
                "op",
                "session",
                "unknown",
                PlaybackBufferStatusKind::Ready,
                None,
                None,
                now,
            ),
            BufferStatusOutcome::Ignored
        );
    }

    #[test]
    fn quorum_impossible_when_remaining_peers_cannot_reach_majority() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        coordinator.start_leader_operation(
            "op".to_string(),
            state("session"),
            PlaybackBufferOperationKind::Start,
            None,
            peers(["a", "b", "c"]),
            now + Duration::from_secs(10),
        );

        coordinator.mark_status(
            "op",
            "session",
            "a",
            PlaybackBufferStatusKind::Failed,
            None,
            Some("download failed".to_string()),
            now,
        );
        assert!(matches!(
            coordinator.mark_status(
                "op",
                "session",
                "b",
                PlaybackBufferStatusKind::Failed,
                None,
                Some("decode failed".to_string()),
                now,
            ),
            BufferStatusOutcome::Accepted(BufferQuorum::Impossible {
                ready: 0,
                failed: 2,
                threshold: 2,
            })
        ));
    }

    #[test]
    fn quorum_times_out() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        coordinator.start_leader_operation(
            "op".to_string(),
            state("session"),
            PlaybackBufferOperationKind::Start,
            None,
            peers(["a", "b", "c"]),
            now + Duration::from_secs(1),
        );

        assert!(matches!(
            coordinator.quorum(now + Duration::from_secs(2)),
            Some(BufferQuorum::TimedOut {
                ready: 0,
                threshold: 2
            })
        ));
    }

    #[test]
    fn view_reports_local_status_and_counts() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        coordinator.start_leader_operation(
            "op".to_string(),
            state("session"),
            PlaybackBufferOperationKind::Resume,
            None,
            peers(["local", "remote", "slow"]),
            now + Duration::from_secs(10),
        );
        coordinator.mark_status(
            "op",
            "session",
            "local",
            PlaybackBufferStatusKind::Ready,
            Some(15_000),
            None,
            now,
        );
        coordinator.mark_status(
            "op",
            "session",
            "remote",
            PlaybackBufferStatusKind::Buffering,
            Some(9_000),
            None,
            now,
        );
        coordinator.mark_status(
            "op",
            "session",
            "slow",
            PlaybackBufferStatusKind::Failed,
            None,
            Some("range failed".to_string()),
            now,
        );

        let view = coordinator.active().unwrap().view("local", now);

        assert_eq!(view.local_status, PlaybackBufferStatusKind::Ready);
        assert_eq!(view.ready, 1);
        assert_eq!(view.buffering, 1);
        assert_eq!(view.failed, 1);
        assert_eq!(view.threshold, 2);
        assert_eq!(view.eligible_peers, 3);
        assert_eq!(view.buffered_until_ms, Some(15_000));
    }

    #[test]
    fn matching_playback_state_clears_completed_operation() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        coordinator.start_leader_operation(
            "op".to_string(),
            track_state("session", false),
            PlaybackBufferOperationKind::Start,
            None,
            peers(["local", "remote"]),
            now + Duration::from_secs(10),
        );

        assert!(
            coordinator
                .clear_matching_playback_state(&state("other-session"))
                .is_none()
        );
        assert!(coordinator.active().is_some());
        assert!(
            coordinator
                .clear_matching_playback_state(&track_state("session", false))
                .is_none()
        );
        assert!(coordinator.active().is_some());
        assert_eq!(
            coordinator
                .clear_matching_playback_state(&track_state("session", true))
                .map(|operation| operation.operation_id),
            Some("op".to_string())
        );
        assert!(coordinator.active().is_none());
    }

    #[test]
    fn idle_playback_state_clears_matching_operation() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        coordinator.start_leader_operation(
            "op".to_string(),
            track_state("session", false),
            PlaybackBufferOperationKind::Seek,
            None,
            peers(["local", "remote"]),
            now + Duration::from_secs(10),
        );

        assert_eq!(
            coordinator
                .clear_matching_playback_state(&state("session"))
                .map(|operation| operation.operation_id),
            Some("op".to_string())
        );
    }

    #[test]
    fn reordered_snapshots_do_not_interrupt_remote_seek_preparation() {
        let mut buffer = BufferCoordinator::new();
        let mut seek = track_state("session", false);
        seek.state_version = 12;
        seek.position_ms = 503_123;
        buffer.receive_remote_operation(
            "seek".into(),
            None,
            seek.clone(),
            PlaybackBufferOperationKind::Seek,
            peers(["listener", "leader"]),
            Duration::from_secs(90),
            Instant::now(),
        );
        for (version, playing) in [(10, true), (11, false)] {
            let mut previous = track_state("session", playing);
            previous.state_version = version;
            assert!(buffer.active().unwrap().precedes_commit(&previous));
            assert!(buffer.clear_matching_playback_state(&previous).is_none());
        }
        seek.playing = true;
        assert!(!buffer.active().unwrap().precedes_commit(&seek));
        assert!(buffer.clear_matching_playback_state(&seek).is_some());
    }

    #[test]
    fn duplicate_remote_prepare_preserves_local_status() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        coordinator.receive_remote_operation(
            "op".to_string(),
            None,
            state("session"),
            PlaybackBufferOperationKind::Seek,
            peers(["local", "leader"]),
            Duration::from_secs(10),
            now,
        );
        coordinator.mark_status(
            "op",
            "session",
            "local",
            PlaybackBufferStatusKind::Ready,
            Some(12_000),
            None,
            now,
        );

        coordinator.receive_remote_operation(
            "op".to_string(),
            None,
            state("session"),
            PlaybackBufferOperationKind::Seek,
            peers(["local", "leader"]),
            Duration::from_secs(10),
            now + Duration::from_secs(2),
        );

        assert_eq!(
            coordinator
                .status_for("op", "session", "local")
                .map(|status| status.status),
            Some(PlaybackBufferStatusKind::Ready)
        );
    }

    #[test]
    fn status_matches_detects_duplicate_peer_status() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        coordinator.receive_remote_operation(
            "op".to_string(),
            None,
            state("session"),
            PlaybackBufferOperationKind::Resume,
            peers(["local", "leader"]),
            Duration::from_secs(10),
            now,
        );
        let status = BufferPeerStatus {
            status: PlaybackBufferStatusKind::Ready,
            buffered_until_ms: Some(12_000),
            error: None,
        };

        coordinator.mark_status(
            "op",
            "session",
            "local",
            status.status.clone(),
            status.buffered_until_ms,
            status.error.clone(),
            now,
        );

        assert!(coordinator.status_matches("op", "session", "local", &status));
        assert!(!coordinator.status_matches(
            "op",
            "session",
            "local",
            &BufferPeerStatus {
                status: PlaybackBufferStatusKind::Buffering,
                ..status
            }
        ));
    }

    #[test]
    fn deadline_bounds_prepare_even_when_a_remote_majority_is_ready() {
        let now = Instant::now();
        let mut buffer = BufferCoordinator::new();
        buffer.start_leader_operation(
            "op".into(),
            state("s"),
            PlaybackBufferOperationKind::Start,
            Some("item".into()),
            peers(["a", "b", "local"]),
            now + Duration::from_secs(90),
        );
        for peer in ["a", "b"] {
            buffer.mark_status(
                "op",
                "s",
                peer,
                PlaybackBufferStatusKind::Ready,
                None,
                None,
                now,
            );
        }
        assert!(matches!(
            buffer.quorum(now),
            Some(BufferQuorum::Ready { .. })
        ));
        assert!(matches!(
            buffer.quorum(now + Duration::from_secs(90)),
            Some(BufferQuorum::TimedOut { .. })
        ));
        let view = buffer.active().unwrap().view("local", now);
        assert_eq!(view.queue_item_id.as_deref(), Some("item"));
        assert_eq!((view.ready, view.eligible_peers, view.threshold), (2, 3, 2));
    }

    #[test]
    fn removing_expected_peer_recomputes_quorum() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        coordinator.start_leader_operation(
            "op".to_string(),
            state("session"),
            PlaybackBufferOperationKind::Seek,
            None,
            peers(["local", "slow"]),
            now + Duration::from_secs(10),
        );
        coordinator.mark_status(
            "op",
            "session",
            "local",
            PlaybackBufferStatusKind::Ready,
            Some(12_000),
            None,
            now,
        );

        assert!(matches!(
            coordinator.quorum(now),
            Some(BufferQuorum::Waiting {
                ready: 1,
                threshold: 2,
                ..
            })
        ));
        assert!(matches!(
            coordinator.remove_expected_peer("slow", now),
            Some(BufferQuorum::Ready {
                ready: 1,
                threshold: 1
            })
        ));
        let view = coordinator.active().unwrap().view("local", now);
        assert_eq!(view.eligible_peers, 1);
        assert_eq!(view.ready, 1);
        assert_eq!(view.threshold, 1);
    }

    #[test]
    fn prepare_republish_is_rate_limited() {
        let now = Instant::now();
        let mut coordinator = BufferCoordinator::new();
        coordinator.start_leader_operation(
            "op".to_string(),
            state("session"),
            PlaybackBufferOperationKind::Seek,
            None,
            peers(["local", "remote"]),
            now + Duration::from_secs(10),
        );

        assert!(
            coordinator
                .prepare_republish_due(now, Duration::from_secs(2))
                .is_some()
        );
        assert!(
            coordinator
                .prepare_republish_due(now + Duration::from_secs(1), Duration::from_secs(2))
                .is_none()
        );
        assert!(
            coordinator
                .prepare_republish_due(now + Duration::from_secs(2), Duration::from_secs(2))
                .is_some()
        );
    }
}
