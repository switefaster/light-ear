use std::collections::{HashSet, VecDeque};
use std::time::Duration;

use libp2p::PeerId;
use tokio::time::Instant;

#[cfg(test)]
use crate::core::PlaybackTrack;
use crate::core::{
    ActiveVote, PlaybackState, QueueItem, QueueState, VoteAction, VoteProposal,
    VoteTerminalOutcome, VoteView, format_duration_ms,
};

#[derive(Debug, Clone)]
pub(crate) enum PlaybackPhase {
    Idle { state: Option<PlaybackState> },
    Preparing(PreparingPlayback),
    Active(PlaybackState),
}

#[derive(Debug, Clone)]
pub(crate) struct PreparingPlayback {
    pub(crate) state: PlaybackState,
    pub(crate) expected_peers: HashSet<String>,
    pub(crate) ready_peers: HashSet<String>,
    pub(crate) deadline: Instant,
}

#[derive(Default)]
pub(crate) struct MusicState {
    pub(crate) queue: VecDeque<QueueItem>,
    pub(crate) queue_version: u64,
    pub(crate) queue_updated_at: i64,
    pub(crate) playback_version: u64,
    pub(crate) active_vote: Option<ActiveVote>,
    pub(crate) playback_phase: PlaybackPhase,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum PlaybackReadyOutcome {
    Marked { ready: usize, expected: usize },
    Ignored,
}

#[derive(Debug, Clone)]
pub(crate) struct PlaybackStart {
    pub(crate) state: PlaybackState,
    pub(crate) reason: &'static str,
    pub(crate) ready: usize,
    pub(crate) expected: usize,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct VoteInvalidation {
    pub(crate) reason: &'static str,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum VoteCastOutcome {
    Accepted { vote_id: String },
    Duplicate { vote_id: String },
}

#[derive(Debug, Clone)]
pub(crate) enum VoteResolution {
    Passed(VoteProposal),
    Rejected(VoteProposal),
}

#[derive(Debug, Clone)]
pub(crate) struct RemoteQueueApply {
    pub(crate) state: QueueState,
    pub(crate) invalidated_vote: Option<VoteInvalidation>,
}

#[derive(Debug, Clone)]
pub(crate) struct BeginPrepare {
    pub(crate) canceled: Option<PlaybackCancel>,
    pub(crate) state: PlaybackState,
    pub(crate) expected_peers: HashSet<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct PlaybackCancel {
    pub(crate) session_id: String,
    pub(crate) reason: String,
}

impl Default for PlaybackPhase {
    fn default() -> Self {
        Self::Idle { state: None }
    }
}

impl PreparingPlayback {
    fn leader(state: PlaybackState, expected_peers: HashSet<String>, deadline: Instant) -> Self {
        Self {
            state,
            expected_peers,
            ready_peers: HashSet::new(),
            deadline,
        }
    }

    fn follower(state: PlaybackState) -> Self {
        Self {
            state,
            expected_peers: HashSet::new(),
            ready_peers: HashSet::new(),
            deadline: Instant::now(),
        }
    }

    fn is_leader(&self, local_peer_id: PeerId) -> bool {
        self.state.leader_peer_id == local_peer_id.to_string()
    }

    fn mark_ready(&mut self, peer_id: &str) -> bool {
        if self.expected_peers.contains(peer_id) {
            self.ready_peers.insert(peer_id.to_string())
        } else {
            false
        }
    }

    fn ready_count(&self) -> usize {
        self.ready_peers.len()
    }

    fn expected_count(&self) -> usize {
        self.expected_peers.len()
    }

    fn is_ready(&self) -> bool {
        self.expected_peers.is_subset(&self.ready_peers)
    }
}

impl MusicState {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn queue_state(&self, local_peer_id: PeerId) -> QueueState {
        QueueState {
            version: self.queue_version,
            updated_at_micros: self.queue_updated_at,
            updated_by: local_peer_id.to_string(),
            items: self.queue.iter().cloned().collect(),
        }
    }

    pub(crate) fn mark_queue_changed(&mut self, updated_at_micros: i64) {
        self.queue_version = self.queue_version.saturating_add(1);
        self.queue_updated_at = updated_at_micros;
    }

    pub(crate) fn append_queue_item(&mut self, item: QueueItem) -> usize {
        let index = self.queue.len();
        self.queue.push_back(item);
        index
    }

    pub(crate) fn remove_queue_index(&mut self, index: usize) -> Option<QueueItem> {
        index
            .checked_sub(1)
            .and_then(|index| self.queue.remove(index))
    }

    pub(crate) fn pop_next_queue_item(&mut self) -> Option<QueueItem> {
        self.queue.pop_front()
    }

    pub(crate) fn apply_remote_queue_state(
        &mut self,
        state: QueueState,
    ) -> Option<RemoteQueueApply> {
        if !should_apply_queue_state(self.queue_version, self.queue_updated_at, &state) {
            return None;
        }

        self.queue = VecDeque::from(state.items.clone());
        self.queue_version = state.version;
        self.queue_updated_at = state.updated_at_micros;
        let invalidated_vote = self.invalidate_stale_queue_vote();

        Some(RemoteQueueApply {
            state,
            invalidated_vote,
        })
    }

    pub(crate) fn playback_state(&self) -> Option<&PlaybackState> {
        match &self.playback_phase {
            PlaybackPhase::Idle { state } => state.as_ref(),
            PlaybackPhase::Preparing(preparing) => Some(&preparing.state),
            PlaybackPhase::Active(state) => Some(state),
        }
    }

    pub(crate) fn playback_state_mut(&mut self) -> Option<&mut PlaybackState> {
        match &mut self.playback_phase {
            PlaybackPhase::Idle { state } => state.as_mut(),
            PlaybackPhase::Preparing(preparing) => Some(&mut preparing.state),
            PlaybackPhase::Active(state) => Some(state),
        }
    }

    pub(crate) fn has_pending_playback(&self) -> bool {
        matches!(self.playback_phase, PlaybackPhase::Preparing(_))
    }

    pub(crate) fn has_track(&self) -> bool {
        self.playback_state()
            .and_then(|state| state.track.as_ref())
            .is_some()
    }

    pub(crate) fn can_start_next(&self) -> bool {
        !self.has_pending_playback() && !self.has_track()
    }

    pub(crate) fn begin_playback_prepare(
        &mut self,
        item: QueueItem,
        expected_peers: HashSet<String>,
        deadline: Instant,
        local_peer_id: PeerId,
        now_micros: i64,
    ) -> BeginPrepare {
        let canceled =
            self.take_local_pending_cancel(local_peer_id, "superseded by next queue item");
        self.playback_version = self.playback_version.saturating_add(1);

        let state = PlaybackState {
            session_id: format!("{local_peer_id}:{now_micros}:{}", item.track.track_id),
            leader_peer_id: local_peer_id.to_string(),
            track: Some(item.track),
            track_requested_by: Some(item.requested_by),
            state_version: self.playback_version,
            issued_at_micros: now_micros,
            playing: false,
            position_ms: 0,
            anchor_time_micros: now_micros,
            rate: 1.0,
        };
        self.playback_phase = PlaybackPhase::Preparing(PreparingPlayback::leader(
            state.clone(),
            expected_peers.clone(),
            deadline,
        ));

        BeginPrepare {
            canceled,
            state,
            expected_peers,
        }
    }

    pub(crate) fn mark_playback_ready(
        &mut self,
        session_id: &str,
        peer_id: &str,
        local_peer_id: PeerId,
    ) -> PlaybackReadyOutcome {
        let PlaybackPhase::Preparing(preparing) = &mut self.playback_phase else {
            return PlaybackReadyOutcome::Ignored;
        };
        if preparing.state.session_id != session_id || !preparing.is_leader(local_peer_id) {
            return PlaybackReadyOutcome::Ignored;
        }
        if !preparing.mark_ready(peer_id) {
            return PlaybackReadyOutcome::Ignored;
        }

        PlaybackReadyOutcome::Marked {
            ready: preparing.ready_count(),
            expected: preparing.expected_count(),
        }
    }

    pub(crate) fn remove_pending_peer(&mut self, peer_id: &str) -> bool {
        let PlaybackPhase::Preparing(preparing) = &mut self.playback_phase else {
            return false;
        };

        let expected_removed = preparing.expected_peers.remove(peer_id);
        let ready_removed = preparing.ready_peers.remove(peer_id);
        expected_removed || ready_removed
    }

    pub(crate) fn maybe_start_pending_playback(
        &mut self,
        local_peer_id: PeerId,
        now: Instant,
        now_micros: i64,
        start_delay: Duration,
    ) -> Option<PlaybackStart> {
        let PlaybackPhase::Preparing(preparing) = &self.playback_phase else {
            return None;
        };
        if !preparing.is_leader(local_peer_id) {
            return None;
        }

        let timed_out = now >= preparing.deadline;
        if !preparing.is_ready() && !timed_out {
            return None;
        }

        let (mut state, ready, expected, reason) = if let PlaybackPhase::Preparing(preparing) =
            std::mem::replace(
                &mut self.playback_phase,
                PlaybackPhase::Idle { state: None },
            ) {
            let reason = if preparing.is_ready() {
                "all peers ready"
            } else {
                "ready wait timed out"
            };
            let ready = preparing.ready_count();
            let expected = preparing.expected_count();
            (preparing.state, ready, expected, reason)
        } else {
            return None;
        };

        self.playback_version = self.playback_version.saturating_add(1);
        state.state_version = self.playback_version;
        state.issued_at_micros = now_micros;
        state.playing = true;
        state.position_ms = 0;
        state.anchor_time_micros = now_micros + duration_micros(start_delay);

        self.playback_phase = PlaybackPhase::Active(state.clone());
        Some(PlaybackStart {
            state,
            reason,
            ready,
            expected,
        })
    }

    pub(crate) fn take_local_pending_cancel(
        &mut self,
        local_peer_id: PeerId,
        reason: &str,
    ) -> Option<PlaybackCancel> {
        let should_cancel = matches!(
            &self.playback_phase,
            PlaybackPhase::Preparing(preparing) if preparing.is_leader(local_peer_id)
        );
        if !should_cancel {
            return None;
        }

        if let PlaybackPhase::Preparing(preparing) = std::mem::replace(
            &mut self.playback_phase,
            PlaybackPhase::Idle { state: None },
        ) {
            Some(PlaybackCancel {
                session_id: preparing.state.session_id,
                reason: reason.to_string(),
            })
        } else {
            None
        }
    }

    pub(crate) fn set_remote_playback_prepare(
        &mut self,
        state: PlaybackState,
    ) -> Option<VoteInvalidation> {
        let invalidated = self.invalidate_playback_vote_if_track_changes(&state);
        self.playback_phase = PlaybackPhase::Preparing(PreparingPlayback::follower(state));
        invalidated
    }

    pub(crate) fn set_playback_state(&mut self, state: PlaybackState) -> Option<VoteInvalidation> {
        let invalidated = self.invalidate_playback_vote_if_track_changes(&state);
        if state.track.is_some() {
            self.playback_phase = PlaybackPhase::Active(state);
        } else {
            self.playback_phase = PlaybackPhase::Idle { state: Some(state) };
        }
        invalidated
    }

    pub(crate) fn cancel_playback(&mut self, session_id: &str) -> Option<VoteInvalidation> {
        let matches_session = self
            .playback_state()
            .is_some_and(|state| state.session_id == session_id);
        if !matches_session {
            return None;
        }

        let invalidated = self.invalidate_playback_vote("playback changed during vote");
        self.playback_phase = PlaybackPhase::Idle { state: None };
        invalidated
    }

    pub(crate) fn stop_current_playback(
        &mut self,
        local_peer_id: PeerId,
        now_micros: i64,
    ) -> PlaybackState {
        self.playback_version = self.playback_version.saturating_add(1);
        let state = PlaybackState {
            session_id: format!("{local_peer_id}:{now_micros}:idle"),
            leader_peer_id: local_peer_id.to_string(),
            track: None,
            track_requested_by: None,
            state_version: self.playback_version,
            issued_at_micros: now_micros,
            playing: false,
            position_ms: 0,
            anchor_time_micros: now_micros,
            rate: 1.0,
        };
        self.playback_phase = PlaybackPhase::Idle {
            state: Some(state.clone()),
        };
        state
    }

    pub(crate) fn stop_current_playback_for_vote(
        &mut self,
        actor_peer_id: PeerId,
        vote_micros: i64,
    ) -> PlaybackState {
        let version = self.vote_playback_version(vote_micros);
        let state = PlaybackState {
            session_id: format!("{actor_peer_id}:{vote_micros}:idle"),
            leader_peer_id: actor_peer_id.to_string(),
            track: None,
            track_requested_by: None,
            state_version: version,
            issued_at_micros: vote_micros,
            playing: false,
            position_ms: 0,
            anchor_time_micros: vote_micros,
            rate: 1.0,
        };
        self.playback_phase = PlaybackPhase::Idle {
            state: Some(state.clone()),
        };
        state
    }

    pub(crate) fn pause_playback_for_vote(
        &mut self,
        actor_peer_id: PeerId,
        vote_micros: i64,
    ) -> Option<PlaybackState> {
        let version = self.vote_playback_version(vote_micros);
        self.update_playback_state_with_version(vote_micros, version, |state, version| {
            let position_ms = playback_position_ms(state, vote_micros);
            state.state_version = version;
            state.issued_at_micros = vote_micros;
            state.playing = false;
            state.position_ms = position_ms;
            state.anchor_time_micros = vote_micros;
            state.leader_peer_id = actor_peer_id.to_string();
        })
    }

    pub(crate) fn resume_playback_for_vote(
        &mut self,
        actor_peer_id: PeerId,
        vote_micros: i64,
    ) -> Option<PlaybackState> {
        let version = self.vote_playback_version(vote_micros);
        self.update_playback_state_with_version(vote_micros, version, |state, version| {
            state.state_version = version;
            state.issued_at_micros = vote_micros;
            state.playing = true;
            state.anchor_time_micros = vote_micros;
            state.leader_peer_id = actor_peer_id.to_string();
        })
    }

    pub(crate) fn seek_playback(
        &mut self,
        local_peer_id: PeerId,
        position_ms: u64,
        now_micros: i64,
    ) -> Option<PlaybackState> {
        self.update_playback_state(now_micros, |state, version| {
            let position_ms = clamp_playback_position_ms(state, position_ms);
            let playing = state.playing && can_play_at_position(state, position_ms);
            state.state_version = version;
            state.issued_at_micros = now_micros;
            state.playing = playing;
            state.position_ms = position_ms;
            state.anchor_time_micros = now_micros;
            state.leader_peer_id = local_peer_id.to_string();
        })
    }

    pub(crate) fn seek_playback_for_vote(
        &mut self,
        actor_peer_id: PeerId,
        position_ms: u64,
        vote_micros: i64,
    ) -> Option<PlaybackState> {
        let version = self.vote_playback_version(vote_micros);
        self.update_playback_state_with_version(vote_micros, version, |state, version| {
            let position_ms = clamp_playback_position_ms(state, position_ms);
            state.state_version = version;
            state.issued_at_micros = vote_micros;
            state.position_ms = position_ms;
            state.anchor_time_micros = vote_micros;
            state.leader_peer_id = actor_peer_id.to_string();
        })
    }

    pub(crate) fn mark_queue_vote_applied(&mut self, updated_at_micros: i64) {
        self.mark_queue_changed(updated_at_micros);
    }

    fn update_playback_state(
        &mut self,
        _now_micros: i64,
        update: impl FnOnce(&mut PlaybackState, u64),
    ) -> Option<PlaybackState> {
        self.playback_state()?;
        self.playback_version = self.playback_version.saturating_add(1);
        let version = self.playback_version;
        self.update_playback_state_with_version(_now_micros, version, update)
    }

    fn update_playback_state_with_version(
        &mut self,
        _now_micros: i64,
        version: u64,
        update: impl FnOnce(&mut PlaybackState, u64),
    ) -> Option<PlaybackState> {
        self.playback_state()?;
        let state = self.playback_state_mut()?;
        update(state, version);
        Some(state.clone())
    }

    fn vote_playback_version(&mut self, vote_micros: i64) -> u64 {
        let version = u64::try_from(vote_micros).unwrap_or(0);
        self.playback_version = self.playback_version.max(version);
        version
    }

    pub(crate) fn start_vote(&mut self, proposal: VoteProposal, deadline: Instant) {
        let mut vote = ActiveVote::new(proposal.clone(), deadline);
        let _ = vote.vote(proposal.proposer.clone(), true);
        self.active_vote = Some(vote);
    }

    pub(crate) fn receive_vote_proposal(
        &mut self,
        proposal: VoteProposal,
        deadline: Instant,
    ) -> Result<(), &'static str> {
        if self.active_vote.is_some() {
            return Err("another vote is active");
        }
        if let Some(reason) = self.stale_vote_reason(&proposal) {
            return Err(reason);
        }

        let mut vote = ActiveVote::new(proposal.clone(), deadline);
        let _ = vote.vote(proposal.proposer.clone(), true);
        self.active_vote = Some(vote);
        Ok(())
    }

    pub(crate) fn cast_vote(&mut self, peer_id: String, approve: bool) -> Option<VoteCastOutcome> {
        let vote = self.active_vote.as_mut()?;
        let vote_id = vote.proposal.vote_id.clone();
        if vote.vote(peer_id, approve) {
            Some(VoteCastOutcome::Accepted { vote_id })
        } else {
            Some(VoteCastOutcome::Duplicate { vote_id })
        }
    }

    pub(crate) fn cast_vote_for(&mut self, vote_id: &str, peer_id: String, approve: bool) -> bool {
        let Some(vote) = self.active_vote.as_mut() else {
            return false;
        };
        if vote.proposal.vote_id != vote_id {
            return false;
        }

        vote.vote(peer_id, approve)
    }

    pub(crate) fn resolve_vote(
        &mut self,
        threshold: usize,
        eligible_peers: usize,
    ) -> Option<VoteResolution> {
        let vote = self.active_vote.as_ref()?;
        match vote.terminal_outcome(threshold, eligible_peers) {
            VoteTerminalOutcome::Pending => None,
            VoteTerminalOutcome::Passed
                if self.ready_vote_waiting_for_queue(threshold).is_some() =>
            {
                None
            }
            VoteTerminalOutcome::Passed => self
                .active_vote
                .take()
                .map(|vote| VoteResolution::Passed(vote.proposal)),
            VoteTerminalOutcome::Rejected => self
                .active_vote
                .take()
                .map(|vote| VoteResolution::Rejected(vote.proposal)),
        }
    }

    pub(crate) fn ready_vote_waiting_for_queue(&self, threshold: usize) -> Option<VoteProposal> {
        let vote = self.active_vote.as_ref()?;
        if vote.approval_count() < threshold {
            return None;
        }
        matches!(
            vote.proposal.action,
            VoteAction::Remove { .. } | VoteAction::Move { .. }
        )
        .then_some(())
        .filter(|_| vote.proposal.queue_version > self.queue_version)
        .map(|_| vote.proposal.clone())
    }

    pub(crate) fn take_timed_out_vote(&mut self, now: Instant) -> Option<ActiveVote> {
        if self
            .active_vote
            .as_ref()
            .is_some_and(|vote| now >= vote.deadline)
        {
            self.active_vote.take()
        } else {
            None
        }
    }

    pub(crate) fn vote_view(
        &self,
        threshold: usize,
        eligible_peers: usize,
        local_peer_id: &str,
    ) -> Option<VoteView> {
        self.active_vote.as_ref().map(|vote| VoteView {
            vote_id: vote.proposal.vote_id.clone(),
            proposer: vote.proposal.proposer.clone(),
            action_label: describe_vote_action(&vote.proposal.action, &self.queue),
            approvals: vote.approvals.len(),
            rejections: vote.rejections.len(),
            threshold,
            eligible_peers,
            pending: vote.pending_count(eligible_peers),
            local_vote: vote.local_vote(local_peer_id),
        })
    }

    pub(crate) fn stale_vote_reason(&self, proposal: &VoteProposal) -> Option<&'static str> {
        match &proposal.action {
            VoteAction::Remove { item_id } | VoteAction::Move { item_id, .. } => {
                if proposal.queue_version < self.queue_version {
                    return Some("queue changed during vote");
                }
                if proposal.queue_version == self.queue_version {
                    return (!self.queue.iter().any(|item| item.item_id == *item_id))
                        .then_some("queue item is no longer available");
                }
                None
            }
            VoteAction::Pause | VoteAction::Resume | VoteAction::Skip | VoteAction::Seek { .. } => {
                let current_session = self
                    .playback_state()
                    .and_then(|state| state.track.as_ref().map(|_| state.session_id.as_str()));
                let Some(current_session) = current_session else {
                    return Some("no active playback");
                };
                (proposal.playback_session_id.as_deref() != Some(current_session))
                    .then_some("playback changed during vote")
            }
        }
    }

    pub(crate) fn should_execute_vote_locally(
        &self,
        proposal: &VoteProposal,
        local_peer_id: PeerId,
    ) -> bool {
        let _ = local_peer_id;
        self.stale_vote_reason(proposal).is_none()
    }

    fn invalidate_stale_queue_vote(&mut self) -> Option<VoteInvalidation> {
        let reason = self.active_vote.as_ref().and_then(|vote| {
            matches!(
                vote.proposal.action,
                VoteAction::Remove { .. } | VoteAction::Move { .. }
            )
            .then(|| self.stale_vote_reason(&vote.proposal))
            .flatten()
        });

        reason.map(|reason| {
            self.active_vote.take();
            VoteInvalidation { reason }
        })
    }

    pub(crate) fn invalidate_playback_vote(
        &mut self,
        reason: &'static str,
    ) -> Option<VoteInvalidation> {
        let should_invalidate = self.active_vote.as_ref().is_some_and(|vote| {
            matches!(
                vote.proposal.action,
                VoteAction::Pause | VoteAction::Resume | VoteAction::Skip | VoteAction::Seek { .. }
            )
        });
        if should_invalidate {
            self.active_vote.take();
            Some(VoteInvalidation { reason })
        } else {
            None
        }
    }

    fn invalidate_playback_vote_if_track_changes(
        &mut self,
        next: &PlaybackState,
    ) -> Option<VoteInvalidation> {
        let should_invalidate = self.active_vote.as_ref().is_some_and(|vote| {
            matches!(
                vote.proposal.action,
                VoteAction::Pause | VoteAction::Resume | VoteAction::Skip | VoteAction::Seek { .. }
            ) && self.playback_track_key() != playback_track_key(next)
        });
        if should_invalidate {
            self.active_vote.take();
            Some(VoteInvalidation {
                reason: "playback changed during vote",
            })
        } else {
            None
        }
    }

    fn playback_track_key(&self) -> Option<(&str, &str)> {
        self.playback_state().and_then(playback_track_key)
    }
}

pub(crate) fn majority_threshold(total_peers: usize) -> usize {
    total_peers / 2 + 1
}

pub(crate) fn can_control_playback(state: &PlaybackState, local_peer_id: PeerId) -> bool {
    state
        .track_requested_by
        .as_ref()
        .is_some_and(|requester| requester == &local_peer_id.to_string())
}

fn playback_track_key(state: &PlaybackState) -> Option<(&str, &str)> {
    state
        .track
        .as_ref()
        .map(|track| (state.session_id.as_str(), track.track_id.as_str()))
}

pub(crate) fn queue_item_at(queue: &VecDeque<QueueItem>, index: usize) -> Option<&QueueItem> {
    index.checked_sub(1).and_then(|index| queue.get(index))
}

pub(crate) fn describe_vote_action(action: &VoteAction, queue: &VecDeque<QueueItem>) -> String {
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

pub(crate) fn should_apply_queue_state(
    local_version: u64,
    local_updated_at: i64,
    state: &QueueState,
) -> bool {
    is_queue_state_newer(
        state.version,
        state.updated_at_micros,
        local_version,
        local_updated_at,
    )
}

pub(crate) fn is_queue_state_newer(
    candidate_version: u64,
    candidate_updated_at: i64,
    local_version: u64,
    local_updated_at: i64,
) -> bool {
    candidate_updated_at > local_updated_at
        || (candidate_updated_at == local_updated_at && candidate_version > local_version)
}

pub(crate) fn should_apply_playback_state(
    current: Option<&PlaybackState>,
    next: &PlaybackState,
) -> bool {
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

pub(crate) fn normalize_remote_playback_state(
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

pub(crate) fn playback_position_ms(state: &PlaybackState, now_micros: i64) -> u64 {
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

pub(crate) fn clamp_playback_position_ms(state: &PlaybackState, position_ms: u64) -> u64 {
    playback_duration_ms(state).map_or(position_ms, |duration| position_ms.min(duration))
}

pub(crate) fn can_play_at_position(state: &PlaybackState, position_ms: u64) -> bool {
    playback_duration_ms(state).is_none_or(|duration| position_ms < duration)
}

pub(crate) fn playback_should_be_audible(state: &PlaybackState, now_micros: i64) -> bool {
    state.playing
        && now_micros >= state.anchor_time_micros
        && can_play_at_position(state, playback_position_ms(state, now_micros))
}

pub(crate) fn duration_micros(duration: Duration) -> i64 {
    i64::try_from(duration.as_micros()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use libp2p::identity;

    use super::*;

    fn peer_id() -> PeerId {
        identity::Keypair::generate_ed25519().public().to_peer_id()
    }

    fn track(id: &str, duration_ms: u64) -> PlaybackTrack {
        PlaybackTrack {
            track_id: id.to_string(),
            title: id.to_string(),
            source_kind: "bilibili".to_string(),
            bvid: "BV1A4411N7".to_string(),
            part: 1,
            duration_ms,
            audio_url: "https://example.test/audio.m4a".to_string(),
            referer: "https://www.bilibili.com/video/BV1A4411N7".to_string(),
        }
    }

    fn queue_item(id: &str) -> QueueItem {
        QueueItem {
            item_id: id.to_string(),
            track: track(id, 10_000),
            requested_by: "alice".to_string(),
            added_at_micros: 1_700_000_000_000_000,
        }
    }

    fn queue_state(version: u64, updated_at_micros: i64, items: Vec<QueueItem>) -> QueueState {
        QueueState {
            version,
            updated_at_micros,
            updated_by: "remote".to_string(),
            items,
        }
    }

    fn playback_state(
        leader: PeerId,
        requested_by: PeerId,
        session_id: &str,
        playing: bool,
        position_ms: u64,
        anchor_time_micros: i64,
        duration_ms: u64,
    ) -> PlaybackState {
        PlaybackState {
            session_id: session_id.to_string(),
            leader_peer_id: leader.to_string(),
            track: Some(track("track-1", duration_ms)),
            track_requested_by: Some(requested_by.to_string()),
            state_version: 1,
            issued_at_micros: anchor_time_micros,
            playing,
            position_ms,
            anchor_time_micros,
            rate: 1.0,
        }
    }

    fn proposal(proposer: PeerId, action: VoteAction, queue_version: u64) -> VoteProposal {
        VoteProposal {
            vote_id: "vote-1".to_string(),
            proposer: proposer.to_string(),
            action,
            queue_version,
            playback_session_id: Some("session".to_string()),
            created_at_micros: 1_700_000_000_000_000,
        }
    }

    #[test]
    fn queue_state_newer_uses_timestamp_then_version() {
        assert!(is_queue_state_newer(1, 101, 99, 100));
        assert!(!is_queue_state_newer(99, 100, 1, 101));
        assert!(is_queue_state_newer(2, 100, 1, 100));
        assert!(!is_queue_state_newer(1, 100, 1, 100));

        assert!(should_apply_queue_state(
            1,
            100,
            &queue_state(2, 100, Vec::new())
        ));
        assert!(!should_apply_queue_state(
            2,
            100,
            &queue_state(1, 100, Vec::new())
        ));
    }

    #[test]
    fn majority_threshold_requires_strict_majority() {
        assert_eq!(majority_threshold(1), 1);
        assert_eq!(majority_threshold(2), 2);
        assert_eq!(majority_threshold(3), 2);
        assert_eq!(majority_threshold(4), 3);
        assert_eq!(majority_threshold(5), 3);
    }

    #[test]
    fn queue_item_at_uses_one_based_indexes() {
        let queue = VecDeque::from([queue_item("first"), queue_item("second")]);

        assert_eq!(
            queue_item_at(&queue, 0).map(|item| item.item_id.as_str()),
            None
        );
        assert_eq!(
            queue_item_at(&queue, 1).map(|item| item.item_id.as_str()),
            Some("first")
        );
        assert_eq!(
            queue_item_at(&queue, 2).map(|item| item.item_id.as_str()),
            Some("second")
        );
        assert_eq!(
            queue_item_at(&queue, 3).map(|item| item.item_id.as_str()),
            None
        );
    }

    #[test]
    fn playback_position_math_respects_pause_anchor_and_duration() {
        let leader = peer_id();
        let requester = peer_id();
        let paused = playback_state(
            leader, requester, "session", false, 3_000, 1_000_000, 10_000,
        );
        assert_eq!(playback_position_ms(&paused, 5_000_000), 3_000);
        assert!(!playback_should_be_audible(&paused, 5_000_000));

        let leader = peer_id();
        let requester = peer_id();
        let playing = playback_state(leader, requester, "session", true, 1_000, 1_000_000, 10_000);
        assert_eq!(playback_position_ms(&playing, 3_500_000), 3_500);
        assert!(playback_should_be_audible(&playing, 3_500_000));

        let leader = peer_id();
        let requester = peer_id();
        let future_anchor =
            playback_state(leader, requester, "session", true, 1_000, 5_000_000, 10_000);
        assert_eq!(playback_position_ms(&future_anchor, 3_500_000), 1_000);
        assert!(!playback_should_be_audible(&future_anchor, 3_500_000));

        let leader = peer_id();
        let requester = peer_id();
        let over_duration =
            playback_state(leader, requester, "session", true, 9_500, 1_000_000, 10_000);
        assert_eq!(playback_position_ms(&over_duration, 3_000_000), 10_000);
        assert_eq!(clamp_playback_position_ms(&over_duration, 12_000), 10_000);
        assert!(!playback_should_be_audible(&over_duration, 3_000_000));
    }

    #[test]
    fn remote_queue_update_invalidates_queue_vote() {
        let proposer = peer_id();
        let mut music = MusicState::new();
        music.queue.push_back(queue_item("old"));
        music.queue_version = 1;
        music.start_vote(
            proposal(
                proposer,
                VoteAction::Remove {
                    item_id: "old".to_string(),
                },
                1,
            ),
            Instant::now() + Duration::from_secs(1),
        );

        let outcome = music
            .apply_remote_queue_state(queue_state(2, 100, vec![queue_item("new")]))
            .unwrap();

        assert_eq!(
            outcome.invalidated_vote,
            Some(VoteInvalidation {
                reason: "queue changed during vote"
            })
        );
        assert!(music.active_vote.is_none());
        assert_eq!(
            music.queue.front().map(|item| item.item_id.as_str()),
            Some("new")
        );
    }

    #[test]
    fn remote_queue_update_keeps_future_vote_when_context_arrives() {
        let proposer = peer_id();
        let mut music = MusicState::new();
        music.queue_version = 1;
        music.start_vote(
            proposal(
                proposer,
                VoteAction::Move {
                    item_id: "future".to_string(),
                    to_index: 0,
                },
                2,
            ),
            Instant::now() + Duration::from_secs(1),
        );

        let outcome = music
            .apply_remote_queue_state(queue_state(2, 100, vec![queue_item("future")]))
            .unwrap();

        assert_eq!(outcome.invalidated_vote, None);
        assert!(music.active_vote.is_some());
    }

    #[test]
    fn remote_playback_update_invalidates_playback_vote() {
        let local = peer_id();
        let mut music = MusicState::new();
        music.playback_phase = PlaybackPhase::Active(playback_state(
            local, local, "session", true, 0, 1_000_000, 10_000,
        ));
        music.start_vote(
            proposal(local, VoteAction::Pause, 0),
            Instant::now() + Duration::from_secs(1),
        );

        let remote = peer_id();
        let invalidated = music.set_playback_state(playback_state(
            remote,
            remote,
            "new-session",
            true,
            0,
            2_000_000,
            10_000,
        ));

        assert_eq!(
            invalidated,
            Some(VoteInvalidation {
                reason: "playback changed during vote"
            })
        );
        assert!(music.active_vote.is_none());
    }

    #[test]
    fn same_session_playback_update_keeps_playback_vote() {
        let local = peer_id();
        let mut music = MusicState::new();
        music.playback_phase = PlaybackPhase::Active(playback_state(
            local, local, "session", true, 0, 1_000_000, 10_000,
        ));
        music.start_vote(
            proposal(local, VoteAction::Pause, 0),
            Instant::now() + Duration::from_secs(1),
        );

        let invalidated = music.set_playback_state(playback_state(
            local, local, "session", true, 2_000, 2_000_000, 10_000,
        ));

        assert_eq!(invalidated, None);
        assert!(music.active_vote.is_some());
    }

    #[test]
    fn playback_vote_without_active_track_is_stale() {
        let proposer = peer_id();
        let music = MusicState::new();
        let proposal = VoteProposal {
            playback_session_id: None,
            ..proposal(proposer, VoteAction::Pause, 0)
        };

        assert_eq!(
            music.stale_vote_reason(&proposal),
            Some("no active playback")
        );
    }

    #[test]
    fn passed_vote_is_executable_by_all_room_peers() {
        let proposer = peer_id();
        let other = peer_id();
        let mut music = MusicState::new();
        music.queue.push_back(queue_item("current"));
        music.queue_version = 1;

        let proposal = proposal(
            proposer,
            VoteAction::Remove {
                item_id: "current".to_string(),
            },
            1,
        );

        assert!(music.should_execute_vote_locally(&proposal, other));
    }

    #[test]
    fn stale_vote_proposal_is_rejected_on_receipt() {
        let proposer = peer_id();
        let mut music = MusicState::new();
        music.queue.push_back(queue_item("current"));
        music.queue_version = 2;

        let err = music
            .receive_vote_proposal(
                proposal(
                    proposer,
                    VoteAction::Remove {
                        item_id: "current".to_string(),
                    },
                    1,
                ),
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap_err();

        assert_eq!(err, "queue changed during vote");
        assert!(music.active_vote.is_none());
    }

    #[test]
    fn future_queue_vote_proposal_is_accepted_while_waiting_for_sync() {
        let proposer = peer_id();
        let mut music = MusicState::new();
        music.queue_version = 1;

        music
            .receive_vote_proposal(
                proposal(
                    proposer,
                    VoteAction::Move {
                        item_id: "future-item".to_string(),
                        to_index: 0,
                    },
                    2,
                ),
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap();

        assert!(music.active_vote.is_some());
    }

    #[test]
    fn passed_future_queue_vote_waits_until_queue_catches_up() {
        let proposer = peer_id();
        let voter = peer_id();
        let mut music = MusicState::new();
        music.queue_version = 1;
        music.start_vote(
            proposal(
                proposer,
                VoteAction::Move {
                    item_id: "future".to_string(),
                    to_index: 0,
                },
                2,
            ),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(music.cast_vote_for("vote-1", voter.to_string(), true));

        let waiting = music.ready_vote_waiting_for_queue(2).unwrap();
        assert_eq!(waiting.queue_version, 2);
        assert!(music.resolve_vote(2, 2).is_none());

        let outcome = music
            .apply_remote_queue_state(queue_state(2, 100, vec![queue_item("future")]))
            .unwrap();
        assert_eq!(outcome.invalidated_vote, None);
        assert!(music.ready_vote_waiting_for_queue(2).is_none());
        assert!(matches!(
            music.resolve_vote(2, 2),
            Some(VoteResolution::Passed(_))
        ));
    }

    #[test]
    fn stale_playback_vote_proposal_is_rejected_on_receipt() {
        let proposer = peer_id();
        let local = peer_id();
        let mut music = MusicState::new();
        music.playback_phase = PlaybackPhase::Active(playback_state(
            local, local, "current", true, 0, 1_000_000, 10_000,
        ));

        let err = music
            .receive_vote_proposal(
                proposal(proposer, VoteAction::Pause, 0),
                Instant::now() + Duration::from_secs(1),
            )
            .unwrap_err();

        assert_eq!(err, "playback changed during vote");
        assert!(music.active_vote.is_none());
    }

    #[test]
    fn remote_prepare_and_cancel_invalidate_playback_votes() {
        let local = peer_id();
        let remote = peer_id();
        let mut music = MusicState::new();
        music.playback_phase = PlaybackPhase::Active(playback_state(
            local, local, "session", true, 0, 1_000_000, 10_000,
        ));
        music.start_vote(
            proposal(local, VoteAction::Pause, 0),
            Instant::now() + Duration::from_secs(1),
        );

        let invalidated = music.set_remote_playback_prepare(playback_state(
            remote,
            remote,
            "remote-session",
            false,
            0,
            2_000_000,
            10_000,
        ));
        assert_eq!(
            invalidated,
            Some(VoteInvalidation {
                reason: "playback changed during vote"
            })
        );

        music.start_vote(
            proposal(local, VoteAction::Skip, 0),
            Instant::now() + Duration::from_secs(1),
        );
        let invalidated = music.cancel_playback("remote-session");

        assert_eq!(
            invalidated,
            Some(VoteInvalidation {
                reason: "playback changed during vote"
            })
        );
        assert!(music.active_vote.is_none());
    }

    #[test]
    fn ballot_for_wrong_vote_id_is_ignored_by_resolve() {
        let proposer = peer_id();
        let voter = peer_id();
        let mut music = MusicState::new();
        music.start_vote(
            VoteProposal {
                vote_id: "vote-a".to_string(),
                proposer: proposer.to_string(),
                action: VoteAction::Pause,
                queue_version: 0,
                playback_session_id: Some("session".to_string()),
                created_at_micros: 1,
            },
            Instant::now() + Duration::from_secs(1),
        );

        assert!(!music.cast_vote_for("vote-b", voter.to_string(), true));
        assert_eq!(
            music.active_vote.as_ref().map(ActiveVote::approval_count),
            Some(1)
        );
    }

    #[test]
    fn duplicate_ballot_for_current_vote_is_ignored() {
        let proposer = peer_id();
        let voter = peer_id();
        let mut music = MusicState::new();
        music.start_vote(
            proposal(proposer, VoteAction::Pause, 0),
            Instant::now() + Duration::from_secs(1),
        );

        assert!(music.cast_vote_for("vote-1", voter.to_string(), true));
        assert!(!music.cast_vote_for("vote-1", voter.to_string(), false));

        let vote = music.active_vote.as_ref().unwrap();
        assert_eq!(vote.approval_count(), 2);
        assert_eq!(vote.rejection_count(), 0);
    }

    #[test]
    fn vote_resolution_rejects_when_majority_is_impossible() {
        let proposer = peer_id();
        let voter_one = peer_id();
        let voter_two = peer_id();
        let mut music = MusicState::new();
        music.start_vote(
            proposal(proposer, VoteAction::Pause, 0),
            Instant::now() + Duration::from_secs(1),
        );

        assert!(music.cast_vote_for("vote-1", voter_one.to_string(), false));
        assert!(matches!(music.resolve_vote(2, 3), None));
        assert!(music.cast_vote_for("vote-1", voter_two.to_string(), false));

        assert!(matches!(
            music.resolve_vote(2, 3),
            Some(VoteResolution::Rejected(_))
        ));
        assert!(music.active_vote.is_none());
    }

    #[test]
    fn vote_view_reports_pending_and_local_vote() {
        let proposer = peer_id();
        let voter = peer_id();
        let mut music = MusicState::new();
        music.start_vote(
            proposal(proposer, VoteAction::Pause, 0),
            Instant::now() + Duration::from_secs(1),
        );
        assert!(music.cast_vote_for("vote-1", voter.to_string(), false));

        let view = music.vote_view(2, 4, &voter.to_string()).unwrap();
        assert_eq!(view.approvals, 1);
        assert_eq!(view.rejections, 1);
        assert_eq!(view.pending, 2);
        assert_eq!(view.local_vote, Some(false));
    }

    #[test]
    fn pending_playback_starts_after_expected_peer_disconnects() {
        let local = peer_id();
        let peer = peer_id();
        let item = queue_item("track");
        let expected = HashSet::from([local.to_string(), peer.to_string()]);
        let mut music = MusicState::new();
        let prepare = music.begin_playback_prepare(
            item,
            expected,
            Instant::now() + Duration::from_secs(60),
            local,
            1_000_000,
        );
        assert_eq!(prepare.expected_peers.len(), 2);

        assert!(matches!(
            music.mark_playback_ready(&prepare.state.session_id, &local.to_string(), local),
            PlaybackReadyOutcome::Marked {
                ready: 1,
                expected: 2
            }
        ));
        assert!(music.remove_pending_peer(&peer.to_string()));

        let start = music
            .maybe_start_pending_playback(
                local,
                Instant::now(),
                2_000_000,
                Duration::from_millis(1500),
            )
            .unwrap();
        assert_eq!(start.reason, "all peers ready");
        assert_eq!(start.ready, 1);
        assert_eq!(start.expected, 1);
    }

    #[test]
    fn duplicate_or_unknown_playback_ready_does_not_change_ready_count() {
        let local = peer_id();
        let peer = peer_id();
        let unknown = peer_id();
        let item = queue_item("track");
        let expected = HashSet::from([local.to_string(), peer.to_string()]);
        let mut music = MusicState::new();
        let prepare = music.begin_playback_prepare(
            item,
            expected,
            Instant::now() + Duration::from_secs(60),
            local,
            1_000_000,
        );

        assert!(matches!(
            music.mark_playback_ready(&prepare.state.session_id, &peer.to_string(), local),
            PlaybackReadyOutcome::Marked {
                ready: 1,
                expected: 2
            }
        ));
        assert_eq!(
            music.mark_playback_ready(&prepare.state.session_id, &peer.to_string(), local),
            PlaybackReadyOutcome::Ignored
        );
        assert_eq!(
            music.mark_playback_ready(&prepare.state.session_id, &unknown.to_string(), local),
            PlaybackReadyOutcome::Ignored
        );
        let PlaybackPhase::Preparing(preparing) = &music.playback_phase else {
            panic!("expected preparing playback");
        };
        assert_eq!(preparing.ready_count(), 1);
    }

    #[test]
    fn skip_vote_during_prepare_clears_pending_with_deterministic_idle_state() {
        let leader = peer_id();
        let proposer = peer_id();
        let item = queue_item("track");
        let expected = HashSet::from([leader.to_string(), proposer.to_string()]);
        let mut music = MusicState::new();
        let prepare = music.begin_playback_prepare(
            item,
            expected,
            Instant::now() + Duration::from_secs(60),
            leader,
            1_000_000,
        );

        assert!(music.has_pending_playback());
        let state = music.stop_current_playback_for_vote(proposer, 2_000_000);

        assert!(!music.has_pending_playback());
        assert!(!music.has_track());
        assert_ne!(state.session_id, prepare.state.session_id);
        assert_eq!(state.session_id, format!("{proposer}:2000000:idle"));
        assert_eq!(state.leader_peer_id, proposer.to_string());
        assert!(state.track.is_none());
        assert_eq!(state.track_requested_by, None);
        assert_eq!(state.state_version, 2_000_000);
        assert_eq!(state.issued_at_micros, 2_000_000);
        assert!(!state.playing);
        assert_eq!(state.position_ms, 0);
        assert_eq!(state.anchor_time_micros, 2_000_000);
    }

    #[test]
    fn playback_vote_application_uses_vote_timestamp_and_actor() {
        let leader = peer_id();
        let proposer = peer_id();
        let requester = peer_id();
        let mut first = MusicState::new();
        let mut second = MusicState::new();
        first.playback_version = 7;
        second.playback_version = 42;
        first.playback_phase = PlaybackPhase::Active(playback_state(
            leader, requester, "session", true, 1_000, 1_000_000, 10_000,
        ));
        second.playback_phase = PlaybackPhase::Active(playback_state(
            leader, requester, "session", true, 1_000, 1_000_000, 10_000,
        ));

        let first_state = first
            .seek_playback_for_vote(proposer, 4_000, 2_000_000)
            .unwrap();
        let second_state = second
            .seek_playback_for_vote(proposer, 4_000, 2_000_000)
            .unwrap();

        assert_eq!(first_state.session_id, second_state.session_id);
        assert_eq!(first_state.leader_peer_id, proposer.to_string());
        assert_eq!(second_state.leader_peer_id, proposer.to_string());
        assert_eq!(first_state.state_version, 2_000_000);
        assert_eq!(second_state.state_version, 2_000_000);
        assert_eq!(first_state.issued_at_micros, 2_000_000);
        assert_eq!(second_state.issued_at_micros, 2_000_000);
        assert_eq!(first_state.position_ms, 4_000);
        assert_eq!(second_state.position_ms, 4_000);
        assert_eq!(first_state.anchor_time_micros, 2_000_000);
        assert_eq!(second_state.anchor_time_micros, 2_000_000);
        assert_eq!(first_state.playing, second_state.playing);
    }

    #[test]
    fn idle_playback_does_not_block_next_queued_item() {
        let local = peer_id();
        let mut music = MusicState::new();
        let idle = PlaybackState {
            session_id: "idle".to_string(),
            leader_peer_id: local.to_string(),
            track: None,
            track_requested_by: None,
            state_version: 1,
            issued_at_micros: 1_000_000,
            playing: false,
            position_ms: 0,
            anchor_time_micros: 1_000_000,
            rate: 1.0,
        };
        music.playback_phase = PlaybackPhase::Idle { state: Some(idle) };
        music.queue.push_back(queue_item("next"));

        assert!(music.can_start_next());
        assert!(music.pop_next_queue_item().is_some());
    }
}
