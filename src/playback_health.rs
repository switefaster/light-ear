use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::time::Instant;

use crate::core::PlaybackBufferStatusKind;
use crate::music_state::majority_threshold;

#[derive(Debug, Clone)]
pub(crate) struct PlaybackHealthStatus {
    pub(crate) status: PlaybackBufferStatusKind,
    pub(crate) buffered_until_ms: Option<u64>,
    updated_at: Instant,
}

#[derive(Debug, Default)]
pub(crate) struct PlaybackHealth {
    session_id: Option<String>,
    statuses: HashMap<String, PlaybackHealthStatus>,
    published_statuses: HashMap<String, PlaybackHealthStatus>,
    loss_since: Option<Instant>,
    paused_for_loss: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlaybackHealthDecision {
    Stable { healthy: usize, threshold: usize },
    MajorityLost { healthy: usize, threshold: usize },
}

impl PlaybackHealth {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn reset_session(&mut self, session_id: &str) {
        if self.session_id.as_deref() == Some(session_id) {
            return;
        }
        self.session_id = Some(session_id.to_string());
        self.statuses.clear();
        self.published_statuses.clear();
        self.loss_since = None;
        self.paused_for_loss = false;
    }

    pub(crate) fn clear(&mut self) {
        self.session_id = None;
        self.statuses.clear();
        self.published_statuses.clear();
        self.loss_since = None;
        self.paused_for_loss = false;
    }

    pub(crate) fn mark_status(
        &mut self,
        session_id: &str,
        peer_id: &str,
        status: PlaybackBufferStatusKind,
        buffered_until_ms: Option<u64>,
        now: Instant,
    ) {
        self.reset_session(session_id);
        self.statuses.insert(
            peer_id.to_string(),
            PlaybackHealthStatus {
                status,
                buffered_until_ms,
                updated_at: now,
            },
        );
    }

    pub(crate) fn mark_local_status_for_publish(
        &mut self,
        session_id: &str,
        peer_id: &str,
        status: PlaybackBufferStatusKind,
        buffered_until_ms: Option<u64>,
        now: Instant,
        min_interval: Duration,
        _min_buffer_advance_ms: u64,
    ) -> bool {
        self.reset_session(session_id);
        let next = PlaybackHealthStatus {
            status,
            buffered_until_ms,
            updated_at: now,
        };
        let should_publish = self.published_statuses.get(peer_id).is_none_or(|previous| {
            let interval_elapsed =
                now.saturating_duration_since(previous.updated_at) >= min_interval;
            let buffer_advanced = previous
                .buffered_until_ms
                .zip(next.buffered_until_ms)
                .is_none_or(|(previous, next)| next > previous);
            previous.status != next.status || (interval_elapsed && buffer_advanced)
        });

        self.statuses.insert(peer_id.to_string(), next.clone());
        if should_publish {
            self.published_statuses.insert(peer_id.to_string(), next);
        }
        should_publish
    }

    pub(crate) fn evaluate(
        &mut self,
        session_id: &str,
        expected_peers: &HashSet<String>,
        playback_position_ms: u64,
        now: Instant,
        loss_grace: Duration,
        stale_after: Duration,
    ) -> PlaybackHealthDecision {
        self.reset_session(session_id);
        let threshold = majority_threshold(expected_peers.len());
        let healthy = expected_peers
            .iter()
            .filter(|peer_id| {
                self.statuses
                    .get(*peer_id)
                    .is_some_and(|status| status.is_healthy(playback_position_ms, now, stale_after))
            })
            .count();

        if healthy >= threshold {
            self.loss_since = None;
            self.paused_for_loss = false;
            return PlaybackHealthDecision::Stable { healthy, threshold };
        }

        let loss_since = *self.loss_since.get_or_insert(now);
        if self.paused_for_loss || now.saturating_duration_since(loss_since) < loss_grace {
            return PlaybackHealthDecision::Stable { healthy, threshold };
        }

        self.paused_for_loss = true;
        PlaybackHealthDecision::MajorityLost { healthy, threshold }
    }
}

impl PlaybackHealthStatus {
    fn is_healthy(&self, playback_position_ms: u64, now: Instant, stale_after: Duration) -> bool {
        if self.status != PlaybackBufferStatusKind::Ready {
            return false;
        }
        let Some(buffered_until) = self.buffered_until_ms else {
            return false;
        };
        if buffered_until < playback_position_ms {
            return false;
        }

        if now.saturating_duration_since(self.updated_at) <= stale_after {
            return true;
        }

        let stale_slack_ms = u64::try_from(stale_after.as_millis()).unwrap_or(u64::MAX);
        buffered_until >= playback_position_ms.saturating_add(stale_slack_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peers<const N: usize>(ids: [&str; N]) -> HashSet<String> {
        ids.into_iter().map(str::to_string).collect()
    }

    #[test]
    fn majority_loss_requires_grace_window() {
        let now = Instant::now();
        let mut health = PlaybackHealth::new();
        let peers = peers(["local", "remote", "slow"]);
        health.mark_status(
            "session",
            "local",
            PlaybackBufferStatusKind::Ready,
            Some(10_000),
            now,
        );

        assert_eq!(
            health.evaluate(
                "session",
                &peers,
                4_000,
                now,
                Duration::from_secs(3),
                Duration::from_secs(5),
            ),
            PlaybackHealthDecision::Stable {
                healthy: 1,
                threshold: 2
            }
        );
        assert_eq!(
            health.evaluate(
                "session",
                &peers,
                4_000,
                now + Duration::from_secs(3),
                Duration::from_secs(3),
                Duration::from_secs(5),
            ),
            PlaybackHealthDecision::MajorityLost {
                healthy: 1,
                threshold: 2
            }
        );
    }

    #[test]
    fn majority_recovery_resets_loss_state() {
        let now = Instant::now();
        let mut health = PlaybackHealth::new();
        let peers = peers(["local", "remote", "slow"]);
        health.mark_status(
            "session",
            "local",
            PlaybackBufferStatusKind::Ready,
            Some(10_000),
            now,
        );
        assert!(matches!(
            health.evaluate(
                "session",
                &peers,
                4_000,
                now,
                Duration::from_secs(3),
                Duration::from_secs(5),
            ),
            PlaybackHealthDecision::Stable { .. }
        ));
        assert!(matches!(
            health.evaluate(
                "session",
                &peers,
                4_000,
                now + Duration::from_secs(3),
                Duration::from_secs(3),
                Duration::from_secs(5),
            ),
            PlaybackHealthDecision::MajorityLost { .. }
        ));

        health.mark_status(
            "session",
            "remote",
            PlaybackBufferStatusKind::Ready,
            Some(12_000),
            now + Duration::from_secs(4),
        );
        assert_eq!(
            health.evaluate(
                "session",
                &peers,
                5_000,
                now + Duration::from_secs(4),
                Duration::from_secs(3),
                Duration::from_secs(5),
            ),
            PlaybackHealthDecision::Stable {
                healthy: 2,
                threshold: 2
            }
        );

        health.mark_status(
            "session",
            "remote",
            PlaybackBufferStatusKind::Buffering,
            Some(5_000),
            now + Duration::from_secs(5),
        );
        assert!(matches!(
            health.evaluate(
                "session",
                &peers,
                6_000,
                now + Duration::from_secs(5),
                Duration::from_secs(3),
                Duration::from_secs(5),
            ),
            PlaybackHealthDecision::Stable { .. }
        ));
        assert_eq!(
            health.evaluate(
                "session",
                &peers,
                6_000,
                now + Duration::from_secs(8),
                Duration::from_secs(3),
                Duration::from_secs(5),
            ),
            PlaybackHealthDecision::MajorityLost {
                healthy: 0,
                threshold: 2
            }
        );
    }

    #[test]
    fn stale_health_needs_extra_buffer_ahead() {
        let now = Instant::now();
        let mut health = PlaybackHealth::new();
        let peers = peers(["local"]);
        health.mark_status(
            "session",
            "local",
            PlaybackBufferStatusKind::Ready,
            Some(10_000),
            now,
        );

        assert_eq!(
            health.evaluate(
                "session",
                &peers,
                6_000,
                now + Duration::from_secs(6),
                Duration::from_secs(3),
                Duration::from_secs(5),
            ),
            PlaybackHealthDecision::Stable {
                healthy: 0,
                threshold: 1
            }
        );

        assert_eq!(
            health.evaluate(
                "session",
                &peers,
                4_000,
                now + Duration::from_secs(6),
                Duration::from_secs(3),
                Duration::from_secs(5),
            ),
            PlaybackHealthDecision::Stable {
                healthy: 1,
                threshold: 1
            }
        );
    }

    #[test]
    fn local_health_publish_is_rate_limited_without_hiding_status_changes() {
        let now = Instant::now();
        let mut health = PlaybackHealth::new();

        assert!(health.mark_local_status_for_publish(
            "session",
            "local",
            PlaybackBufferStatusKind::Ready,
            Some(12_000),
            now,
            Duration::from_secs(1),
            5_000,
        ));
        assert!(!health.mark_local_status_for_publish(
            "session",
            "local",
            PlaybackBufferStatusKind::Ready,
            Some(13_000),
            now + Duration::from_millis(100),
            Duration::from_secs(1),
            5_000,
        ));
        assert!(health.mark_local_status_for_publish(
            "session",
            "local",
            PlaybackBufferStatusKind::Buffering,
            Some(13_000),
            now + Duration::from_millis(200),
            Duration::from_secs(1),
            5_000,
        ));
        assert!(health.mark_local_status_for_publish(
            "session",
            "local",
            PlaybackBufferStatusKind::Buffering,
            Some(14_000),
            now + Duration::from_secs(2),
            Duration::from_secs(1),
            5_000,
        ));
    }
}
