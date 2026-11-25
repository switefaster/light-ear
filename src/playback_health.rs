use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tokio::time::Instant;

use crate::core::{PlaybackBufferStatusKind, PlaybackCacheStatus, PlaybackCacheView};
use crate::media_cache::{LOW_WATERMARK_MS, covers_track_end};
use crate::music_state::majority_threshold;

const LOSS_GRACE: Duration = Duration::from_secs(3);
const STALE_AFTER: Duration = Duration::from_secs(5);
const PUBLISH_MIN_INTERVAL: Duration = Duration::from_secs(1);

impl PlaybackCacheView {
    pub(crate) fn ready_through_track_end(&self, decoder_finished: bool) -> bool {
        self.status == PlaybackCacheStatus::Ready
            && self.buffered_until_ms > self.buffered_from_ms
            && covers_track_end(self.buffered_until_ms, self.duration_ms, decoder_finished)
    }
}

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
        ready_through_end: bool,
    ) -> bool {
        self.reset_session(session_id);
        let next = PlaybackHealthStatus {
            status,
            buffered_until_ms,
            updated_at: now,
        };
        let should_publish = self.published_statuses.get(peer_id).is_none_or(|previous| {
            let interval_elapsed =
                now.saturating_duration_since(previous.updated_at) >= PUBLISH_MIN_INTERVAL;
            let buffer_advanced = previous
                .buffered_until_ms
                .zip(next.buffered_until_ms)
                .is_none_or(|(previous, next)| next > previous);
            previous.status != next.status
                || ((interval_elapsed || ready_through_end) && buffer_advanced)
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
        track_duration_ms: u64,
        now: Instant,
    ) -> PlaybackHealthDecision {
        self.reset_session(session_id);
        let threshold = majority_threshold(expected_peers.len());
        let healthy = expected_peers
            .iter()
            .filter(|peer_id| {
                self.statuses.get(*peer_id).is_some_and(|status| {
                    status.is_healthy(playback_position_ms, track_duration_ms, now)
                })
            })
            .count();

        if healthy >= threshold {
            self.loss_since = None;
            self.paused_for_loss = false;
            return PlaybackHealthDecision::Stable { healthy, threshold };
        }

        let loss_since = *self.loss_since.get_or_insert(now);
        if self.paused_for_loss || now.saturating_duration_since(loss_since) < LOSS_GRACE {
            return PlaybackHealthDecision::Stable { healthy, threshold };
        }

        self.paused_for_loss = true;
        PlaybackHealthDecision::MajorityLost { healthy, threshold }
    }
}

impl PlaybackHealthStatus {
    fn is_healthy(&self, position_ms: u64, duration_ms: u64, now: Instant) -> bool {
        if self.status != PlaybackBufferStatusKind::Ready {
            return false;
        }
        let Some(buffered_until) = self.buffered_until_ms else {
            return false;
        };
        let mut required_until_ms = position_ms.saturating_add(LOW_WATERMARK_MS);
        if now.saturating_duration_since(self.updated_at) > STALE_AFTER {
            required_until_ms = required_until_ms.saturating_add(STALE_AFTER.as_millis() as u64);
        }
        buffered_until >= required_until_ms.min(duration_ms)
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
            Some(15_000),
            now,
        );

        assert_eq!(
            health.evaluate("session", &peers, 4_000, 120_000, now),
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
                120_000,
                now + Duration::from_secs(3),
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
            Some(15_000),
            now,
        );
        assert!(matches!(
            health.evaluate("session", &peers, 4_000, 120_000, now),
            PlaybackHealthDecision::Stable { .. }
        ));
        assert!(matches!(
            health.evaluate(
                "session",
                &peers,
                4_000,
                120_000,
                now + Duration::from_secs(3),
            ),
            PlaybackHealthDecision::MajorityLost { .. }
        ));

        health.mark_status(
            "session",
            "remote",
            PlaybackBufferStatusKind::Ready,
            Some(17_000),
            now + Duration::from_secs(4),
        );
        assert_eq!(
            health.evaluate(
                "session",
                &peers,
                5_000,
                120_000,
                now + Duration::from_secs(4),
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
                120_000,
                now + Duration::from_secs(5),
            ),
            PlaybackHealthDecision::Stable { .. }
        ));
        assert_eq!(
            health.evaluate(
                "session",
                &peers,
                6_000,
                120_000,
                now + Duration::from_secs(8),
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
            Some(15_000),
            now,
        );

        assert_eq!(
            health.evaluate(
                "session",
                &peers,
                6_000,
                120_000,
                now + Duration::from_secs(6),
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
                120_000,
                now + Duration::from_secs(6),
            ),
            PlaybackHealthDecision::Stable {
                healthy: 1,
                threshold: 1
            }
        );
    }

    #[test]
    fn fully_buffered_solo_playback_stays_healthy_to_the_end() {
        let now = Instant::now();
        let members = peers(["local"]);
        for duration_ms in [3_000, 30_000] {
            let mut health = PlaybackHealth::new();
            health.mark_local_status_for_publish(
                "session",
                "local",
                PlaybackBufferStatusKind::Ready,
                Some(duration_ms),
                now,
                true,
            );
            for position_ms in (0..=duration_ms).step_by(500) {
                assert_eq!(
                    health.evaluate(
                        "session",
                        &members,
                        position_ms,
                        duration_ms,
                        now + Duration::from_millis(position_ms)
                    ),
                    PlaybackHealthDecision::Stable {
                        healthy: 1,
                        threshold: 1
                    },
                    "position {position_ms} of {duration_ms}"
                );
            }
        }
    }

    #[test]
    fn complete_remote_report_arrives_once_and_a_real_tail_underrun_still_pauses() {
        use PlaybackBufferStatusKind::{Buffering, Ready};
        let now = Instant::now();
        let mut sender = PlaybackHealth::new();
        let mut leader = PlaybackHealth::new();
        let members = peers(["local", "remote"]);
        let end = 30_000;
        leader.mark_status("s", "local", Ready, Some(end), now);
        for (until, millis, through_end, should_send) in [
            (24_000, 0, false, true),
            (27_000, 100, false, false),
            (end, 200, true, true),
            (end, 300, true, false),
            (end, 2_000, true, false),
        ] {
            let at = now + Duration::from_millis(millis);
            let send = sender.mark_local_status_for_publish(
                "s",
                "remote",
                Ready,
                Some(until),
                at,
                through_end,
            );
            assert_eq!(send, should_send);
            if send {
                leader.mark_status("s", "remote", Ready, Some(until), at);
            }
        }
        let tail = now + Duration::from_secs(24);
        assert_eq!(
            leader.evaluate("s", &members, 24_000, end, tail),
            PlaybackHealthDecision::Stable {
                healthy: 2,
                threshold: 2
            }
        );
        leader.mark_status("s", "remote", Buffering, Some(end), tail);
        assert_eq!(
            leader.evaluate("s", &members, 24_000, end, tail),
            PlaybackHealthDecision::Stable {
                healthy: 1,
                threshold: 2
            }
        );
        assert_eq!(
            leader.evaluate("s", &members, 27_000, end, tail + LOSS_GRACE),
            PlaybackHealthDecision::MajorityLost {
                healthy: 1,
                threshold: 2
            }
        );
    }

    #[test]
    fn only_decoded_eof_can_cover_a_fractional_duration_gap() {
        let mut view = PlaybackCacheView {
            session_id: "s".into(),
            track_id: "t".into(),
            status: PlaybackCacheStatus::Ready,
            buffered_from_ms: 5_000,
            buffered_until_ms: 29_630,
            duration_ms: 30_000,
            error: None,
        };
        assert!(!view.ready_through_track_end(false));
        assert!(view.ready_through_track_end(true));
        view.buffered_until_ms = 29_000;
        assert!(!view.ready_through_track_end(true));
        view.buffered_until_ms = 30_000;
        assert!(view.ready_through_track_end(false));
        view.status = PlaybackCacheStatus::Buffering;
        assert!(!view.ready_through_track_end(true));
        view.status = PlaybackCacheStatus::Ready;
        view.buffered_from_ms = 30_000;
        assert!(!view.ready_through_track_end(true));
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
            false,
        ));
        assert!(!health.mark_local_status_for_publish(
            "session",
            "local",
            PlaybackBufferStatusKind::Ready,
            Some(13_000),
            now + Duration::from_millis(100),
            false,
        ));
        assert!(health.mark_local_status_for_publish(
            "session",
            "local",
            PlaybackBufferStatusKind::Buffering,
            Some(13_000),
            now + Duration::from_millis(200),
            false,
        ));
        assert!(health.mark_local_status_for_publish(
            "session",
            "local",
            PlaybackBufferStatusKind::Buffering,
            Some(14_000),
            now + Duration::from_secs(2),
            false,
        ));
    }
}
