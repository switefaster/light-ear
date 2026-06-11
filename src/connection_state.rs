use std::collections::{HashMap, HashSet};
use std::time::Duration;

use libp2p::{Multiaddr, PeerId, swarm::ConnectionId};
use tokio::time::Instant;

use crate::core::PeerConnectionView;

pub(crate) const DIRECT_PROMOTION_RETRY_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const DIRECT_PROMOTION_MEDIUM_RETRY_INTERVAL: Duration = Duration::from_secs(120);
pub(crate) const DIRECT_PROMOTION_SLOW_RETRY_INTERVAL: Duration = Duration::from_secs(600);
pub(crate) const DIRECT_PROMOTION_MEDIUM_RETRY_FAILURES: u32 = 3;
pub(crate) const DIRECT_PROMOTION_SLOW_RETRY_FAILURES: u32 = 6;
pub(crate) const DIRECT_PROMOTION_MAX_FAILURES: u32 = 10;
pub(crate) const DIRECT_PROMOTION_FAILURE_DEDUP_WINDOW: Duration = Duration::from_secs(5);
pub(crate) const GOSSIP_WARMUP_TIMEOUT: Duration = Duration::from_secs(5);
pub(crate) const GOSSIP_WARMUP_CHECK_INTERVAL: Duration = Duration::from_millis(500);
pub(crate) const DIRECT_RELAY_HANDOFF_GRACE: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum RelayCloseReason {
    HandoffSettled,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) enum ConnectionEffect {
    Status(String),
    TrackGossipPeer(PeerId),
    UntrackGossipPeer(PeerId),
    ResetBackoff(PeerId),
    DialDirect {
        peer_id: PeerId,
        addresses: Vec<Multiaddr>,
    },
    CloseRelayConnections {
        peer_id: PeerId,
        connection_ids: Vec<ConnectionId>,
        reason: RelayCloseReason,
    },
    CloseEarlyDirectConnection {
        peer_id: PeerId,
        connection_id: ConnectionId,
    },
}

#[derive(Debug, Default)]
pub(crate) struct PeerConnectionRoutes {
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

    pub(crate) fn is_relay_only(&self) -> bool {
        self.direct.is_empty() && !self.relayed.is_empty()
    }

    pub(crate) fn has_direct(&self) -> bool {
        !self.direct.is_empty()
    }

    pub(crate) fn has_relayed(&self) -> bool {
        !self.relayed.is_empty()
    }

    pub(crate) fn relayed_connections(&self) -> Vec<ConnectionId> {
        self.relayed.iter().copied().collect()
    }

    fn direct_count(&self) -> usize {
        self.direct.len()
    }

    fn relayed_count(&self) -> usize {
        self.relayed.len()
    }
}

#[derive(Debug, Default)]
pub(crate) struct DirectPromotionBackoff {
    pub(crate) attempts: u32,
    pub(crate) failures: u32,
    last_attempt: Option<Instant>,
    last_failure: Option<Instant>,
    pub(crate) in_flight: bool,
    pub(crate) suspended_reported: bool,
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

    fn retry_remaining(&self, now: Instant) -> Option<Duration> {
        let last_attempt = self.last_attempt?;
        self.retry_interval()
            .checked_sub(now.saturating_duration_since(last_attempt))
    }

    pub(crate) fn should_attempt(&self, now: Instant) -> bool {
        !self.in_flight
            && self.failures < DIRECT_PROMOTION_MAX_FAILURES
            && self.retry_remaining(now).is_none()
    }

    fn mark_attempt(&mut self, now: Instant) {
        self.attempts = self.attempts.saturating_add(1);
        self.last_attempt = Some(now);
        self.in_flight = true;
    }

    pub(crate) fn mark_failure(&mut self, now: Instant) -> DirectPromotionFailureOutcome {
        self.in_flight = false;
        if self.last_failure.is_some_and(|last_failure| {
            now.saturating_duration_since(last_failure) < DIRECT_PROMOTION_FAILURE_DEDUP_WINDOW
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

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(crate) enum DirectPromotionFailureOutcome {
    Counted {
        failures: u32,
        retry_after: Option<Duration>,
    },
    Duplicate,
}

#[derive(Debug)]
struct GossipsubWarmup {
    started_at: Instant,
}

impl GossipsubWarmup {
    fn new(now: Instant) -> Self {
        Self { started_at: now }
    }

    fn is_expired(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.started_at) >= GOSSIP_WARMUP_TIMEOUT
    }
}

#[derive(Debug)]
pub(crate) struct ConnectionState {
    local_peer_id: PeerId,
    routes: HashMap<PeerId, PeerConnectionRoutes>,
    direct_addresses: HashMap<PeerId, HashSet<Multiaddr>>,
    backoffs: HashMap<PeerId, DirectPromotionBackoff>,
    warmups: HashMap<PeerId, GossipsubWarmup>,
    warmup_completed: HashSet<PeerId>,
    relay_handoffs: HashMap<PeerId, Instant>,
    chat_subscribers: HashSet<PeerId>,
    gossip_peers: HashSet<PeerId>,
}

impl ConnectionState {
    pub(crate) fn new(local_peer_id: PeerId) -> Self {
        Self {
            local_peer_id,
            routes: HashMap::new(),
            direct_addresses: HashMap::new(),
            backoffs: HashMap::new(),
            warmups: HashMap::new(),
            warmup_completed: HashSet::new(),
            relay_handoffs: HashMap::new(),
            chat_subscribers: HashSet::new(),
            gossip_peers: HashSet::new(),
        }
    }

    pub(crate) fn routes(&self) -> &HashMap<PeerId, PeerConnectionRoutes> {
        &self.routes
    }

    pub(crate) fn peer_views(&self, rendezvous_nodes: &HashSet<PeerId>) -> Vec<PeerConnectionView> {
        let mut peer_ids = HashSet::new();
        peer_ids.extend(self.routes.keys().copied());
        peer_ids.extend(self.direct_addresses.keys().copied());
        peer_ids.extend(self.backoffs.keys().copied());
        peer_ids.extend(self.chat_subscribers.iter().copied());
        peer_ids.extend(rendezvous_nodes.iter().copied());
        peer_ids.remove(&self.local_peer_id);

        let mut views = peer_ids
            .into_iter()
            .map(|peer_id| {
                let routes = self.routes.get(&peer_id);
                let direct_connections = routes
                    .map(PeerConnectionRoutes::direct_count)
                    .unwrap_or_default();
                let relayed_connections = routes
                    .map(PeerConnectionRoutes::relayed_count)
                    .unwrap_or_default();
                let route = match (direct_connections > 0, relayed_connections > 0) {
                    (true, true) => "direct+relay",
                    (true, false) => "direct",
                    (false, true) => "relay",
                    (false, false) => "known",
                }
                .to_string();
                let backoff = self.backoffs.get(&peer_id);

                PeerConnectionView {
                    peer_id: peer_id.to_string(),
                    kind: if rendezvous_nodes.contains(&peer_id) {
                        "rendezvous".to_string()
                    } else {
                        "room".to_string()
                    },
                    route,
                    direct_connections,
                    relayed_connections,
                    direct_address_count: self
                        .direct_addresses
                        .get(&peer_id)
                        .map(HashSet::len)
                        .unwrap_or_default(),
                    chat_subscribed: self.chat_subscribers.contains(&peer_id),
                    direct_promotion_attempts: backoff
                        .map(|backoff| backoff.attempts)
                        .unwrap_or_default(),
                    direct_promotion_failures: backoff
                        .map(|backoff| backoff.failures)
                        .unwrap_or_default(),
                    direct_promotion_in_flight: backoff
                        .map(|backoff| backoff.in_flight)
                        .unwrap_or_default(),
                    direct_promotion_suspended: backoff
                        .map(|backoff| backoff.failures >= DIRECT_PROMOTION_MAX_FAILURES)
                        .unwrap_or_default(),
                }
            })
            .collect::<Vec<_>>();

        views.sort_by(|left, right| {
            left.kind
                .cmp(&right.kind)
                .then_with(|| left.route.cmp(&right.route))
                .then_with(|| left.peer_id.cmp(&right.peer_id))
        });
        views
    }

    #[cfg(test)]
    pub(crate) fn backoff_mut(&mut self, peer_id: PeerId) -> &mut DirectPromotionBackoff {
        self.backoffs.entry(peer_id).or_default()
    }

    pub(crate) fn is_chat_subscribed(&self, peer_id: PeerId) -> bool {
        self.chat_subscribers.contains(&peer_id)
    }

    pub(crate) fn is_relay_only(&self, peer_id: PeerId) -> bool {
        self.routes
            .get(&peer_id)
            .is_some_and(PeerConnectionRoutes::is_relay_only)
    }

    pub(crate) fn has_direct(&self, peer_id: PeerId) -> bool {
        self.routes
            .get(&peer_id)
            .is_some_and(PeerConnectionRoutes::has_direct)
    }

    pub(crate) fn has_relayed(&self, peer_id: PeerId) -> bool {
        self.routes
            .get(&peer_id)
            .is_some_and(PeerConnectionRoutes::has_relayed)
    }

    pub(crate) fn connection_established(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        is_relayed: bool,
        is_rendezvous_node: bool,
        now: Instant,
    ) -> Vec<ConnectionEffect> {
        self.routes
            .entry(peer_id)
            .or_default()
            .add(connection_id, is_relayed);

        let mut effects = Vec::new();
        if !is_rendezvous_node {
            effects.extend(
                self.track_gossip_peer(peer_id, Some(format!("tracking {peer_id} as gossip peer"))),
            );
        }

        if is_relayed {
            effects.push(ConnectionEffect::Status(format!(
                "connected {peer_id} via relay"
            )));
            if !is_rendezvous_node && !self.is_chat_subscribed(peer_id) {
                effects.extend(self.start_gossip_warmup(peer_id, now));
            }
            self.maybe_promote_relayed_peer(peer_id, now, &mut effects);
            return effects;
        }

        let has_relayed_route = self.has_relayed(peer_id);
        let promotion_allowed = !has_relayed_route || self.is_chat_subscribed(peer_id) || {
            let mut warmup_effects = Vec::new();
            let allowed = self.gossip_warmup_allows_promotion(peer_id, now, &mut warmup_effects);
            effects.extend(warmup_effects);
            allowed
        };

        if promotion_allowed {
            effects.extend(self.reset_backoff(peer_id));
            if self.is_chat_subscribed(peer_id) {
                if self.has_relayed(peer_id) {
                    self.schedule_relay_handoff(peer_id, now, &mut effects);
                } else {
                    effects.push(ConnectionEffect::Status(format!(
                        "connected {peer_id} directly"
                    )));
                }
            } else if has_relayed_route {
                effects.push(ConnectionEffect::Status(format!(
                    "promoted {peer_id} to direct connection after gossip warmup timeout; keeping relay until chat subscription is ready"
                )));
            } else {
                effects.push(ConnectionEffect::Status(format!(
                    "connected {peer_id} directly"
                )));
            }
        } else {
            effects.push(ConnectionEffect::CloseEarlyDirectConnection {
                peer_id,
                connection_id,
            });
        }

        effects
    }

    pub(crate) fn connection_closed(
        &mut self,
        peer_id: PeerId,
        connection_id: ConnectionId,
        was_relayed: bool,
        remaining_established: u32,
    ) -> Vec<ConnectionEffect> {
        if let Some(routes) = self.routes.get_mut(&peer_id) {
            routes.remove(connection_id, was_relayed);
            if routes.is_empty() {
                self.routes.remove(&peer_id);
            }
        }
        if !self.has_direct(peer_id) || !self.has_relayed(peer_id) {
            self.relay_handoffs.remove(&peer_id);
        }

        let mut effects = Vec::new();
        if remaining_established > 0 {
            let route = if was_relayed { "relay" } else { "direct" };
            effects.push(ConnectionEffect::Status(format!(
                "{route} connection closed {peer_id}; {remaining_established} link(s) remain"
            )));
        } else {
            self.backoffs.remove(&peer_id);
            self.chat_subscribers.remove(&peer_id);
            self.direct_addresses.remove(&peer_id);
            self.warmups.remove(&peer_id);
            self.warmup_completed.remove(&peer_id);
            self.relay_handoffs.remove(&peer_id);
            effects.push(ConnectionEffect::Status(format!("disconnected {peer_id}")));
            effects.extend(self.untrack_gossip_peer(peer_id));
        }
        effects
    }

    pub(crate) fn track_gossip_peer(
        &mut self,
        peer_id: PeerId,
        status: Option<String>,
    ) -> Vec<ConnectionEffect> {
        if !self.gossip_peers.insert(peer_id) {
            return Vec::new();
        }

        let mut effects = vec![ConnectionEffect::TrackGossipPeer(peer_id)];
        if let Some(status) = status {
            effects.push(ConnectionEffect::Status(status));
        }
        effects
    }

    pub(crate) fn untrack_gossip_peer(&mut self, peer_id: PeerId) -> Vec<ConnectionEffect> {
        if self.gossip_peers.remove(&peer_id) {
            vec![ConnectionEffect::UntrackGossipPeer(peer_id)]
        } else {
            Vec::new()
        }
    }

    pub(crate) fn learn_direct_addresses<I>(
        &mut self,
        peer_id: PeerId,
        addresses: I,
        now: Instant,
    ) -> Vec<ConnectionEffect>
    where
        I: IntoIterator<Item = Multiaddr>,
    {
        let added = self.remember_direct_addresses(peer_id, addresses);
        if added == 0 {
            return Vec::new();
        }

        let mut effects = self.reset_backoff(peer_id);
        self.maybe_promote_relayed_peer(peer_id, now, &mut effects);
        effects
    }

    pub(crate) fn forget_direct_address(&mut self, peer_id: PeerId, address: Multiaddr) -> bool {
        let Some(address) = normalize_direct_peer_address(peer_id, address) else {
            return false;
        };
        let Some(known_addresses) = self.direct_addresses.get_mut(&peer_id) else {
            return false;
        };

        let removed = known_addresses.remove(&address);
        if known_addresses.is_empty() {
            self.direct_addresses.remove(&peer_id);
        }
        removed
    }

    pub(crate) fn chat_subscribed(
        &mut self,
        peer_id: PeerId,
        now: Instant,
    ) -> Vec<ConnectionEffect> {
        self.chat_subscribers.insert(peer_id);
        self.warmups.remove(&peer_id);
        self.warmup_completed.remove(&peer_id);
        let mut effects = vec![ConnectionEffect::Status(format!(
            "peer {peer_id} subscribed to chat"
        ))];
        self.maybe_promote_relayed_peer(peer_id, now, &mut effects);

        if self.has_direct(peer_id) {
            self.schedule_relay_handoff(peer_id, now, &mut effects);
        }

        effects
    }

    pub(crate) fn chat_unsubscribed(&mut self, peer_id: PeerId) -> Vec<ConnectionEffect> {
        if self.routes.contains_key(&peer_id) {
            self.relay_handoffs.remove(&peer_id);
            return vec![ConnectionEffect::Status(format!(
                "peer {peer_id} chat unsubscribe observed while still connected; keeping readiness until disconnect"
            ))];
        }

        self.chat_subscribers.remove(&peer_id);
        vec![ConnectionEffect::Status(format!(
            "peer {peer_id} unsubscribed from chat"
        ))]
    }

    pub(crate) fn promotion_tick(&mut self, now: Instant) -> Vec<ConnectionEffect> {
        let peers = self.routes.keys().copied().collect::<Vec<_>>();
        let mut effects = Vec::new();
        for peer_id in peers {
            self.maybe_promote_relayed_peer(peer_id, now, &mut effects);
        }
        effects
    }

    pub(crate) fn warmup_tick(&mut self, now: Instant) -> Vec<ConnectionEffect> {
        let peers = self
            .warmups
            .iter()
            .filter_map(|(peer_id, warmup)| warmup.is_expired(now).then_some(*peer_id))
            .collect::<Vec<_>>();
        let handoffs = self
            .relay_handoffs
            .iter()
            .filter_map(|(peer_id, deadline)| (*deadline <= now).then_some(*peer_id))
            .collect::<Vec<_>>();

        let mut effects = Vec::new();
        for peer_id in peers {
            self.maybe_promote_relayed_peer(peer_id, now, &mut effects);
        }
        for peer_id in handoffs {
            self.close_relay_after_handoff(peer_id, &mut effects);
        }
        effects
    }

    pub(crate) fn dcutr_succeeded(&mut self, peer_id: PeerId) -> Vec<ConnectionEffect> {
        self.reset_backoff(peer_id)
    }

    pub(crate) fn dcutr_failed(
        &mut self,
        peer_id: PeerId,
        reason: String,
        now: Instant,
    ) -> Vec<ConnectionEffect> {
        if self.is_relay_only(peer_id) {
            self.record_direct_promotion_failure(peer_id, format!("DCUtR failed: {reason}"), now)
        } else {
            vec![ConnectionEffect::Status(format!(
                "direct upgrade failed with {peer_id}: {reason}"
            ))]
        }
    }

    pub(crate) fn outgoing_connection_error(
        &mut self,
        peer_id: PeerId,
        reason: String,
        now: Instant,
    ) -> Vec<ConnectionEffect> {
        if self
            .backoffs
            .get(&peer_id)
            .is_some_and(|backoff| backoff.in_flight)
        {
            self.record_direct_promotion_failure(peer_id, reason, now)
        } else {
            vec![ConnectionEffect::Status(format!(
                "outgoing connection error {peer_id}: {reason}"
            ))]
        }
    }

    pub(crate) fn record_direct_promotion_failure(
        &mut self,
        peer_id: PeerId,
        reason: String,
        now: Instant,
    ) -> Vec<ConnectionEffect> {
        let outcome = self.backoffs.entry(peer_id).or_default().mark_failure(now);

        let DirectPromotionFailureOutcome::Counted {
            failures,
            retry_after,
        } = outcome
        else {
            return Vec::new();
        };

        if let Some(retry_after) = retry_after {
            vec![ConnectionEffect::Status(format!(
                "direct promotion failed for {peer_id} ({failures}/{DIRECT_PROMOTION_MAX_FAILURES}): {reason}; retrying in {}",
                format_retry_duration(retry_after)
            ))]
        } else {
            if let Some(backoff) = self.backoffs.get_mut(&peer_id) {
                backoff.suspended_reported = true;
            }
            vec![ConnectionEffect::Status(format!(
                "direct promotion suspended for {peer_id} after {failures} failures: {reason}; waiting for new direct addresses"
            ))]
        }
    }

    fn remember_direct_addresses<I>(&mut self, peer_id: PeerId, addresses: I) -> usize
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

        let known_addresses = self.direct_addresses.entry(peer_id).or_default();
        addresses
            .into_iter()
            .filter(|address| known_addresses.insert(address.clone()))
            .count()
    }

    fn start_gossip_warmup(&mut self, peer_id: PeerId, now: Instant) -> Vec<ConnectionEffect> {
        if self.warmup_completed.contains(&peer_id) {
            return Vec::new();
        }
        if self.warmups.contains_key(&peer_id) {
            return Vec::new();
        }

        self.warmups.insert(peer_id, GossipsubWarmup::new(now));
        vec![ConnectionEffect::Status(format!(
            "waiting up to {} for {peer_id} to subscribe to chat before direct promotion",
            format_retry_duration(GOSSIP_WARMUP_TIMEOUT)
        ))]
    }

    fn gossip_warmup_allows_promotion(
        &mut self,
        peer_id: PeerId,
        now: Instant,
        effects: &mut Vec<ConnectionEffect>,
    ) -> bool {
        if self.is_chat_subscribed(peer_id) {
            self.warmups.remove(&peer_id);
            self.warmup_completed.remove(&peer_id);
            return true;
        }
        if self.warmup_completed.contains(&peer_id) {
            return true;
        }

        let Some(warmup) = self.warmups.get(&peer_id) else {
            effects.extend(self.start_gossip_warmup(peer_id, now));
            return false;
        };
        if !warmup.is_expired(now) {
            return false;
        }

        self.warmups.remove(&peer_id);
        self.warmup_completed.insert(peer_id);
        effects.push(ConnectionEffect::Status(format!(
            "gossipsub warmup timed out for {peer_id}; trying direct promotion anyway"
        )));
        true
    }

    fn maybe_promote_relayed_peer(
        &mut self,
        peer_id: PeerId,
        now: Instant,
        effects: &mut Vec<ConnectionEffect>,
    ) {
        if peer_id == self.local_peer_id || !self.is_relay_only(peer_id) {
            return;
        }

        let Some(addresses) = self.direct_addresses.get(&peer_id).cloned() else {
            return;
        };
        if addresses.is_empty() {
            return;
        }

        if !self.gossip_warmup_allows_promotion(peer_id, now, effects) {
            return;
        }

        let suspended_failures = {
            let backoff = self.backoffs.entry(peer_id).or_default();
            if backoff.failures >= DIRECT_PROMOTION_MAX_FAILURES {
                if backoff.suspended_reported {
                    return;
                }
                backoff.suspended_reported = true;
                Some(backoff.failures)
            } else {
                if !backoff.should_attempt(now) {
                    return;
                }
                backoff.mark_attempt(now);
                None
            }
        };
        if let Some(failures) = suspended_failures {
            effects.push(ConnectionEffect::Status(format!(
                "direct promotion suspended for {peer_id} after {failures} failures; waiting for new direct addresses"
            )));
            return;
        }

        let addresses = prioritize_multiaddrs(addresses.into_iter().collect());
        let address_count = addresses.len();
        effects.push(ConnectionEffect::DialDirect { peer_id, addresses });
        effects.push(ConnectionEffect::Status(format!(
            "trying direct connection to {peer_id} ({address_count} candidate address(es))"
        )));
    }

    fn reset_backoff(&mut self, peer_id: PeerId) -> Vec<ConnectionEffect> {
        if self.backoffs.remove(&peer_id).is_some() {
            vec![ConnectionEffect::ResetBackoff(peer_id)]
        } else {
            Vec::new()
        }
    }

    fn relay_connections(&self, peer_id: PeerId) -> Vec<ConnectionId> {
        self.routes
            .get(&peer_id)
            .map(PeerConnectionRoutes::relayed_connections)
            .unwrap_or_default()
    }

    fn schedule_relay_handoff(
        &mut self,
        peer_id: PeerId,
        now: Instant,
        effects: &mut Vec<ConnectionEffect>,
    ) {
        if !self.has_direct(peer_id) || !self.has_relayed(peer_id) {
            self.relay_handoffs.remove(&peer_id);
            return;
        }

        let deadline = now + DIRECT_RELAY_HANDOFF_GRACE;
        if self.relay_handoffs.insert(peer_id, deadline).is_none() {
            effects.push(ConnectionEffect::Status(format!(
                "direct connection ready with {peer_id}; keeping relay for {} handoff",
                format_retry_duration(DIRECT_RELAY_HANDOFF_GRACE)
            )));
        }
    }

    fn close_relay_after_handoff(&mut self, peer_id: PeerId, effects: &mut Vec<ConnectionEffect>) {
        if !self.has_direct(peer_id) || !self.has_relayed(peer_id) {
            self.relay_handoffs.remove(&peer_id);
            return;
        }
        if !self.is_chat_subscribed(peer_id) {
            self.relay_handoffs.remove(&peer_id);
            return;
        }

        let relay_connections = self.relay_connections(peer_id);
        self.relay_handoffs.remove(&peer_id);
        if !relay_connections.is_empty() {
            effects.push(ConnectionEffect::CloseRelayConnections {
                peer_id,
                connection_ids: relay_connections,
                reason: RelayCloseReason::HandoffSettled,
            });
        }
    }
}

pub(crate) fn format_retry_duration(duration: Duration) -> String {
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

pub(crate) fn prioritize_multiaddrs(addrs: Vec<Multiaddr>) -> Vec<Multiaddr> {
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

pub(crate) fn normalize_peer_address(peer_id: PeerId, address: Multiaddr) -> Option<Multiaddr> {
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

pub(crate) fn normalize_direct_peer_address(
    peer_id: PeerId,
    address: Multiaddr,
) -> Option<Multiaddr> {
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

pub(crate) fn is_relay_address(address: &Multiaddr) -> bool {
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

pub(crate) fn peer_id_from_multiaddr(address: &Multiaddr) -> Option<PeerId> {
    address
        .iter()
        .filter_map(|protocol| match protocol {
            libp2p::multiaddr::Protocol::P2p(peer_id) => Some(peer_id),
            _ => None,
        })
        .last()
}

#[cfg(test)]
mod tests {
    use libp2p::identity;

    use super::*;

    fn peer_id() -> PeerId {
        identity::Keypair::generate_ed25519().public().to_peer_id()
    }

    fn cid(id: usize) -> ConnectionId {
        ConnectionId::new_unchecked(id)
    }

    fn addr(peer_id: PeerId, port: u16) -> Multiaddr {
        format!("/ip4/192.0.2.10/tcp/{port}/p2p/{peer_id}")
            .parse()
            .unwrap()
    }

    fn bare_addr(port: u16) -> Multiaddr {
        format!("/ip4/192.0.2.10/tcp/{port}").parse().unwrap()
    }

    fn status_text(effects: &[ConnectionEffect]) -> Vec<&str> {
        effects
            .iter()
            .filter_map(|effect| match effect {
                ConnectionEffect::Status(status) => Some(status.as_str()),
                _ => None,
            })
            .collect()
    }

    fn has_dial_for(effects: &[ConnectionEffect], peer_id: PeerId) -> bool {
        effects.iter().any(|effect| {
            matches!(effect, ConnectionEffect::DialDirect { peer_id: effect_peer, .. } if *effect_peer == peer_id)
        })
    }

    fn has_close_relay(effects: &[ConnectionEffect], peer_id: PeerId) -> bool {
        effects.iter().any(|effect| {
            matches!(effect, ConnectionEffect::CloseRelayConnections { peer_id: effect_peer, .. } if *effect_peer == peer_id)
        })
    }

    #[test]
    fn direct_and_relay_routes_clear_after_last_connection_closes() {
        let local = peer_id();
        let peer = peer_id();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, Instant::now());
        state.connection_established(peer, cid(2), false, false, Instant::now());

        assert!(state.has_direct(peer));
        assert!(state.has_relayed(peer));

        let effects = state.connection_closed(peer, cid(2), false, 1);
        assert!(state.has_relayed(peer));
        assert!(!state.has_direct(peer));
        assert!(
            status_text(&effects)
                .iter()
                .any(|status| status.contains("direct connection closed"))
        );

        let effects = state.connection_closed(peer, cid(1), true, 0);
        assert!(!state.routes().contains_key(&peer));
        assert!(matches!(
            effects.as_slice(),
            [
                ConnectionEffect::Status(_),
                ConnectionEffect::UntrackGossipPeer(_)
            ]
        ));
    }

    #[test]
    fn disconnect_clears_cached_direct_addresses_from_peer_view() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        state.learn_direct_addresses(peer, [addr(peer, 4001)], now);
        assert_eq!(state.peer_views(&HashSet::new()).len(), 1);

        state.connection_closed(peer, cid(1), true, 0);

        assert!(state.peer_views(&HashSet::new()).is_empty());
    }

    #[test]
    fn relay_only_with_direct_addresses_and_chat_ready_dials_direct() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        state.chat_subscribed(peer, now);
        let effects = state.learn_direct_addresses(peer, [addr(peer, 4001)], now);

        assert!(has_dial_for(&effects, peer));
    }

    #[test]
    fn relay_only_with_warmup_pending_does_not_dial() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        let effects = state.learn_direct_addresses(peer, [addr(peer, 4001)], now);

        assert!(!has_dial_for(&effects, peer));
    }

    #[test]
    fn warmup_expiry_allows_dial_but_keeps_relay_until_chat_ready() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        state.learn_direct_addresses(peer, [addr(peer, 4001)], now);

        let effects = state.warmup_tick(now + GOSSIP_WARMUP_TIMEOUT);
        assert!(has_dial_for(&effects, peer));

        let effects = state.connection_established(
            peer,
            cid(2),
            false,
            false,
            now + GOSSIP_WARMUP_TIMEOUT + Duration::from_secs(1),
        );
        assert!(
            status_text(&effects)
                .iter()
                .any(|status| status.contains("keeping relay until chat subscription"))
        );
        assert!(state.has_relayed(peer));
    }

    #[test]
    fn chat_subscription_schedules_relay_handoff_when_direct_route_exists() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        state.connection_established(peer, cid(2), false, false, now + GOSSIP_WARMUP_TIMEOUT);

        let effects = state.chat_subscribed(peer, now + GOSSIP_WARMUP_TIMEOUT);
        assert!(
            status_text(&effects)
                .iter()
                .any(|status| status.contains("keeping relay"))
        );
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, ConnectionEffect::CloseRelayConnections { .. }))
        );

        let effects = state.warmup_tick(now + GOSSIP_WARMUP_TIMEOUT + DIRECT_RELAY_HANDOFF_GRACE);
        assert!(effects.iter().any(|effect| {
            matches!(
                effect,
                ConnectionEffect::CloseRelayConnections {
                    peer_id: effect_peer,
                    connection_ids,
                    reason: RelayCloseReason::HandoffSettled,
                } if *effect_peer == peer && connection_ids == &vec![cid(1)]
            )
        }));
    }

    #[test]
    fn direct_handoff_keeps_relay_if_direct_drops_before_grace() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        state.chat_subscribed(peer, now);
        state.connection_established(peer, cid(2), false, false, now + Duration::from_secs(1));
        state.connection_closed(peer, cid(2), false, 1);

        let effects = state.warmup_tick(now + DIRECT_RELAY_HANDOFF_GRACE + Duration::from_secs(1));
        assert!(state.has_relayed(peer));
        assert!(!state.has_direct(peer));
        assert!(
            !effects
                .iter()
                .any(|effect| matches!(effect, ConnectionEffect::CloseRelayConnections { .. }))
        );
    }

    #[test]
    fn unsubscribe_while_connected_does_not_clear_chat_readiness() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        state.chat_subscribed(peer, now);

        let effects = state.chat_unsubscribed(peer);

        assert!(state.is_chat_subscribed(peer));
        assert!(
            status_text(&effects)
                .iter()
                .any(|status| status.contains("still connected"))
        );
    }

    #[test]
    fn early_direct_connection_before_warmup_closes_direct() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        let effects = state.connection_established(peer, cid(2), false, false, now);

        assert!(matches!(
            effects.last(),
            Some(ConnectionEffect::CloseEarlyDirectConnection {
                peer_id: effect_peer,
                connection_id,
            }) if *effect_peer == peer && *connection_id == cid(2)
        ));
    }

    #[test]
    fn direct_dial_in_flight_prevents_duplicate_tick_dial() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        state.chat_subscribed(peer, now);
        let first = state.learn_direct_addresses(peer, [addr(peer, 4001)], now);
        let second = state.promotion_tick(now + Duration::from_secs(1));

        assert!(has_dial_for(&first, peer));
        assert!(!has_dial_for(&second, peer));
    }

    #[test]
    fn peer_views_report_routes_and_direct_promotion_counters() {
        let local = peer_id();
        let relay_peer = peer_id();
        let direct_peer = peer_id();
        let rendezvous = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(relay_peer, cid(1), true, false, now);
        state.chat_subscribed(relay_peer, now);
        assert!(has_dial_for(
            &state.learn_direct_addresses(relay_peer, [addr(relay_peer, 4001)], now),
            relay_peer
        ));
        state.outgoing_connection_error(
            relay_peer,
            "outgoing direct dial failed".to_string(),
            now + Duration::from_secs(1),
        );
        state.connection_established(direct_peer, cid(2), false, false, now);
        state.connection_established(rendezvous, cid(3), true, true, now);

        let views = state.peer_views(&HashSet::from([rendezvous]));
        let relay_view = views
            .iter()
            .find(|view| view.peer_id == relay_peer.to_string())
            .unwrap();
        assert_eq!(relay_view.kind, "room");
        assert_eq!(relay_view.route, "relay");
        assert_eq!(relay_view.relayed_connections, 1);
        assert_eq!(relay_view.direct_address_count, 1);
        assert!(relay_view.chat_subscribed);
        assert_eq!(relay_view.direct_promotion_attempts, 1);
        assert_eq!(relay_view.direct_promotion_failures, 1);

        let direct_view = views
            .iter()
            .find(|view| view.peer_id == direct_peer.to_string())
            .unwrap();
        assert_eq!(direct_view.route, "direct");
        assert_eq!(direct_view.direct_connections, 1);

        let rendezvous_view = views
            .iter()
            .find(|view| view.peer_id == rendezvous.to_string())
            .unwrap();
        assert_eq!(rendezvous_view.kind, "rendezvous");
        assert_eq!(rendezvous_view.route, "relay");
    }

    #[test]
    fn direct_promotion_failure_keeps_relay_route_available() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        state.chat_subscribed(peer, now);
        assert!(has_dial_for(
            &state.learn_direct_addresses(peer, [addr(peer, 4001)], now),
            peer
        ));

        state.outgoing_connection_error(
            peer,
            "outgoing direct dial failed".to_string(),
            now + Duration::from_secs(1),
        );

        assert!(state.has_relayed(peer));
        assert!(state.is_relay_only(peer));
    }

    #[test]
    fn direct_promotion_failures_and_ticks_never_close_relay() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        state.chat_subscribed(peer, now);
        assert!(has_dial_for(
            &state.learn_direct_addresses(peer, [addr(peer, 4001)], now),
            peer
        ));

        let first_failure = state.outgoing_connection_error(
            peer,
            "outgoing direct dial failed".to_string(),
            now + Duration::from_secs(1),
        );
        assert!(!has_close_relay(&first_failure, peer));

        let retry =
            state.promotion_tick(now + DIRECT_PROMOTION_RETRY_INTERVAL + Duration::from_secs(1));
        assert!(has_dial_for(&retry, peer));
        assert!(!has_close_relay(&retry, peer));

        let second_failure = state.outgoing_connection_error(
            peer,
            "outgoing direct dial failed".to_string(),
            now + DIRECT_PROMOTION_RETRY_INTERVAL + Duration::from_secs(2),
        );
        assert!(!has_close_relay(&second_failure, peer));

        let timer_effects = state.warmup_tick(
            now + DIRECT_PROMOTION_RETRY_INTERVAL
                + DIRECT_RELAY_HANDOFF_GRACE
                + Duration::from_secs(2),
        );
        assert!(!has_close_relay(&timer_effects, peer));
        assert!(state.has_relayed(peer));
        assert!(state.is_relay_only(peer));
    }

    #[test]
    fn direct_dial_and_dcutr_failures_share_backoff_and_suspend() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);

        state.connection_established(peer, cid(1), true, false, now);
        state.chat_subscribed(peer, now);
        state.learn_direct_addresses(peer, [addr(peer, 4001)], now);

        let first = state.outgoing_connection_error(
            peer,
            "outgoing direct dial failed".to_string(),
            now + Duration::from_secs(1),
        );
        assert!(
            status_text(&first)
                .iter()
                .any(|status| status.contains("(1/10)"))
        );

        let duplicate = state.dcutr_failed(
            peer,
            "DCUtR failed".to_string(),
            now + Duration::from_secs(2),
        );
        assert!(duplicate.is_empty());

        let mut failure_time = now + DIRECT_PROMOTION_FAILURE_DEDUP_WINDOW + Duration::from_secs(2);
        for expected_failures in 2..=DIRECT_PROMOTION_MAX_FAILURES {
            let effects = state.dcutr_failed(peer, "DCUtR failed".to_string(), failure_time);
            let expected_retry_after = match expected_failures {
                DIRECT_PROMOTION_MAX_FAILURES => None,
                0..DIRECT_PROMOTION_MEDIUM_RETRY_FAILURES => Some(DIRECT_PROMOTION_RETRY_INTERVAL),
                DIRECT_PROMOTION_MEDIUM_RETRY_FAILURES..DIRECT_PROMOTION_SLOW_RETRY_FAILURES => {
                    Some(DIRECT_PROMOTION_MEDIUM_RETRY_INTERVAL)
                }
                _ => Some(DIRECT_PROMOTION_SLOW_RETRY_INTERVAL),
            };

            let backoff = state.backoffs.get(&peer).unwrap();
            assert_eq!(backoff.failures, expected_failures);
            let statuses = status_text(&effects).join("\n");
            if let Some(retry_after) = expected_retry_after {
                assert!(statuses.contains(&format_retry_duration(retry_after)));
            } else {
                assert!(statuses.contains("suspended"));
            }
            failure_time += DIRECT_PROMOTION_FAILURE_DEDUP_WINDOW + Duration::from_secs(1);
        }

        assert_eq!(
            state.backoffs.get(&peer).map(|backoff| backoff.failures),
            Some(DIRECT_PROMOTION_MAX_FAILURES)
        );
    }

    #[test]
    fn new_direct_address_resets_suspended_backoff() {
        let local = peer_id();
        let peer = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);
        let backoff = state.backoff_mut(peer);
        backoff.failures = DIRECT_PROMOTION_MAX_FAILURES;
        backoff.suspended_reported = true;

        let effects = state.learn_direct_addresses(peer, [addr(peer, 4001)], now);

        assert!(matches!(
            effects.first(),
            Some(ConnectionEffect::ResetBackoff(effect_peer)) if *effect_peer == peer
        ));
        assert!(state.backoffs.get(&peer).is_none());
    }

    #[test]
    fn unrelated_failure_only_reports_status_for_that_peer() {
        let local = peer_id();
        let peer = peer_id();
        let other = peer_id();
        let now = Instant::now();
        let mut state = ConnectionState::new(local);
        state.connection_established(other, cid(1), true, false, now);

        let effects = state.outgoing_connection_error(peer, "regular dial failed".to_string(), now);

        assert!(
            status_text(&effects)
                .iter()
                .any(|status| status.contains(&peer.to_string()))
        );
        assert!(state.routes().contains_key(&other));
    }

    #[test]
    fn peer_address_normalization_appends_or_rejects_peer_ids() {
        let peer = peer_id();
        let other_peer = peer_id();

        let base = bare_addr(4001);
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
}
