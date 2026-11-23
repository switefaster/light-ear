//! Preserve the transport connection used by an inbound publication. The upstream
//! behaviour reports its immediate peer; a peer can have both a circuit and a direct socket.
use libp2p::{
    Multiaddr, PeerId,
    core::{Endpoint, transport::PortUse},
    gossipsub as gs,
    swarm::{
        ConnectionDenied, ConnectionId, FromSwarm, NetworkBehaviour, THandler, THandlerInEvent,
        THandlerOutEvent, ToSwarm,
    },
};
use std::{
    collections::{HashMap, VecDeque},
    ops::{Deref, DerefMut},
    task::{Context, Poll, Waker},
};

type Action = ToSwarm<gs::Event, THandlerInEvent<gs::Behaviour>>;
pub(super) struct Observed {
    inner: gs::Behaviour,
    queued: VecDeque<Action>,
    received: HashMap<gs::MessageId, ConnectionId>,
    waker: Option<Waker>,
}
impl Observed {
    pub fn new(inner: gs::Behaviour) -> Self {
        Self {
            inner,
            queued: Default::default(),
            received: Default::default(),
            waker: None,
        }
    }
    pub fn connection(&mut self, id: &gs::MessageId) -> Option<ConnectionId> {
        self.received.remove(id)
    }
    fn drain(&mut self, connection: Option<ConnectionId>) {
        let mut cx = Context::from_waker(self.waker.as_ref().unwrap_or(Waker::noop()));
        while let Poll::Ready(action) = self.inner.poll(&mut cx) {
            if let (Some(cid), ToSwarm::GenerateEvent(gs::Event::Message { message_id, .. })) =
                (connection, &action)
            {
                self.received.insert(message_id.clone(), cid);
            }
            self.queued.push_back(action);
        }
    }
}
impl Deref for Observed {
    type Target = gs::Behaviour;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for Observed {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
impl NetworkBehaviour for Observed {
    type ConnectionHandler = THandler<gs::Behaviour>;
    type ToSwarm = gs::Event;
    fn handle_established_inbound_connection(
        &mut self,
        id: ConnectionId,
        p: PeerId,
        l: &Multiaddr,
        r: &Multiaddr,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.inner
            .handle_established_inbound_connection(id, p, l, r)
    }
    fn handle_established_outbound_connection(
        &mut self,
        id: ConnectionId,
        p: PeerId,
        a: &Multiaddr,
        e: Endpoint,
        u: PortUse,
    ) -> Result<THandler<Self>, ConnectionDenied> {
        self.inner
            .handle_established_outbound_connection(id, p, a, e, u)
    }
    fn on_swarm_event(&mut self, event: FromSwarm) {
        self.inner.on_swarm_event(event)
    }
    fn on_connection_handler_event(
        &mut self,
        p: PeerId,
        c: ConnectionId,
        event: THandlerOutEvent<Self>,
    ) {
        self.drain(None);
        self.inner.on_connection_handler_event(p, c, event);
        self.drain(Some(c));
    }
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Action> {
        self.waker = Some(cx.waker().clone());
        if let Some(action) = self.queued.pop_front() {
            return Poll::Ready(action);
        }
        self.inner.poll(cx)
    }
}
