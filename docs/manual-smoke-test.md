# Manual Smoke Test

This guide is for the real-world cases that have historically broken most
often: relay fallback, direct promotion, queue/vote convergence, playback
readiness, and slow peer synchronization.

## Setup

Use one unique topic per run, for example `link-ear.smoke.2026-06-11.1`.
Record the commit, OS, network type, and app build for every participant.

Recommended topology:

- Relay host: public or otherwise reachable TCP/UDP endpoint.
- Peer A: relay-only or behind a restrictive NAT.
- Peer B: direct-capable, preferably on a network that can accept direct routes.
- Peer C: slower or unstable network.

Start the relay/rendezvous node:

```powershell
cargo run --bin link-ear-relay -- --port 4001 --web-addr 0.0.0.0:8080
```

Open the relay dashboard and note the printed relay peer id:

```text
http://127.0.0.1:8080/
```

Each desktop peer should join the same topic and relay address:

```text
/ip4/<relay-ip>/tcp/4001/p2p/<relay-peer-id>
```

Keep the desktop status log and peer overview open while testing. If a check
fails, copy the last 20 log entries from each peer and note whether the peer
overview shows `direct`, `relay`, or `direct+relay`.

## Preflight

- All peers show the same topic.
- All peers eventually show the correct room peer count, excluding the relay.
- The relay dashboard shows rendezvous registrations for the topic.
- Peer overview marks the relay/rendezvous node as infrastructure, not a room
  peer.
- Peer overview shows every connected room peer as chat `ready` before testing
  queue, playback, or vote propagation.
- Chat messages from every peer are visible on every other peer.
- If a peer sees a gossipsub unsupported, unsubscribed, or no-subscribers
  status, treat that peer as not ready; room messages no longer use direct
  fallback.

## Chat And History

1. Peer A sends three chat messages quickly.
2. Peer C disconnects for at least 20 seconds.
3. Peer B sends two messages while C is gone.
4. Peer C reconnects through the same relay.

Pass signals:

- Peer C receives the missing messages without requiring a restart.
- Logs show history summary/request/response activity over gossipsub.
- Duplicate messages do not appear.
- The chat history panel can scroll back to older messages.

Capture on failure:

- Local peer id for every peer.
- Last history-related log entries.
- Whether peer overview showed the expected target peer as chat `ready`.

## Queue Rules

1. Peer A enqueues a Bilibili track.
2. Peer B enqueues another track.
3. Peer C enqueues a third track.

Pass signals:

- New tracks always append to the end of the queue.
- No vote appears for enqueue.
- All peers converge on the same queue order and queue version.

Then test queue management:

1. Peer B moves Peer C's item to position 1.
2. All peers approve the move vote.
3. Peer A tries to remove Peer B's item.
4. All peers approve the remove vote.
5. Peer C removes its own remaining item.

Pass signals:

- Move always creates a vote.
- Removing another peer's item creates a vote.
- Removing your own requested item is direct and does not create a vote.
- After every accepted operation, all peers show the same queue by `item_id`.

Capture on failure:

- Vote id, action label, approvals/rejections, threshold.
- Queue snapshot from every peer.
- Whether the failed operation referenced an item id that no longer exists.

## Playback Readiness And Finish

1. Enqueue at least two tracks.
2. Let playback start naturally.
3. Watch the prepare phase on all peers.
4. Disconnect Peer C before it reports ready.
5. Keep Peer A and Peer B connected.
6. Start another track and request skip while at least one peer is still
   streaming/preparing audio.

Pass signals:

- Expected ready peers include only real room peers, not relay/rendezvous.
- When Peer C disconnects, the remaining expected set can still start.
- No peer remains forever at `starting playback ... ready wait timed out`.
- When a track finishes, followers move to idle locally and the leader starts
  the next queued item.
- Skip can be voted on while stream prepare is still in flight; stale stream
  events do not restart the skipped session.

Capture on failure:

- Expected peer list and ready count.
- Playback session id.
- Whether the stuck peer is leader or follower.

## Playback Votes

With an active track:

1. Peer B starts a pause vote.
2. Let one normal playback state refresh happen during the vote.
3. Approve the pause vote from a majority.
4. Repeat for resume and skip.
5. In a separate vote, have the same peer try to vote yes and then no.
6. In a three-peer room, have two peers reject a vote.

Pass signals:

- The vote modal remains visible during same-session playback refreshes.
- Pause, resume, and skip always require a vote.
- Vote result applies deterministically on every peer that receives it.
- Each peer gets one ballot per vote; repeat ballots do not change the count.
- The vote modal shows yes, no, and pending counts, with rejection count visible
  in red.
- A vote closes early when majority is reached or when enough rejections make
  majority impossible.

Seek rules:

1. The current track requester seeks directly.
2. A different peer seeks.
3. Approve the seek vote.

Pass signals:

- Requester seek is direct.
- Other peer seek creates a vote.
- Vote result applies by playback session id; stale sessions are rejected.

## Direct Promotion And Relay Fallback

1. Wait for peers to discover direct candidate addresses.
2. Confirm direct promotion attempts in peer overview.
3. Force or observe a direct dial/DCUtR failure if possible.
4. Continue sending chat and queue/vote actions.

Pass signals:

- Direct dial/DCUtR failure increments promotion counters/backoff.
- Existing relay routes remain available after direct promotion failure.
- Messages, history sync, queue sync, playback state, and votes still deliver
  via relay or direct-message fallback.
- If a direct route later succeeds and chat subscription is ready, relay links
  first remain during the handoff grace period, then may close for that peer.
- If direct drops during the handoff grace period, the peer returns to relay
  delivery instead of getting stuck at chat subscription `wait`.

Capture on failure:

- Peer overview route and promotion counters before/after failure.
- Last direct promotion status lines.
- Whether the failed peer still appears in the relay dashboard.

## UI Regression Checks

- Chinese IME composition does not submit partial text when pressing Enter.
- Linux WebKit/iBus/fcitx input can enter Chinese text in the composer.
- Queue drawer has no insert-position field.
- Pointer drag or keyboard move controls open a move vote.
- The combined play/pause button maps to pause while playing and resume while
  paused; it is disabled while idle.
- Volume changes are local only and do not create votes or playback state
  messages.
- Peer overview opens from `N peers` and shows direct/relay diagnostics.
- Status log search and filters work without losing new status lines.
- Duplicate display names can send chat; peer id remains the unique identity.
- Queue requester and playback leader labels show display aliases when known,
  while still allowing short peer id disambiguation.
- Long playback titles scroll or truncate without overlapping transport
  controls.
- The empty chat view does not show an extra framed empty panel.

## Run Exit Criteria

The run passes only if all peers agree on chat history, queue order, active vote
state, and playback state after every operation. A direct promotion failure is
allowed, but it must not break relay-backed message delivery.

If any peer diverges, save:

- The topic name.
- Relay peer id and dashboard snapshot.
- Desktop peer overview for each participant.
- Last 20 status log entries from each participant.
- The exact user action that triggered divergence.
