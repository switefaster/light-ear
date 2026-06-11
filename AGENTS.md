# link-ear agent notes

## Project goal

`link-ear` is a P2P chat and shared listening app. Peers chat, co-manage a music
queue, and try to keep playback state close enough that everyone hears the same
track at roughly the same time.

The networking layer uses libp2p. Peers can meet through a public
relay/rendezvous node, then promote relay-backed routes to direct peer-to-peer
connections when direct addresses become usable so relay traffic stays bounded.

## Main entrypoints

- `src/backend.rs`: main libp2p runtime, protocol message handling, history sync,
  queue sync, voting, playback state sync, relay-to-direct promotion.
- `src/core.rs`: shared protocol and UI-facing data types plus small utilities.
- `src/main.rs`: legacy terminal UI bootstrap and command parsing. Keep it
  compiling for now, but desktop/Tauri is the primary user-facing path.
- `src/bin/link-ear-relay.rs`: public relay/rendezvous node plus topology
  dashboard.
- `src-tauri/src/main.rs`: Tauri command bridge into the Rust backend.
- `desktop/src/main.jsx`: React/Vite desktop UI.
- `ARCHITECTURE.md`: current architecture boundaries, state-machine rules,
  timing assumptions, and change guidance.

## Useful checks

Run the narrowest relevant check first, then broaden if the change crosses
boundaries:

- `cargo test --lib`
- `cargo check`
- `cargo check --bin link-ear-relay`
- `cargo check --manifest-path src-tauri\Cargo.toml`
- `npm.cmd run build`

For relay promotion policy or rendezvous behavior, include
`cargo check --bin link-ear-relay`. For desktop bridge work, include the Tauri
manifest check and the frontend build.

For backend network convergence changes, run the loopback smoke test as a quick
guardrail:

- `cargo test --lib local_loopback_smoke_syncs_chat_history_between_two_backends`

## Current collaboration rules

- Enqueue always appends to the queue and does not require a vote. Keep the
  Tauri command's `position` field compatible, but do not give it behavior
  unless the product rule changes.
- Queue move always requires a vote. Queue remove is direct only for the peer
  that requested that queue item; other peers must vote.
- Queue remove/move votes based on a newer queue version should stay visible
  while the peer catches up; request queue sync from the proposer instead of
  immediately discarding the vote as stale.
- If such a queue vote reaches majority before local queue sync catches up,
  keep the active vote pending and apply it after the matching queue snapshot
  arrives. Queue snapshot application should immediately retry resolving an
  already-ready vote instead of waiting for a later periodic tick.
- Seek is direct only for the current track requester. Pause, resume, and skip
  always require a vote. Empty queue or no active playback should be rejected
  before creating a vote.
- The desktop transport exposes play/pause as a single state-driven control.
  It should still map to the existing `pause` and `resume` backend commands.
- Volume is a local playback setting. It should not enter room votes or shared
  playback wire state, and changing it should not rebuild the audio sink.
- Bilibili/audio downloads for playback prepare run off the main swarm event
  loop. Skip/cancel may arrive while a download is in flight; stale download
  results must be ignored by session id and track id.
- Each peer may cast only one ballot per vote. Votes should resolve early when
  they reach majority or when remaining pending peers can no longer make the
  vote pass. UI vote views should expose approvals, rejections, pending count,
  and the local peer's ballot without changing the P2P wire schema.
- Playback votes should apply deterministically on every peer from the proposal
  timestamp and proposer identity. Only the peer whose id matches the resulting
  playback state's `leader_peer_id` should publish that playback state, because
  inbound wire source validation requires playback leaders to match the message
  source peer.
- Vote thresholds and playback ready expected peers count real room peers only:
  include the local peer, exclude relay/rendezvous infrastructure peers.
- Gossipsub publish paths for chat, history sync, queue sync, playback, and
  votes should keep direct-message fallback working for connected room peers.
- Direct-message fallback is only a reliability fallback for
  `NoPeersSubscribedToTopic`; do not dual-send when gossipsub publish succeeds
  or returns duplicate noise. Direct inbound messages should reuse the normal
  `WireMessage` handler and reject actor fields that do not match the
  authenticated source peer.
- Direct promotion should not close relay links immediately on the same tick as
  a new direct connection. Keep relay during the short handoff grace period; if
  direct drops during that window, relay remains the reliability path.
- The client and relay node should keep `Swarm` idle connection timeout explicit
  and much longer than libp2p's 10s default. Relay-only room peers may be idle
  while gossipsub is warming up or direct promotion is failing; the default idle
  timeout can otherwise close the relay route without any local handoff effect.
- The peer overview UI is a diagnostic snapshot of local connection state. Keep
  it UI-facing only: route type, direct/relay link counts, known direct address
  count, chat subscription readiness, and direct promotion counters must not be
  added to the P2P wire schema.
- Direct fallback for targeted history/queue sync requests should send only to
  the target peer. Track request-response failures for those fallback requests
  and clear the matching request cooldown so slow peers can retry sync.
- Rendezvous registration TTL should stay short and be refreshed periodically,
  because clients often exit without unregistering. Do not cache rendezvous
  direct addresses for peers that are not currently connected; use those
  addresses for the immediate disconnected dial only, and let successful
  connections/identify repopulate local peer state.
- When connected room peers drop to zero after previously being connected,
  enter zero-peer recovery: keep relay/rendezvous infrastructure connected,
  periodically run full rendezvous discovery without the previous cookie, and
  schedule history/queue sync bursts when peers return. Offline local chat
  records must still merge by message id and normalized timestamp after
  reconnection.
- History summary handling should consider both message count and newest
  timestamp; equal counts do not prove histories have converged.
- Desktop chat history must remain scrollable for old messages. New messages
  may auto-follow only while the user is already near the latest message. The
  chat composer should handle IME composition correctly on Linux/WebKit as well
  as Windows.
- Peer display names are UI aliases learned from name claims and chat/history
  records. Queue requester and playback leader labels should use those aliases
  when available, but peer id remains the unique identity.
- Desktop status logs are structured on the frontend from existing status
  events. Keep backend status text low-noise and actionable; do not add P2P wire
  fields solely to support log filtering or presentation.

## Manual smoke test

Use `docs/test-report-audit.md` to map report items to current code/test
evidence, then use `docs/manual-smoke-test.md` for repeatable multi-peer
testing. Prefer a three-person room: one relay-only peer, one direct-capable
peer, and one slower or unstable peer. The run should verify chat/history sync,
queue management rules, playback readiness, vote stability, relay fallback
after direct promotion failure, peer overview, status log filtering, IME input,
and duplicate display names.

## Repository constraints

- Keep Cargo on the default upstream registry/source behavior. Do not add mirror
  overrides unless the user explicitly asks for that.
- Do not commit generated or dependency outputs. The expected ignored paths are
  `target/`, `src-tauri/target/`, `src-tauri/gen/`, `desktop/dist/`, and
  `node_modules/`.
- Prefer tests around pure protocol/state logic before larger refactors. Good
  first targets are history insertion, queue version ordering, vote thresholds,
  playback position math, address normalization, and Bilibili signing helpers.
- When adding or changing behavior, add or update focused tests where it is
  practical, especially for protocol rules, state-machine transitions, and
  network edge cases.
- Avoid broad rewrites of `src/backend.rs` until there are enough tests to lock
  current behavior.
- Update this file when project structure, verification commands, workflow
  expectations, or important implementation constraints change, so future work
  starts from current guidance.
