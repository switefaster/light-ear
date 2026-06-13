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
- `src/bin/link-ear-relay.rs`: public relay/rendezvous node plus topology
  dashboard.
- `src-tauri/src/main.rs`: Tauri command bridge into the Rust backend.
- `desktop/src/main.jsx`: React/Vite desktop UI.
- `ARCHITECTURE.md`: current architecture boundaries, state-machine rules,
  timing assumptions, and change guidance.
- `.github/workflows/build.yml`: push-triggered matrix CI build for Windows,
  Linux, and macOS. It runs frontend/Rust checks and builds the relay binary
  plus Tauri desktop bundle, but does not upload artifacts.

## Useful checks

Run the narrowest relevant check first, then broaden if the change crosses
boundaries:

- `cargo test --lib`
- `cargo check`
- `cargo check --bin link-ear-relay`
- `cargo check --manifest-path src-tauri\Cargo.toml`
- `npm.cmd run build`
- Optional local AAC enhancement check: `cargo check --features fdk-aac-decoder`

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
- Bilibili/audio streaming prepare runs off the main swarm event loop. Range
  download and Symphonia decode events must be checked by session id, operation
  id, and track id before they affect playback or buffer quorum.
- Bilibili resolve should select plausible media URLs only. Do not add a
  resolve-time decoder probe that can reject otherwise usable tracks because of
  partial Range metadata, missing `moov`, or decoder-specific AAC limitations;
  decode failures belong to streaming prepare and must converge there.
- When Bilibili DASH audio includes `SegmentBase`, preserve the initialization
  and index byte ranges on `PlaybackTrack`. The streaming cache should request
  those metadata ranges before probing or seeking so fragmented MP4 audio can be
  demuxed without waiting for unrelated sequential download progress.
- AAC decoding defaults to Symphonia's native decoder on the normal build. The
  optional `fdk-aac-decoder` feature registers the FDK AAC Symphonia adapter
  instead and must stay opt-in because it brings native/library licensing
  constraints. Do not register native AAC and FDK AAC in the same codec
  registry.
- Long-video playback uses short-lived buffer coordination around the existing
  authoritative `PlaybackState`. `PlaybackBufferPrepare`,
  `PlaybackBufferStatus`, and `PlaybackBufferCancel` are temporary quorum and
  diagnostic messages; do not treat them as room playback truth.
- Start, seek, and resume should wait for a strict majority of real room peers
  to report buffered readiness before the leader publishes the playable
  `PlaybackState`. Queue items are removed only after start quorum succeeds.
- Local audio output availability must not block the room. If output cannot be
  opened, try to restore it first; if that fails, report ready for prepare/quorum
  so other peers can continue, send a local status, and keep retrying output
  recovery while playback or buffer preparation is active.
- Buffer quorum counts real room peers only, including the local peer and
  excluding relay/rendezvous infrastructure. Leader-side media failure must
  cancel or converge the operation; follower failure should stay local except
  for its buffer status report.
- Current buffer operations should shrink their expected peer set when a peer
  disconnects or libp2p reports it as a gossipsub slow peer. Temporary
  `PlaybackBufferPrepare` / `PlaybackBufferStatus` publish failures should be
  reported as status, but must not abort local prepare, local ready, or quorum
  resolution.
- Gossipsub `AllQueuesFull` means room message delivery is congested, not that
  the backend should exit. Treat it as a recoverable publish outcome, keep local
  state machines moving, and rely on existing state/summary republish paths for
  convergence.
- Event-triggered sync announcements must be coalesced. mDNS, connection, and
  subscription events can arrive several times for the same peer/address set;
  do not publish immediate history/queue/name summaries or music snapshots for
  every duplicate event.
- Long-video playback uses HTTP Range cache plus background decode. The player
  may start once the requested position has a ready playback window. Seek beyond
  the decoded window should restart streaming prepare at the target position and
  prioritize the requested byte range instead of waiting for earlier sequential
  download progress to catch up. Preserve the existing byte cache while
  replacing only the stale decoder/PCM window when possible.
- Active playback buffer health is temporary coordination, not room truth.
  `PlaybackBufferHealth` lets the leader pause when a strict majority of real
  room peers falls below the low watermark for the grace window, then resume
  through the normal buffer quorum path. Health/cache progress updates must be
  rate-limited before gossipsub publish; UI-local cache updates may be more
  frequent, but room health messages should not be emitted per decoded packet.
- Cache progress is local UI state. `PlaybackCache` is sent to the desktop to
  render local cached/decoded progress and must not be mirrored into
  `PlaybackState`.
- Music download, decode, or playback-sync failures must converge explicitly
  instead of leaving a peer in fake playback. A leader failure should
  publish cancel/idle for the affected session and then try the next queued
  item; a follower failure should stop local playback, clear the local playback
  view, and suppress re-applying the failed session until a new session or idle
  state arrives, but it must not erase the room playback state from
  `MusicState` or make `start_next_if_idle` think the peer is idle.
- If a local buffer operation is superseded by remote authoritative playback or
  a remote buffer prepare, abandon/cancel the local operation without consuming
  its queue item. A later local `stream canceled` event from the abandoned
  operation is not a media failure and must not skip the queued track.
- Audio player `Prepared`, `Cache`, `Buffering`, `Failed`, and `Ended` events
  must match the current buffer operation or the current playback
  `session_id + track_id` before they update buffer health, UI cache state,
  status, or playback failure handling. Stale events from canceled decoder or
  downloader tasks should be ignored silently.
- While a start/seek/resume buffer operation is active for the same
  `session_id + track_id`, local cache, buffering, and failure events must carry
  that operation id to be accepted. This prevents old decoder windows from
  keeping seek UI and quorum state pinned to the pre-seek position.
- Local audio output device errors, such as headphone hotplug or default-device
  changes, should reopen the default output and reattach the current sink when
  possible. The player also polls the cross-platform default output device id
  while active (about 1s) and idle (about 5s) to notice default-device changes
  without platform-specific listeners. This is local recovery and must not
  publish room playback changes or mark the media session failed by itself.
- If an output error or default-device change arrives while no track/session is
  loaded, drop the old output handle and reopen lazily on the next playback
  prepare instead of repeatedly rebuilding an idle stream.
- If the app starts before Linux/Wayland audio output is available, keep the
  backend running and retry opening the default output while playback or buffer
  preparation is active. Include host/default-device diagnostics in output-open
  errors so user logs identify missing default devices versus sink-open
  failures.
- Open local audio output through the rodio default-sink fallback path. On Linux,
  `DeviceSinkBuilder::from_default_device()` can fail while querying
  `alsa:default` config even though another output device would work; keep the
  fallback that enumerates alternative non-null output devices.
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
- Room protocol messages for chat, history sync, queue sync, playback, buffer
  coordination, and votes are gossipsub-only. Do not add direct-message fallback
  or request-response delivery for room messages.
- Treat `NoPeersSubscribedToTopic` and gossipsub unsupported/unsubscribed peers
  as visible readiness problems. Report actionable status, keep peer overview
  `chat_subscribed` strict, and let history/queue sync retry when subscription
  readiness returns.
- Direct promotion should not close relay links immediately on the same tick as
  a new direct connection. Keep relay during the short handoff grace period; if
  direct drops during that window, relay remains the reliability path.
- The client and relay node should keep `Swarm` idle connection timeout explicit
  and much longer than libp2p's 10s default. Relay-only room peers may be idle
  while gossipsub is warming up or direct promotion is failing; the default idle
  timeout can otherwise close the relay route without any local handoff effect.
- The relay server should keep relay circuit limits suitable for long-lived
  chat/playback sessions. In libp2p-relay 0.21, `max_circuit_bytes = 0`
  disables the byte cap, but `max_circuit_duration = 0` would time out
  immediately; use a long positive duration instead.
- The peer overview UI is a diagnostic snapshot of local connection state. Keep
  it UI-facing only: route type, direct/relay link counts, known direct address
  count, chat subscription readiness, and direct promotion counters must not be
  added to the P2P wire schema.
- Targeted history/queue sync requests still use existing `target` fields, but
  they are broadcast over gossipsub and ignored by non-target peers. Do not set
  request cooldowns when publish fails because no peers are subscribed.
- Rendezvous registration TTL should follow the libp2p default floor of about
  two hours to avoid noisy refresh traffic. Clients should unregister from
  rendezvous on graceful shutdown; crashed clients may remain discoverable until
  TTL expiry. Do not cache rendezvous direct addresses for peers that are not
  currently connected; use those addresses for the immediate disconnected dial
  only, and let successful connections/identify repopulate local peer state.
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
- Clipboard-based Bilibili auto-fill should use the Tauri clipboard-manager
  command first and fall back to the WebView `navigator.clipboard` API only if
  the command fails. On Linux, clipboard reads must not run on the main thread.
- Peer display names are UI aliases learned from name claims and chat/history
  records. Queue requester and playback leader labels should use those aliases
  when available, but peer id remains the unique identity.
- Desktop status logs are structured on the frontend from existing status
  events. Keep backend status text low-noise and actionable; do not add P2P wire
  fields solely to support log filtering or presentation. Log export should use
  the Tauri dialog plugin from a Rust command to ask for a save path, then write
  JSONL from Rust; do not rely on WebView Blob/download behavior for desktop
  exports.

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
