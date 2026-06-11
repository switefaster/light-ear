import React, { useEffect, useMemo, useRef, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  ArrowDown,
  ArrowLeft,
  ArrowUp,
  ChevronDown,
  Check,
  GripVertical,
  ListMusic,
  Pause,
  Play,
  Radio,
  Search,
  Send,
  Shuffle,
  Signal,
  SkipForward,
  Trash2,
  Vote,
  Volume2,
  WifiOff,
  X,
} from "lucide-react";
import "./styles.css";

const tauri = window.__TAURI__;
const isPreview = !tauri?.core?.invoke;
const invoke = tauri?.core?.invoke ?? previewInvoke;
const listen = tauri?.event?.listen ?? previewListen;
const previewListeners = new Map();

const initialConfig = {
  name: "link-ear",
  topic: "link-ear.chat.v1",
  peers: "",
  relays: "",
  noMdns: false,
};

const initialRoom = {
  messages: [],
  statuses: [],
  logs: [],
  playback: null,
  peerCount: 0,
  localPeerId: "",
  backendRunning: false,
  backendStarting: false,
  queue: null,
  vote: null,
  peers: [],
  peerNames: [],
  hadPeers: false,
  peerDropAlert: false,
};

function App() {
  const [config, setConfig] = useState(initialConfig);
  const [room, setRoom] = useState(initialRoom);
  const [logOpen, setLogOpen] = useState(false);
  const isConnected = room.backendRunning && Boolean(room.localPeerId);

  if (!import.meta.env.DEV) {
    document.addEventListener('contextmenu', (e) => e.preventDefault());
  }

  useEffect(() => {
    let mounted = true;
    let cleanupEvent = () => {};
    let cleanupError = () => {};

    listen("backend-event", ({ payload }) => {
      if (!mounted) return;
      setRoom((current) => applyBackendEvent(current, payload));
    }).then((unlisten) => {
      cleanupEvent = unlisten;
    });

    listen("backend-error", ({ payload }) => {
      if (!mounted) return;
      setRoom((current) => appendStatus(current, `backend error: ${payload}`));
    }).then((unlisten) => {
      cleanupError = unlisten;
    });

    return () => {
      mounted = false;
      cleanupEvent();
      cleanupError();
    };
  }, []);

  useEffect(() => {
    if (!isPreview) {
      setRoom((current) => appendStatus(current, "choose a room identity and connect"));
    }
  }, []);

  async function callCommand(command, args = {}, options = {}) {
    const requiresBackend = options.requiresBackend ?? true;
    if (requiresBackend && !room.backendRunning) {
      setRoom((current) => appendStatus(current, "connect to a room first"));
      return false;
    }

    try {
      await invoke(command, args);
      return true;
    } catch (error) {
      setRoom((current) => appendStatus(current, formatError(error)));
      return false;
    }
  }

  async function startBackend(event) {
    event.preventDefault();
    if (room.backendRunning || room.backendStarting) return;

    setRoom((current) => ({
      ...appendStatus(current, "starting backend"),
      backendStarting: true,
    }));

    const started = await callCommand("start_backend", {
      config: {
        name: config.name.trim() || "link-ear",
        topic: config.topic.trim() || "link-ear.chat.v1",
        listen: [],
        peer: lines(config.peers),
        relay: lines(config.relays),
        noMdns: config.noMdns,
      },
    }, { requiresBackend: false });

    if (started) {
      setRoom((current) => ({
        ...appendStatus(current, "backend command channel ready"),
        backendRunning: true,
        backendStarting: false,
        hadPeers: false,
        peerDropAlert: false,
      }));
    } else {
      setRoom((current) => ({
        ...current,
        backendStarting: false,
      }));
    }
  }

  return (
    <>
      {!isConnected ? (
        <SetupPage
          config={config}
          room={room}
          setConfig={setConfig}
          onSubmit={startBackend}
          onOpenLog={() => setLogOpen(true)}
        />
      ) : (
        <RoomConsole
          config={config}
          room={room}
          setRoom={setRoom}
          callCommand={callCommand}
          onOpenLog={() => setLogOpen(true)}
        />
      )}

      {logOpen && (
        <StatusLogModal
          statuses={room.statuses}
          logs={room.logs}
          onClose={() => setLogOpen(false)}
        />
      )}
    </>
  );
}

function SetupPage({ config, room, setConfig, onSubmit, onOpenLog }) {
  const statusText = room.backendStarting ? "starting" : "offline";
  const [showPeerSettings, setShowPeerSettings] = useState(false);

  return (
    <main className="setup-page" data-backend={statusText}>
      <section className="setup-card" aria-label="Connection setup">
        <div className="setup-titlebar">
          <div className="setup-identity">
            <div className="compact-mark" aria-hidden="true"><span></span></div>
            <div>
              <p className="overline">link-ear</p>
              <h1>Connection</h1>
            </div>
          </div>
          <span className="backend-state">
            <span className="status-light" aria-hidden="true"></span>
            {statusText}
          </span>
        </div>

        <form className="setup-form" onSubmit={onSubmit}>
          <div className="setup-grid setup-grid-compact">
            <Field label="Name">
              <input
                value={config.name}
                autoComplete="off"
                onChange={(event) => setConfigValue(setConfig, "name", event.target.value)}
              />
            </Field>

            <Field label="Topic">
              <input
                value={config.topic}
                autoComplete="off"
                onChange={(event) => setConfigValue(setConfig, "topic", event.target.value)}
              />
            </Field>
          </div>

          <Field label="Relay / Rendezvous">
            <textarea
              rows="4"
              value={config.relays}
              placeholder="/ip4/.../tcp/.../p2p/..."
              onChange={(event) => setConfigValue(setConfig, "relays", event.target.value)}
            />
          </Field>

          <div className={`optional-peers${showPeerSettings ? " open" : ""}`}>
            <button
              className="btn subtle optional-toggle"
              type="button"
              onClick={() => setShowPeerSettings((current) => !current)}
            >
              <ChevronDown size={17} aria-hidden="true" />
              Direct peers
            </button>

            {showPeerSettings && (
              <Field label="Peers">
                <textarea
                  rows="4"
                  value={config.peers}
                  placeholder="/ip6/.../tcp/.../p2p/..."
                  onChange={(event) => setConfigValue(setConfig, "peers", event.target.value)}
                />
              </Field>
            )}
          </div>

          <div className="setup-actions">
            <label className="toggle">
              <input
                type="checkbox"
                checked={config.noMdns}
                onChange={(event) => setConfigValue(setConfig, "noMdns", event.target.checked)}
              />
              <span className="toggle-box" aria-hidden="true"></span>
              <span>mDNS off</span>
            </label>
            <button className="btn primary" type="submit" disabled={room.backendStarting}>
              <Radio size={18} aria-hidden="true" />
              {room.backendStarting ? "Starting" : "Connect"}
            </button>
          </div>
        </form>

        {room.statuses.length > 0 && (
          <StatusFeed statuses={room.statuses} compact maxLines={2} onOpenLog={onOpenLog} />
        )}
      </section>
    </main>
  );
}

function RoomConsole({ config, room, setRoom, callCommand, onOpenLog }) {
  const [chatText, setChatText] = useState("");
  const [chatComposing, setChatComposing] = useState(false);
  const [queueOpen, setQueueOpen] = useState(false);
  const [peerOverviewOpen, setPeerOverviewOpen] = useState(false);
  const [pendingMove, setPendingMove] = useState(null);
  const [pendingSeek, setPendingSeek] = useState(null);
  const [volume, setVolume] = useState(100);

  const playback = room.playback;
  const queueCount = room.queue?.items?.length ?? 0;
  const progress = playback
    ? Math.min(100, Math.max(0, (playback.position_ms / Math.max(playback.duration_ms || 0, 1)) * 100))
    : 0;
  const peerNames = useMemo(
    () => buildPeerNames(room.messages, room.localPeerId, config.name, room.peerNames),
    [room.messages, room.localPeerId, config.name, room.peerNames],
  );
  const displayName = (peerId, explicitName) => peerDisplayName(peerId, peerNames, explicitName);

  async function sendChat(event) {
    event.preventDefault();
    if (chatComposing || event.nativeEvent?.isComposing) return;
    const text = chatText.trim();
    if (!text) return;
    if (await callCommand("send_chat", { text })) {
      setChatText("");
    }
  }

  function previewBackToSetup() {
    if (!isPreview) return;
    setRoom(initialRoom);
  }

  async function openQueue() {
    setQueueOpen(true);
    await callCommand("show_queue");
  }

  async function confirmMove() {
    if (!pendingMove) return;
    const moved = await callCommand("move_queue_item", {
      from: pendingMove.from,
      to: pendingMove.to,
    });
    if (moved) {
      setPendingMove(null);
    }
  }

  async function confirmSeek() {
    if (!pendingSeek) return;
    const seeked = await callCommand("seek", {
      seconds: Math.round(pendingSeek.positionMs / 1000),
    });
    if (seeked) {
      setPendingSeek(null);
    }
  }

  async function changeVolume(nextVolume) {
    const percent = Math.min(100, Math.max(0, Number(nextVolume) || 0));
    setVolume(percent);
    await callCommand("set_volume", { percent });
  }

  const peersDisconnected = room.peerDropAlert && room.peerCount === 0;

  return (
    <main className="console-page" data-backend="running">
      <RoomNavBar
        config={config}
        room={room}
        queueCount={queueCount}
        onBack={previewBackToSetup}
        onQueue={openQueue}
        onOpenPeers={() => setPeerOverviewOpen(true)}
        onOpenLog={onOpenLog}
        displayName={displayName}
      />

      <section
        className={`chat-stage${peersDisconnected ? " has-connection-alert" : ""}`}
        aria-label="link-ear room chat"
      >
        {peersDisconnected && (
          <ConnectionAlert onOpenPeers={() => setPeerOverviewOpen(true)} />
        )}
        <section className="panel chat-panel room-chat-panel">
          <div className="chat-head">
            <div>
              <p className="overline">Room</p>
              <h2>Chat</h2>
            </div>
            <span className="count-chip">{room.messages.length}</span>
          </div>

          <MessageList messages={room.messages} />

          <form className="composer" onSubmit={sendChat}>
            <textarea
              value={chatText}
              autoComplete="off"
              placeholder="Message the room"
              rows="1"
              onChange={(event) => setChatText(event.target.value)}
              onCompositionStart={() => setChatComposing(true)}
              onCompositionEnd={() => setChatComposing(false)}
              onKeyDown={(event) => {
                if (event.key !== "Enter") return;
                if (event.shiftKey) return;
                if (chatComposing || event.nativeEvent?.isComposing) {
                  event.preventDefault();
                  return;
                }
                event.preventDefault();
                event.currentTarget.form?.requestSubmit();
              }}
            />
            <button className="btn primary" type="submit">
              <Send size={17} aria-hidden="true" />
              Send
            </button>
          </form>
        </section>
      </section>

      <PlayerDock
        playback={playback}
        progress={progress}
        volume={volume}
        onCommand={callCommand}
        onSeekRequest={setPendingSeek}
        onVolumeChange={changeVolume}
        displayName={displayName}
      />

      <QueueDrawer
        open={queueOpen}
        queue={room.queue}
        callCommand={callCommand}
        onClose={() => setQueueOpen(false)}
        onRequestMove={setPendingMove}
        displayName={displayName}
      />

      {pendingMove && (
        <ConfirmMoveModal
          move={pendingMove}
          onCancel={() => setPendingMove(null)}
          onConfirm={confirmMove}
        />
      )}

      {pendingSeek && (
        <ConfirmSeekModal
          seek={pendingSeek}
          onCancel={() => setPendingSeek(null)}
          onConfirm={confirmSeek}
        />
      )}

      {peerOverviewOpen && (
        <PeerOverviewModal
          peers={room.peers}
          peerCount={room.peerCount}
          displayName={displayName}
          onClose={() => setPeerOverviewOpen(false)}
        />
      )}

      {room.vote && (
        <VoteModal
          vote={room.vote}
          onVote={(approve) => callCommand("vote", { approve })}
          displayName={displayName}
        />
      )}
    </main>
  );
}

function ConnectionAlert({ onOpenPeers }) {
  return (
    <button
      className="connection-alert"
      type="button"
      onClick={onOpenPeers}
      aria-live="polite"
    >
      <WifiOff size={17} aria-hidden="true" />
      <span>
        <strong>Room peers disconnected</strong>
        <small>chat, votes, and sync are waiting for a peer to reconnect</small>
      </span>
    </button>
  );
}

function RoomNavBar({
  config,
  room,
  queueCount,
  onBack,
  onQueue,
  onOpenPeers,
  onOpenLog,
  displayName,
}) {
  const latestStatus = room.statuses.at(-1) ?? "quiet";
  const localName = displayName(room.localPeerId);
  const hasRoomPeers = room.peerCount > 0;
  const peerDropAlert = room.peerDropAlert && !hasRoomPeers;
  const peerStateClass = peerDropAlert ? "peer-alert" : hasRoomPeers ? "peer-online" : "peer-solo";
  const peerCountLabel = `${room.peerCount} peer${room.peerCount === 1 ? "" : "s"}`;
  const peerLabel = peerDropAlert ? "disconnected" : hasRoomPeers ? peerCountLabel : "solo";
  const peerAriaLabel = peerDropAlert
    ? "Open peer overview: room peers disconnected"
    : `Open peer overview: ${peerCountLabel}`;
  const PeerIcon = peerDropAlert ? WifiOff : Signal;

  return (
    <nav className="room-navbar" aria-label="Room session">
      <div className="nav-brand">
        {isPreview && (
          <button className="back-link" type="button" onClick={onBack}>
            <ArrowLeft size={17} aria-hidden="true" />
            setup
          </button>
        )}
        <div className="compact-mark" aria-hidden="true"><span></span></div>
        <strong>link-ear</strong>
      </div>

      <div className="nav-meta">
        <span className="nav-chip" title={room.localPeerId}>
          <Radio size={14} aria-hidden="true" />
          {localName}
        </span>
        <button
          className={`nav-chip peer-nav-button ${peerStateClass}`}
          type="button"
          onClick={onOpenPeers}
          aria-label={peerAriaLabel}
        >
          <PeerIcon size={14} aria-hidden="true" />
          {peerLabel}
        </button>
        <span className="nav-chip topic-chip" title={config.topic}>{config.topic}</span>
      </div>

      <button
        className="status-ticker"
        type="button"
        title={latestStatus}
        aria-label={`Open status log: ${latestStatus}`}
        onClick={onOpenLog}
      >
        <span className="status-light" aria-hidden="true"></span>
        <span>{latestStatus}</span>
      </button>

      <button className="btn ghost queue-nav-button" type="button" onClick={onQueue}>
        <ListMusic size={17} aria-hidden="true" />
        Queue
        <span className="button-count">{queueCount}</span>
      </button>
    </nav>
  );
}

function PlayerDock({
  playback,
  progress,
  volume,
  onCommand,
  onSeekRequest,
  onVolumeChange,
  displayName,
}) {
  const leaderName = playback ? displayName(playback.leader_peer_id, playback.leader_name) : "";
  const playPauseCommand = playback?.playing ? "pause" : "resume";
  const playPauseLabel = playback ? (playback.playing ? "Pause" : "Resume") : "Play/Pause";

  return (
    <section className="player-dock" aria-label="Playback controls">
      <div className="player-track">
        <p className="overline">Now Playing</p>
        <h2 className={playback?.title && playback.title.length > 28 ? "track-title scrolling" : "track-title"}>
          <span>{playback?.title ?? "Idle"}</span>
        </h2>
        <div className="playback-details">
          <span className={`pill ${playback?.playing ? "playing" : playback ? "paused" : "neutral"}`}>
            {playback ? (playback.playing ? "playing" : "paused") : "standing by"}
          </span>
          <span>{playback ? `leader ${leaderName}` : "no track selected"}</span>
        </div>
      </div>

      <div className="player-scrub-area">
        <SeekBar playback={playback} progress={progress} onSeekRequest={onSeekRequest} />
        <div className="time-row">
          <span>{formatMs(playback?.position_ms)}</span>
          <span>{formatMs(playback?.duration_ms)}</span>
        </div>
      </div>

      <div className="transport-buttons">
        <IconButton
          label={playPauseLabel}
          disabled={!playback}
          onClick={() => onCommand(playPauseCommand)}
        >
          {playback?.playing ? (
            <Pause size={20} aria-hidden="true" />
          ) : (
            <Play size={20} aria-hidden="true" />
          )}
        </IconButton>
        <IconButton label="Skip" danger disabled={!playback} onClick={() => onCommand("skip")}>
          <SkipForward size={20} aria-hidden="true" />
        </IconButton>
        <label className="volume-control" title="Local volume">
          <Volume2 size={17} aria-hidden="true" />
          <input
            type="range"
            min="0"
            max="100"
            step="1"
            value={volume}
            aria-label="Local volume"
            onChange={(event) => onVolumeChange(event.target.value)}
          />
          <span>{volume}%</span>
        </label>
      </div>
    </section>
  );
}

function SeekBar({ playback, progress, onSeekRequest }) {
  const [draftPercent, setDraftPercent] = useState(null);
  const canSeek = Boolean(playback?.duration_ms);
  const displayedProgress = draftPercent ?? progress;

  function seekFromEvent(event) {
    const rect = event.currentTarget.getBoundingClientRect();
    const ratio = Math.min(1, Math.max(0, (event.clientX - rect.left) / Math.max(rect.width, 1)));
    const positionMs = Math.round((playback?.duration_ms ?? 0) * ratio);
    return { ratio, positionMs };
  }

  function updateDraft(event) {
    if (!canSeek) return;
    const { ratio } = seekFromEvent(event);
    setDraftPercent(ratio * 100);
  }

  function requestSeek(event) {
    if (!canSeek) return;
    const { positionMs } = seekFromEvent(event);
    setDraftPercent(null);
    onSeekRequest({
      title: playback.title,
      fromMs: playback.position_ms,
      positionMs,
    });
  }

  function requestKeyboardSeek(event) {
    if (!canSeek || !["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault();
    const step = 10_000;
    const current = playback.position_ms ?? 0;
    const duration = playback.duration_ms ?? 0;
    const positionMs = event.key === "Home"
      ? 0
      : event.key === "End"
        ? duration
        : Math.min(duration, Math.max(0, current + (event.key === "ArrowRight" ? step : -step)));
    onSeekRequest({
      title: playback.title,
      fromMs: current,
      positionMs,
    });
  }

  return (
    <div
      className={`scrubber${canSeek ? "" : " disabled"}`}
      role="slider"
      tabIndex={canSeek ? 0 : -1}
      aria-label="Seek playback"
      aria-valuemin="0"
      aria-valuemax={Math.round((playback?.duration_ms ?? 0) / 1000)}
      aria-valuenow={Math.round((playback?.position_ms ?? 0) / 1000)}
      onKeyDown={requestKeyboardSeek}
      onPointerDown={(event) => {
        if (!canSeek) return;
        event.currentTarget.setPointerCapture(event.pointerId);
        updateDraft(event);
      }}
      onPointerMove={(event) => {
        if (event.currentTarget.hasPointerCapture(event.pointerId)) {
          updateDraft(event);
        }
      }}
      onPointerUp={(event) => {
        if (event.currentTarget.hasPointerCapture(event.pointerId)) {
          event.currentTarget.releasePointerCapture(event.pointerId);
          requestSeek(event);
        }
      }}
      onPointerCancel={() => setDraftPercent(null)}
    >
      <span className="scrubber-fill" style={{ width: `${displayedProgress}%` }}></span>
      <span className="scrubber-thumb" style={{ left: `${displayedProgress}%` }}></span>
    </div>
  );
}

function QueueDrawer({ open, queue, callCommand, onClose, onRequestMove, displayName }) {
  const [queueForm, setQueueForm] = useState({ bvid: "", part: "" });
  const items = queue?.items ?? [];

  async function enqueue(event) {
    event.preventDefault();
    const bvid = queueForm.bvid.trim();
    if (!bvid) return;
    const queued = await callCommand("enqueue_bilibili", {
      bvid,
      part: numberOrNull(queueForm.part),
    });
    if (queued) {
      setQueueForm({ bvid: "", part: "" });
    }
  }

  return (
    <>
      {open && <button className="drawer-scrim" type="button" aria-label="Close queue" onClick={onClose}></button>}
      <aside className={`queue-drawer${open ? " open" : ""}`} aria-hidden={!open} aria-label="Queue widget">
        <div className="drawer-head">
          <div>
            <p className="overline">Music</p>
            <h2>Queue</h2>
          </div>
          <IconButton label="Close queue" onClick={onClose}>
            <X size={19} aria-hidden="true" />
          </IconButton>
        </div>

        <form className="stack queue-add-form" onSubmit={enqueue}>
          <div className="field-row queue-add-row">
            <input
              value={queueForm.bvid}
              autoComplete="off"
              placeholder="BV id"
              aria-label="Bilibili BV id"
              onChange={(event) => setQueueValue(setQueueForm, "bvid", event.target.value)}
            />
            <input
              value={queueForm.part}
              type="number"
              min="1"
              placeholder="part"
              aria-label="Part"
              onChange={(event) => setQueueValue(setQueueForm, "part", event.target.value)}
            />
            <button className="btn primary" type="submit">
              <ListMusic size={17} aria-hidden="true" />
              Add
            </button>
          </div>
        </form>

        <QueueList
          items={items}
          onRemove={(index) => callCommand("remove_queue_item", { index })}
          onRequestMove={onRequestMove}
          displayName={displayName}
        />
      </aside>
    </>
  );
}

function QueueList({ items, onRemove, onRequestMove, displayName }) {
  const [dragIndex, setDragIndex] = useState(null);
  const [overIndex, setOverIndex] = useState(null);

  if (items.length === 0) {
    return <div className="empty-state queue-empty">queue is empty</div>;
  }

  function clearDrag() {
    setDragIndex(null);
    setOverIndex(null);
  }

  function indexFromPointer(event) {
    const element = document.elementFromPoint(event.clientX, event.clientY);
    const card = element?.closest?.("[data-queue-index]");
    const index = Number(card?.dataset.queueIndex);
    return Number.isInteger(index) ? index : null;
  }

  function requestMove(fromIndex, toIndex) {
    if (
      fromIndex === null
      || toIndex === null
      || fromIndex === toIndex
      || toIndex < 0
      || toIndex >= items.length
    ) {
      return;
    }
    const item = items[fromIndex];
    if (!item) {
      return;
    }
    onRequestMove({
      from: fromIndex + 1,
      to: toIndex + 1,
      title: item.track.title,
      meta: `${item.track.bvid} P${item.track.part || 1}`,
    });
  }

  return (
    <div className="queue-list">
      {items.map((item, index) => (
        <article
          className={`queue-card${dragIndex === index ? " dragging" : ""}${overIndex === index && dragIndex !== index ? " drag-over" : ""}`}
          key={item.item_id}
          data-queue-index={index}
        >
          <button
            className="queue-drag-handle"
            type="button"
            title={`Drag ${item.track.title}`}
            aria-label={`Move ${item.track.title}`}
            onPointerDown={(event) => {
              event.preventDefault();
              event.currentTarget.setPointerCapture(event.pointerId);
              setDragIndex(index);
              setOverIndex(index);
            }}
            onPointerMove={(event) => {
              if (dragIndex === null) return;
              const nextIndex = indexFromPointer(event);
              if (nextIndex !== null) {
                setOverIndex(nextIndex);
              }
            }}
            onPointerUp={(event) => {
              if (event.currentTarget.hasPointerCapture(event.pointerId)) {
                event.currentTarget.releasePointerCapture(event.pointerId);
              }
              requestMove(dragIndex ?? index, overIndex ?? index);
              clearDrag();
            }}
            onPointerCancel={clearDrag}
            onKeyDown={(event) => {
              if (!["ArrowUp", "ArrowDown"].includes(event.key)) return;
              event.preventDefault();
              requestMove(index, index + (event.key === "ArrowDown" ? 1 : -1));
            }}
          >
            <GripVertical size={17} aria-hidden="true" />
          </button>
          <div className="queue-index">{index + 1}</div>
          <div className="queue-track">
            <strong>{item.track.title}</strong>
            <span>
              {item.track.bvid} P{item.track.part || 1} - {formatMs(item.track.duration_ms)}
            </span>
            <small>by {displayName(item.requested_by, item.requested_by_name)}</small>
          </div>
          <div className="queue-move-actions">
            <IconButton
              label={`Move ${item.track.title} up`}
              disabled={index === 0}
              onClick={() => requestMove(index, index - 1)}
            >
              <ArrowUp size={16} aria-hidden="true" />
            </IconButton>
            <IconButton
              label={`Move ${item.track.title} down`}
              disabled={index === items.length - 1}
              onClick={() => requestMove(index, index + 1)}
            >
              <ArrowDown size={16} aria-hidden="true" />
            </IconButton>
          </div>
          <IconButton label={`Remove ${item.track.title}`} danger onClick={() => onRemove(index + 1)}>
            <Trash2 size={17} aria-hidden="true" />
          </IconButton>
        </article>
      ))}
    </div>
  );
}

function ConfirmMoveModal({ move, onCancel, onConfirm }) {
  const [busy, setBusy] = useState(false);

  async function confirm() {
    setBusy(true);
    await onConfirm();
    setBusy(false);
  }

  return (
    <div className="modal-scrim" role="dialog" aria-modal="true" aria-label="Confirm queue move">
      <section className="modal-card">
        <div className="panel-head">
          <p className="overline">Confirm</p>
          <h2>Request Move Vote</h2>
        </div>
        <p>
          Move <strong>{move.title}</strong> from #{move.from} to #{move.to}.
          This asks the backend to start a room vote.
        </p>
        <p className="modal-meta">{move.meta}</p>
        <div className="modal-actions">
          <button className="btn subtle" type="button" onClick={onCancel} disabled={busy}>Cancel</button>
          <button className="btn primary" type="button" onClick={confirm} disabled={busy}>
            <Shuffle size={17} aria-hidden="true" />
            Confirm
          </button>
        </div>
      </section>
    </div>
  );
}

function ConfirmSeekModal({ seek, onCancel, onConfirm }) {
  const [busy, setBusy] = useState(false);

  async function confirm() {
    setBusy(true);
    await onConfirm();
    setBusy(false);
  }

  return (
    <div className="modal-scrim" role="dialog" aria-modal="true" aria-label="Confirm seek">
      <section className="modal-card">
        <div className="panel-head">
          <p className="overline">Confirm</p>
          <h2>Seek Playback</h2>
        </div>
        <p>
          Seek <strong>{seek.title}</strong> from {formatMs(seek.fromMs)}
          to {formatMs(seek.positionMs)}.
        </p>
        <p className="modal-meta">The backend may request a room vote if this peer cannot control the track.</p>
        <div className="modal-actions">
          <button className="btn subtle" type="button" onClick={onCancel} disabled={busy}>Cancel</button>
          <button className="btn primary" type="button" onClick={confirm} disabled={busy}>
            <Shuffle size={17} aria-hidden="true" />
            Confirm
          </button>
        </div>
      </section>
    </div>
  );
}

function VoteModal({ vote, onVote, displayName }) {
  const [busy, setBusy] = useState(false);
  const eligible = Math.max(vote.eligible_peers ?? vote.threshold ?? 1, 1);
  const approvalWidth = Math.min(100, (vote.approvals / eligible) * 100);
  const rejectionWidth = Math.min(100, (vote.rejections / eligible) * 100);
  const localVote = vote.local_vote;
  const hasVoted = localVote === true || localVote === false;

  async function cast(approve) {
    if (hasVoted) return;
    setBusy(true);
    await onVote(approve);
    setBusy(false);
  }

  return (
    <div className="modal-scrim vote-scrim" role="dialog" aria-modal="true" aria-label="Room vote">
      <section className="modal-card vote-modal">
        <div className="panel-head">
          <p className="overline">Room vote</p>
          <h2>{vote.action_label}</h2>
        </div>
        <div className="vote-summary">
          <span>requested by {displayName(vote.proposer, vote.proposer_name)}</span>
          <strong>{vote.approvals}/{vote.threshold}</strong>
        </div>
        <div className="vote-meter" aria-hidden="true">
          <span className="vote-meter-yes" style={{ width: `${approvalWidth}%` }}></span>
          <span className="vote-meter-no" style={{ width: `${rejectionWidth}%` }}></span>
        </div>
        <div className="vote-counts">
          <span className="vote-count yes">{vote.approvals} yes</span>
          <span className="vote-count no">{vote.rejections} no</span>
          <span className="vote-count pending">{vote.pending ?? 0} pending</span>
        </div>
        {hasVoted && (
          <p className="modal-meta">you voted {localVote ? "yes" : "no"}</p>
        )}
        <div className="modal-actions">
          <button className="btn approve" type="button" onClick={() => cast(true)} disabled={busy || hasVoted}>
            <Check size={17} aria-hidden="true" />
            Yes
          </button>
          <button className="btn reject" type="button" onClick={() => cast(false)} disabled={busy || hasVoted}>
            <Vote size={17} aria-hidden="true" />
            No
          </button>
        </div>
      </section>
    </div>
  );
}

function StatusLogModal({ statuses, logs, onClose }) {
  const listRef = useRef(null);
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState("all");
  const entries = logs?.length ? logs : statuses.map((line, index) => createLogEntry(line, index));
  const summary = summarizeLogs(entries);
  const normalizedQuery = query.trim().toLowerCase();
  const visibleEntries = entries.filter((entry) => {
    const matchesQuery =
      !normalizedQuery ||
      entry.text.toLowerCase().includes(normalizedQuery) ||
      entry.category.toLowerCase().includes(normalizedQuery) ||
      entry.level.toLowerCase().includes(normalizedQuery);
    const matchesFilter =
      filter === "all" || entry.level === filter || entry.category === filter;
    return matchesQuery && matchesFilter;
  });

  useEffect(() => {
    function closeOnEscape(event) {
      if (event.key === "Escape") {
        onClose();
      }
    }

    window.addEventListener("keydown", closeOnEscape);
    return () => window.removeEventListener("keydown", closeOnEscape);
  }, [onClose]);

  useEffect(() => {
    const list = listRef.current;
    if (list) {
      list.scrollTop = list.scrollHeight;
    }
  }, [visibleEntries.length]);

  return (
    <div
      className="modal-scrim log-scrim"
      role="dialog"
      aria-modal="true"
      aria-label="Status log"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
    >
      <section className="modal-card log-modal">
        <div className="panel-head split">
          <div>
            <p className="overline">Status</p>
            <h2>Full Log</h2>
          </div>
          <IconButton label="Close status log" onClick={onClose}>
            <X size={18} aria-hidden="true" />
          </IconButton>
        </div>

        <div className="log-toolbar">
          <label className="log-search">
            <Search size={15} aria-hidden="true" />
            <input
              value={query}
              type="search"
              placeholder="Search log"
              aria-label="Search status log"
              onChange={(event) => setQuery(event.target.value)}
            />
          </label>
          <select
            value={filter}
            aria-label="Filter status log"
            onChange={(event) => setFilter(event.target.value)}
          >
            <option value="all">All</option>
            <option value="error">Errors</option>
            <option value="warn">Warnings</option>
            <option value="success">Success</option>
            <option value="info">Info</option>
            <option value="network">Network</option>
            <option value="sync">Sync</option>
            <option value="playback">Playback</option>
            <option value="queue">Queue</option>
            <option value="vote">Vote</option>
            <option value="system">System</option>
          </select>
        </div>

        <div className="log-summary" aria-label="Log summary">
          <span className="log-chip error">{summary.error}</span>
          <span className="log-chip warn">{summary.warn}</span>
          <span className="log-chip success">{summary.success}</span>
          <span className="log-chip info">{summary.info}</span>
        </div>

        <div className="log-list" role="log" aria-live="polite" ref={listRef}>
          {visibleEntries.length === 0 ? (
            <div className="empty-state">quiet</div>
          ) : (
            visibleEntries.map((entry) => (
              <article className={`log-entry log-${entry.level}`} key={entry.id}>
                <div className="log-entry-meta">
                  <span>{formatLogTime(entry.at)}</span>
                  <strong>{entry.level}</strong>
                  <span>{entry.category}</span>
                </div>
                <p>{entry.text}</p>
              </article>
            ))
          )}
        </div>
      </section>
    </div>
  );
}

function PeerOverviewModal({ peers, peerCount, displayName, onClose }) {
  useEffect(() => {
    function closeOnEscape(event) {
      if (event.key === "Escape") {
        onClose();
      }
    }

    window.addEventListener("keydown", closeOnEscape);
    return () => window.removeEventListener("keydown", closeOnEscape);
  }, [onClose]);

  const roomPeers = peers.filter((peer) => peer.kind !== "rendezvous");
  const infraPeers = peers.filter((peer) => peer.kind === "rendezvous");

  return (
    <div
      className="modal-scrim peer-scrim"
      role="dialog"
      aria-modal="true"
      aria-label="Peer overview"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) {
          onClose();
        }
      }}
    >
      <section className="modal-card peer-modal">
        <div className="panel-head split">
          <div>
            <p className="overline">Network</p>
            <h2>{peerCount} peers</h2>
          </div>
          <IconButton label="Close peer overview" onClick={onClose}>
            <X size={18} aria-hidden="true" />
          </IconButton>
        </div>

        <PeerGroup
          title="Room"
          peers={roomPeers}
          empty="no connected room peers"
          displayName={displayName}
        />
        {infraPeers.length > 0 && (
          <PeerGroup
            title="Infrastructure"
            peers={infraPeers}
            empty=""
            displayName={displayName}
          />
        )}
      </section>
    </div>
  );
}

function PeerGroup({ title, peers, empty, displayName }) {
  return (
    <section className="peer-group">
      <div className="peer-group-head">
        <h3>{title}</h3>
        <span>{peers.length}</span>
      </div>

      {peers.length === 0 ? (
        <div className="empty-state">{empty}</div>
      ) : (
        <div className="peer-list">
          {peers.map((peer) => (
            <article className="peer-row" key={peer.peer_id}>
              <div className="peer-row-main">
                <strong title={peer.peer_id}>{displayName(peer.peer_id)}</strong>
                <span>{shortPeer(peer.peer_id)}</span>
              </div>
              <span className={`route-pill route-${routeClass(peer.route)}`}>{peer.route}</span>
              <dl className="peer-stats">
                <div>
                  <dt>links</dt>
                  <dd>{peer.direct_connections}d / {peer.relayed_connections}r</dd>
                </div>
                <div>
                  <dt>addr</dt>
                  <dd>{peer.direct_address_count}</dd>
                </div>
                <div>
                  <dt>chat</dt>
                  <dd>{peer.chat_subscribed ? "ready" : "wait"}</dd>
                </div>
                <div>
                  <dt>direct</dt>
                  <dd>
                    {peer.direct_promotion_attempts}/{peer.direct_promotion_failures}
                    {peer.direct_promotion_in_flight ? " now" : ""}
                    {peer.direct_promotion_suspended ? " hold" : ""}
                  </dd>
                </div>
              </dl>
            </article>
          ))}
        </div>
      )}
    </section>
  );
}

function routeClass(route) {
  return String(route || "known").replace(/[^a-z0-9]+/gi, "-").toLowerCase();
}

function Brand({ localPeerId }) {
  return (
    <header className="brand-block">
      <div className="mark" aria-hidden="true"><span></span></div>
      <div className="brand-copy">
        <p className="overline">link-ear</p>
        <h1>link-ear</h1>
        <p className="peer-chip">{localPeerId || "offline"}</p>
      </div>
    </header>
  );
}

function Field({ label, children }) {
  return (
    <label className="field">
      <span>{label}</span>
      {children}
    </label>
  );
}

function IconButton({ label, danger = false, disabled = false, children, onClick }) {
  return (
    <button
      className={`icon-button${danger ? " danger" : ""}`}
      type="button"
      title={label}
      aria-label={label}
      onClick={onClick}
      disabled={disabled}
    >
      {children}
    </button>
  );
}

function MessageList({ messages }) {
  const viewportRef = useRef(null);
  const stickToBottomRef = useRef(true);
  const [showJump, setShowJump] = useState(false);
  const rendered = useMemo(() => messages.map((record) => ({
    ...record,
    time: new Date(normalizeMicros(record.sent_at) / 1000).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    }),
  })), [messages]);

  function scrollToLatest() {
    const node = viewportRef.current;
    if (!node) return;
    node.scrollTop = node.scrollHeight;
    stickToBottomRef.current = true;
    setShowJump(false);
  }

  function updateScrollIntent() {
    const node = viewportRef.current;
    if (!node) return;
    const distanceToBottom = node.scrollHeight - node.scrollTop - node.clientHeight;
    const nearBottom = distanceToBottom < 36;
    stickToBottomRef.current = nearBottom;
    setShowJump(!nearBottom && rendered.length > 0);
  }

  useEffect(() => {
    if (!stickToBottomRef.current) return;
    const frame = requestAnimationFrame(scrollToLatest);
    return () => cancelAnimationFrame(frame);
  }, [rendered.length]);

  return (
    <div
      className={`messages${rendered.length === 0 ? " messages-empty" : ""}`}
      ref={viewportRef}
      onScroll={updateScrollIntent}
    >
      <div className="messages-stack">
        {rendered.length === 0 ? (
          <div className="empty-state">no messages</div>
        ) : (
          rendered.map((record) => (
            <article className="message" key={record.id}>
              <header>
                <strong>{record.author}</strong>
                <time>{record.time}</time>
              </header>
              <p>{record.text}</p>
            </article>
          ))
        )}
      </div>
      {showJump && (
        <button className="scroll-latest" type="button" onClick={scrollToLatest}>
          Latest
        </button>
      )}
    </div>
  );
}

function StatusFeed({ statuses, compact = false, maxLines = 1, onOpenLog }) {
  const className = `status${compact ? " compact-status" : ""}${onOpenLog ? " status-trigger" : ""}`;

  if (statuses.length === 0) {
    if (onOpenLog) {
      return (
        <button className={className} type="button" onClick={onOpenLog} aria-label="Open status log">
          <span className="empty-state">quiet</span>
        </button>
      );
    }

    return <div className={className}><div className="empty-state">quiet</div></div>;
  }

  const children = statuses.slice(-maxLines).map((line, index) => (
    <span className="status-line" key={`${line}-${index}`}>{line}</span>
  ));

  if (onOpenLog) {
    const latestStatus = statuses.at(-1);

    return (
      <button
        className={className}
        type="button"
        title={latestStatus}
        aria-label={`Open status log: ${latestStatus}`}
        onClick={onOpenLog}
      >
        {children}
      </button>
    );
  }

  return <div className={className}>{children}</div>;
}

function applyBackendEvent(current, event) {
  switch (event.type) {
    case "status":
      return appendStatus(current, event.payload);
    case "peer_count": {
      const peerCount = Math.max(0, Number(event.payload) || 0);
      const previousPeerCount = Math.max(0, Number(current.peerCount) || 0);
      const lostSomePeers = current.hadPeers && peerCount < previousPeerCount;
      const lostAllPeers = lostSomePeers && peerCount === 0;
      const next = {
        ...current,
        peerCount,
        hadPeers: current.hadPeers || peerCount > 0,
        peerDropAlert: lostAllPeers ? true : peerCount > 0 ? false : current.peerDropAlert,
      };
      if (lostAllPeers) {
        return appendStatus(next, "all room peers disconnected");
      }
      if (lostSomePeers) {
        return appendStatus(next, `room peer count dropped to ${peerCount}`);
      }
      return next;
    }
    case "local_peer_id":
      return {
        ...current,
        localPeerId: event.payload,
        backendRunning: true,
        backendStarting: false,
      };
    case "history":
      return { ...current, messages: event.payload };
    case "playback":
      return { ...current, playback: event.payload };
    case "queue":
      return { ...current, queue: event.payload };
    case "vote":
      return { ...current, vote: event.payload };
    case "peers":
      return { ...current, peers: event.payload ?? [] };
    case "peer_names":
      return { ...current, peerNames: event.payload ?? [] };
    default:
      return current;
  }
}

function appendStatus(room, status) {
  const text = String(status ?? "");
  const previousLogs = room.logs ?? room.statuses.map((line, index) => createLogEntry(line, index));
  const entry = createLogEntry(text, previousLogs.length);

  return {
    ...room,
    statuses: room.statuses.concat(text).slice(-80),
    logs: previousLogs.concat(entry).slice(-300),
  };
}

function setConfigValue(setConfig, key, value) {
  setConfig((current) => ({ ...current, [key]: value }));
}

function setQueueValue(setState, key, value) {
  setState((current) => ({ ...current, [key]: value }));
}

function lines(value) {
  return value
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
}

function numberOrNull(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : null;
}

function formatMs(value) {
  const seconds = Math.floor((value || 0) / 1000);
  return `${String(Math.floor(seconds / 60)).padStart(2, "0")}:${String(seconds % 60).padStart(2, "0")}`;
}

function normalizeMicros(value) {
  const abs = Math.abs(value || 0);
  if (abs < 10_000_000_000) return value * 1_000_000;
  if (abs < 10_000_000_000_000) return value * 1_000;
  return value;
}

function createLogEntry(status, index) {
  const text = String(status ?? "");
  const { level, category } = classifyLogLine(text);
  return {
    id: `${Date.now()}-${index}-${text.slice(0, 24)}`,
    at: Date.now(),
    text,
    level,
    category,
  };
}

function classifyLogLine(text) {
  const line = text.toLowerCase();
  const level = (() => {
    if (/\b(failed|failure|error|unavailable|invalid|rejected)\b/.test(line)) return "error";
    if (/\b(timeout|timed out|retry|suspended|ignored|no active|no peers|waiting|slow)\b/.test(line)) {
      return "warn";
    }
    if (/\b(ready|connected|queued|removed|moved|accepted|succeeded|published|joined)\b/.test(line)) {
      return "success";
    }
    return "info";
  })();

  const category = (() => {
    if (/\b(vote|ballot)\b/.test(line)) return "vote";
    if (/\b(queue|queued|enqueue|item)\b/.test(line)) return "queue";
    if (/\b(playback|audio|seek|volume|bilibili|now:|preparing|downloading)\b/.test(line)) return "playback";
    if (/\b(history|sync|snapshot|summary)\b/.test(line)) return "sync";
    if (/\b(direct|relay|rendezvous|gossip|gossipsub|mdns|identify|peer|dial|connection|listen)\b/.test(line)) {
      return "network";
    }
    return "system";
  })();

  return { level, category };
}

function summarizeLogs(entries) {
  return entries.reduce(
    (counts, entry) => ({
      ...counts,
      [entry.level]: (counts[entry.level] ?? 0) + 1,
    }),
    { error: 0, warn: 0, success: 0, info: 0 },
  );
}

function formatLogTime(value) {
  return new Date(value).toLocaleTimeString([], {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

function buildPeerNames(messages, localPeerId, localName, claims = []) {
  const names = new Map();
  if (localPeerId && localName) {
    names.set(localPeerId, localName);
  }
  for (const record of messages) {
    if (record.peer_id && record.author) {
      names.set(record.peer_id, record.author);
    }
  }
  for (const claim of claims || []) {
    if (claim.peer_id && claim.name) {
      names.set(claim.peer_id, claim.name);
    }
  }
  return names;
}

function peerDisplayName(peerId, peerNames, explicitName) {
  if (explicitName) return explicitName;
  const text = String(peerId || "");
  if (!text) return "unknown";
  return peerNames.get(text) ?? shortPeer(text);
}

function shortPeer(value) {
  const text = String(value || "");
  if (!text) return "no leader";
  return text.length > 16 ? `${text.slice(0, 9)}...${text.slice(-4)}` : text;
}

function formatError(error) {
  if (typeof error === "string") return error;
  if (error && typeof error.message === "string") return error.message;
  return JSON.stringify(error);
}

function previewListen(event, handler) {
  const handlers = previewListeners.get(event) ?? [];
  handlers.push(handler);
  previewListeners.set(event, handlers);
  return Promise.resolve(() => {
    previewListeners.set(event, handlers.filter((item) => item !== handler));
  });
}

async function previewInvoke(command, args = {}) {
  await new Promise((resolve) => window.setTimeout(resolve, 140));

  switch (command) {
    case "start_backend":
      emitPreview("backend-event", { type: "local_peer_id", payload: "12D3KooW-local-preview" });
      emitPreview("backend-event", { type: "peer_count", payload: 3 });
      emitPreview("backend-event", { type: "status", payload: `joined topic ${args.config.topic}` });
      emitPreview("backend-event", { type: "history", payload: previewMessages() });
      emitPreview("backend-event", { type: "playback", payload: previewPlayback() });
      emitPreview("backend-event", { type: "queue", payload: previewQueue() });
      emitPreview("backend-event", { type: "peers", payload: previewPeers() });
      emitPreview("backend-event", { type: "peer_names", payload: previewPeerNames() });
      return;
    case "send_chat":
      emitPreview("backend-event", {
        type: "history",
        payload: previewMessages().concat({
          id: `preview-${Date.now()}`,
          author: "you",
          text: args.text,
          sent_at: Date.now() * 1000,
        }),
      });
      return;
    case "enqueue_bilibili":
      emitPreview("backend-event", { type: "status", payload: `queued ${args.bvid || "BV1preview"} part ${args.part || 1}` });
      emitPreview("backend-event", { type: "queue", payload: previewQueue(args) });
      return;
    case "show_queue":
      emitPreview("backend-event", { type: "status", payload: "queue: 1 active, 2 waiting" });
      emitPreview("backend-event", { type: "queue", payload: previewQueue() });
      return;
    case "move_queue_item":
      emitPreview("backend-event", {
        type: "vote",
        payload: {
          vote_id: `preview-vote-${Date.now()}`,
          proposer: "12D3KooW-local-preview",
          action_label: `move queue item #${args.from} to #${args.to}`,
          approvals: 1,
          rejections: 0,
          threshold: 2,
          eligible_peers: 3,
          pending: 2,
          local_vote: true,
        },
      });
      emitPreview("backend-event", { type: "status", payload: "move vote requested" });
      return;
    case "vote":
      emitPreview("backend-event", { type: "vote", payload: null });
      emitPreview("backend-event", { type: "status", payload: `${args.approve ? "yes" : "no"} vote sent` });
      return;
    case "seek":
      emitPreview("backend-event", {
        type: "playback",
        payload: {
          ...previewPlayback(),
          position_ms: Math.max(0, Math.round(args.seconds || 0) * 1000),
        },
      });
      emitPreview("backend-event", { type: "status", payload: `seek accepted ${args.seconds || 0}s` });
      return;
    case "pause":
      emitPreview("backend-event", {
        type: "playback",
        payload: {
          ...previewPlayback(),
          playing: false,
        },
      });
      emitPreview("backend-event", { type: "status", payload: "pause accepted" });
      return;
    case "resume":
      emitPreview("backend-event", {
        type: "playback",
        payload: {
          ...previewPlayback(),
          playing: true,
        },
      });
      emitPreview("backend-event", { type: "status", payload: "resume accepted" });
      return;
    case "skip":
    case "remove_queue_item":
      emitPreview("backend-event", { type: "status", payload: `${command} accepted` });
      return;
    case "set_volume":
      emitPreview("backend-event", { type: "status", payload: `local volume set to ${args.percent}%` });
      return;
    default:
      return;
  }
}

function emitPreview(event, payload) {
  for (const handler of previewListeners.get(event) ?? []) {
    handler({ payload });
  }
}

function previewMessages() {
  const now = Date.now() * 1000;
  return [
    {
      id: "preview-1",
      peer_id: "12D3KooW-alice",
      author: "alice",
      text: "Found a clean live version. Queue it after this one?",
      sent_at: now - 240_000_000,
    },
    {
      id: "preview-2",
      peer_id: "12D3KooW-bob",
      author: "bob",
      text: "Yes. The drift correction feels steady now.",
      sent_at: now - 90_000_000,
    },
  ];
}

function previewPeerNames() {
  return [
    { peer_id: "12D3KooW-alice", name: "alice" },
    { peer_id: "12D3KooW-bob", name: "bob" },
  ];
}

function previewPlayback() {
  return {
    title: "Bilibili session warmup",
    playing: true,
    position_ms: 83_000,
    duration_ms: 244_000,
    leader_peer_id: "12D3KooW-leader-preview",
    leader_name: "alice",
  };
}

function previewPeers() {
  return [
    {
      peer_id: "12D3KooW-alice",
      kind: "room",
      route: "direct",
      direct_connections: 1,
      relayed_connections: 0,
      direct_address_count: 2,
      chat_subscribed: true,
      direct_promotion_attempts: 1,
      direct_promotion_failures: 0,
      direct_promotion_in_flight: false,
      direct_promotion_suspended: false,
    },
    {
      peer_id: "12D3KooW-bob",
      kind: "room",
      route: "relay",
      direct_connections: 0,
      relayed_connections: 1,
      direct_address_count: 1,
      chat_subscribed: true,
      direct_promotion_attempts: 4,
      direct_promotion_failures: 3,
      direct_promotion_in_flight: false,
      direct_promotion_suspended: false,
    },
    {
      peer_id: "12D3KooW-rendezvous",
      kind: "rendezvous",
      route: "relay",
      direct_connections: 0,
      relayed_connections: 1,
      direct_address_count: 0,
      chat_subscribed: false,
      direct_promotion_attempts: 0,
      direct_promotion_failures: 0,
      direct_promotion_in_flight: false,
      direct_promotion_suspended: false,
    },
  ];
}

function previewQueue(extra = {}) {
  const now = Date.now() * 1000;
  const extraBvid = extra.bvid || "BV1preview";
  return {
    version: 4,
    updated_at_micros: now,
    updated_by: "12D3KooW-local-preview",
    items: [
      previewQueueItem("preview-q-1", "Night market sync test", "BV1A4411N7", 1, 214_000, "12D3KooW-alice", now - 420_000_000),
      previewQueueItem("preview-q-2", "Live house encore", "BV1xK4y1C7", 2, 268_000, "12D3KooW-bob", now - 260_000_000),
      previewQueueItem("preview-q-3", extra.bvid ? `Queued ${extraBvid}` : "Late train ambient", extraBvid, extra.part || 1, 188_000, "12D3KooW-local-preview", now),
    ],
  };
}

function previewQueueItem(itemId, title, bvid, part, durationMs, requestedBy, addedAt) {
  return {
    item_id: itemId,
    requested_by: requestedBy,
    added_at_micros: addedAt,
    track: {
      track_id: `${bvid}:${part}`,
      title,
      source_kind: "bilibili",
      bvid,
      part,
      duration_ms: durationMs,
      audio_url: "",
      referer: "",
    },
  };
}

createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
