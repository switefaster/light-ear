import React, { useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import {
  ArrowLeft,
  ChevronDown,
  Check,
  GripVertical,
  ListMusic,
  Pause,
  Play,
  Radio,
  Send,
  Shuffle,
  Signal,
  SkipForward,
  Trash2,
  Vote,
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
  playback: null,
  peerCount: 0,
  localPeerId: "",
  backendRunning: false,
  backendStarting: false,
  queue: null,
  vote: null,
};

function App() {
  const [config, setConfig] = useState(initialConfig);
  const [room, setRoom] = useState(initialRoom);
  const [logOpen, setLogOpen] = useState(false);
  const isConnected = room.backendRunning && Boolean(room.localPeerId);

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
  const [queueOpen, setQueueOpen] = useState(false);
  const [pendingMove, setPendingMove] = useState(null);
  const [pendingSeek, setPendingSeek] = useState(null);

  const playback = room.playback;
  const queueCount = room.queue?.items?.length ?? 0;
  const progress = playback
    ? Math.min(100, Math.max(0, (playback.position_ms / Math.max(playback.duration_ms || 0, 1)) * 100))
    : 0;
  const peerNames = useMemo(
    () => buildPeerNames(room.messages, room.localPeerId, config.name),
    [room.messages, room.localPeerId, config.name],
  );
  const displayName = (peerId, explicitName) => peerDisplayName(peerId, peerNames, explicitName);

  async function sendChat(event) {
    event.preventDefault();
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

  return (
    <main className="console-page" data-backend="running">
      <RoomNavBar
        config={config}
        room={room}
        queueCount={queueCount}
        onBack={previewBackToSetup}
        onQueue={openQueue}
        onOpenLog={onOpenLog}
        displayName={displayName}
      />

      <section className="chat-stage" aria-label="link-ear room chat">
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
            <input
              value={chatText}
              autoComplete="off"
              placeholder="Message the room"
              onChange={(event) => setChatText(event.target.value)}
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
        onCommand={callCommand}
        onSeekRequest={setPendingSeek}
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

function RoomNavBar({ config, room, queueCount, onBack, onQueue, onOpenLog, displayName }) {
  const latestStatus = room.statuses.at(-1) ?? "quiet";
  const localName = displayName(room.localPeerId);

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
        <span className="nav-chip">
          <Signal size={14} aria-hidden="true" />
          {room.peerCount} peers
        </span>
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

function PlayerDock({ playback, progress, onCommand, onSeekRequest, displayName }) {
  const leaderName = playback ? displayName(playback.leader_peer_id, playback.leader_name) : "";

  return (
    <section className="player-dock" aria-label="Playback controls">
      <div className="player-track">
        <p className="overline">Now Playing</p>
        <h2>{playback?.title ?? "Idle"}</h2>
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
        <IconButton label="Pause" onClick={() => onCommand("pause")}>
          <Pause size={20} aria-hidden="true" />
        </IconButton>
        <IconButton label="Resume" onClick={() => onCommand("resume")}>
          <Play size={20} aria-hidden="true" />
        </IconButton>
        <IconButton label="Skip" danger onClick={() => onCommand("skip")}>
          <SkipForward size={20} aria-hidden="true" />
        </IconButton>
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
  const [queueForm, setQueueForm] = useState({ bvid: "", part: "", position: "" });
  const items = queue?.items ?? [];

  async function enqueue(event) {
    event.preventDefault();
    const bvid = queueForm.bvid.trim();
    if (!bvid) return;
    const queued = await callCommand("enqueue_bilibili", {
      bvid,
      part: numberOrNull(queueForm.part),
      position: numberOrNull(queueForm.position),
    });
    if (queued) {
      setQueueForm({ bvid: "", part: "", position: "" });
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
          <div className="field-row">
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
          </div>
          <div className="field-row">
            <input
              value={queueForm.position}
              type="number"
              min="1"
              placeholder="insert position"
              aria-label="Insert position"
              onChange={(event) => setQueueValue(setQueueForm, "position", event.target.value)}
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

  function requestMove(fromIndex, toIndex) {
    if (fromIndex === null || toIndex === null || fromIndex === toIndex) {
      clearDrag();
      return;
    }
    const item = items[fromIndex];
    if (!item) {
      clearDrag();
      return;
    }
    onRequestMove({
      from: fromIndex + 1,
      to: toIndex + 1,
      title: item.track.title,
      meta: `${item.track.bvid} P${item.track.part || 1}`,
    });
    clearDrag();
  }

  return (
    <div className="queue-list">
      {items.map((item, index) => (
        <article
          className={`queue-card${dragIndex === index ? " dragging" : ""}${overIndex === index && dragIndex !== index ? " drag-over" : ""}`}
          key={item.item_id}
          draggable
          onDragStart={(event) => {
            event.dataTransfer.effectAllowed = "move";
            event.dataTransfer.setData("text/plain", String(index));
            setDragIndex(index);
          }}
          onDragEnter={() => setOverIndex(index)}
          onDragOver={(event) => {
            event.preventDefault();
            event.dataTransfer.dropEffect = "move";
            setOverIndex(index);
          }}
          onDrop={(event) => {
            event.preventDefault();
            const from = Number(event.dataTransfer.getData("text/plain"));
            requestMove(Number.isFinite(from) ? from : dragIndex, index);
          }}
          onDragEnd={clearDrag}
        >
          <span className="drag-handle" title="Drag to move" aria-hidden="true">
            <GripVertical size={17} />
          </span>
          <div className="queue-index">{index + 1}</div>
          <div className="queue-track">
            <strong>{item.track.title}</strong>
            <span>
              {item.track.bvid} P{item.track.part || 1} - {formatMs(item.track.duration_ms)}
            </span>
            <small>by {displayName(item.requested_by, item.requested_by_name)}</small>
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
  const approvalWidth = Math.min(100, (vote.approvals / Math.max(vote.threshold, 1)) * 100);

  async function cast(approve) {
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
          <span style={{ width: `${approvalWidth}%` }}></span>
        </div>
        <p className="modal-meta">{vote.rejections} rejected</p>
        <div className="modal-actions">
          <button className="btn approve" type="button" onClick={() => cast(true)} disabled={busy}>
            <Check size={17} aria-hidden="true" />
            Yes
          </button>
          <button className="btn reject" type="button" onClick={() => cast(false)} disabled={busy}>
            <Vote size={17} aria-hidden="true" />
            No
          </button>
        </div>
      </section>
    </div>
  );
}

function StatusLogModal({ statuses, onClose }) {
  useEffect(() => {
    function closeOnEscape(event) {
      if (event.key === "Escape") {
        onClose();
      }
    }

    window.addEventListener("keydown", closeOnEscape);
    return () => window.removeEventListener("keydown", closeOnEscape);
  }, [onClose]);

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

        <div className="log-list" role="log" aria-live="polite">
          {statuses.length === 0 ? (
            <div className="empty-state">quiet</div>
          ) : (
            statuses.map((line, index) => (
              <article className="log-entry" key={`${line}-${index}`}>
                <span>{String(index + 1).padStart(2, "0")}</span>
                <p>{line}</p>
              </article>
            ))
          )}
        </div>
      </section>
    </div>
  );
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

function IconButton({ label, danger = false, children, onClick }) {
  return (
    <button className={`icon-button${danger ? " danger" : ""}`} type="button" title={label} aria-label={label} onClick={onClick}>
      {children}
    </button>
  );
}

function MessageList({ messages }) {
  const rendered = useMemo(() => messages.map((record) => ({
    ...record,
    time: new Date(normalizeMicros(record.sent_at) / 1000).toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
    }),
  })), [messages]);

  if (rendered.length === 0) {
    return <div className="empty-state">no messages</div>;
  }

  return (
    <div className="messages">
      {rendered.map((record) => (
        <article className="message" key={record.id}>
          <header>
            <strong>{record.author}</strong>
            <time>{record.time}</time>
          </header>
          <p>{record.text}</p>
        </article>
      ))}
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
    case "peer_count":
      return { ...current, peerCount: event.payload };
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
    default:
      return current;
  }
}

function appendStatus(room, status) {
  return {
    ...room,
    statuses: room.statuses.concat(status).slice(-80),
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

function buildPeerNames(messages, localPeerId, localName) {
  const names = new Map();
  if (localPeerId && localName) {
    names.set(localPeerId, localName);
  }
  for (const record of messages) {
    if (record.peer_id && record.author) {
      names.set(record.peer_id, record.author);
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
    case "resume":
    case "skip":
    case "remove_queue_item":
      emitPreview("backend-event", { type: "status", payload: `${command} accepted` });
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
