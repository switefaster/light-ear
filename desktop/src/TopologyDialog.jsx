import React, { useEffect, useMemo, useRef, useState } from "react";
import { Maximize, Minus, Plus, X } from "lucide-react";
import { buildTopology } from "./topology.js";

function routeName(link) {
  if (link.direct && link.circuit) return "Direct + circuit";
  return link.direct ? "Direct" : "Circuit";
}

function linkTitle(link) {
  const trees = ["Tree 1", "Tree 2"].filter((_, i) => link.lanes & (1 << i));
  return [routeName(link), ...trees, link.rtt_ms != null && link.rtt_ms + " ms RTT",
    link.degraded && "Degraded"].filter(Boolean).join(" · ");
}

export default function TopologyDialog({ room, displayName, onClose }) {
  const dialog = useRef(null);
  const stage = useRef(null);
  const drag = useRef(null);
  const [size, setSize] = useState({ width: 720, height: 320 });
  const [camera, setCamera] = useState(null);
  const [selection, setSelection] = useState(room.localPeerId);
  const [lane, setLane] = useState(0);
  const { topology, peers, localPeerId, active, networkMetrics: metrics } = room;
  const graph = useMemo(() => buildTopology(topology, peers, localPeerId, active),
    [topology, peers, localPeerId, active]);
  const selected = graph.nodes.get(selection) ?? graph.nodes.get(localPeerId);
  const neighbors = graph.adjacency.get(selected.id);
  const stats = selected.peer;
  const received = metrics?.received_bytes[selected.id];
  const frame = camera ?? {
    x: graph.width / 2, y: graph.height / 2,
    scale: Math.min(1.35, (size.width - 22) / graph.width, (size.height - 30) / graph.height),
  };
  const viewWidth = size.width / frame.scale;
  const viewHeight = size.height / frame.scale;
  const name = id => id === localPeerId ? "You" : displayName(id);
  const links = lane ? graph.links.filter(link => link.lanes & lane) : graph.links;

  useEffect(() => {
    const element = dialog.current;
    element.showModal();
    const observer = new ResizeObserver(([entry]) => {
      const { width, height } = entry.contentRect;
      if (width && height) setSize({ width, height });
    });
    observer.observe(stage.current);
    return () => { observer.disconnect(); element.close(); };
  }, []);

  function inspect(id, center = false) {
    setSelection(id);
    if (center) {
      const { x, y } = graph.nodes.get(id);
      setCamera({ x, y, scale: Math.max(frame.scale, 1) });
    }
  }

  function zoom(factor) {
    setCamera({ ...frame, scale: Math.max(Math.min(frame.scale, 0.04), Math.min(4, frame.scale * factor)) });
  }

  return (
    <dialog className="modal-card topology-dialog" ref={dialog}
      aria-labelledby="topology-heading" onCancel={event => { event.preventDefault(); onClose(); }}
      onClick={event => {
        if (event.target !== dialog.current) return;
        const r = dialog.current.getBoundingClientRect();
        if (event.clientX < r.left || event.clientX > r.right || event.clientY < r.top || event.clientY > r.bottom) onClose();
      }}>
      <header className="panel-head split">
        <div><h2 id="topology-heading">Network topology</h2>
          <p>{graph.nodes.size} {graph.nodes.size === 1 ? "node" : "nodes"} · {graph.links.length} {graph.links.length === 1 ? "link" : "links"}</p>
        </div>
        <button type="button" className="icon-button" aria-label="Close topology" onClick={onClose}><X size={18} /></button>
      </header>
      <div className="topology-toolbar">
        <div className="topology-tabs" role="group" aria-label="Show links">
          {["All links", "Tree 1", "Tree 2"].map((label, i) =>
            <button key={label} aria-pressed={lane === i} onClick={() => setLane(i)}>{label}</button>)}
        </div>
        <div className="topology-tools">
          <button aria-label="Zoom out" title="Zoom out" onClick={() => zoom(1 / 1.5)}><Minus size={16} /></button>
          <button aria-label="Zoom in" title="Zoom in" onClick={() => zoom(1.5)}><Plus size={16} /></button>
          <button aria-label="Fit network" title="Fit network" onClick={() => setCamera(null)}><Maximize size={16} /></button>
        </div>
      </div>
      <div className="topology-stage" ref={stage}>
        <svg role="group" aria-label="Peer connections" viewBox={[frame.x - viewWidth / 2,
          frame.y - viewHeight / 2, viewWidth, viewHeight].join(" ")}
          onPointerDown={event => {
            if (event.button !== 0 || event.target.closest("[data-node]")) return;
            event.currentTarget.setPointerCapture(event.pointerId);
            drag.current = { sx: event.clientX, sy: event.clientY, ...frame };
          }}
          onPointerMove={event => {
            const start = drag.current;
            if (start) setCamera({ scale: start.scale, x: start.x - (event.clientX - start.sx) / start.scale,
              y: start.y - (event.clientY - start.sy) / start.scale });
          }} onLostPointerCapture={() => { drag.current = null; }}>
          {links.map(link => {
            const a = graph.nodes.get(link.source), b = graph.nodes.get(link.target);
            const line = a.x === b.x
              ? `M ${a.x} ${a.y} Q ${a.x + 68} ${(a.y + b.y) / 2} ${b.x} ${b.y}`
              : `M ${a.x} ${a.y} L ${b.x} ${b.y}`;
            return <g key={a.id + "/" + b.id}><title>{name(a.id)} ↔ {name(b.id)} · {linkTitle(link)}</title>
              {link.direct && <path d={line} className="topology-direct" />}
              {link.circuit && <path d={line} className="topology-circuit" />}
            </g>;
          })}
          {[...graph.nodes.values()].map(node => {
            const label = name(node.id);
            const isLocal = node.id === localPeerId;
            const isSelected = selected.id === node.id;
            return <g key={node.id} data-node="" className={["topology-node", isLocal && "self",
              !node.reachable && "unreachable", node.infrastructure && "infra"].filter(Boolean).join(" ")}
              transform={`translate(${node.x} ${node.y})`} role="button" aria-label={label + " · " + node.id}
              aria-pressed={isSelected} tabIndex={isSelected ? 0 : -1} onClick={() => inspect(node.id)}
              onKeyDown={event => { if (["Enter", " "].includes(event.key)) { event.preventDefault(); inspect(node.id); } }}>
              <title>{label} · {node.id}</title>
              <circle r="17" className="node-target" />
              {isSelected && <circle r="13" className="node-ring" />}
              {node.infrastructure ? <rect className="node-dot" x="-6" y="-6" width="12" height="12" rx="2" />
                : <circle className="node-dot" r={isLocal ? 7 : 6} />}
              {(graph.nodes.size < 45 || frame.scale > 0.65 || isSelected || isLocal) &&
                <text y="26" textAnchor="middle">{label.length > 18 ? label.slice(0, 15) + "…" : label}</text>}
            </g>;
          })}
        </svg>
      </div>
      <div className="topology-key"><span className="key-direct">Direct</span><span className="key-circuit">Circuit</span>
        {[...graph.nodes.values()].some(n => n.infrastructure) && <span className="key-server">Relay server</span>}
        {lane !== 0 && <span>{links.length} reported tree links</span>}
      </div>
      <section className="topology-inspect">
        <label>Inspect peer
          <select value={selected.id} onChange={event => inspect(event.target.value, true)}>
            {[...graph.nodes.keys()].map(id => <option value={id} key={id}>{name(id)} · {id.slice(-8)}</option>)}
          </select>
        </label>
        <div><strong>{selected.infrastructure ? "Relay / rendezvous" : selected.room || (selected.directory ? "Browsing" : "Known peer")}</strong>
          <p>{neighbors.length} {neighbors.length === 1 ? "link" : "links"}{!selected.reachable && " · No confirmed path from you"}</p>
        </div>
        <code>{selected.id}</code>
        {stats && <p>Local sockets {stats.direct_connections} direct / {stats.relayed_connections} circuit · {stats.direct_address_count} direct addresses
          {!selected.infrastructure && <> · Chat {stats.chat_subscribed ? "ready" : "waiting"} · Direct attempts {stats.direct_promotion_attempts} / {stats.direct_promotion_failures} failed
            {stats.direct_promotion_in_flight && " · Attempt in progress"}{stats.direct_promotion_suspended && " · Attempts suspended"}</>}</p>}
        {stats?.direct_connections > 0 && stats?.relayed_connections > 0 && <p>The circuit stays available during handoff while the client verifies delivery over a peer path.</p>}
        {received && <p>Room payload via this peer · {received.map(bytes => bytes.toLocaleString() + " B").join(" / ")}</p>}
      </section>
      {metrics && <div className="topology-delivery">
        <span>Worst receiver p95 <b>{metrics.worst_receiver_p95_ms.map(v => v == null ? "pending" : v + " ms").join(" / ")}</b></span>
        <span>Circuit payload <b>{metrics.circuit_payload_bytes.toLocaleString()} B</b></span>
        <span>Directory received <b>{metrics.directory_bytes.toLocaleString()} B</b></span>
      </div>}
    </dialog>
  );
}
