import React, { useEffect, useRef, useState } from "react";
import { ArrowLeftRight, Headphones, LogOut, RefreshCw, ScrollText, Settings, X } from "lucide-react";

const sections = [
  { id: "audio", name: "Audio output", icon: Headphones },
  { id: "transfer", name: "Media transfer", icon: ArrowLeftRight },
  { id: "logs", name: "Status log", icon: ScrollText },
];

export default function SettingsDialog({ section, onSelect, onClose, room, invoke, displayName, onLeave, children }) {
  const dialog = useRef(null);
  useEffect(() => {
    const element = dialog.current;
    element.showModal();
    return () => element.close();
  }, []);

  return <dialog className="settings-dialog" ref={dialog} aria-labelledby="settings-title"
    onCancel={event => { event.preventDefault(); onClose(); }}
    onClick={event => {
      if (event.target !== dialog.current) return;
      const box = dialog.current.getBoundingClientRect();
      if (event.clientX < box.left || event.clientX > box.right || event.clientY < box.top || event.clientY > box.bottom) onClose();
    }}>
    <header className="settings-head"><div className="drawer-title"><Settings size={21} /><h2 id="settings-title">Settings</h2></div>
      <button type="button" className="icon-button" aria-label="Close settings" onClick={onClose}><X size={18} /></button>
    </header>
    <div className="settings-layout">
      <nav className="settings-nav" aria-label="Settings sections">{sections.map(({ id, name, icon: Icon }) =>
          <button type="button" key={id} aria-current={section === id ? "page" : undefined} onClick={() => onSelect(id)}>
            <Icon size={16} />{name}
          </button>)}
        {room.active && <button type="button" className="leave-room" onClick={onLeave}><LogOut size={15} />Leave room</button>}
      </nav>
      <section className="settings-content" aria-labelledby="settings-section">
        <h3 id="settings-section">{sections.find(item => item.id === section).name}</h3>
        {section === "audio" && <AudioSettings invoke={invoke} running={room.backendRunning} />}
        {section === "transfer" && <TransferSettings invoke={invoke} room={room} displayName={displayName} />}
        {section === "logs" && children}
      </section>
    </div>
    <footer className="settings-credit">App icon by ChatGPT</footer>
  </dialog>;
}

function AudioSettings({ invoke, running }) {
  const [selected, setSelected] = useState(() => localStorage.getItem("audioOutputDevice") || "");
  const [devices, setDevices] = useState([]);
  const [refresh, setRefresh] = useState(0);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    let active = true;
    setLoading(true);
    setError("");
    invoke("list_output_devices").then(list => { if (active) setDevices(list); })
      .catch(err => { if (active) setError(String(err)); })
      .finally(() => { if (active) setLoading(false); });
    return () => { active = false; };
  }, [invoke, refresh]);

  async function select(device) {
    setSaving(true);
    setError("");
    try {
      if (running) await invoke("set_output_device", { device: device || null });
      if (device) localStorage.setItem("audioOutputDevice", device);
      else localStorage.removeItem("audioOutputDevice");
      setSelected(device);
    } catch (err) {
      setError(String(err));
    } finally {
      setSaving(false);
    }
  }
  const system = devices.find(d => d.is_default);
  const missing = selected && !loading && !devices.some(d => d.id === selected);
  return <div className="settings-pane">
    <label className="field" htmlFor="output-device">Playback device</label>
    <div className="settings-control-row">
      <select id="output-device" value={selected} onChange={e => select(e.target.value)} disabled={loading || saving}>
        <option value="">System default{system ? ` · ${system.name}` : ""}</option>
        {selected && !devices.some(d => d.id === selected) && <option value={selected}>{loading ? "Loading device…" : "Saved device (disconnected)"}</option>}
        {devices.map(d => <option key={d.id} value={d.id}>{d.name}</option>)}
      </select>
      <button type="button" className="icon-button" aria-label="Refresh output devices" title="Refresh output devices"
        disabled={loading || saving} onClick={() => setRefresh(n => n + 1)}><RefreshCw size={16} /></button>
    </div>
    <p className="setting-note">{missing ? "Playback follows the system default until this device reconnects." :
      selected ? "If this device disconnects, playback follows the system default." : "Playback follows changes to your system’s default output."}</p>
    {error && <p role="alert" className="setting-error">{error}</p>}
  </div>;
}

function TransferSettings({ invoke, room, displayName }) {
  const [saved, setSaved] = useState(() => localStorage.getItem("mediaUploadKiB") || "1024");
  const [limit, setLimit] = useState(saved);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const metrics = room.mediaMetrics;
  const neighbors = Object.entries(metrics?.neighbors ?? {});
  async function save(event) {
    event.preventDefault();
    setError("");
    setBusy(true);
    try {
      const kib = Number(limit);
      if (room.backendRunning) await invoke("set_media_upload_limit", { kib });
      localStorage.setItem("mediaUploadKiB", String(kib));
      setSaved(String(kib));
      setLimit(String(kib));
    } catch (err) {
      setError(String(err));
    } finally {
      setBusy(false);
    }
  }
  return <div className="settings-pane">
    <form onSubmit={save}>
      <label className="field" htmlFor="media-limit">Media upload limit</label>
      <div className="settings-control-row">
        <div className="unit-input"><input id="media-limit" type="number" min="1" max="102400" step="1" required
          value={limit} onChange={event => setLimit(event.target.value)} aria-describedby="limit-unit" /><span id="limit-unit">KiB/s</span></div>
        <button className="btn subtle" type="submit" disabled={busy || limit === saved}>Apply</button>
      </div>
      {error && <p role="alert" className="setting-error">{error}</p>}
    </form>
    <h4>Peer traffic</h4>
    {metrics && <dl className="transfer-totals">
      <div><dt>Worst listener p95</dt><dd>{metrics.worst_receiver_p95_ms == null ? "—" : `${metrics.worst_receiver_p95_ms} ms`}</dd></div>
      <div><dt>Retries</dt><dd>{metrics.retries}</dd></div>
    </dl>}
    {neighbors.length ? <div className="transfer-table"><table>
      <thead><tr><th>Peer</th><th>Sent</th><th>Received</th><th>Relay</th></tr></thead>
      <tbody>{neighbors.map(([id, traffic]) => <tr key={id}><th title={id}>{displayName(id)}</th>
        {traffic.map((bytes, i) => <td key={i}>{Math.round(bytes / 1024).toLocaleString()} KiB</td>)}</tr>)}</tbody>
    </table></div> : <p className="setting-note">No peer audio transfers yet.</p>}
  </div>;
}
