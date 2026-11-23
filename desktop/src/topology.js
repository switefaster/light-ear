// The directory supplies remote edges. For this client's own edges, the socket
// counts include infrastructure and distinguish a parallel circuit from a direct link.
export function buildTopology(snapshot, peers, localId, activeRoom) {
  const nodes = new Map();
  const edges = new Map();
  const key = (a, b) => a < b ? a + "/" + b : b + "/" + a;
  for (const [id, room] of Object.entries(snapshot?.rooms_by_peer ?? {})) {
    nodes.set(id, { id, room, directory: true });
  }
  nodes.set(localId, { id: localId, room: activeRoom, directory: true });
  for (const link of snapshot?.links ?? []) {
    edges.set(key(link.source, link.target), { ...link, circuit: !link.direct });
  }
  for (const peer of peers) {
    const id = peer.peer_id;
    nodes.set(id, { ...nodes.get(id), id, peer, infrastructure: peer.kind === "rendezvous" });
    const direct = peer.direct_connections > 0;
    const circuit = peer.relayed_connections > 0;
    const pair = key(localId, id);
    if (direct || circuit) {
      edges.set(pair, { ...edges.get(pair), source: localId, target: id, direct, circuit });
    } else {
      edges.delete(pair);
    }
  }
  const links = [...edges.values()];
  const adjacency = new Map([...nodes.keys()].map(id => [id, []]));
  for (const link of links) {
    adjacency.get(link.source).push(link.target);
    adjacency.get(link.target).push(link.source);
  }
  for (const neighbors of adjacency.values()) neighbors.sort();

  // Breadth-first columns expose hop distance. Disconnected components get
  // separate bands, so a retained directory entry never implies connectivity.
  const placed = new Set();
  let width = 0;
  let height = 0;
  for (const root of [localId, ...[...nodes.keys()].sort()]) {
    if (placed.has(root)) continue;
    placed.add(root);
    let level = [root];
    const columns = [];
    while (level.length) {
      columns.push(level);
      const next = [];
      for (const id of level) {
        for (const neighbor of adjacency.get(id)) {
          if (placed.has(neighbor)) continue;
          placed.add(neighbor);
          next.push(neighbor);
        }
      }
      level = next;
    }
    const band = Math.max(...columns.map(column => column.length)) * 62;
    columns.forEach((column, depth) => column.forEach((id, row) => {
      Object.assign(nodes.get(id), {
        x: 80 + depth * 150,
        y: height + band / 2 + (row - (column.length - 1) / 2) * 62 - 6,
        reachable: root === localId,
      });
    }));
    height += band + 20;
    width = Math.max(width, 160 + (columns.length - 1) * 150);
  }
  return { nodes, links, adjacency, width, height: height - 20 };
}
