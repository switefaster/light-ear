use crate::core::{TopologyLink, TopologyView};
use crate::directory::{Directory, Link};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};

type Graph = BTreeMap<String, BTreeMap<String, u64>>;
pub type Edges = BTreeSet<(String, String)>;
fn edge(a: &str, b: &str) -> (String, String) {
    if a < b {
        (a.into(), b.into())
    } else {
        (b.into(), a.into())
    }
}

fn reciprocal_links(directory: &Directory) -> impl Iterator<Item = (&String, &Link, &Link)> {
    directory.records.iter().flat_map(move |(a, record)| {
        record.record.links.iter().filter_map(move |link| {
            let b = &link.peer;
            if a >= b {
                return None;
            }
            let back = directory
                .records
                .get(b)?
                .record
                .links
                .iter()
                .find(|l| &l.peer == a)?;
            Some((a, link, back))
        })
    })
}

pub fn view(directory: &Directory) -> TopologyView {
    TopologyView {
        rooms_by_peer: directory
            .records
            .iter()
            .map(|(id, entry)| (id.clone(), entry.record.room.clone()))
            .collect(),
        links: reciprocal_links(directory)
            .map(|(a, link, back)| TopologyLink {
                source: a.clone(),
                target: link.peer.clone(),
                direct: link.direct && back.direct,
                lanes: link.lanes & back.lanes,
                rtt_ms: link.rtt_ms.max(back.rtt_ms),
                degraded: link.degraded || back.degraded,
                media_bytes: link.media_rx.saturating_add(back.media_rx),
                media_relay_bytes: link.media_relay_rx.saturating_add(back.media_relay_rx),
            })
            .collect(),
    }
}

fn graph(directory: &Directory) -> Graph {
    let mut graph: Graph = directory
        .records
        .keys()
        .map(|id| (id.clone(), BTreeMap::new()))
        .collect();
    for (a, link, back) in reciprocal_links(directory) {
        let b = &link.peer;
        let load = u64::from(
            directory.records[a]
                .record
                .upload_utilization
                .max(directory.records[b].record.upload_utilization),
        );
        let cost =
            load + if link.degraded || back.degraded {
                2_000_000_000
            } else {
                0
            } + u64::from(link.rtt_ms.max(back.rtt_ms).clamp(1, 5000))
                + if link.direct && back.direct {
                    0
                } else {
                    1_000_000_000
                };
        graph.entry(a.clone()).or_default().insert(b.clone(), cost);
        graph.entry(b.clone()).or_default().insert(a.clone(), cost);
    }
    graph
}
fn reachable(g: &Graph, from: &str, avoid: Option<&str>) -> BTreeSet<String> {
    let mut seen = BTreeSet::from([from.to_owned()]);
    let mut todo = VecDeque::from([from]);
    while let Some(id) = todo.pop_front() {
        if let Some(links) = g.get(id) {
            for next in links.keys() {
                if Some(next.as_str()) != avoid && seen.insert(next.clone()) {
                    todo.push_back(next)
                }
            }
        }
    }
    seen
}
// A DFS rooted at this peer finds all subtrees that would lose their path
// to us if their parent disappeared. Back edges protect a subtree from that cut.
fn across_cuts(graph: &Graph, local: &str) -> BTreeSet<String> {
    struct Search<'a> {
        graph: &'a Graph,
        order: BTreeMap<String, usize>,
        nodes: Vec<String>,
        cuts: Vec<(usize, usize)>,
    }
    impl Search<'_> {
        fn visit(&mut self, node: &str, parent: Option<&str>) -> usize {
            let entry = self.nodes.len();
            self.nodes.push(node.to_owned());
            self.order.insert(node.to_owned(), entry);
            let mut low = entry;
            for next in self.graph[node].keys() {
                if Some(next.as_str()) == parent {
                    continue;
                }
                if let Some(&index) = self.order.get(next) {
                    low = low.min(index);
                    continue;
                }
                let first = self.nodes.len();
                let back = self.visit(next, Some(node));
                if parent.is_some() && back >= entry {
                    self.cuts.push((first, self.nodes.len()));
                }
                low = low.min(back);
            }
            low
        }
    }
    let mut search = Search {
        graph,
        order: Default::default(),
        nodes: Vec::new(),
        cuts: Vec::new(),
    };
    if graph.contains_key(local) {
        search.visit(local, None);
    }
    let mut delta = vec![0i32; search.nodes.len() + 1];
    for (start, end) in search.cuts {
        delta[start] += 1;
        delta[end] -= 1;
    }
    let mut depth = 0;
    search
        .nodes
        .into_iter()
        .zip(delta)
        .filter_map(|(id, d)| {
            depth += d;
            (depth > 0).then_some(id)
        })
        .collect()
}
fn tree(g: &Graph, root: &str) -> Edges {
    let mut edges = Edges::new();
    let mut distances = BTreeMap::new();
    let mut heap = BinaryHeap::from([Reverse((0u64, root.to_string(), root.to_string()))]);
    while let Some(Reverse((cost, node, parent))) = heap.pop() {
        if distances.contains_key(&node) {
            continue;
        }
        distances.insert(node.clone(), cost);
        if parent != node {
            edges.insert(edge(&parent, &node));
        }
        for (next, delay) in &g[&node] {
            if !distances.contains_key(next) {
                heap.push(Reverse((cost + delay, next.clone(), node.clone())));
            }
        }
    }
    edges
}
// Minimize re-use across the whole tree. Two shortest-path trees with the
// same root can otherwise keep both sides of a crossing pinned to that root.
fn alternate_tree(g: &Graph, component: &BTreeSet<String>, primary: &Edges) -> Edges {
    let ids: Vec<_> = component.iter().collect();
    let indices: BTreeMap<_, _> = ids
        .iter()
        .enumerate()
        .map(|(i, p)| (p.as_str(), i))
        .collect();
    let mut neighbors = vec![Vec::new(); ids.len()];
    for (a, b) in primary {
        let (i, j) = (indices[a.as_str()], indices[b.as_str()]);
        neighbors[i].push(j);
        neighbors[j].push(i);
    }
    let mut penalty = BTreeMap::new();
    fn count(
        i: usize,
        parent: usize,
        neighbors: &[Vec<usize>],
        cost: &mut BTreeMap<(usize, usize), u64>,
    ) -> usize {
        let mut size = 1;
        for &j in &neighbors[i] {
            if j != parent {
                let n = count(j, i, neighbors, cost);
                size += n;
                cost.insert(
                    (i.min(j), i.max(j)),
                    10_000 + 100 * (n * (neighbors.len() - n)) as u64,
                );
            }
        }
        size
    }
    count(0, usize::MAX, &neighbors, &mut penalty);
    let mut choices = Vec::new();
    for (i, a) in ids.iter().enumerate() {
        for (b, delay) in &g[*a] {
            let j = indices[b.as_str()];
            if i < j {
                choices.push((delay + penalty.get(&(i, j)).copied().unwrap_or(0), i, j));
            }
        }
    }
    choices.sort_unstable();
    let mut parent = (0..ids.len()).collect::<Vec<_>>();
    fn root(parent: &mut [usize], mut i: usize) -> usize {
        while i != parent[i] {
            parent[i] = parent[parent[i]];
            i = parent[i];
        }
        i
    }
    let mut result = Edges::new();
    for (_, i, j) in choices {
        let a = root(&mut parent, i);
        let b = root(&mut parent, j);
        if a != b {
            parent[a] = b;
            result.insert(edge(ids[i], ids[j]));
        }
    }
    result
}
#[derive(Default, Clone, PartialEq, Eq)]
pub struct Plan {
    pub neighbors: [BTreeSet<String>; 2],
    pub rooms: [BTreeSet<String>; 2],
    pub trees: [Edges; 2],
    pub candidates: BTreeSet<String>,
    pub reachable: BTreeSet<String>,
    pub healthy_peer_paths: BTreeSet<String>,
    pub constrained: bool,
}
pub fn plan(directory: &Directory, local: &str) -> Plan {
    let g = graph(directory);
    let component = reachable(&g, local, None);
    let mut p = Plan::default();
    if !g.contains_key(local) {
        return p;
    }
    p.reachable = component.clone();
    let mut direct = g.clone();
    for links in direct.values_mut() {
        links.retain(|_, cost| *cost < 1_000_000_000);
    }
    p.healthy_peer_paths = reachable(&direct, local, None);
    let root = component.first().unwrap();
    p.trees[0] = tree(&g, root);
    let mut recovery = g.clone();
    for (a, ns) in &mut recovery {
        for (b, cost) in ns {
            let requested = directory.records[a].record.repairs.contains(b)
                || directory.records[b].record.repairs.contains(a);
            if requested && *cost >= 1_000_000_000 {
                *cost = 1;
            }
        }
    }
    p.trees[1] = alternate_tree(&recovery, &component, &p.trees[0]);
    // Inspect the union of confirmed forwarding edges too. A spare socket need not
    // yet belong to either room tree.
    let lanes = |from: &str, to: &str| {
        directory.records[from]
            .record
            .links
            .iter()
            .find(|l| l.peer == to)
            .map_or(0, |l| l.lanes)
    };
    let mut forwarding = g.clone();
    for (a, ns) in forwarding.iter_mut() {
        ns.retain(|b, _| lanes(a, b) & lanes(b, a) != 0);
    }
    if let Some(room) = g
        .get(local)
        .and_then(|_| directory.records[local].record.room.as_ref())
    {
        let mut room_graph = forwarding.clone();
        for (a, links) in &mut room_graph {
            links.retain(|b, _| {
                let subscribed =
                    |id: &str, i: usize| directory.records[id].record.leases[i].contains(room);
                let shared = lanes(a, b) & lanes(b, a);
                (0..2).any(|i| shared & (1 << i) != 0 && subscribed(a, i) && subscribed(b, i))
            });
        }
        let room_component = reachable(&room_graph, local, None);
        p.candidates
            .extend(directory.members(room).difference(&room_component).cloned());
        let cuts = across_cuts(&room_graph, local);
        p.constrained |= !cuts.is_empty();
        p.candidates.extend(cuts);
    }
    for network in [&g, &forwarding] {
        let candidates = across_cuts(network, local);
        p.constrained |= !candidates.is_empty();
        p.candidates.extend(candidates);
    }
    for i in 0..2 {
        let mut branches = Graph::new();
        for (a, b) in &p.trees[i] {
            branches.entry(a.clone()).or_default().insert(b.clone(), 1);
            branches.entry(b.clone()).or_default().insert(a.clone(), 1);
        }
        let mut sides = BTreeMap::<String, usize>::new();
        for neighbor in branches.get(local).into_iter().flat_map(|n| n.keys()) {
            p.neighbors[i].insert(neighbor.clone());
            let rooms = reachable(&branches, neighbor, Some(local))
                .iter()
                .filter_map(|id| directory.records[id].record.room.clone())
                .collect::<BTreeSet<_>>();
            for room in rooms {
                *sides.entry(room).or_default() += 1
            }
        }
        p.rooms[i] = sides
            .into_iter()
            .filter(|(_, n)| *n >= 2)
            .map(|(r, _)| r)
            .collect();
        if let Some(room) = &directory.records[local].record.room {
            p.rooms[i].insert(room.clone());
        }
    }
    p
}

#[cfg(test)]
mod tests {
    use super::*;
    fn network(edges: &[(&str, &str, u64)]) -> Graph {
        let mut g = Graph::new();
        for &(a, b, w) in edges {
            g.entry(a.into()).or_default().insert(b.into(), w);
            g.entry(b.into()).or_default().insert(a.into(), w);
        }
        g
    }
    #[test]
    fn two_trees_cover_256_peers_after_islands_merge() {
        let mut g = Graph::new();
        for n in 0..256 {
            for hop in [1, 5] {
                let a = n.to_string();
                let b = (n / 128 * 128 + (n + hop) % 128).to_string();
                g.entry(a.clone()).or_default().insert(b.clone(), 2);
                g.entry(b).or_default().insert(a, 2);
            }
        }
        g.get_mut("42").unwrap().insert("200".into(), 25);
        g.get_mut("200").unwrap().insert("42".into(), 25);
        assert!(across_cuts(&g, "0").contains("210"));
        g.get_mut("17").unwrap().insert("190".into(), 25);
        g.get_mut("190").unwrap().insert("17".into(), 25);
        let component = reachable(&g, "0", None);
        assert_eq!(component.len(), 256);
        assert!(across_cuts(&g, "0").is_empty());
        let primary = tree(&g, "0");
        assert_eq!(primary.len(), 255);
        assert_eq!(alternate_tree(&g, &component, &primary).len(), 255);
    }
    #[test]
    fn finds_vertex_cut_with_redundant_adjacent_edges() {
        let g = network(&[
            ("west", "w2", 1),
            ("west", "bridge", 1),
            ("w2", "bridge", 1),
            ("bridge", "east", 1),
            ("bridge", "e2", 1),
            ("east", "e2", 1),
        ]);
        assert_eq!(
            across_cuts(&g, "west"),
            BTreeSet::from(["east".into(), "e2".into()])
        );
    }
    #[test]
    fn room_cuts_require_a_reciprocal_edge_on_the_subscribed_tree() {
        use crate::directory::{Link, Presence, Signed};
        let key = libp2p::identity::Keypair::generate_ed25519();
        let mut directory = Directory::default();
        for (owner, neighbors) in [
            ("west", [("alice", 2), ("east", 1)]),
            ("alice", [("west", 2), ("east", 2)]),
            ("east", [("alice", 2), ("west", 1)]),
        ] {
            let record = Presence {
                owner: owner.into(),
                sequence: 1,
                issued: 1,
                room: Some("music".into()),
                addresses: vec![],
                transit_rooms: 8,
                upload_utilization: 0,
                media_p95_ms: None,
                leases: [vec![], vec!["music".into()]],
                repairs: vec![],
                links: neighbors
                    .into_iter()
                    .map(|(peer, lanes)| Link {
                        peer: peer.into(),
                        direct: true,
                        rtt_ms: 50,
                        lanes,
                        degraded: lanes == 1,
                        payload_bytes: [0; 2],
                        circuit_bytes: 0,
                        media_rx: 0,
                        media_relay_rx: 0,
                    })
                    .collect(),
            };
            directory
                .records
                .insert(owner.into(), Signed::new(record, &key));
        }
        let p = plan(&directory, "west");
        assert!(p.constrained && p.candidates.contains("east"));
        assert!(p.healthy_peer_paths.contains("east"));
        directory.records.get_mut("alice").unwrap().record.links[1].degraded = true;
        let p = plan(&directory, "west");
        assert!(p.reachable.contains("east") && !p.healthy_peer_paths.contains("east"));
        // The desktop graph also shows a remote-to-remote edge, but only while
        // both reports still include it, and only with mutually reported lanes.
        directory.records.get_mut("east").unwrap().record.links[0].lanes = 3;
        let snapshot = view(&directory);
        let remote = snapshot
            .links
            .iter()
            .find(|l| l.source == "alice" && l.target == "east")
            .unwrap();
        assert_eq!(remote.lanes, 2);
        assert!(remote.degraded);
        directory
            .records
            .get_mut("east")
            .unwrap()
            .record
            .links
            .remove(0);
        let snapshot = view(&directory);
        assert_eq!(snapshot.rooms_by_peer.len(), 3);
        assert!(
            !snapshot
                .links
                .iter()
                .any(|l| l.source == "alice" && l.target == "east")
        );
    }
    #[test]
    fn healthy_peer_path_excludes_circuit_from_both_trees() {
        let g = network(&[
            ("west", "helper", 5000),
            ("helper", "east", 5000),
            ("west", "east", 1_000_000_001),
        ]);
        let primary = tree(&g, "west");
        let alternate = alternate_tree(&g, &reachable(&g, "west", None), &primary);
        assert!(!primary.contains(&edge("west", "east")));
        assert_eq!(primary, alternate);
        let broken = network(&[("west", "helper", 5000), ("west", "east", 1_000_000_001)]);
        assert!(tree(&broken, "west").contains(&edge("west", "east")));
    }
}
