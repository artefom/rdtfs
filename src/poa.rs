use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Debug)]
struct Node {
    id: isize,
    base: char,
    in_edges: HashMap<isize, Edge>,
    out_edges: HashMap<isize, Edge>,
    aligned_to: Vec<isize>,
}

fn add_edge(
    id: isize,
    edgeset: &mut HashMap<isize, Edge>,
    neighbour_id: isize,
    label: &str,
    from_neighbour: bool,
) {
    if neighbour_id == -1 {
        return;
    }

    if edgeset.contains_key(&neighbour_id) {
        edgeset
            .get_mut(&neighbour_id)
            .unwrap()
            .add_label(label.to_string());
    } else {
        let edge = if from_neighbour {
            Edge::new(neighbour_id, id, vec![label.to_string()])
        } else {
            Edge::new(id, neighbour_id, vec![label.to_string()])
        };
        edgeset.insert(neighbour_id, edge);
    }
}

impl Node {
    fn new(node_id: isize, base: char) -> Node {
        Node {
            id: node_id,
            base: base,
            in_edges: HashMap::new(),
            out_edges: HashMap::new(),
            aligned_to: vec![],
        }
    }

    fn add_in_edge(&mut self, neighbour_id: isize, label: &str) {
        add_edge(self.id, &mut self.in_edges, neighbour_id, label, true);
    }

    fn add_out_edge(&mut self, neighbour_id: isize, label: &str) {
        add_edge(self.id, &mut self.out_edges, neighbour_id, label, false);
    }

    fn next_node(&self, label: &str) -> Option<isize> {
        self.out_edges
            .iter()
            .find(|(_, edge)| edge.labels.contains(&label.to_string()))
            .map(|(id, _)| *id)
    }

    fn in_degree(&self) -> usize {
        self.in_edges.len()
    }

    fn out_degree(&self) -> usize {
        self.out_edges.len()
    }

    fn labels(&self) -> Vec<String> {
        let mut labelset = HashSet::new();
        for edge in self.in_edges.values() {
            for label in &edge.labels {
                labelset.insert(label.clone());
            }
        }
        for edge in self.out_edges.values() {
            for label in &edge.labels {
                labelset.insert(label.clone());
            }
        }
        labelset.into_iter().collect()
    }
}

#[derive(Debug)]
struct Edge {
    in_node_id: isize,
    out_node_id: isize,
    labels: Vec<String>,
}

impl Edge {
    fn new(in_node_id: isize, out_node_id: isize, labels: Vec<String>) -> Edge {
        Edge {
            in_node_id: in_node_id,
            out_node_id: out_node_id,
            labels: labels,
        }
    }

    fn add_label(&mut self, newlabel: String) {
        if !self.labels.contains(&newlabel) {
            self.labels.push(newlabel);
        }
    }
}

#[derive(Debug)]
struct PseudoNode {
    pnode_id: isize,
    predecessors: Vec<isize>,
    successors: Vec<isize>,
    node_ids: Vec<isize>,
}

pub struct POAGraph {
    next_node_id: isize,
    nnodes: isize,
    nedges: isize,
    nodedict: HashMap<isize, Node>,
    nodeidlist: Vec<isize>,
    needsort: bool,
    labels: Vec<String>,
    seqs: Vec<String>,
    starts: Vec<isize>,
}

impl POAGraph {
    pub fn new() -> Self {
        POAGraph {
            next_node_id: 0,
            nnodes: 0,
            nedges: 0,
            nodedict: HashMap::new(),
            nodeidlist: Vec::new(),
            needsort: false,
            labels: Vec::new(),
            seqs: Vec::new(),
            starts: Vec::new(),
        }
    }

    pub fn add_unmatched_seq(&mut self, seq: &str, label: &str, update_sequences: bool) {
        let mut first_id: Option<isize> = None;
        let mut last_id: Option<isize> = None;
        let needed_sort = self.needsort;

        for base in seq.chars() {
            let node_id = self.add_node(base);
            if first_id.is_none() {
                first_id = Some(node_id);
            }

            if let Some(last) = last_id {
                self.add_edge(last, node_id, label);
            }

            last_id = Some(node_id);
        }

        self.needsort = needed_sort;
        if update_sequences {
            self.seqs.push(seq.to_string());
            self.labels.push(label.to_string());
            if let Some(f) = first_id {
                self.starts.push(f);
            }
        }
    }

    fn add_edge(&mut self, start: isize, end: isize, label: &str) {
        if !self.nodedict.contains_key(&start) {
            panic!("addEdge: Start node not in graph: {}", start);
        }

        if !self.nodedict.contains_key(&end) {
            panic!("addEdge: End node not in graph: {}", end);
        }

        let old_node_edges = {
            let start_node = self.nodedict.get(&start).unwrap();
            let end_node = self.nodedict.get(&end).unwrap();

            start_node.out_degree() + end_node.in_degree()
        };

        self.nodedict
            .get_mut(&start)
            .unwrap()
            .add_out_edge(end, label);

        self.nodedict
            .get_mut(&end)
            .unwrap()
            .add_in_edge(start, label);

        let new_node_edges = {
            let start_node = self.nodedict.get(&start).unwrap();
            let end_node = self.nodedict.get(&end).unwrap();

            start_node.out_degree() + end_node.in_degree()
        };

        if new_node_edges != old_node_edges {
            self.nedges += 1;
        }

        self.needsort = true;
    }

    fn add_node(&mut self, base: char) -> isize {
        let nid = self.next_node_id;
        let new_node = Node::new(nid, base);
        self.nodedict.insert(nid, new_node);
        self.nodeidlist.push(nid);
        self.nnodes += 1;
        self.next_node_id += 1;
        self.needsort = true;
        nid
    }

    pub fn simplified_graph_rep(&self) -> Vec<PseudoNode> {
        // TODO: The need for this suggests that the way the graph is currently represented
        // isn't really right and needs some rethinking.

        let mut node_to_pn = HashMap::new();
        let mut pn_to_nodes = HashMap::new();

        // Find the mappings from nodes to pseudonodes
        let mut cur_pnid: isize = 0;
        for (_, node) in &self.nodedict {
            if !node_to_pn.contains_key(&node.id) {
                let mut node_ids = Vec::new();
                node_ids.push(node.id);
                node_ids.extend_from_slice(&node.aligned_to);

                pn_to_nodes.insert(cur_pnid, node_ids.clone());
                for nid in node_ids {
                    node_to_pn.insert(nid, cur_pnid);
                }
                cur_pnid += 1;
            }
        }

        // Create the pseudonodes
        let mut pseudonodes = Vec::new();
        for pnid in 0..cur_pnid {
            let nids = pn_to_nodes.get(&pnid).unwrap().clone();
            let mut preds = Vec::new();
            let mut succs = Vec::new();
            for nid in &nids {
                let node = self.nodedict.get(nid).unwrap();
                for (_, in_edge) in &node.in_edges {
                    preds.push(*node_to_pn.get(&in_edge.out_node_id).unwrap());
                }
                for (_, out_edge) in &node.out_edges {
                    succs.push(*node_to_pn.get(&out_edge.in_node_id).unwrap());
                }
            }
            let pn = PseudoNode {
                pnode_id: pnid,
                predecessors: preds,
                successors: succs,
                node_ids: nids,
            };
            pseudonodes.push(pn);
        }
        pseudonodes
    }

    pub fn toposort(&mut self) {
        // Sorted node list so that all incoming edges come from nodes earlier in the list.
        let mut sortedlist: Vec<isize> = Vec::new();
        let mut completed: HashSet<isize> = HashSet::new();

        let pseudonodes = self.simplified_graph_rep();

        fn dfs(
            start: isize,
            complete: &mut HashSet<isize>,
            sortedlist: &mut Vec<isize>,
            pseudonodes: &[PseudoNode],
        ) {
            let mut stack: Vec<isize> = vec![start];
            let mut started: HashSet<isize> = HashSet::new();
            while !stack.is_empty() {
                let pnode_id = stack.pop().unwrap();

                if complete.contains(&pnode_id) {
                    continue;
                }

                if started.contains(&pnode_id) {
                    complete.insert(pnode_id);
                    for &nid in &pseudonodes[pnode_id as usize].node_ids {
                        sortedlist.insert(0, nid);
                    }
                    started.remove(&pnode_id);
                    continue;
                }

                let successors = &pseudonodes[pnode_id as usize].successors;
                started.insert(pnode_id);
                stack.push(pnode_id);
                for s in successors {
                    stack.push(*s);
                }
            }
        }

        while sortedlist.len() < self.nnodes as usize {
            let mut found: Option<isize> = None;
            for pnid in 0..pseudonodes.len() {
                if !completed.contains(&(pnid as isize))
                    && pseudonodes[pnid].predecessors.len() == 0
                {
                    found = Some(pnid as isize);
                    break;
                }
            }
            assert!(found.is_some());
            dfs(
                found.unwrap(),
                &mut completed,
                &mut sortedlist,
                &pseudonodes,
            );
        }

        assert_eq!(sortedlist.len(), self.nnodes as usize);
        self.nodeidlist = sortedlist;
        self.needsort = false;
    }
}

pub fn align<'a, T: Hash + Eq + Clone + Debug>(seqs: &[&'a [T]]) -> Vec<&'a T> {
    let mut graph = POAGraph::new();

    graph.add_unmatched_seq("ABECD", "seq-1", true);
    graph.add_unmatched_seq("ABCD", "seq-2", true);

    // graph.toposort();

    vec![]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let seq1 = vec![28, 29, 2, 69, 63, 70, 30, 82, 31, 81, 3];
        let seq2 = vec![28, 68, 67, 29, 66, 65, 64, 2, 3];
        let seq3 = vec![28, 68, 67, 29, 66, 65, 64, 2, 30, 3];
        let seq4 = vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 31, 3];
        let result = align(&[&seq1, &seq2, &seq3, &seq4]);
        println!("Result: {:?}", result);
    }
}
