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
    pub fn new(seq: Option<&str>, label: &str) -> Self {
        let mut poa = POAGraph {
            next_node_id: 0,
            nnodes: 0,
            nedges: 0,
            nodedict: HashMap::new(),
            nodeidlist: Vec::new(),
            needsort: false,
            labels: Vec::new(),
            seqs: Vec::new(),
            starts: Vec::new(),
        };

        if let Some(s) = seq {
            poa.add_unmatched_seq(s, label, true);
        }

        poa
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

    // The other functions would be implemented in a similar fashion
    // ...
}

pub fn align<'a, T: Hash + Eq + Clone + Debug>(seqs: &[&'a [T]]) -> Vec<&'a T> {
    todo!()
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
