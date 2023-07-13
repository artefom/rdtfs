use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
    hash::Hash,
};

use itertools::Itertools;

use crate::poa::dp::partial_order_sequence_matching;

use self::dp::Dag;

mod dp;

#[derive(Hash, PartialEq, Eq, Clone, Copy)]
struct GraphNode {
    id: usize,
}

impl Debug for GraphNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "N{}", self.id)
    }
}

struct GraphNodeInfo<T> {
    base: T,

    // Mapping to sequence id positions
    positions: Vec<(usize, usize)>,
}

struct PoaGraph<T> {
    nodes: Vec<GraphNode>,
    incoming_edges: HashMap<GraphNode, HashSet<GraphNode>>,
    outgoing_edges: HashMap<GraphNode, HashSet<GraphNode>>,

    // Info attached to nodes
    node_info: HashMap<GraphNode, GraphNodeInfo<T>>,

    // For adding new nodes
    next_node_id: usize,
}

struct Alignment {
    node: Option<GraphNode>,
    offset: Option<usize>,
}

impl<T> Dag<T, GraphNode> for PoaGraph<T>
where
    T: PartialEq + Eq + Debug,
{
    fn roots(&self) -> Vec<GraphNode> {
        let mut result = Vec::new();

        for item in &self.nodes {
            if !self.incoming_edges.contains_key(item) {
                result.push(item.clone())
            }
        }

        result
    }

    fn leafs(&self) -> Vec<GraphNode> {
        let mut result = Vec::new();

        for item in &self.nodes {
            if !self.outgoing_edges.contains_key(item) {
                result.push(item.clone())
            }
        }

        result
    }

    fn base(&self, handle: GraphNode) -> &T {
        &self.node_info.get(&handle).unwrap().base
    }

    fn next(&self, handle: GraphNode) -> Vec<GraphNode> {
        let mut result = Vec::new();

        let Some(outgoing_edges) = self.outgoing_edges.get(&handle) else {
            return result
        };

        for node in outgoing_edges {
            result.push(node.clone())
        }

        result
    }

    fn previous(&self, handle: GraphNode) -> Vec<GraphNode> {
        let mut result = Vec::new();

        let Some(incoming_edges) = self.incoming_edges.get(&handle) else {
            return result
        };
        for node in incoming_edges {
            result.push(node.clone())
        }

        result
    }
}

impl<T> PoaGraph<T>
where
    T: Eq + PartialEq + Debug + Clone + Copy,
{
    fn new() -> Self {
        PoaGraph {
            nodes: Vec::new(),
            incoming_edges: HashMap::new(),
            outgoing_edges: HashMap::new(),
            node_info: HashMap::new(),
            next_node_id: 0,
        }
    }

    fn roots<V>(&self) -> V
    where
        V: FromIterator<GraphNode>,
    {
        self.nodes
            .iter()
            .cloned()
            .filter(|node| !self.incoming_edges.contains_key(node))
            .collect()
    }

    /// Align new sequence to the existing graph
    fn align(&self, sequence: &Vec<T>) -> (Option<i32>, Vec<(Option<GraphNode>, Option<usize>)>) {
        partial_order_sequence_matching(self, sequence)
    }

    fn add_node(&mut self, info: GraphNodeInfo<T>) -> GraphNode {
        let graph_node = GraphNode {
            id: self.next_node_id,
        };
        self.next_node_id += 1;
        self.nodes.push(graph_node);

        // Insert info
        self.node_info.insert(graph_node, info);

        graph_node
    }

    fn add_edge(&mut self, from: GraphNode, to: GraphNode) {
        self.incoming_edges
            .entry(to)
            .or_insert(HashSet::new())
            .insert(from);
        self.outgoing_edges
            .entry(from)
            .or_insert(HashSet::new())
            .insert(to);
    }

    fn add(&mut self, sequence_id: usize, sequence: &Vec<T>) {
        let (_, alignment) = self.align(sequence);

        let mut last_node = None;

        for (node, offset) in alignment {
            // let base = offset.map(|x| sequence[x]);

            let next_node = match (node, offset) {
                (None, None) => todo!(), // This should not happen
                (None, Some(offset)) => self.add_node(GraphNodeInfo {
                    base: sequence[offset],
                    positions: vec![(sequence_id, offset)],
                }),
                (Some(node), None) => continue, // Node is skipped, do not add edge
                (Some(node), Some(offset)) => {
                    let node_info = self.node_info.get_mut(&node).unwrap();

                    if node_info.positions.contains(&(sequence_id, offset)) {
                        panic!("Duplicate position")
                    };

                    node_info.positions.push((sequence_id, offset));
                    node
                } // Match, everything is good
            };

            if let Some(last_node) = last_node {
                self.add_edge(last_node, next_node);
            }

            last_node = Some(next_node);
        }
    }
}

/// Alignes multiple sequences into one
pub fn align<T: Hash + Eq + Clone + Debug + Copy>(seqs: &[&Vec<T>]) -> (Vec<T>, Vec<Vec<usize>>) {
    let mut graph = PoaGraph::new();

    let mut unmerged_sequences = seqs.iter().enumerate().collect_vec();

    // Start from the longest sequence
    let greatest_sequence = unmerged_sequences
        .iter()
        .position_max_by_key(|x| x.1.len())
        .unwrap();
    let (greatest_sequence_id, greatest_sequence) =
        unmerged_sequences.swap_remove(greatest_sequence);

    graph.add(greatest_sequence_id, greatest_sequence);

    // Add most fitting sequences on each iteration
    loop {
        let best_alignment = unmerged_sequences
            .iter()
            .enumerate()
            .map(|(idx, (_, seq))| (idx, graph.align(seq).0))
            .filter_map(|(idx, score)| {
                let score = match score {
                    Some(score) => score,
                    None => return None,
                };
                Some((idx, score))
            })
            .max_by_key(|(seq_id, score)| *score);

        let Some((best_idx, _)) = best_alignment else {
            break;
        };
        let (best_seq_id, best_seq) = unmerged_sequences.get(best_idx).unwrap();
        graph.add(*best_seq_id, best_seq);
        unmerged_sequences.swap_remove(best_idx);
    }

    // Aligned sequence offset
    let mut sequence_offsets: Vec<Vec<usize>> = Vec::with_capacity(seqs.len());
    for i in 0..seqs.len() {
        sequence_offsets.push(Vec::new());
    }

    let mut aligned_sequence = Vec::new();

    for (node_seq_id, node) in graph.toposort().iter().enumerate() {
        let node_info = graph.node_info.get(node).unwrap();

        let base = graph.base(*node);

        aligned_sequence.push(base.clone());

        for (seq_id, _) in &node_info.positions {
            sequence_offsets.get_mut(*seq_id).unwrap().push(node_seq_id)
        }
    }

    (aligned_sequence, sequence_offsets)
}

#[test]
fn test() {
    let sequences = vec![
        vec![28, 29, 2, 69, 63, 70, 30, 82, 31, 81, 3],
        vec![28, 68, 67, 29, 66, 65, 64, 2, 3],
        vec![28, 68, 67, 29, 66, 65, 64, 2, 30, 3],
        vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 31, 3],
        vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 82, 31, 81, 3],
        vec![28, 68, 67, 66, 65, 64, 2, 30],
    ];

    let slices = sequences.iter().collect_vec();

    let (consensus, alignments) = align(&slices);

    let mut table = HashMap::new();
    for (seq_id, aligment) in alignments.iter().enumerate() {
        for (seq_offset, offset) in aligment.iter().enumerate() {
            table.insert((seq_id, *offset), sequences[seq_id][seq_offset]);
        }
    }

    println!();
    println!("Aligned table");

    for letter in &consensus {
        print!("{letter: >3?}")
    }
    println!();

    for letter in &consensus {
        print!("---")
    }
    println!();

    for (seq_id, seq) in sequences.iter().enumerate() {
        for offset in 0..consensus.len() {
            match table.get(&(seq_id, offset)) {
                Some(value) => {
                    print!("{value: >3?}")
                }
                None => {
                    print!("{: >3}", '-')
                }
            }
        }
        println!()
    }
    println!();
}
