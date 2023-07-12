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

struct Alignment<T> {
    node: Option<GraphNode>,
    base: Option<T>,
}

impl<'a, T> Dag<T, GraphNode> for PoaGraph<T>
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
    fn align(&self, sequence: &Vec<T>) -> Vec<Alignment<T>> {
        let alignment = partial_order_sequence_matching(self, sequence);

        let mut result: Vec<Alignment<T>> = Vec::with_capacity(alignment.capacity());

        for (left, right) in alignment {
            let base = if let Some(index) = right {
                Some(&sequence[index])
            } else {
                None
            };

            result.push(Alignment {
                node: left,
                base: base.cloned(),
            })
        }

        result
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

    fn add(&mut self, sequence: &Vec<T>) {
        let alignment = self.align(sequence);

        println!("Received alignment");

        let mut last_node = None;

        for alignment in alignment {
            let next_node = match (alignment.node, alignment.base) {
                (None, None) => todo!(), // This should not happen
                (None, Some(base)) => self.add_node(GraphNodeInfo { base }),
                (Some(node), None) => continue, // Node is skipped, do not add edge
                (Some(node), Some(base)) => node, // Match, everything is good
            };

            if let Some(last_node) = last_node {
                self.add_edge(last_node, next_node);
            }

            last_node = Some(next_node);
        }
    }
}

/// Alignes multiple sequences into one
pub fn align<'a, T: Hash + Eq + Clone + Debug + Copy>(seqs: &[&'a Vec<T>]) -> Vec<&'a T> {
    let mut graph = PoaGraph::new();

    for sequence in seqs {
        graph.add(sequence);
    }

    println!();
    println!("flowchart TD");
    for prev in &graph.nodes {
        for next in graph.next(*prev) {
            let prev_base = graph.base(prev.clone());
            let next_base = graph.base(next.clone());
            println!("    {prev:?}({prev_base:?}) --> {next:?}({next_base:?})");
        }
    }
    println!();

    println!("Alignment graph formed, check it out");

    todo!()
}

#[test]
fn basic() {
    let vec1 = vec![1, 2, 3, 5];

    let vec2 = vec![99, 98];

    let vec3 = vec![3, 5, 99, 98];

    let result = align(&vec![&vec1, &vec2, &vec3]);

    // let sequences = vec![
    //     vec![28, 29, 2, 69, 63, 70, 30, 82, 31, 81, 3],
    //     vec![28, 68, 67, 29, 66, 65, 64, 2, 3],
    //     vec![28, 68, 67, 29, 66, 65, 64, 2, 30, 3],
    //     vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 31, 3],
    //     vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 82, 31, 81, 3],
    //     vec![28, 68, 67, 66, 65, 64, 2, 30],
    // ];

    // let slices = sequences.iter().collect_vec();

    // let result = align(&slices);
}
