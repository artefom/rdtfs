use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt::Debug,
    hash::Hash,
};

use itertools::Itertools;

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

struct GraphNodeInfo<'a, T> {
    base: &'a T,
}

struct PoaGraph<'a, T> {
    nodes: Vec<GraphNode>,
    incoming_edges: HashMap<GraphNode, HashSet<GraphNode>>,
    outgoing_edges: HashMap<GraphNode, HashSet<GraphNode>>,

    // Info attached to nodes
    node_info: HashMap<GraphNode, GraphNodeInfo<'a, T>>,

    // For adding new nodes
    next_node_id: usize,
}

struct Alignment<'a, T> {
    node: Option<GraphNode>,
    base: Option<&'a T>,
}


impl<'a, T> PoaGraph<'a, T>
where
    T: Eq + PartialEq + Debug,
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

    /// Perform a topological sort of the graph nodes.
    /// Note: Assumes that the graph is a DAG, so it doesn't check for cycles.
    fn topological_sort(&self) -> Vec<GraphNode> {
        // Gets all nodes with no incoming edges.
        // Let's call them roots
        let mut roots: VecDeque<GraphNode> = self
            .nodes
            .iter()
            .cloned()
            .filter(|node| !self.incoming_edges.contains_key(node))
            .collect();

        // Number of incomming edges
        let mut incoming_edges_count: HashMap<GraphNode, usize> = self
            .incoming_edges
            .iter()
            .map(|(key, value)| (key.clone(), value.len()))
            .collect();

        // The final topologically sorted list
        let mut sorted: Vec<GraphNode> = Vec::new();

        while let Some(node) = roots.pop_front() {
            sorted.push(node);

            let Some(outgoing_edges) = self.outgoing_edges.get(&node) else {
                continue;
            };

            for out_node in outgoing_edges {
                let count = incoming_edges_count.get_mut(out_node).unwrap();
                *count -= 1;
                if *count == 0 {
                    roots.push_back(*out_node);
                }
            }
        }

        sorted
    }

    /// Align new sequence to the existing graph
    fn align(&self, sequence: &'a [T]) -> Vec<Alignment<'a, T>> {
        // Corner-case when empty
        if sequence.len() == 0 {
            return Vec::new();
        }

        // Return early of graph is empty
        if self.nodes.len() == 0 {
            let mut result = Vec::new();

            for base in sequence {
                result.push(Alignment {
                    node: None,
                    base: Some(base),
                })
            }

            return result;
        }

        println!("Aligining {:?}", sequence);

        let bases = self
            .topological_sort()
            .iter()
            .map(|x| self.node_info[x].base)
            .collect_vec();

        println!("toposort {:?}", self.topological_sort());
        println!("bases: {:?}", bases);

        println!("Incomming edges {:?}", self.incoming_edges);
        println!("Outgoing edges {:?}", self.outgoing_edges);
        println!();

        // Dynamic programming scores
        let gap_score: i64 = -1;

        // Initialize dynamic programming data
        // maps specific graph node and offset to score and backtrack info
        let mut dp_data: HashMap<(GraphNode, i64), i64> = HashMap::new();

        // Scores for matches and mismatches
        let mismatch_score: i64 = -100;
        let match_score: i64 = 10;
        let gap_score: i64 = -1;

        for node in self.topological_sort() {
            println!("Computing for {:?}", node);

            // This must exist
            let base = self.node_info[&node].base;

            // Get a list of incomming edges (if it is empty, we're having a root node)
            let Some(incomming_edges) = self.incoming_edges.get(&node) else {
                // Process a root node

                for offset in 0..sequence.len() {
                    let offset_i64: i64 =  offset.try_into().unwrap();
                    let num_skipped: i64 = offset.try_into().unwrap();
                    
                    // Insert match
                    if *base == sequence[offset] {
                        dp_data.insert((node, offset_i64), num_skipped * -1 + match_score);
                    } else {
                        dp_data.insert((node, offset_i64), num_skipped * -1 + mismatch_score);
                    }
                    
                }

                // Insert gap
                dp_data.insert((node, -1), gap_score);
                println!("Result dp data: {:?}", dp_data);
                continue;
            };

            for offset in 1..sequence.len() {
                println!("Computing offset {:?}", offset);
                let offset: i64 = offset.try_into().unwrap();

                let mut prev_positions: Vec<(GraphNode, i64, i64)> = Vec::new();

                // Skip node, gap
                if dp_data.contains_key(&(node, offset - 1)) {
                    prev_positions.push((node, offset - 1, gap_score));
                } else {
                    println!("Could not add previous position {:?} - it does not exist", (node,offset-1));
                }

                println!("Incomming edges: {:?}", incomming_edges);

                for prev_node in incomming_edges {
                    // Skip offset, gap
                    prev_positions.push((*prev_node, offset, -1));

                    // Increment both node and offset (match)
                    if offset > 0 {
                        prev_positions.push((*prev_node, offset - 1, 1));
                    }
                }

                println!("Prev positions: {:?}", prev_positions);

                // Calc max new score
                let mut new_scores: Vec<(GraphNode, i64, i64)> = Vec::new();
                for (prev_node, prev_offset, score_delta) in prev_positions {
                    let new_score = dp_data[&(prev_node, prev_offset)] + score_delta;
                    new_scores.push((prev_node, prev_offset, new_score));
                }

                println!("New scores: {:?}", new_scores);
            }

            println!("Result dp data: {:?}", dp_data);
        }

        todo!()
    }

    fn add_node(&mut self, info: GraphNodeInfo<'a, T>) -> GraphNode {
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

    fn add(&mut self, sequence: &'a [T]) {
        let alignment = self.align(sequence);

        let mut last_node = None;

        for alignment in alignment {
            let next_node = match (alignment.node, alignment.base) {
                (None, None) => todo!(),
                (None, Some(base)) => self.add_node(GraphNodeInfo { base }),
                (Some(node), None) => todo!(),
                (Some(node), Some(base)) => todo!(),
            };

            if let Some(last_node) = last_node {
                self.add_edge(last_node, next_node);
            }

            last_node = Some(next_node);
        }
    }
}

/// Alignes multiple sequences into one
pub fn align<'a, T: Hash + Eq + Clone + Debug>(seqs: &[&'a [T]]) -> Vec<&'a T> {
    let mut graph = PoaGraph::new();

    for sequence in seqs {
        graph.add(sequence);
    }

    todo!()
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::align;

    #[test]
    fn basic() {
        let sequences = vec![
            vec![28, 29, 2, 69, 63, 70, 30, 82, 31, 81, 3],
            vec![28, 68, 67, 29, 66, 65, 64, 2, 3],
            vec![28, 68, 67, 29, 66, 65, 64, 2, 30, 3],
            vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 31, 3],
            vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 82, 31, 81, 3],
            vec![28, 68, 67, 66, 65, 64, 2, 30],
        ];

        let slices = sequences.iter().map(|x| x.as_slice()).collect_vec();

        let result = align(&slices);

    }
}
