use std::cmp;
use std::collections::{HashMap, HashSet};
use std::fmt::{Debug, Display};
use std::hash::Hash;
use std::marker::PhantomData;

struct POAGraph<'a, T> {
    nodes: Vec<Node<'a, T>>,
}

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
struct NodeHandle {
    node_id: usize,
}

impl Debug for NodeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "N{}", self.node_id)
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
struct Edge {
    from: NodeHandle, // outNodeID
    to: NodeHandle,   // inNodeID
}

struct Node<'a, T> {
    handle: NodeHandle,
    base: &'a T,
    in_edges: HashSet<Edge>,
    out_edges: HashSet<Edge>,
    aligned_to: Vec<NodeHandle>,
}

impl<'a, T> Node<'a, T> {
    fn new(handle: NodeHandle, base: &'a T) -> Self {
        Node {
            base,
            handle,
            in_edges: HashSet::new(),
            out_edges: HashSet::new(),
            aligned_to: Vec::new(),
        }
    }
    fn add_out_edge(&mut self, handle: NodeHandle) -> bool {
        self.out_edges.insert(Edge {
            from: self.handle,
            to: handle,
        })
    }

    fn add_in_edge(&mut self, handle: NodeHandle) -> bool {
        self.in_edges.insert(Edge {
            from: handle,
            to: self.handle,
        })
    }
}

#[derive(Clone, Copy, Hash, Eq, PartialEq)]
struct PseudoNodeHandle {
    pseudo_node_id: usize,
}
impl Debug for PseudoNodeHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PN{}", self.pseudo_node_id)
    }
}

// Pseudo-nodes are grouping
// together the 'aligned' nodes from the graph

struct PseudoNode {
    handle: PseudoNodeHandle,
    handles: Vec<NodeHandle>,
    successors: Vec<PseudoNodeHandle>,
    predecessors: Vec<PseudoNodeHandle>,
}

impl Debug for PseudoNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?} -> ", self.predecessors)?;
        write!(f, "{:?}", self.handle)?;
        write!(f, " -> {:?}: ", self.successors)?;
        write!(f, "{:?}", self.handles)?;
        Ok(())
    }
}

impl<'a, T> POAGraph<'a, T>
where
    T: Debug + Eq,
{
    fn new() -> Self {
        POAGraph { nodes: Vec::new() }
    }

    fn n_nodes(&self) -> usize {
        self.nodes.len()
    }

    // Get representation of the grapg in 'pseudonodes'
    // That are easier to use in toposort
    fn simplified_graph_rep(&self) -> Vec<PseudoNode> {
        // Mapping of nodes to psaudo nodes
        let mut node_to_pseudo_node: HashMap<NodeHandle, PseudoNodeHandle> = HashMap::new();

        let mut pseudo_nodes: Vec<PseudoNode> = Vec::new();

        // Create pseudo-nodes
        for node in &self.nodes {
            // Do nothing if node is already part of some other pseudo-node
            if node_to_pseudo_node.contains_key(&node.handle) {
                continue;
            }

            // Get handles for all nodes that are aligned together
            let mut handles = Vec::with_capacity(node.aligned_to.len() + 1);
            handles.push(node.handle);
            handles.extend(&node.aligned_to);

            let pseudo_node_handle = PseudoNodeHandle {
                pseudo_node_id: pseudo_nodes.len(),
            };

            let pseudo_node = PseudoNode {
                handle: pseudo_node_handle,
                handles,
                successors: Vec::new(),
                predecessors: Vec::new(),
            };

            // Update node ot pseud-node table
            for handle in &pseudo_node.handles {
                node_to_pseudo_node.insert(*handle, pseudo_node_handle);
            }

            pseudo_nodes.push(pseudo_node);
        }

        // Update successors and predecessors
        for pseudo_node in &mut pseudo_nodes {
            for handle in &pseudo_node.handles {
                let node = self.get_node(&handle);

                for in_edge in &node.in_edges {
                    pseudo_node
                        .predecessors
                        .push(node_to_pseudo_node[&in_edge.from]);
                }

                for out_edge in &node.out_edges {
                    pseudo_node
                        .successors
                        .push(node_to_pseudo_node[&out_edge.to])
                }
            }
        }

        pseudo_nodes
    }

    // Sorts node list so that all incoming edges come from nodes earlier in the list.
    fn toposort(&self) -> Vec<NodeHandle> {
        let pseudonodes = self.simplified_graph_rep();

        let mut sorted_list: Vec<NodeHandle> = Vec::new();
        let mut completed: HashSet<PseudoNodeHandle> = HashSet::new();

        while sorted_list.len() < self.n_nodes() {
            // Find start node
            let mut start = None;
            for pseudonode in &pseudonodes {
                if !completed.contains(&pseudonode.handle) && pseudonode.predecessors.len() == 0 {
                    start = Some(pseudonode.handle);
                    break;
                }
            }

            // Launch dfs
            let mut stack: Vec<PseudoNodeHandle> = vec![start.unwrap()];
            let mut started: HashSet<PseudoNodeHandle> = HashSet::new();

            while stack.len() > 0 {
                let pseudo_node = &pseudonodes[stack.pop().unwrap().pseudo_node_id];

                // This node was already completed
                if completed.contains(&pseudo_node.handle) {
                    continue;
                }

                if started.contains(&pseudo_node.handle) {
                    completed.insert(pseudo_node.handle);
                    // Todo: Remove insertion to the beginning of the list
                    for node_handle in &pseudo_node.handles {
                        sorted_list.insert(0, *node_handle);
                    }
                    started.remove(&pseudo_node.handle);
                    continue;
                }

                started.insert(pseudo_node.handle);
                stack.push(pseudo_node.handle);
                stack.extend(pseudo_node.successors.iter());
            }
        }

        sorted_list
    }

    fn add_node(&mut self, base: &'a T) -> NodeHandle {
        let handle = NodeHandle {
            node_id: self.nodes.len(),
        };
        self.nodes.push(Node::new(handle, base));
        handle
    }

    fn get_node_mut(&mut self, handle: &NodeHandle) -> &mut Node<'a, T> {
        self.nodes.get_mut(handle.node_id).unwrap()
    }

    fn get_node(&self, handle: &NodeHandle) -> &Node<'a, T> {
        self.nodes.get(handle.node_id).unwrap()
    }

    fn add_edge(&mut self, from: NodeHandle, to: NodeHandle) {
        self.get_node_mut(&from).add_out_edge(to);
        self.get_node_mut(&to).add_in_edge(from);
    }

    // Add a completely independant (sub)string to the graph,
    // and return node index to initial and final node

    fn add_unmatched_seq(&mut self, seq: &'a [T]) -> (NodeHandle, NodeHandle) {
        let mut seq_iter = seq.iter();

        // Guaranteed to have at least one element in the sequence
        let first = self.add_node(seq_iter.next().unwrap());

        // Process special case when we have added only one node
        let Some(next) = seq_iter.next() else {
            return (first, first);
        };

        let mut last = self.add_node(next);

        self.add_edge(first, last);

        for base in seq_iter {
            let next = self.add_node(base);
            self.add_edge(last, next);
            last = next; // Update last
        }

        (first, last)
    }

    fn incorporate_seq_alignment(&mut self, alignment: SequenceAlignment, seq: &'a [T]) {
        let newseq = seq;
        let stringidxs: Vec<Option<usize>> = alignment.alignment.iter().map(|x| x.0).collect();
        let nodeidxs: Vec<Option<NodeHandle>> = alignment.alignment.iter().map(|x| x.1).collect();

        println!("newseq: {newseq:?}");
        println!("stringidxs: {stringidxs:?}");
        println!("nodeidxs: {nodeidxs:?}");

        let validstringidxs: Vec<usize> = stringidxs
            .iter()
            .filter(|x| x.is_some())
            .map(|x| x.unwrap())
            .collect();

        let start_seq_idx = *validstringidxs.first().unwrap();
        let end_seq_idx = *validstringidxs.last().unwrap();

        let mut first_id: Option<NodeHandle> = None;
        let mut tail_id: Option<NodeHandle> = None;
        let mut head_id: Option<NodeHandle> = None;

        // head, tail of sequence may be unaligned; just add those into the
        // graph directly
        if start_seq_idx > 0 {
            let (added_first, added_last) = self.add_unmatched_seq(&newseq[..start_seq_idx]);

            first_id = Some(added_first);
            head_id = Some(added_last);
        }
        if end_seq_idx < (newseq.len() - 1) {
            let (added_first, _) = self.add_unmatched_seq(&newseq[end_seq_idx + 1..]);
            tail_id = Some(added_first)
        }

        for (sindex, match_id) in alignment.alignment {
            let Some(sindex) = sindex else {
                continue;
            };

            let base = &newseq[sindex];

            let node_id: NodeHandle = 'bar: {
                let Some(match_id) = match_id else {
                    break 'bar self.add_node(base);
                };

                let node = self.get_node(&match_id);

                if node.base == base {
                    break 'bar match_id;
                };

                let other_aligns = &node.aligned_to;

                let mut found: Option<NodeHandle> = None;

                for other_handle in other_aligns {
                    if self.get_node(other_handle).base == base {
                        found = Some(*other_handle);
                    }
                }

                if let Some(found) = found {
                    break 'bar found;
                }

                // We need to drop dependency on borrowed 'node' here, so clone this shit
                let other_aligns = other_aligns.clone();
                let node = ();

                let new_node_handle = self.add_node(base);

                // Populate alignment of new node
                let new_node = self.get_node_mut(&new_node_handle);
                new_node.aligned_to.extend(other_aligns.iter());
                new_node.aligned_to.push(match_id);

                // Add new node to our current match
                self.get_node_mut(&match_id)
                    .aligned_to
                    .push(new_node_handle);

                // Populate alignment of other nodes
                for other_node in &other_aligns {
                    self.get_node_mut(other_node)
                        .aligned_to
                        .push(new_node_handle);
                }

                new_node_handle
            };

            if let Some(head_id) = head_id {
                self.add_edge(head_id, node_id);
            }
            head_id = Some(node_id);
            if first_id.is_none() {
                first_id = head_id;
            }
        }

        // finished the unaligned portion: now add an edge from the current headID to the tailID.
        if let Some(head_id) = head_id {
            if let Some(tail_id) = tail_id {
                self.add_edge(head_id, tail_id);
            }
        };

        // validstringidxs = [si for si in stringidxs if si is not None]
    }
}

#[derive(Default)]
struct DynamicProgrammingScore {
    score: i64,
    back_graph_idx: usize,
    back_str_idx: usize,
}

#[derive(Default)]
struct DynamicProgrammingData {
    row_length: usize,
    node_handle_to_idx: HashMap<NodeHandle, usize>,
    data: Vec<DynamicProgrammingScore>,
}

impl Debug for DynamicProgrammingData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Dynamic programming scores\n")?;

        let num_rows = self.data.len() / self.row_length;

        for row_id in 0..num_rows {
            write!(f, "[")?;
            for col_id in 0..self.row_length {
                write!(f, "{: >3}", self.score(row_id, col_id))?;
            }
            write!(f, "]\n")?;
        }

        write!(f, "String backtrack\n")?;

        let num_rows = self.data.len() / self.row_length;

        for row_id in 0..num_rows {
            write!(f, "[")?;
            for col_id in 0..self.row_length {
                write!(f, "{: >3}", self.back_str_idx(row_id, col_id))?;
            }
            write!(f, "]\n")?;
        }

        write!(f, "Graph backtrack\n")?;

        let num_rows = self.data.len() / self.row_length;

        for row_id in 0..num_rows {
            write!(f, "[")?;
            for col_id in 0..self.row_length {
                write!(f, "{: >3}", self.back_graph_idx(row_id, col_id))?;
            }
            write!(f, "]\n")?;
        }

        Ok(())
    }
}

impl DynamicProgrammingData {
    fn new<'a, T: Debug + Eq>(
        gap_score: i64,
        graph: &POAGraph<'a, T>,
        n_nodes: usize,
        seq_len: usize,
    ) -> Self {
        // Create matrix and fill with default values
        let mut data = Vec::with_capacity((n_nodes + 1) * (seq_len + 1));
        data.resize_with(
            (n_nodes + 1) * (seq_len + 1),
            DynamicProgrammingScore::default,
        );

        // Mapping of node handles to idx in dynamic programming matrix
        let mut node_handle_to_idx: HashMap<NodeHandle, usize> = HashMap::new();
        for (idx, node_handle) in graph.toposort().iter().enumerate() {
            node_handle_to_idx.insert(*node_handle, idx + 1);
        }

        let mut dp_data = DynamicProgrammingData {
            row_length: (seq_len + 1),
            node_handle_to_idx,
            data,
        };

        // Initialize scores
        for (score, seq_id) in (0..(seq_len + 1)).enumerate() {
            dp_data.set_score(0, seq_id, score as i64 * gap_score);
        }

        for (idx, node_handle) in graph.toposort().iter().enumerate() {
            let node = graph.get_node(&node_handle);
            let mut prev_idxs = dp_data.prev_indexes(node).into_iter();
            let mut best = dp_data.score(prev_idxs.next().unwrap(), 0);
            for prev_idx in prev_idxs {
                best = cmp::max(best, dp_data.score(prev_idx, 0));
            }
            dp_data.set_score(idx + 1, 0, best + gap_score)
        }

        dp_data
    }

    fn score(&self, i: usize, j: usize) -> i64 {
        self.data[i * self.row_length + j].score
    }

    fn back_graph_idx(&self, i: usize, j: usize) -> usize {
        self.data[i * self.row_length + j].back_graph_idx
    }

    fn back_str_idx(&self, i: usize, j: usize) -> usize {
        self.data[i * self.row_length + j].back_str_idx
    }

    fn prev_indexes<'a, T>(&self, node: &Node<'a, T>) -> Vec<usize> {
        let mut prev = Vec::new();

        for in_edge in &node.in_edges {
            prev.push(*self.node_handle_to_idx.get(&in_edge.from).unwrap());
        }

        // Return 0 by default
        if prev.len() == 0 {
            return vec![0];
        }

        prev
    }

    fn set_score(&mut self, i: usize, j: usize, score: i64) {
        self.data[i * self.row_length + j].score = score;
    }

    fn set_back_graph_idx(&mut self, i: usize, j: usize, prev: usize) {
        self.data[i * self.row_length + j].back_graph_idx = prev;
    }

    fn set_back_str_idx(&mut self, i: usize, j: usize, prev: usize) {
        self.data[i * self.row_length + j].back_str_idx = prev;
    }
}

struct SequenceAlignment {
    alignment: Vec<(Option<usize>, Option<NodeHandle>)>,
}

impl Debug for SequenceAlignment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (str_idx, node_idx) in &self.alignment {
            write!(f, "(")?;

            match str_idx {
                Some(value) => write!(f, "{}", value)?,
                None => write!(f, "None")?,
            };

            write!(f, ", ")?;

            match node_idx {
                Some(value) => write!(f, "{:?}", value)?,
                None => write!(f, "None")?,
            };

            write!(f, ") ")?;
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum Candidate {
    INS,
    DEL,
    MATCH,
}

impl Ord for Candidate {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd for Candidate {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        let self_order = match self {
            Candidate::INS => 2,
            Candidate::DEL => 1,
            Candidate::MATCH => 3,
        };

        let other_order = match other {
            Candidate::INS => 2,
            Candidate::DEL => 1,
            Candidate::MATCH => 3,
        };

        self_order.partial_cmp(&other_order)
    }
}

fn matchscore<T: Eq>(c1: T, c2: T) -> i64 {
    if c1 == c2 {
        return 1;
    } else {
        return -1;
    }
}

impl SequenceAlignment {
    fn new<'a, T: Eq + Debug>(sequence: &[T], graph: &POAGraph<'a, T>) -> Self {
        println!("Creating Sequencealignment");

        let gap = -1;

        let mut dp = DynamicProgrammingData::new(gap, graph, graph.n_nodes(), sequence.len());

        let mut candidates: Vec<(i64, usize, usize, Candidate)> = Vec::new();

        // Go over topologically-sorted graph
        for (i, handle) in graph.toposort().iter().enumerate() {
            let node = graph.get_node(handle);
            let pbase = node.base;
            for (j, sbase) in sequence.iter().enumerate() {
                candidates.clear();

                // Adding gap
                candidates.push((dp.score(i + 1, j) + gap, i + 1, j, Candidate::INS));

                // Iterate over previous ids
                for prev_index in dp.prev_indexes(node) {
                    candidates.push((
                        dp.score(prev_index, j + 1) + gap,
                        prev_index,
                        j + 1,
                        Candidate::DEL,
                    ));
                    candidates.push((
                        dp.score(prev_index, j) + matchscore(sbase, pbase),
                        prev_index,
                        j,
                        Candidate::MATCH,
                    ))
                }

                let best_candidate = candidates
                    .iter()
                    .max_by_key(|x| (x.0, x.1, x.2, x.3))
                    .unwrap();

                dp.set_score(i + 1, j + 1, best_candidate.0);
                dp.set_back_graph_idx(i + 1, j + 1, best_candidate.1);
                dp.set_back_str_idx(i + 1, j + 1, best_candidate.2);
            }
        }

        println!("{:?}", dp);

        // Backtrack
        let mut best_i = graph.n_nodes();
        let mut best_j = sequence.len();

        println!("best_i: {best_i}, best_j: {best_j}");

        let mut terminal_indices = Vec::new();
        for (idx, handle) in graph.toposort().iter().enumerate() {
            if graph.get_node(handle).out_edges.len() == 0 {
                terminal_indices.push(idx);
            }
        }

        // Find best i and j
        let mut terminal_indices = terminal_indices.into_iter();
        best_i = terminal_indices.next().unwrap() + 1;
        let mut bestscore = dp.score(best_i, best_j);
        for terminal_idx in terminal_indices {
            let score = dp.score(terminal_idx + 1, best_j);
            if score > bestscore {
                bestscore = score;
                best_i = terminal_idx + 1;
            }
        }

        println!("best_i: {best_i}, best_j: {best_j}");

        let node_handles = graph.toposort();

        // println!(node_handles);

        let mut alignment = Vec::new();

        while best_i > 0 || best_j > 0 {
            println!("-------------");
            let next_i = dp.back_graph_idx(best_i, best_j);
            let next_j = dp.back_str_idx(best_i, best_j);
            println!("best_i: {best_i}, best_j: {best_j}");
            println!("next_i: {next_i}, next_j: {next_j}");

            let curstridx = best_j - 1;
            let curnodeidx = node_handles[best_i - 1];

            let mut curstridx_insert = Some(curstridx);
            let mut curnodeidx_insert = Some(curnodeidx);

            if next_j == best_j {
                curstridx_insert = None;
            }

            if next_i == best_i {
                curnodeidx_insert = None;
            }

            println!("Adding alignment {curstridx_insert:?} {curnodeidx_insert:?}");
            alignment.insert(0, (curstridx_insert, curnodeidx_insert));

            best_i = next_i;
            best_j = next_j;
        }

        let result = SequenceAlignment { alignment };

        println!("Alignment: {result:?}");

        result
    }
}

pub fn align<'a, T: Hash + Eq + Clone + Debug>(seqs: &[&'a [T]]) -> Vec<&'a T> {
    let mut graph: POAGraph<T> = POAGraph::new();

    let mut seqs = seqs.iter();

    let first = seqs.next().unwrap();

    graph.add_unmatched_seq(first);

    for seq in seqs {
        let alignment = SequenceAlignment::new(seq, &graph);
        graph.incorporate_seq_alignment(alignment, seq);
    }

    vec![]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        // sequences = ["ACED", "BCD", "AD"]
        let seq1 = vec!["A", "C", "E", "D"];
        let seq2 = vec!["B", "C", "D"];
        let seq3 = vec!["A", "D"];

        // let seq1 = vec![28, 29, 2, 69, 63, 70, 30, 82, 31, 81, 3];
        // let seq2 = vec![28, 68, 67, 29, 66, 65, 64, 2, 3];
        // let seq3 = vec![28, 68, 67, 29, 66, 65, 64, 2, 30, 3];
        // let seq4 = vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 31, 3];
        let result = align(&[&seq1, &seq2, &seq3]);
        println!("");
        println!("Result: {:?}", result);
    }
}
