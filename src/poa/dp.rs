/// Dynamic-programming alignment algorithm
/// for partial-order DAGS
use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
};

use itertools::Itertools;

use super::GraphNode;

/// Matches two sequences using dynamic programming, returns a sequence of matched elements
pub fn sequence_matching_dynamic_programming<'a, T>(
    seq1: &'a [T],
    seq2: &'a [T],
) -> Vec<(Option<&'a T>, Option<&'a T>)>
where
    T: Eq + PartialEq + Clone + Debug,
{
    let m = seq1.len();
    let n = seq2.len();

    let mut dp = HashMap::new();

    // Backtrack
    let mut backtrack: HashMap<(usize, usize), (usize, usize)> = HashMap::new();

    let gap_penalty: i32 = -1;

    // Fill starting gap penalties
    for i in 0..=m {
        dp.insert((i, 0), i as i32 * gap_penalty);
    }

    for j in 0..=n {
        dp.insert((0, j), j as i32 * gap_penalty);
    }

    // Create scan order
    let mut scan_order = Vec::new();
    for i in 0..m {
        for j in 0..n {
            scan_order.push((i, j));
        }
    }

    for (i, j) in scan_order {
        let current_pos = (i, j);

        let next_i = i + 1;
        let next_j = j + 1;

        let next_pos = (next_i, next_j);
        let prev_left = (i, next_j);
        let prev_up = (next_i, j);

        let (pos, score) = if seq1[i] == seq2[j] {
            // match
            let current_score = dp.get(&current_pos).unwrap();

            if dp.contains_key(&next_pos) {
                panic!("Duplicate key!")
            }

            (current_pos, current_score + 1)
        } else {
            // gap
            let left = dp.get(&prev_left).unwrap();
            let up = dp.get(&prev_up).unwrap();

            if dp.contains_key(&next_pos) {
                panic!("Duplicate key!")
            }
            if left > up {
                (prev_left, *left)
            } else {
                (prev_up, *up)
            }
        };

        dp.insert(next_pos, score);
        backtrack.insert(next_pos, pos);
    }

    println!("Resulting Dynamic profile");
    for i in 0..m {
        for j in 0..n {
            let value = match dp.get(&(i, j)) {
                Some(value) => format!("{}", value),
                None => format!("-"),
            };

            print!("{: >4}", value)
        }
        println!()
    }

    // backtrack solution
    let mut i = m;
    let mut j = n;

    let mut result = Vec::new();

    loop {
        match backtrack.get(&(i, j)) {
            Some((prev_i, prev_j)) => {
                let i_val = if *prev_i != i {
                    Some(&seq1[*prev_i])
                } else {
                    None
                };
                let j_val = if *prev_j != j {
                    Some(&seq2[*prev_j])
                } else {
                    None
                };

                result.push((i_val, j_val));

                i = *prev_i;
                j = *prev_j;
            }
            None => {
                let i_val = &seq1[i];
                let j_val = &seq2[j];
                break;
            }
        }
    }

    result.reverse();

    result
}

// TODO: replace this with dfs
fn iter_nodes<T, H, D>(dag: &D) -> Vec<H>
where
    H: Hash + Eq + PartialEq + Clone + Copy,
    D: Dag<T, H>,
{
    let mut nodes: VecDeque<H> = dag.roots().iter().cloned().collect();

    let mut result = Vec::new();

    loop {
        let Some(next_node) = nodes.pop_front() else {
            break
        };

        for next_node in dag.next(next_node.clone()) {
            nodes.push_back(next_node)
        }

        result.push(next_node);
    }
    result
}

/// Directed-acyclic-graph for representation of partial order sequences
/// Where H - node handle
pub trait Dag<T, H>
where
    H: Hash + Eq + PartialEq + Clone + Copy,

    // todo: wtf?
    Self: Sized,
{
    /// Get roots of dag, returns ids of elements
    fn roots(&self) -> Vec<H>;

    /// Get leafs of a dag, returns ids of elements
    fn leafs(&self) -> Vec<H>;

    /// Get base element
    fn base(&self, handle: H) -> &T;

    /// Get next eleents for specific element
    fn next(&self, handle: H) -> Vec<H>;

    /// Get previous elements for specific element
    fn previous(&self, handle: H) -> Vec<H>;

    /// Topologically sort elements of dag
    fn toposort(&self) -> Vec<H> {
        // Gets all nodes with no incoming edges.
        // Let's call them roots
        let mut roots: VecDeque<H> = self.roots().iter().cloned().collect();

        // Number of incomming edges
        let mut incoming_edges_count: HashMap<H, usize> = iter_nodes(self)
            .iter()
            .map(|node| (node.clone(), self.previous(*node).len()))
            .collect();

        // The final topologically sorted list
        let mut sorted: Vec<H> = Vec::new();

        while let Some(node) = roots.pop_front() {
            sorted.push(node);
            for out_node in self.next(node.clone()) {
                let count = incoming_edges_count.get_mut(&out_node).unwrap();
                *count -= 1;
                if *count == 0 {
                    roots.push_back(out_node);
                }
            }
        }

        sorted
    }

    /// Returns 'depth' of all of the nodes
    /// the depth is minimum number of edges we need to visit
    /// to get to the specific node from the any of the root nodes
    fn node_depth(&self) -> HashMap<H, usize> {
        let mut result: HashMap<H, usize> = HashMap::new();

        for element in self.toposort() {
            let new_score = self
                .previous(element)
                .iter()
                .map(|x| result.get(x).unwrap() + 1)
                .min()
                .unwrap_or(0);
            result.insert(element, new_score);
        }

        result
    }
}

impl<T> Dag<T, usize> for Vec<T> {
    fn roots(&self) -> Vec<usize> {
        if self.len() == 0 {
            return vec![];
        }
        vec![0]
    }

    fn leafs(&self) -> Vec<usize> {
        if self.len() == 0 {
            return vec![];
        }
        vec![self.len() - 1]
    }

    fn next(&self, element: usize) -> Vec<usize> {
        if element >= self.len() - 1 {
            return vec![];
        }
        return vec![element + 1];
    }

    fn previous(&self, element: usize) -> Vec<usize> {
        if element == 0 {
            return vec![];
        }

        return vec![element - 1];
    }

    fn base(&self, element: usize) -> &T {
        self.get::<usize>(element).unwrap()
    }
}

struct ReverseDfs<'a, D, H, T>
where
    D: Dag<T, H>,
    H: Hash + Eq + PartialEq + Clone + Copy,
{
    dag: &'a D,
    next_nodes: VecDeque<H>,
    _phantom1: PhantomData<H>,
    _phantom2: PhantomData<T>,
}

impl<'a, D, H, T> ReverseDfs<'a, D, H, T>
where
    D: Dag<T, H>,
    H: Hash + Eq + PartialEq + Clone + Copy,
{
    fn new(dag: &'a D) -> Self {
        let roots = dag.leafs();
        let mut next_nodes = VecDeque::with_capacity(roots.len());

        for node in roots {
            next_nodes.push_back(node)
        }

        ReverseDfs {
            dag,
            next_nodes,
            _phantom1: PhantomData,
            _phantom2: PhantomData,
        }
    }

    fn from_node(dag: &'a D, nodes: Vec<H>) -> Self {
        let mut next_nodes = VecDeque::with_capacity(nodes.len());
        next_nodes.extend(nodes);
        ReverseDfs {
            dag,
            next_nodes,
            _phantom1: PhantomData,
            _phantom2: PhantomData,
        }
    }
}

impl<'a, D, H, T> Iterator for ReverseDfs<'a, D, H, T>
where
    D: Dag<T, H>,
    H: Hash + Eq + PartialEq + Clone + Copy,
{
    type Item = H;

    fn next(&mut self) -> Option<Self::Item> {
        let Some(next_node) = self.next_nodes.pop_front() else {
            return None
        };

        self.next_nodes.extend(self.dag.previous(next_node).iter());

        Some(next_node)
    }
}

/// The same as regular sequence matching
/// but operates over sequences with partial order
pub fn partial_order_sequence_matching<'a, T, H1, H2, D1, D2>(
    seq1: &'a D1,
    seq2: &'a D2,
) -> Vec<(Option<H1>, Option<H2>)>
where
    T: Eq + PartialEq + Clone + Debug,
    H1: Hash + Eq + PartialEq + Debug + Clone + Copy,
    H2: Hash + Eq + PartialEq + Debug + Clone + Copy,
    D1: Dag<T, H1>,
    D2: Dag<T, H2>,
{
    // let m = seq1.len();
    // let n = seq2.len();

    let mut dp: HashMap<(Option<H1>, Option<H2>), i32> = HashMap::new();
    let mut backtrack: HashMap<(Option<H1>, Option<H2>), (Option<H1>, Option<H2>)> = HashMap::new();

    let gap_penalty: i32 = -1;

    let seq1_depths = seq1.node_depth();
    let seq2_depths = seq2.node_depth();

    // Add one extra depth
    let seq1_max_depth = seq1
        .leafs()
        .iter()
        .map(|x| seq1_depths[x])
        .max()
        .unwrap_or(0);
    let seq2_max_depth = seq2
        .leafs()
        .iter()
        .map(|x| seq2_depths[x])
        .max()
        .unwrap_or(0);

    // Fill starting gap penalties
    for i in seq1.toposort() {}

    // Fill starting gap penalties
    for i in seq1.toposort() {
        let score = (*seq1_depths.get(&i).unwrap() as i32) * gap_penalty;
        for root in seq2.roots() {
            dp.insert((Some(i), Some(root)), score);
        }
    }

    for j in seq2.toposort() {
        let score = (*seq2_depths.get(&j).unwrap() as i32) * gap_penalty;
        for root in seq1.roots() {
            dp.insert((Some(root), Some(j)), score);
        }
    }

    // Add 'fake' end nodes
    let score = (seq1_max_depth as i32) * gap_penalty;
    for root in seq2.roots() {
        dp.insert((None, Some(root)), score);
    }
    let score = (seq2_max_depth as i32) * gap_penalty;
    for root in seq1.roots() {
        dp.insert((Some(root), None), score);
    }

    // Create scan order
    let mut scan_order = Vec::new();
    for i in seq1.toposort() {
        for j in seq2.toposort() {
            scan_order.push((Some(i), Some(j)));
        }
    }

    // Launch dynamic profile
    for (i, j) in scan_order {
        let current_pos = (i, j);

        let mut next_positions: Vec<(Option<H1>, Option<H2>)> = Vec::new();

        let mut seq1_next = seq1.next(i.unwrap()).iter().map(|x| Some(*x)).collect_vec();
        let mut seq2_next = seq2.next(j.unwrap()).iter().map(|x| Some(*x)).collect_vec();

        // Add 'fake' next for leaf nodes
        if seq1_next.len() == 0 {
            seq1_next.push(None)
        }

        if seq2_next.len() == 0 {
            seq2_next.push(None)
        }

        for next_i in seq1_next {
            for next_j in &seq2_next {
                next_positions.push((next_i, *next_j));
            }
        }

        for (next_i, next_j) in next_positions {
            let next_pos = (next_i, next_j);
            let prev_left = (i, next_j);
            let prev_up = (next_i, j);

            let (pos, score) = if seq1.base(i.unwrap()) == seq2.base(j.unwrap()) {
                // match
                let current_score = dp.get(&current_pos).unwrap();
                (current_pos, current_score + 1)
            } else {
                // gap
                let left = dp.get(&prev_left).unwrap();
                let up = dp.get(&prev_up).unwrap();

                if left > up {
                    (prev_left, *left)
                } else {
                    (prev_up, *up)
                }
            };

            if let Some(old_score) = dp.get(&next_pos) {
                if score > *old_score {
                    dp.insert(next_pos, score);
                    backtrack.insert(next_pos, pos);
                }
            } else {
                dp.insert(next_pos, score);
                backtrack.insert(next_pos, pos);
            }
        }
    }

    println!("Dynamic profile");
    for i in seq1.toposort() {
        for j in seq2.toposort() {
            let value = match dp.get(&(Some(i), Some(j))) {
                Some(value) => format!("{}", value),
                None => format!("-"),
            };

            print!("{: >4}", value)
        }
        println!()
    }

    // backtrack solution
    let mut i = None;
    let mut j = None;

    let mut result = Vec::new();

    loop {
        let Some((prev_i, prev_j)) = backtrack.get(&(i, j)) else {
            break;
        };

        let i_val = if *prev_i != i {
            Some(prev_i.unwrap())
        } else {
            None
        };

        let j_val = if *prev_j != j {
            Some(prev_j.unwrap())
        } else {
            None
        };

        result.push((i_val, j_val));

        i = *prev_i;
        j = *prev_j;
    }

    if let Some(i) = i {
        for item in ReverseDfs::from_node(seq1, seq1.previous(i)) {
            result.push((Some(item), None));
        }
    } else {
        for item in ReverseDfs::new(seq1) {
            result.push((Some(item), None));
        }
    }

    if let Some(j) = j {
        for item in ReverseDfs::from_node(seq2, seq2.previous(j)) {
            result.push((None, Some(item)));
        }
    } else {
        for item in ReverseDfs::new(seq2) {
            result.push((None, Some(item)));
        }
    }

    result.reverse();

    result
}

#[test]
fn basic() {
    // vec![28,         29,             2, 69, 63, 70, 30, 82, 31, 81, 3],
    // vec![28, 68, 67, 29, 66, 65, 64, 2,                             3],

    let sequences = vec![
        vec![28, 29, 2, 69, 63, 70, 30, 82, 31, 81, 3],
        vec![28, 68, 67, 29, 66, 65, 64, 2, 3],
        vec![28, 68, 67, 29, 66, 65, 64, 2, 30, 3],
        vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 31, 3],
        vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 82, 31, 81, 3],
        vec![28, 68, 67, 66, 65, 64, 2, 30],
    ];

    let result = partial_order_sequence_matching(&sequences[0], &sequences[1]);

    for (item, _) in &result {
        match item {
            Some(value) => print!("{: <3}", value),
            None => print!("{: <3}", '-'),
        }
    }
    println!();

    for (_, item) in &result {
        match item {
            Some(value) => print!("{: <3}", value),
            None => print!("{: <3}", '-'),
        }
    }
    println!();
}
