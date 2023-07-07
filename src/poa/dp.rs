use std::{collections::HashMap, fmt::Debug};

use itertools::Itertools;

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

/// Directed-acyclic-graph for representation of partial order sequences
pub trait Dag<T> {
    /// Get roots of dag, returns ids of elements
    fn roots(&self) -> Vec<usize>;

    /// Get number of nodes
    fn len(&self) -> usize;

    /// Get leafs of a dag, returns ids of elements
    fn leafs(&self) -> Vec<usize>;

    fn get_value(&self, element: usize) -> &T;

    /// Get next eleents for specific element
    fn next(&self, element: usize) -> Vec<usize>;

    /// Get previous elements for specific element
    fn previous(&self, element: usize) -> Vec<usize>;

    /// Topologically sort elements of dag
    fn toposort(&self) -> Vec<usize>;

    /// Returns 'depth' of all of the nodes
    /// the depth is minimum number of edges we need to visit
    /// to get to the specific node from the any of the root nodes
    fn node_depth(&self) -> Vec<usize> {
        let mut result = Vec::with_capacity(self.len());
        result.resize(self.len(), self.len());

        for element in self.toposort() {
            let new_score = self
                .previous(element)
                .iter()
                .map(|x| result[*x] + 1)
                .min()
                .unwrap_or(0);
            result[element] = new_score;
        }

        result
    }
}

impl<T> Dag<T> for Vec<T> {
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

    fn len(&self) -> usize {
        Vec::len(&self)
    }

    fn get_value(&self, element: usize) -> &T {
        self.get::<usize>(element).unwrap()
    }

    fn toposort(&self) -> Vec<usize> {
        (0..self.len()).collect()
    }
}

/// The same as regular sequence matching
/// but operates over sequences with partial order
pub fn partial_order_sequence_matching<'a, T, D>(
    seq1: &'a D,
    seq2: &'a D,
) -> Vec<(Option<&'a T>, Option<&'a T>)>
where
    T: Eq + PartialEq + Clone + Debug,
    D: Dag<T>,
{
    let m = seq1.len();
    let n = seq2.len();

    let mut dp: HashMap<(usize, usize), i32> = HashMap::new();
    let mut backtrack: HashMap<(usize, usize), (usize, usize)> = HashMap::new();

    let gap_penalty: i32 = -1;

    let mut seq1_depths = seq1.node_depth();
    let mut seq2_depths = seq2.node_depth();

    // Add one extra depth
    seq1_depths.push(seq1.leafs().iter().map(|x| seq1_depths[*x]).max().unwrap() + 1);
    seq2_depths.push(seq2.leafs().iter().map(|x| seq2_depths[*x]).max().unwrap() + 1);

    // Fill starting gap penalties
    for i in seq1.toposort() {}

    // Fill starting gap penalties
    for i in seq1.toposort().iter().chain(&[seq1.len()]) {
        let score = (seq1_depths[*i] as i32) * gap_penalty;
        for root in seq2.roots() {
            dp.insert((*i, root), score);
        }
    }

    for j in seq2.toposort().iter().chain(&[seq2.len()]) {
        let score = (seq2_depths[*j] as i32) * gap_penalty;
        for root in seq1.roots() {
            dp.insert((root, *j), score);
        }
    }

    // Create scan order
    let mut scan_order = Vec::new();
    for i in seq1.toposort() {
        for j in seq2.toposort() {
            scan_order.push((i, j));
        }
    }

    // Launch dynamic profile
    for (i, j) in scan_order {
        let current_pos = (i, j);

        let mut next_positions: Vec<(usize, usize)> = Vec::new();

        let mut seq1_next = seq1.next(i);
        let mut seq2_next = seq2.next(j);

        // Add 'fake' next for leaf nodes
        if seq1_next.len() == 0 {
            seq1_next.push(seq1.len())
        }

        if seq2_next.len() == 0 {
            seq2_next.push(seq2.len())
        }

        for next_i in &seq1_next {
            for next_j in &seq2_next {
                next_positions.push((*next_i, *next_j));
            }
        }

        for (next_i, next_j) in next_positions {
            let next_pos = (next_i, next_j);
            let prev_left = (i, next_j);
            let prev_up = (next_i, j);

            let (pos, score) = if seq1.get_value(i) == seq2.get_value(j) {
                // match
                let current_score = dp.get(&current_pos).unwrap();

                if dp.contains_key(&next_pos) {
                    panic!("Duplicate key {:?}", next_pos)
                };

                (current_pos, current_score + 1)
            } else {
                // gap
                let left = dp.get(&prev_left).unwrap();
                let up = dp.get(&prev_up).unwrap();

                if dp.contains_key(&next_pos) {
                    panic!("Duplicate key {:?}", next_pos)
                };

                // dp.insert(next_pos, std::cmp::max(*left, *up));

                if left > up {
                    (prev_left, *left)
                } else {
                    (prev_up, *up)
                }
            };

            dp.insert(next_pos, score);
            backtrack.insert(next_pos, pos);
        }
    }

    println!("Dynamic profile");
    for i in seq1.toposort() {
        for j in seq2.toposort() {
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
                    Some(seq1.get_value(*prev_i))
                } else {
                    None
                };
                let j_val = if *prev_j != j {
                    Some(seq2.get_value(*prev_j))
                } else {
                    None
                };

                result.push((i_val, j_val));

                i = *prev_i;
                j = *prev_j;
            }
            None => {
                let i_val = seq1.get_value(i);
                let j_val = seq2.get_value(j);
                break;
            }
        }
    }

    result.reverse();

    result
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

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

        // let slices = sequences.iter().map(|x| x.as_slice()).collect_vec();

        // let result = align(&slices);

        // let result = sequence_matching_dynamic_programming(&sequences[0], &sequences[1]);

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
}
