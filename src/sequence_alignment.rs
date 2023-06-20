// A B C D E D

// A   C   E D
//   B C D
// A     D

// A C E D
// B C D
// A D

use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
};

use itertools::Itertools;

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct Offsets {
    next_positions: Vec<usize>,
}

impl PartialOrd for Offsets {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        use std::cmp::Ordering::*;
        match self.next_positions.partial_cmp(&other.next_positions) {
            Some(value) => Some(match value {
                Less => Greater,
                Equal => Equal,
                Greater => Less,
            }),
            None => None,
        }

        // self.next_positions
        //     .iter()
        //     .sum::<usize>()
        //     .partial_cmp(&other.next_positions.iter().sum::<usize>())
    }
}

impl Ord for Offsets {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl Offsets {
    fn with_len(len: usize) -> Self {
        let mut next_positions = Vec::with_capacity(len);
        for _ in 0..len {
            next_positions.push(0);
        }
        Offsets { next_positions }
    }

    fn get_current<'a, T>(&self, seqs: &'a [&'a [T]]) -> Option<&'a T> {
        for (seq_i, next_pos) in self.next_positions.iter().enumerate() {
            if *next_pos == 0 {
                continue;
            }
            let cur_pos = next_pos - 1;
            return Some(&seqs[seq_i][cur_pos]);
        }
        return None;
    }

    // Get all possible next profiles from a sequence
    fn iter_next<'a, T>(&self, seqs: &'a [&'a [T]]) -> Vec<Option<&'a T>> {
        let mut items = Vec::new();
        for (next_pos, seq) in self.next_positions.iter().zip(seqs.iter()) {
            if *next_pos < seq.len() {
                items.push(Some(&seq[*next_pos]));
            } else {
                items.push(None);
            }
        }
        items
    }

    fn is_finished<T>(&self, seqs: &[&[T]]) -> bool {
        for (next_pos, seq) in self.next_positions.iter().zip(seqs.iter()) {
            if *next_pos < seq.len() {
                return false;
            }
        }
        true
    }

    // Genreate all possible next profiles from the current one
    fn all_possible_next<'a, 'b, T: Hash + Eq + Clone + Debug>(
        &self,
        seqs: &'b [&[T]],
        next_items: &'a mut Vec<Offsets>,
        next_possible_items: &'a mut HashSet<&'b T>,
        next_ids: &'a mut Vec<usize>,
    ) {
        next_items.clear();
        next_ids.clear();
        next_possible_items.clear();
        for item in self.iter_next(seqs) {
            let Some(item) = item else {
                continue
            };
            next_possible_items.insert(item);
        }

        // Do not extend twice the same stop
        if let Some(current) = self.get_current(seqs) {
            next_possible_items.remove(current);
        }

        for next_item in next_possible_items.iter() {
            // Get all possible ids that can be increased
            let next_ids_iter = self
                .iter_next(seqs)
                .into_iter()
                .enumerate()
                .filter(|(_, elem)| *elem == Some(*next_item))
                .map(|(pos, _)| pos);

            next_ids.clear();
            next_ids.extend(next_ids_iter);

            // Push increment of all items
            let mut next_item = Offsets {
                next_positions: self.next_positions.clone(),
            };
            for inc_item in next_ids.iter() {
                next_item.next_positions[*inc_item] += 1;
            }
            next_items.push(next_item);

            if next_ids.len() == 1 {
                continue;
            }

            for comb in next_ids.iter().combinations(next_ids.len() - 1) {
                let mut next_item = Offsets {
                    next_positions: self.next_positions.clone(),
                };

                for inc_item in &comb {
                    next_item.next_positions[**inc_item] += 1;
                }

                next_items.push(next_item);
            }
        }
    }
}

#[derive(Clone, Debug)]
struct BacktrackInfo {
    total_len: usize,
    source: Option<Offsets>,
}

fn get_finished<'a, T>(
    profiles: &'a HashMap<Offsets, BacktrackInfo>,
    seqs: &[&[T]],
) -> &'a Offsets {
    // Find all finished profiles
    let mut result: Vec<&Offsets> = Vec::new();
    for (item, _) in profiles {
        if item.is_finished(seqs) {
            result.push(item);
        }
    }

    println!("Found {} solutions", result.len());

    // Take some element as a final result. We're guaranteed to have at least one
    // result ant this point
    let result: &Offsets = *result.first().unwrap();

    result
}

fn backtrack_full_path<'a, T>(
    profiles: &'a HashMap<Offsets, BacktrackInfo>,
    seqs: &[&[T]],
) -> Vec<&'a Offsets> {
    let mut result = Vec::new();

    let mut current_item = get_finished(profiles, seqs);

    loop {
        result.push(current_item);

        // Update next current item (Will be None on last iteration)
        let Some(value) = profiles.get(current_item).unwrap().source.as_ref() else {
            break;
        };

        current_item = value;
    }

    result.reverse();

    result
}

fn backtrack_letters<'a, T: Debug + Hash + Eq>(
    result: &[&Offsets],
    seqs: &[&'a [T]],
) -> Vec<&'a T> {
    let mut letters_result: Vec<&T> = Vec::new();
    for (prev, next) in result.iter().tuple_windows() {
        let mut letter: Option<&T> = None;
        for (seq, (prev_position, next_position)) in seqs
            .iter()
            .zip(prev.next_positions.iter().zip(next.next_positions.iter()))
        {
            if prev_position != next_position {
                let cur_letter = &seq[*prev_position];
                if let Some(letter) = letter {
                    if letter != cur_letter {
                        panic!(
                            "Profile positions delta is invalid. Expected one unique element, got {:?} {:?}",
                            letter, cur_letter
                        )
                    }
                } else {
                    letter = Some(cur_letter);
                }
            }
        }
        letters_result.push(letter.expect("Invalid positions delta. No delta found"));
    }
    letters_result
}

fn total_len<'a>(mut offsets: &'a Offsets, profiles: &'a HashMap<Offsets, BacktrackInfo>) -> usize {
    let mut total_len: usize = 0;

    loop {
        total_len += 1;

        let backtrack = profiles.get(offsets).unwrap();

        let Some(source) = &backtrack.source else {
            break;
        };

        offsets = source;
    }

    total_len
}

/// Alignes multiple sequences into one
pub fn align<'a, T: Hash + Eq + Clone + Debug>(seqs: &[&'a [T]]) -> Vec<&'a T> {
    let start_offset = Offsets::with_len(seqs.len());

    let mut profiles: HashMap<Offsets, BacktrackInfo> = HashMap::from([(
        start_offset.clone(),
        BacktrackInfo {
            total_len: 0,
            source: None,
        },
    )]);

    // We store offsets in binary heap
    // Because in dynamic programming we require
    // specific order of item processing
    let mut offsets_heap: BinaryHeap<Offsets> = BinaryHeap::new();
    offsets_heap.push(start_offset.clone());

    // Dynamic programming loop
    let mut counter = 0;

    let mut next_items = Vec::new();
    let mut next_possible_items = HashSet::new();
    let mut next_ids = Vec::new();

    loop {
        counter += 1;

        let Some(prev) = offsets_heap.pop() else {
            break;
        };

        let new_total_len = profiles.get(&prev).unwrap().total_len + 1;

        // Populate next items with all possible items
        prev.all_possible_next(
            seqs,
            &mut next_items,
            &mut next_possible_items,
            &mut next_ids,
        );

        for next in &next_items {
            let Some(backtrack) = profiles.get_mut(&next) else {
                profiles.insert(next.clone(), BacktrackInfo { total_len: new_total_len, source: Some(prev.clone()) });
                offsets_heap.push(next.clone());
                continue;
            };

            if backtrack.total_len > new_total_len {
                backtrack.total_len = new_total_len;
                backtrack.source = Some(prev.clone());
            }
        }
    }

    println!("Number of iterations: {}", counter);

    let recovered = backtrack_full_path(&profiles, seqs);

    let aligned = backtrack_letters(&recovered, seqs);

    println!("Aligned length: {}", aligned.len());

    aligned
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::align;

    #[test]
    fn it_works() {
        // A B C D E D

        // A   C E D
        //   B C   D
        // A       D

        //   A C E D
        // B   C   D
        //   A     D

        // A   C   E D
        //   B C D
        // A     D

        // A C E D
        // B C D
        // A D

        // let seq1 = vec![28, 29, 2, 69, 63, 70, 30, 82, 31, 81, 3];
        // let seq2 = vec![28, 68, 67, 29, 66, 65, 64, 2, 3];
        // let seq3 = vec![28, 68, 67, 29, 66, 65, 64, 2, 30, 3];
        // let seq4 = vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 31, 3];
        // let seq5 = vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 82, 31, 81, 3];
        // let seq6 = vec![28, 68, 67, 66, 65, 64, 2, 30];

        // let seq1 = vec!["A", "C", "E", "D"];
        // let seq2 = vec!["B", "C", "D"];
        // let seq3 = vec!["A", "D"];
        // let seq4 = vec!["A", "B", "C"];

        let sequences = vec![
            vec![28, 29, 2, 69, 63, 70, 30, 82, 31, 81, 3],
            vec![28, 68, 67, 29, 66, 65, 64, 2, 3],
            vec![28, 68, 67, 29, 66, 65, 64, 2, 30, 3],
            vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 31, 3],
            vec![28, 68, 67, 29, 66, 65, 64, 2, 69, 63, 70, 30, 82, 31, 81, 3],
            vec![28, 68, 67, 66, 65, 64, 2, 30],
        ];

        // A B B A A A B B A B A B B B B
        // A - B A - A - B A B A B - - B
        // A B B A A - B B - - A B B B B
        // A - - A A A B B A B A B B - -
        // - B B - A A - B A B - - B B B

        // ABAABABABB
        // ABBAABBABBBB
        // AAAABBABABB
        // BBAABABBBB

        // let sequences = vec!["CD", "DABC", "DEF", "CEF"]
        //     .iter()
        //     .map(|x| x.chars().collect_vec())
        //     .collect_vec();

        // A B B A A A B B A B A B B B B

        // A B B A A B A B B A B B A B B

        // A A B B A A B B A B B A B B
        // A   B   A A B   A B   A B B
        // A   B B A A B B A B B   B B
        // A A     A A B B A B   A B B
        //     B B A A B   A B B   B B

        // let sequences = vec!["ABAABABABB", "ABBAABBABBBB", "AAAABBABABB", "BBAABABBBB"]
        //     .iter()
        //     .map(|x| x.chars().collect_vec())
        //     .collect_vec();

        let slices = sequences.iter().map(|x| x.as_slice()).collect_vec();

        let result = align(&slices);

        println!("Result: {:?}", result);
    }
}
