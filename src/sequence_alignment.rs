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
        self.next_positions
            .iter()
            .sum::<usize>()
            .partial_cmp(&other.next_positions.iter().sum::<usize>())
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
    fn all_possible_next<T: Hash + Eq + Clone + Debug>(&self, seqs: &[&[T]]) -> Vec<Offsets> {
        let mut next_items = Vec::new();
        let mut next_possible_items: HashSet<&T> = HashSet::new();

        for item in self.iter_next(seqs) {
            let Some(item) = item else {
                continue
            };
            next_possible_items.insert(item);
        }

        for next_item in &next_possible_items {
            let next_ids = self
                .iter_next(seqs)
                .into_iter()
                .enumerate()
                .filter(|(_, elem)| *elem == Some(*next_item))
                .map(|(pos, _)| pos);

            for comb in next_ids.powerset() {
                if comb.len() == 0 {
                    continue;
                }

                let mut next_item = Offsets {
                    next_positions: self.next_positions.clone(),
                };

                let mut comb_hs = HashSet::new();

                for item in &comb {
                    comb_hs.insert(item.clone());
                }

                for inc_item in &comb {
                    next_item.next_positions[*inc_item] += 1;
                }

                next_items.push(next_item);
            }
        }

        next_items
    }
}

#[derive(Clone, Debug)]
struct BacktrackInfo {
    cnt: usize,
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

/// Alignes multiple sequences into one
pub fn align<'a, T: Hash + Eq + Clone + Debug>(seqs: &[&'a [T]]) -> Vec<&'a T> {
    let mut profiles: HashMap<Offsets, BacktrackInfo> = HashMap::from([(
        Offsets::with_len(seqs.len()),
        BacktrackInfo {
            cnt: 1,
            source: None,
        },
    )]);

    // let mut profiles_stack: Vec<Offsets> = Vec::new();

    let mut profiles_stack: BinaryHeap<Offsets> = BinaryHeap::new();

    profiles_stack.push(Offsets::with_len(seqs.len()));

    loop {
        let Some(item) = profiles_stack.pop() else {
            break;
        };

        println!("Processing {:?}", item);

        let path_count = profiles.get(&item).unwrap().cnt.clone();

        if item.is_finished(seqs) {
            break;
        }

        for next_item in item.all_possible_next(seqs) {
            use std::collections::hash_map::Entry::*;
            match profiles.entry(next_item.clone()) {
                Occupied(mut entry) => {
                    entry.get_mut().cnt += path_count;
                }
                Vacant(entry) => {
                    // We found an item that we didn't process before
                    // add it to the stack
                    profiles_stack.push(next_item);

                    let profile_path = BacktrackInfo {
                        cnt: path_count,
                        source: Some(item.clone()),
                    };
                    entry.insert(profile_path);
                }
            }
        }

        println!("Profile len: {}", profiles.len())
    }

    let recovered = backtrack_full_path(&profiles, seqs);

    backtrack_letters(&recovered, seqs)
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

        // let sequences = vec!["CD", "DABC", "DEF", "CEF"]
        //     .iter()
        //     .map(|x| x.chars().collect_vec())
        //     .collect_vec();

        let slices = sequences.iter().map(|x| x.as_slice()).collect_vec();
        let result = align(&slices);

        println!("Result: {:?}", result);
    }
}
