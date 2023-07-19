/// Implementation of index
/// for sequences
///
/// Uses pairs of values from sequences to find
/// all sequences that have at least one matching
/// pair to the given one
///
use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    fmt::Display,
    hash::Hash,
    ops::DerefMut,
};

use by_address::ByAddress;

struct PairsIterator<T, IT>
where
    T: Copy,
    IT: Iterator<Item = T> + Clone,
{
    primary_iterator: IT,
    primary_item: Option<T>,
    secondary_iterator: IT,
}

impl<T, IT> PairsIterator<T, IT>
where
    T: Copy,
    IT: Iterator<Item = T> + Clone,
{
    fn new(iterator: IT) -> Self {
        let mut primary_iterator = iterator;
        let primary_item = primary_iterator.next();
        let secondary_iterator = primary_iterator.clone();
        PairsIterator {
            primary_iterator,
            primary_item,
            secondary_iterator,
        }
    }
}

impl<T, IT> Iterator for PairsIterator<T, IT>
where
    T: Copy,
    IT: Iterator<Item = T> + Clone,
{
    type Item = (T, T);

    fn next(&mut self) -> Option<Self::Item> {
        let secondary_item = match self.secondary_iterator.next() {
            Some(value) => value,
            None => {
                // Secondary iterator exhausted, increment primary iterator
                self.primary_item = match self.primary_iterator.next() {
                    Some(value) => Some(value),
                    None => return None,
                };

                // Reset secondary iterator
                self.secondary_iterator = self.primary_iterator.clone();

                // Return next item from next iteration
                match self.secondary_iterator.next() {
                    Some(value) => value,
                    None => return None,
                }
            }
        };

        // If primary item is none, this means we have received empty iterator
        let Some(primary_item) = self.primary_item else {
            return None;
        };

        Some((primary_item, secondary_item))
    }
}

struct SequenceIndex<T, I>
where
    T: Hash + Eq,
    I: std::ops::Deref,
{
    pairs: HashMap<(T, T), Vec<I>>, // Index that maps all pairs to specific sequence ids
    matching_hs: HashSet<ByAddress<I>>,
}

impl<'a, T, I, IT, V> SequenceIndex<T, I>
where
    T: Hash + Eq + Copy,
    V: std::ops::Deref<Target = T> + Copy,
    I: IntoIterator<Item = V, IntoIter = IT> + Copy + std::ops::Deref,
    IT: Iterator<Item = V> + Clone,
{
    fn new() -> Self {
        SequenceIndex {
            pairs: HashMap::new(),
            matching_hs: HashSet::new(),
        }
    }

    fn add_sequence(&mut self, sequence: I) {
        for (v1, v2) in PairsIterator::new(sequence.into_iter()) {
            let key = (*v1, *v2);

            use std::collections::hash_map::Entry::*;
            match self.pairs.entry(key) {
                Occupied(mut entry) => {
                    entry.get_mut().push(sequence);
                }
                Vacant(entry) => {
                    entry.insert(vec![sequence]);
                }
            };
        }
    }

    fn find_matching<V2, I2, IT2>(&mut self, sequence: I2, target: &mut Vec<I>)
    where
        V2: std::ops::Deref<Target = T> + Copy,
        I2: IntoIterator<Item = V2, IntoIter = IT2>,
        IT2: Iterator<Item = V2> + Clone,
    {
        self.matching_hs.clear();

        for (v1, v2) in PairsIterator::new(sequence.into_iter()) {
            let key: (T, T) = (*v1, *v2);

            let Some(found_seqs) = self.pairs.get(&key) else {
                continue;
            };

            for matching_seq in found_seqs {
                self.matching_hs.insert(ByAddress(*matching_seq));
            }
        }

        for item in &self.matching_hs {
            target.push(**item);
        }
    }
}

#[test]
fn test_sequence_index() {
    // for (v1, v2) in PairsIterator::new([1, 2, 3, 4].iter()) {
    //     println!("Pair: {v1} {v2}");
    // }

    let seq1 = vec![1, 2, 3, 4];
    let seq2 = vec![2, 3, 4, 5];

    let mut seq_idx: SequenceIndex<i32, &Vec<i32>> = SequenceIndex::new();

    seq_idx.add_sequence(&seq1);
    seq_idx.add_sequence(&seq2);

    println!("{:?}", seq_idx.pairs);

    let mut matching = Vec::new();
    seq_idx.find_matching(&[3, 4], &mut matching);
    println!("Matching: {:?}", matching);
}
