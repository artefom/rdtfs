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
    fmt::{Debug, Display},
    hash::Hash,
    ops::DerefMut,
};

use by_address::ByAddress;
use rand::seq;

use crate::gtfs::Trip;

pub struct SequenceIndex<T, I>
where
    T: Hash + Eq,
    I: std::ops::Deref,
{
    pub pairs: HashMap<(T, T, T), Vec<I>>, // Index that maps all pairs to specific sequence ids
    matching_hs: HashSet<ByAddress<I>>,

    val_store: Vec<T>,
    sub_seqs_store: Vec<(T, T, T)>,
}

impl<'a, T, I, IT, V> SequenceIndex<T, I>
where
    T: Hash + Eq + Copy,
    V: std::ops::Deref<Target = T>,
    I: IntoIterator<Item = V, IntoIter = IT> + Copy + std::ops::Deref,
    IT: Iterator<Item = V> + Clone,
{
    pub fn new() -> Self {
        SequenceIndex {
            pairs: HashMap::new(),
            matching_hs: HashSet::new(),
            val_store: Vec::new(),
            sub_seqs_store: Vec::new(),
        }
    }

    pub fn add_sequence(&mut self, sequence: I) {
        self.val_store.clear();
        self.sub_seqs_store.clear();

        for item in sequence {
            self.val_store.push(*item)
        }

        get_all_triplets(&self.val_store, &mut self.sub_seqs_store, 1);

        for (v1, v2, v3) in &self.sub_seqs_store {
            let key = (*v1, *v2, *v3);

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

    pub fn find_matching<V2, I2, IT2>(&mut self, sequence: I2, target: &mut Vec<I>)
    where
        V2: std::ops::Deref<Target = T> + Copy,
        I2: IntoIterator<Item = V2, IntoIter = IT2>,
        IT2: Iterator<Item = V2> + Clone,
    {
        self.matching_hs.clear();
        self.val_store.clear();
        self.sub_seqs_store.clear();

        for item in sequence {
            self.val_store.push(*item)
        }

        get_all_triplets(&self.val_store, &mut self.sub_seqs_store, 1);

        for (v1, v2, v3) in &self.sub_seqs_store {
            let key = (*v1, *v2, *v3);

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

use std::cmp;

fn get_all_triplets<'a, T>(seq: &'a [T], out: &mut Vec<(T, T, T)>, max_skips: usize)
where
    T: Hash + Eq + PartialEq + Copy,
{
    for first in 0..seq.len() {
        let first_val = seq[first];
        for second in first + 1..cmp::min(first + 2 + max_skips, seq.len()) {
            let second_val = seq[second];
            for third in second + 1..cmp::min(second + 2 + max_skips, seq.len()) {
                out.push((first_val, second_val, seq[third]));
            }
        }
    }
}

#[test]
fn test_triplets_iterator() {
    let seq1 = vec![1, 2, 3, 4, 5];

    let mut triplets = Vec::new();

    get_all_triplets(&seq1, &mut triplets, 1);

    println!("All triplets: {triplets:?}");
}

#[test]
fn test_sequence_index() {
    // for (v1, v2) in PairsIterator::new([1, 2, 3, 4].iter()) {
    //     println!("Pair: {v1} {v2}");
    // }

    let seq1 = vec![1, 2, 3, 4];
    let seq2 = vec![2, 3, 4, 5];

    let mut seq_idx = SequenceIndex::new();

    seq_idx.add_sequence(&seq1);
    seq_idx.add_sequence(&seq2);

    println!("{:?}", seq_idx.pairs);

    let mut matching = Vec::new();
    seq_idx.find_matching(&[3, 4, 5], &mut matching);
    println!("Matching: {:?}", matching);
}
