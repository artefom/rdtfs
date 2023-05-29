use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
};

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

pub trait PartitionedTable<K, V> {
    fn get_partition(&self, index: usize) -> Option<Box<dyn Iterator<Item = (K, V)>>>;
}

pub fn hmjoin2<K, V1, V2, I1, I2>(iter1: I1, iter2: I2) -> HashMap<K, (Vec<V1>, Vec<V2>)>
where
    I1: Iterator<Item = (K, V1)>,
    I2: Iterator<Item = (K, V2)>,
    K: Hash + Eq,
{
    use std::collections::hash_map::Entry::*;
    let mut result: HashMap<K, (Vec<V1>, Vec<V2>)> = HashMap::new();

    for (key, value) in iter1 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().0.push(value),
            Vacant(entry) => {
                entry.insert((vec![value], vec![]));
            }
        }
    }

    for (key, value) in iter2 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().1.push(value),
            Vacant(entry) => {
                entry.insert((vec![], vec![value]));
            }
        }
    }

    result
}

pub struct Join2<'r, K, V1, V2>
where
    K: Hash + DeserializeOwned,
    V1: DeserializeOwned,
    V2: DeserializeOwned,
{
    reader1: &'r Box<dyn PartitionedTable<K, V1>>,
    reader2: &'r Box<dyn PartitionedTable<K, V2>>,
    current_data: hash_map::IntoIter<K, (Vec<V1>, Vec<V2>)>,
    current_partition: usize,
}

/// Join two tables by given key out-of-memory
/// Can be used for extremely large tales
pub fn join2<'r, K, V1, V2>(
    reader1: &'r Box<dyn PartitionedTable<K, V1>>,
    reader2: &'r Box<dyn PartitionedTable<K, V2>>,
) -> Result<Join2<'r, K, V1, V2>>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    K: Hash + Eq + Clone + DeserializeOwned,
{
    println!("Getting partitions");
    let partition1 = reader1.get_partition(0).unwrap();
    println!("Getting second partition");
    let partition2 = reader2.get_partition(0).unwrap();

    println!("Getting joined");
    let joined = hmjoin2(partition1, partition2).into_iter();

    Ok(Join2 {
        reader1,
        reader2,
        current_data: joined,
        current_partition: 0,
    })
}

impl<'r, K, V1, V2> Iterator for Join2<'r, K, V1, V2>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    K: Hash + Eq + Clone + DeserializeOwned,
{
    type Item = (K, (Vec<V1>, Vec<V2>));

    fn next(&mut self) -> Option<Self::Item> {
        // Get next value and return if it exists
        match self.current_data.next() {
            Some(value) => return Some(value),
            None => (),
        };

        loop {
            self.current_partition += 1;

            let Some(partition1) = self
                .reader1
                .get_partition(self.current_partition) else {
                    return None
                };

            let Some(partition2) = self
                .reader2
                .get_partition(self.current_partition) else {
                    return None
                };

            let mut joined = hmjoin2(partition1, partition2).into_iter();

            let Some(next_value) = joined.next() else {
                continue;
            };

            self.current_data = joined;
            return Some(next_value);
        }
    }
}
