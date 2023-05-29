use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
};

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

pub trait PartitionedTable<K, V> {
    fn get_partition(&self, index: usize) -> Option<Box<dyn Iterator<Item = (K, V)>>>;
}

pub fn hmjoin3<K, V1, V2, V3, I1, I2, I3>(
    iter1: I1,
    iter2: I2,
    iter3: I3,
) -> HashMap<K, (Vec<V1>, Vec<V2>, Vec<V3>)>
where
    I1: Iterator<Item = (K, V1)>,
    I2: Iterator<Item = (K, V2)>,
    I3: Iterator<Item = (K, V3)>,
    K: Hash + Eq,
{
    use std::collections::hash_map::Entry::*;
    let mut result: HashMap<K, (Vec<V1>, Vec<V2>, Vec<V3>)> = HashMap::new();

    for (key, value) in iter1 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().0.push(value),
            Vacant(entry) => {
                entry.insert((vec![value], vec![], vec![]));
            }
        }
    }

    for (key, value) in iter2 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().1.push(value),
            Vacant(entry) => {
                entry.insert((vec![], vec![value], vec![]));
            }
        }
    }

    for (key, value) in iter3 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().2.push(value),
            Vacant(entry) => {
                entry.insert((vec![], vec![], vec![value]));
            }
        }
    }
    result
}

pub struct Join3<'r, K, V1, V2, V3>
where
    K: Hash + DeserializeOwned,
    V1: DeserializeOwned,
    V2: DeserializeOwned,
    V3: DeserializeOwned,
{
    reader1: &'r Box<dyn PartitionedTable<K, V1>>,
    reader2: &'r Box<dyn PartitionedTable<K, V2>>,
    reader3: &'r Box<dyn PartitionedTable<K, V3>>,
    current_data: hash_map::IntoIter<K, (Vec<V1>, Vec<V2>, Vec<V3>)>,
    current_partition: usize,
}

impl<'r, K, V1, V2, V3> Iterator for Join3<'r, K, V1, V2, V3>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    V3: Serialize + DeserializeOwned,
    K: Hash + Eq + Clone + DeserializeOwned,
{
    type Item = (K, (Vec<V1>, Vec<V2>, Vec<V3>));

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

            let Some(partition3) = self
                .reader3
                .get_partition(self.current_partition) else {
                    return None
                };

            let mut joined = hmjoin3(partition1, partition2, partition3).into_iter();

            let Some(next_value) = joined.next() else {
                continue;
            };

            self.current_data = joined;
            return Some(next_value);
        }
    }
}

/// Join two tables by given key out-of-memory
/// Can be used for extremely large tales
pub fn join3<'r, K, V1, V2, V3>(
    reader1: &'r Box<dyn PartitionedTable<K, V1>>,
    reader2: &'r Box<dyn PartitionedTable<K, V2>>,
    reader3: &'r Box<dyn PartitionedTable<K, V3>>,
) -> Result<Join3<'r, K, V1, V2, V3>>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    V3: Serialize + DeserializeOwned,
    K: Hash + Eq + Clone + DeserializeOwned,
{
    let partition1 = reader1.get_partition(0).unwrap();
    let partition2 = reader2.get_partition(0).unwrap();
    let partition3 = reader3.get_partition(0).unwrap();
    let joined = hmjoin3(partition1, partition2, partition3).into_iter();
    Ok(Join3 {
        reader1,
        reader2,
        reader3,
        current_data: joined,
        current_partition: 0,
    })
}
