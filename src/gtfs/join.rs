use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
    iter,
    marker::PhantomData,
};

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

pub trait PartitionedTable<K, V> {
    fn get_partition(&self, index: usize) -> Option<Box<dyn Iterator<Item = (K, V)>>>;
}

pub struct EmptyPartitionedTable<K, V> {
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K, V> EmptyPartitionedTable<K, V> {
    pub fn new() -> Self {
        EmptyPartitionedTable {
            _key: PhantomData,
            _value: PhantomData,
        }
    }
}

impl<K, V> Default for EmptyPartitionedTable<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K: 'static, V: 'static> PartitionedTable<K, V> for EmptyPartitionedTable<K, V> {
    fn get_partition(&self, index: usize) -> Option<Box<dyn Iterator<Item = (K, V)>>> {
        Some(Box::new(iter::empty()))
    }
}

pub fn hmjoin4<K, V1, V2, V3, V4, I1, I2, I3, I4>(
    iter1: I1,
    iter2: I2,
    iter3: I3,
    iter4: I4,
) -> HashMap<K, (Vec<V1>, Vec<V2>, Vec<V3>, Vec<V4>)>
where
    I1: Iterator<Item = (K, V1)>,
    I2: Iterator<Item = (K, V2)>,
    I3: Iterator<Item = (K, V3)>,
    I4: Iterator<Item = (K, V4)>,
    K: Hash + Eq,
{
    use std::collections::hash_map::Entry::*;
    let mut result: HashMap<K, (Vec<V1>, Vec<V2>, Vec<V3>, Vec<V4>)> = HashMap::new();

    for (key, value) in iter1 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().0.push(value),
            Vacant(entry) => {
                entry.insert((vec![value], vec![], vec![], vec![]));
            }
        }
    }

    for (key, value) in iter2 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().1.push(value),
            Vacant(entry) => {
                entry.insert((vec![], vec![value], vec![], vec![]));
            }
        }
    }

    for (key, value) in iter3 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().2.push(value),
            Vacant(entry) => {
                entry.insert((vec![], vec![], vec![value], vec![]));
            }
        }
    }

    for (key, value) in iter4 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().3.push(value),
            Vacant(entry) => {
                entry.insert((vec![], vec![], vec![], vec![value]));
            }
        }
    }

    result
}

pub struct Join4<'r, K, V1, V2, V3, V4>
where
    K: Hash + DeserializeOwned,
    V1: DeserializeOwned,
    V2: DeserializeOwned,
    V3: DeserializeOwned,
    V4: DeserializeOwned,
{
    reader1: &'r Box<dyn PartitionedTable<K, V1>>,
    reader2: &'r Box<dyn PartitionedTable<K, V2>>,
    reader3: &'r Box<dyn PartitionedTable<K, V3>>,
    reader4: &'r Box<dyn PartitionedTable<K, V4>>,
    current_data: hash_map::IntoIter<K, (Vec<V1>, Vec<V2>, Vec<V3>, Vec<V4>)>,
    current_partition: usize,
}

impl<'r, K, V1, V2, V3, V4> Iterator for Join4<'r, K, V1, V2, V3, V4>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    V3: Serialize + DeserializeOwned,
    V4: Serialize + DeserializeOwned,
    K: Hash + Eq + Clone + DeserializeOwned,
{
    type Item = (K, (Vec<V1>, Vec<V2>, Vec<V3>, Vec<V4>));

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

            let Some(partition4) = self
                .reader4
                .get_partition(self.current_partition) else {
                    return None
                };

            let mut joined = hmjoin4(partition1, partition2, partition3, partition4).into_iter();

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
pub fn join4<'r, K, V1, V2, V3, V4>(
    reader1: &'r Box<dyn PartitionedTable<K, V1>>,
    reader2: &'r Box<dyn PartitionedTable<K, V2>>,
    reader3: &'r Box<dyn PartitionedTable<K, V3>>,
    reader4: &'r Box<dyn PartitionedTable<K, V4>>,
) -> Result<Join4<'r, K, V1, V2, V3, V4>>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    V3: Serialize + DeserializeOwned,
    V4: Serialize + DeserializeOwned,
    K: Hash + Eq + Clone + DeserializeOwned,
{
    let partition1 = reader1.get_partition(0).unwrap();
    let partition2 = reader2.get_partition(0).unwrap();
    let partition3 = reader3.get_partition(0).unwrap();
    let partition4 = reader4.get_partition(0).unwrap();
    let joined = hmjoin4(partition1, partition2, partition3, partition4).into_iter();
    Ok(Join4 {
        reader1,
        reader2,
        reader3,
        reader4,
        current_data: joined,
        current_partition: 0,
    })
}
