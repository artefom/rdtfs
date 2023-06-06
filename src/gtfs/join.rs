use std::{
    collections::{hash_map, HashMap},
    hash::Hash,
    iter,
    marker::PhantomData,
};

use serde::{de::DeserializeOwned, Serialize};

pub trait PartitionedTable<K, V> {
    fn get_partition(&self, index: usize) -> Option<Box<dyn Iterator<Item = (K, V)>>>;
    fn len(&self) -> usize;
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
    fn get_partition(&self, _index: usize) -> Option<Box<dyn Iterator<Item = (K, V)>>> {
        Some(Box::new(iter::empty()))
    }

    fn len(&self) -> usize {
        0
    }
}

pub fn hmjoin8<K, V1, V2, V3, V4, V5, V6, V7, V8, I1, I2, I3, I4, I5, I6, I7, I8>(
    iter1: I1,
    iter2: I2,
    iter3: I3,
    iter4: I4,
    iter5: I5,
    iter6: I6,
    iter7: I7,
    iter8: I8,
) -> HashMap<
    K,
    (
        Vec<V1>,
        Vec<V2>,
        Vec<V3>,
        Vec<V4>,
        Vec<V5>,
        Vec<V6>,
        Vec<V7>,
        Vec<V8>,
    ),
>
where
    I1: Iterator<Item = (K, V1)>,
    I2: Iterator<Item = (K, V2)>,
    I3: Iterator<Item = (K, V3)>,
    I4: Iterator<Item = (K, V4)>,
    I5: Iterator<Item = (K, V5)>,
    I6: Iterator<Item = (K, V6)>,
    I7: Iterator<Item = (K, V7)>,
    I8: Iterator<Item = (K, V8)>,
    K: Hash + Eq,
{
    use std::collections::hash_map::Entry::*;
    let mut result: HashMap<
        K,
        (
            Vec<V1>,
            Vec<V2>,
            Vec<V3>,
            Vec<V4>,
            Vec<V5>,
            Vec<V6>,
            Vec<V7>,
            Vec<V8>,
        ),
    > = HashMap::new();

    for (key, value) in iter1 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().0.push(value),
            Vacant(entry) => {
                entry.insert((
                    vec![value],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                ));
            }
        }
    }

    for (key, value) in iter2 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().1.push(value),
            Vacant(entry) => {
                entry.insert((
                    vec![],
                    vec![value],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                ));
            }
        }
    }

    for (key, value) in iter3 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().2.push(value),
            Vacant(entry) => {
                entry.insert((
                    vec![],
                    vec![],
                    vec![value],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                ));
            }
        }
    }

    for (key, value) in iter4 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().3.push(value),
            Vacant(entry) => {
                entry.insert((
                    vec![],
                    vec![],
                    vec![],
                    vec![value],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                ));
            }
        }
    }

    for (key, value) in iter5 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().4.push(value),
            Vacant(entry) => {
                entry.insert((
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![value],
                    vec![],
                    vec![],
                    vec![],
                ));
            }
        }
    }

    for (key, value) in iter6 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().5.push(value),
            Vacant(entry) => {
                entry.insert((
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![value],
                    vec![],
                    vec![],
                ));
            }
        }
    }

    for (key, value) in iter7 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().6.push(value),
            Vacant(entry) => {
                entry.insert((
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![value],
                    vec![],
                ));
            }
        }
    }

    for (key, value) in iter8 {
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().7.push(value),
            Vacant(entry) => {
                entry.insert((
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![],
                    vec![value],
                ));
            }
        }
    }
    result
}

pub struct Join8<'r, K, V1, V2, V3, V4, V5, V6, V7, V8>
where
    K: Hash + DeserializeOwned,
    V1: DeserializeOwned,
    V2: DeserializeOwned,
    V3: DeserializeOwned,
    V4: DeserializeOwned,
    V5: DeserializeOwned,
    V6: DeserializeOwned,
    V7: DeserializeOwned,
    V8: DeserializeOwned,
{
    reader1: &'r Box<dyn PartitionedTable<K, V1>>,
    reader2: &'r Box<dyn PartitionedTable<K, V2>>,
    reader3: &'r Box<dyn PartitionedTable<K, V3>>,
    reader4: &'r Box<dyn PartitionedTable<K, V4>>,
    reader5: &'r Box<dyn PartitionedTable<K, V5>>,
    reader6: &'r Box<dyn PartitionedTable<K, V6>>,
    reader7: &'r Box<dyn PartitionedTable<K, V7>>,
    reader8: &'r Box<dyn PartitionedTable<K, V8>>,
    current_data: hash_map::IntoIter<
        K,
        (
            Vec<V1>,
            Vec<V2>,
            Vec<V3>,
            Vec<V4>,
            Vec<V5>,
            Vec<V6>,
            Vec<V7>,
            Vec<V8>,
        ),
    >,
    current_partition: usize,
}

impl<'r, K, V1, V2, V3, V4, V5, V6, V7, V8> Iterator
    for Join8<'r, K, V1, V2, V3, V4, V5, V6, V7, V8>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    V3: Serialize + DeserializeOwned,
    V4: Serialize + DeserializeOwned,
    V5: Serialize + DeserializeOwned,
    V6: Serialize + DeserializeOwned,
    V7: Serialize + DeserializeOwned,
    V8: Serialize + DeserializeOwned,
    K: Hash + Eq + Clone + DeserializeOwned,
{
    type Item = (
        K,
        (
            Vec<V1>,
            Vec<V2>,
            Vec<V3>,
            Vec<V4>,
            Vec<V5>,
            Vec<V6>,
            Vec<V7>,
            Vec<V8>,
        ),
    );

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

            let Some(partition5) = self
                .reader5
                .get_partition(self.current_partition) else {
                    return None
                };

            let Some(partition6) = self
                .reader6
                .get_partition(self.current_partition) else {
                    return None
                };

            let Some(partition7) = self
                .reader7
                .get_partition(self.current_partition) else {
                    return None
                };

            let Some(partition8) = self
                .reader8
                .get_partition(self.current_partition) else {
                    return None
                };

            let mut joined = hmjoin8(
                partition1, partition2, partition3, partition4, partition5, partition6, partition7,
                partition8,
            )
            .into_iter();

            let Some(next_value) = joined.next() else {
                continue;
            };

            self.current_data = joined;
            return Some(next_value);
        }
    }
}

/// Join tables by given key out-of-memory
/// Can be used for extremely large tales
pub fn join8<'r, K, V1, V2, V3, V4, V5, V6, V7, V8>(
    reader1: &'r Box<dyn PartitionedTable<K, V1>>,
    reader2: &'r Box<dyn PartitionedTable<K, V2>>,
    reader3: &'r Box<dyn PartitionedTable<K, V3>>,
    reader4: &'r Box<dyn PartitionedTable<K, V4>>,
    reader5: &'r Box<dyn PartitionedTable<K, V5>>,
    reader6: &'r Box<dyn PartitionedTable<K, V6>>,
    reader7: &'r Box<dyn PartitionedTable<K, V7>>,
    reader8: &'r Box<dyn PartitionedTable<K, V8>>,
) -> Join8<'r, K, V1, V2, V3, V4, V5, V6, V7, V8>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    V3: Serialize + DeserializeOwned,
    V4: Serialize + DeserializeOwned,
    V5: Serialize + DeserializeOwned,
    V6: Serialize + DeserializeOwned,
    V7: Serialize + DeserializeOwned,
    V8: Serialize + DeserializeOwned,
    K: Hash + Eq + Clone + DeserializeOwned,
{
    let partition1 = reader1.get_partition(0).unwrap();
    let partition2 = reader2.get_partition(0).unwrap();
    let partition3 = reader3.get_partition(0).unwrap();
    let partition4 = reader4.get_partition(0).unwrap();
    let partition5 = reader5.get_partition(0).unwrap();
    let partition6 = reader6.get_partition(0).unwrap();
    let partition7 = reader7.get_partition(0).unwrap();
    let partition8 = reader8.get_partition(0).unwrap();

    let joined = hmjoin8(
        partition1, partition2, partition3, partition4, partition5, partition6, partition7,
        partition8,
    )
    .into_iter();
    Join8 {
        reader1,
        reader2,
        reader3,
        reader4,
        reader5,
        reader6,
        reader7,
        reader8,
        current_data: joined,
        current_partition: 0,
    }
}
