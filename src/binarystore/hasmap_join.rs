use std::{collections::HashMap, hash::Hash};

pub fn hashmap_join<K, V1, V2, I1, I2>(iter1: I1, iter2: I2) -> HashMap<K, (Vec<V1>, Vec<V2>)>
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
