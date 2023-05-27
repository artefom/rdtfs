use std::{collections::HashMap, hash::Hash};

pub fn hashmap_join<K, V1, V2, I1, I2, K1, K2>(
    iter1: I1,
    key1: K1,
    iter2: I2,
    key2: K2,
) -> HashMap<K, (Vec<V1>, Vec<V2>)>
where
    I1: Iterator<Item = V1>,
    I2: Iterator<Item = V2>,
    K: Hash + Eq + Clone,
    K1: Fn(&V1) -> &K,
    K2: Fn(&V2) -> &K,
{
    use std::collections::hash_map::Entry::*;
    let mut result: HashMap<K, (Vec<V1>, Vec<V2>)> = HashMap::new();

    for value in iter1 {
        let key = key1(&value).clone();
        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().0.push(value),
            Vacant(entry) => {
                entry.insert((vec![value], vec![]));
            }
        }
    }

    for value in iter2 {
        let key = key2(&value).clone();

        match result.entry(key) {
            Occupied(mut entry) => entry.get_mut().1.push(value),
            Vacant(entry) => {
                entry.insert((vec![], vec![value]));
            }
        }
    }

    result
}
