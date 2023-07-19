use std::{collections::HashSet, marker::PhantomData};

use itertools::Itertools;
use ordered_float::OrderedFloat;

trait Cluster<T> {
    fn new() -> Self;
    fn add_one(&mut self, element: &T);
    fn similarity(&mut self, item: &T) -> OrderedFloat<f64>;
}

trait Dataset<T, C> {
    fn len(&self) -> usize;

    fn get(&self, index: usize) -> Option<&T>;

    // Effecient function to get candidates for calculating distances
    fn distance_candidates(
        &self,
        item: &C,
        remaining_items: &Vec<usize>,
        result: &mut HashSet<usize>,
    );
}

struct Clustering<'a, D: Dataset<T, C>, C: Cluster<T>, T> {
    remaining_items: Vec<usize>,
    distance_candidates: HashSet<usize>,
    clusters: Vec<C>,
    dataset: &'a D,
    phantom: PhantomData<T>,
    similarity_threshold: OrderedFloat<f64>,
}

use rand::seq::SliceRandom;
use rand::thread_rng;

impl<'a, D: Dataset<T, C>, C: Cluster<T>, T> Clustering<'a, D, C, T> {
    fn new(dataset: &'a D, similarity_threshold: OrderedFloat<f64>) -> Self {
        let mut remaining_items = (0..dataset.len()).collect_vec();

        // Process items in random order
        remaining_items.shuffle(&mut thread_rng());

        Clustering {
            remaining_items,
            distance_candidates: HashSet::new(),
            clusters: Vec::new(),
            dataset,
            phantom: PhantomData,
            similarity_threshold,
        }
    }
}

impl<'a, D: Dataset<T, C>, C: Cluster<T>, T> Iterator for Clustering<'a, D, C, T> {
    type Item = C;

    fn next(&mut self) -> Option<Self::Item> {
        let Some(random_element) = self.remaining_items.pop() else {
            return None;
        };

        let random_element = self.dataset.get(random_element).unwrap();

        // Create cluster from this element
        let mut cluster: C = C::new();
        cluster.add_one(random_element);

        // Add next elements untill we cannot any more
        loop {
            self.distance_candidates.clear();
            self.dataset.distance_candidates(
                &cluster,
                &self.remaining_items,
                &mut self.distance_candidates,
            );

            let Some((remaining_item_id, similarity)) = self.distance_candidates
                .iter()
                .map(|remaining_item_id| {
                    (
                        remaining_item_id,
                        cluster.similarity(
                            self.dataset
                                .get(*self.remaining_items.get(*remaining_item_id).unwrap())
                                .unwrap(),
                        ),
                    )
                })
                .max_by_key(|(remaining_item_id, similarity)| *similarity) else {
                    break;
                };

            if similarity < self.similarity_threshold {
                break;
            }

            // Remove what we have just added from the remaining items
            let item_id = self.remaining_items.swap_remove(*remaining_item_id);

            let item = self.dataset.get(item_id).unwrap();

            cluster.add_one(item);
        }

        Some(cluster)
    }
}
