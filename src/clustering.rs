use std::{cmp::Ordering, collections::HashMap, fmt::Debug, marker::PhantomData};

use bio::alignment::distance;
use itertools::Itertools;

/// Generic clustering algorithm
/// works by creating cluster for each item and
/// merges them until it cannot any more

trait Cluster<I, D>
where
    D: PartialOrd,
{
    fn new() -> Self;

    fn add_one(&mut self, other: &I);

    fn distance(&self, item: &I) -> D;

    fn item_distance(lhs: &I, rhs: &I) -> D;
}

type ClusterId = usize;
type ItemId = usize;

struct DistanceStorage<I, C, D>
where
    C: Cluster<I, D>,
    I: Clone,
    D: PartialOrd,
{
    next_item_id: usize,
    next_cluster_id: usize,

    items: HashMap<ItemId, I>,

    item_clusters: HashMap<ItemId, ClusterId>,

    clusters: HashMap<ClusterId, C>,

    cluster_distances: HashMap<(ClusterId, ItemId), D>,

    item_distances: HashMap<(ItemId, ItemId), D>,
}

#[derive(Debug)]
enum ItemIdOrClusterId {
    ItemId(usize),
    ClusterId(usize),
}

impl<I, C, D> DistanceStorage<I, C, D>
where
    C: Cluster<I, D> + Debug,
    I: Clone + Debug,
    D: PartialOrd + Debug + Clone + Copy,
{
    fn new() -> Self {
        DistanceStorage {
            next_item_id: 0,
            next_cluster_id: 0,
            clusters: HashMap::new(),
            items: HashMap::new(),
            item_clusters: HashMap::new(),
            cluster_distances: HashMap::new(),
            item_distances: HashMap::new(),
        }
    }

    fn find_closest(&self) -> Option<(ItemIdOrClusterId, usize)> {
        let mut result: Option<(ItemIdOrClusterId, usize)> = None;
        let mut min_distance: Option<D> = None;

        for ((other_item_id, item_id), distance) in &self.item_distances {
            if min_distance
                .map(|x| {
                    x.partial_cmp(distance)
                        .expect("Partial order not supported yet")
                        == Ordering::Greater
                })
                .unwrap_or(true)
            {
                min_distance = Some(*distance);
                result = Some((ItemIdOrClusterId::ItemId(*other_item_id), *item_id))
            }
        }

        for ((cluster_id, item_id), distance) in &self.cluster_distances {
            if min_distance
                .map(|x| {
                    x.partial_cmp(distance)
                        .expect("Partial order not supporteed yet")
                        == Ordering::Greater
                })
                .unwrap_or(true)
            {
                min_distance = Some(*distance);
                result = Some((ItemIdOrClusterId::ClusterId(*cluster_id), *item_id))
            }
        }

        result
    }

    fn update_distances(&mut self, cluster_id: usize) {
        let cluster = self.clusters.get(&cluster_id).unwrap();

        for ((d_cluster_id, item_id), distance) in &mut self.cluster_distances {
            if d_cluster_id == &cluster_id {
                *distance = cluster.distance(self.items.get(item_id).unwrap());
            }
        }
    }

    // Merge item and cluster together
    fn merge(&mut self, item_or_cluster: ItemIdOrClusterId, item_id: usize) -> usize {
        let item = self.items.get(&item_id).unwrap();

        match item_or_cluster {
            ItemIdOrClusterId::ItemId(other_item_id) => {
                let mut new_cluster = C::new();

                let other_item = self.items.get(&other_item_id).unwrap();

                println!("Merging {other_item:?} {item:?}");

                new_cluster.add_one(other_item);
                new_cluster.add_one(item);

                self.items.remove(&item_id);
                self.items.remove(&other_item_id);

                self.item_distances.retain(|(lhs, rhs), distance| {
                    lhs != &item_id
                        && rhs != &item_id
                        && lhs != &other_item_id
                        && rhs != &other_item_id
                });

                self.cluster_distances
                    .retain(|(_, rhs), distance| rhs != &item_id && rhs != &other_item_id);

                self.add_cluster(new_cluster)
            }
            ItemIdOrClusterId::ClusterId(other_cluster_id) => {
                let cluster = self.clusters.get_mut(&other_cluster_id).unwrap();

                println!("Merging {cluster:?} {item:?}");

                cluster.add_one(item);

                self.items.remove(&item_id);

                self.item_distances
                    .retain(|(lhs, rhs), distance| lhs != &item_id && rhs != &item_id);

                self.cluster_distances
                    .retain(|(_, rhs), distance| rhs != &item_id);

                other_cluster_id
            }
        }
    }

    fn add_cluster(&mut self, cluster: C) -> usize {
        let cluster_id = self.next_cluster_id;
        self.next_cluster_id += 1;

        // Update cluster to item distances
        for (item_id, item) in &self.items {
            let distance = cluster.distance(item);

            self.cluster_distances
                .insert((cluster_id, *item_id), distance);
        }

        self.clusters.insert(cluster_id, cluster);

        cluster_id
    }

    fn add_item(&mut self, item: I) {
        let item_id = self.next_item_id;
        self.next_item_id += 1;

        for (other_item_id, other_item) in &self.items {
            let distance = C::item_distance(other_item, &item);

            self.item_distances.insert(
                (*other_item_id, item_id),
                C::item_distance(&other_item, &item),
            );

            self.item_distances.insert(
                (item_id, *other_item_id),
                C::item_distance(&item, &other_item),
            );
        }

        // Update cluster-to-item distances
        for (other_cluster_id, other_cluster) in &self.clusters {
            let distance = other_cluster.distance(&item);

            self.cluster_distances
                .insert((*other_cluster_id, item_id), other_cluster.distance(&item));
        }

        self.items.insert(item_id, item);
    }
}

fn nearest_clustering<I, C, D>(items: Vec<I>) -> Vec<C>
where
    I: Clone + Copy + Debug,
    C: Cluster<I, D> + Debug,
    D: PartialOrd + Debug + Clone + Copy,
{
    let mut distance_storage: DistanceStorage<I, C, D> = DistanceStorage::new();

    for item in items {
        distance_storage.add_item(item);
    }

    loop {
        // Find two closest things to merge
        let Some((cluster_i, item_i)) = distance_storage.find_closest() else {
            break;
        };

        let merged_cluster_id = distance_storage.merge(cluster_i, item_i);

        // Add new base
        distance_storage.update_distances(merged_cluster_id);
    }

    let mut result = Vec::new();

    for (cluster_id, cluster) in distance_storage.clusters {
        result.push(cluster)
    }

    result
}

#[derive(Debug)]
struct FloatCluster {
    items: Vec<f64>,
}

impl Cluster<f64, f64> for FloatCluster {
    fn new() -> Self {
        FloatCluster { items: Vec::new() }
    }

    fn add_one(&mut self, other: &f64) {
        self.items.push(*other);
    }

    fn distance(&self, item: &f64) -> f64 {
        let total: f64 = self.items.iter().sum();
        let mean = total / (self.items.len() as f64);

        let dist = (item - mean).abs();

        dist
    }

    fn item_distance(lhs: &f64, rhs: &f64) -> f64 {
        let dist = (lhs - rhs).abs();

        dist
    }
}

#[test]
fn test() {
    let items = vec![1.0, 2.0, 10.0, 11.0, 12.1, 12.2];

    let clusters = nearest_clustering::<f64, FloatCluster, f64>(items);

    println!("Resulting clusters: {:?}", clusters);
}
