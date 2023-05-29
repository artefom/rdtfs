/// Module for reading gtfs collection
///
use std::{collections::HashMap, hash::Hash};

use serde::{de::DeserializeOwned, Serialize};

use join::JoinReader;

mod join;
pub use self::csv_models::{GtfsFile, GtfsFileType};
pub use self::join::PartitionedTable;

use self::csv_models::{Route, StopTime, Trip};

mod csv_models;

pub trait GtfsStore {
    // fn get_readable<'a>(&'a mut self, file_type: GtfsFileType) -> Option<Box<dyn BufRead + 'a>>;

    fn scan<'a, D: DeserializeOwned + GtfsFile + 'a>(
        &'a mut self,
    ) -> Option<Box<dyn Iterator<Item = D> + 'a>>;

    // fn get_table_reader<'a, D: DeserializeOwned + GtfsFile>(
    //     &'a mut self,
    // ) -> Result<CsvTableReader<Box<dyn BufRead + 'a>, D>> {
    //     let file_type = D::get_file_type();
    //     let read = self.get_readable(file_type);
    //     let Some(read) = read else {
    //             bail!("File {} not found", file_type.file_name())
    //         };
    //     let reader = CsvTableReader::<_, D>::new(read);
    //     Ok(reader)
    // }
}

pub trait TablePartitioner {
    fn partition<I, F, K, V>(
        iter: I,
        num_partitions: usize,
        key: F,
    ) -> Box<dyn join::PartitionedTable<K, V>>
    where
        I: Iterator<Item = V>,
        F: FnMut(&V) -> K,
        K: Hash + Eq + Clone + Serialize + DeserializeOwned + 'static,
        V: Serialize + DeserializeOwned + 'static;
}

pub struct GtfsPartitioned {
    stop_times: Box<dyn PartitionedTable<usize, StopTime>>,
    trips: Box<dyn PartitionedTable<usize, Trip>>,
}

/// Maps string keys to integer ids
#[derive(Default)]
struct KeyStore {
    last_id: usize,
    key_x_id: HashMap<String, usize>,
}

impl KeyStore {
    fn map_id(&mut self, key: String) -> usize {
        use std::collections::hash_map::Entry::*;

        match self.key_x_id.entry(key) {
            Occupied(entry) => *entry.get(),
            Vacant(entry) => {
                self.last_id += 1;
                *entry.insert(self.last_id)
            }
        }
    }
}

impl GtfsPartitioned {
    pub fn from_store<S: GtfsStore, P: TablePartitioner>(store: &mut S) -> Self {
        let num_partitions: usize = 10;

        // Storage of all rotue keys
        let mut route_keys = KeyStore::default();
        let mut trip_id_x_route_id: HashMap<String, usize> = HashMap::new();

        let _routes = P::partition(store.scan::<Route>().unwrap(), num_partitions, |route| {
            route_keys.map_id(route.route_id.clone())
        });

        let trips = P::partition(store.scan::<Trip>().unwrap(), num_partitions, |trip| {
            let route_id = route_keys.map_id(trip.route_id.clone());
            trip_id_x_route_id.insert(trip.trip_id.clone(), route_id);
            route_id
        });

        let stop_times = P::partition(
            store.scan::<StopTime>().unwrap(),
            num_partitions,
            |stop_time| trip_id_x_route_id.get(&stop_time.trip_id).unwrap().clone(),
        );

        GtfsPartitioned { stop_times, trips }
    }

    pub fn iter<'a>(&'a self) -> GtfsIterator<'a> {
        let join = join::join(&self.trips, &self.stop_times).unwrap();

        GtfsIterator { join }
    }
}

pub struct GtfsIterator<'r> {
    join: JoinReader<'r, usize, Trip, StopTime>,
}

pub struct FullRoute {
    pub trips: Vec<Trip>,
    pub stop_times: Vec<StopTime>,
}

impl<'r> Iterator for GtfsIterator<'r> {
    type Item = FullRoute;

    fn next(&mut self) -> Option<Self::Item> {
        let Some((_key, (trips, stop_times))) = self.join.next() else {
            return None
        };

        Some(FullRoute { trips, stop_times })
    }
}
