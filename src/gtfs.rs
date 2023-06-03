use std::collections::HashSet;
/// Module for reading gtfs collection
///
use std::{collections::HashMap, hash::Hash};

use serde::{de::DeserializeOwned, Serialize};

mod join;
pub use self::csv_models::{GtfsFile, GtfsFileType};
use self::join::EmptyPartitionedTable;
pub use self::join::PartitionedTable;
use self::{csv_models::Shape, join::Join6};

use self::csv_models::{Agency, FareAttribute, FareRule, Route, Stop, StopTime, Trip};

mod csv_models;

pub trait GtfsStore {
    fn scan<'a, D: DeserializeOwned + GtfsFile + 'a>(
        &'a mut self,
    ) -> Option<Box<dyn Iterator<Item = D> + 'a>>;
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

    fn multipartition<I, F, K, V>(
        iter: I,
        num_partitions: usize,
        key: F,
    ) -> Box<dyn join::PartitionedTable<K, V>>
    where
        I: Iterator<Item = V>,
        F: FnMut(&V) -> Vec<K>,
        K: Hash + Eq + Clone + Serialize + DeserializeOwned + 'static,
        V: Serialize + DeserializeOwned + 'static;
}

pub struct GtfsPartitioned {
    routes: Box<dyn PartitionedTable<usize, Route>>,
    stop_times: Box<dyn PartitionedTable<usize, StopTime>>,
    trips: Box<dyn PartitionedTable<usize, Trip>>,
    shapes: Box<dyn PartitionedTable<usize, Shape>>,
    fare_rules: Box<dyn PartitionedTable<usize, FareRule>>,
    fare_attributes: Box<dyn PartitionedTable<usize, FareAttribute>>,
    stops: HashMap<String, Stop>,
    agencies: HashMap<String, Agency>,
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

    fn get_id(&self, key: &str) -> Option<&usize> {
        self.key_x_id.get(key)
    }
}

impl GtfsPartitioned {
    pub fn from_store<S: GtfsStore, P: TablePartitioner>(store: &mut S) -> Self {
        let num_partitions: usize = 10;

        // Storage of all rotue keys
        let mut route_keys = KeyStore::default();
        let mut trip_id_x_route_id: HashMap<String, usize> = HashMap::new();
        let mut shape_id_x_route_id: HashMap<String, Vec<usize>> = HashMap::new();

        // Scan agencies
        let mut agencies: HashMap<String, Agency> = HashMap::new();
        for agency in store.scan::<Agency>().unwrap() {
            agencies.insert(agency.agency_id.clone(), agency);
        }

        // This mapping is used to attach fare attributes and rules to trips
        let mut stops: HashMap<String, Stop> = HashMap::new();
        for stop in store.scan::<Stop>().unwrap() {
            stops.insert(stop.stop_id.clone(), stop);
        }

        let routes = P::partition(store.scan::<Route>().unwrap(), num_partitions, |route| {
            route_keys.map_id(route.route_id.clone())
        });

        let trips = P::partition(store.scan::<Trip>().unwrap(), num_partitions, |trip| {
            let route_id = route_keys.get_id(&trip.route_id).unwrap().clone();
            trip_id_x_route_id.insert(trip.trip_id.clone(), route_id);

            // Record shape id mapping
            use std::collections::hash_map::Entry::*;
            if let Some(shape_id) = &trip.shape_id {
                match shape_id_x_route_id.entry(shape_id.clone()) {
                    Occupied(mut entry) => entry.get_mut().push(route_id),
                    Vacant(entry) => {
                        entry.insert(vec![route_id]);
                    }
                }
            }

            route_id
        });

        // This is used to map fare classes to route keys
        let mut zone_id_x_route_key: HashMap<String, usize> = HashMap::new();

        let stop_times = P::partition(
            store.scan::<StopTime>().unwrap().map(|stop_time| {
                if let Some(stop) = stops.get(&stop_time.stop_id) {
                    if let Some(zone_id) = &stop.zone_id {
                        zone_id_x_route_key.insert(
                            zone_id.clone(),
                            trip_id_x_route_id.get(&stop_time.trip_id).unwrap().clone(),
                        );
                    }
                };

                stop_time
            }),
            num_partitions,
            |stop_time| trip_id_x_route_id.get(&stop_time.trip_id).unwrap().clone(),
        );

        let mut fare_x_route_keys: HashMap<String, HashSet<usize>> = HashMap::new();

        // We partition fares by any match of origin, destination or route id
        // It is not guaranteed though that a specific fare will match to any of the routes
        // in one partition
        // for example, route may be A -> B -> C and matched fares
        // can be A -> D, B -> D, C -> D - they will not match to this route since it does
        // not go to D.
        // But hopefully this will allow to greatly reduce amount
        // of fares that we need to take into consideration when matching to specific routes
        // in the future, to have better partitioning we can also use zone id parse to route_key
        // mappings to have better filtering, but it would require loading whole stop_times
        // in memory or partitioning twice :(
        let fare_rules = P::multipartition(
            store.scan::<FareRule>().unwrap(),
            num_partitions,
            |farerule| {
                let mut keys: Vec<usize> = Vec::with_capacity(4);

                if let Some(origin) = &farerule.origin_id {
                    let route_key = zone_id_x_route_key.get(origin).unwrap();
                    keys.push(route_key.clone());
                }

                if let Some(destination) = &farerule.destination_id {
                    let route_key = zone_id_x_route_key.get(destination).unwrap();
                    keys.push(route_key.clone());
                }

                if let Some(contains_id) = &farerule.contains_id {
                    let route_key = zone_id_x_route_key.get(contains_id).unwrap();
                    keys.push(route_key.clone());
                }

                if let Some(route_id) = &farerule.route_id {
                    let route_key = route_keys.get_id(route_id).unwrap();
                    keys.push(route_key.clone());
                }

                use std::collections::hash_map::Entry::*;
                match fare_x_route_keys.entry(farerule.fare_id.clone()) {
                    Occupied(mut entry) => {
                        entry.get_mut().extend(keys.iter());
                    }
                    Vacant(entry) => {
                        entry.insert(keys.iter().cloned().collect());
                    }
                }

                keys
            },
        );

        // Convert hashmaps to vectors
        let fare_x_route_keys: HashMap<String, Vec<usize>> = fare_x_route_keys
            .into_iter()
            .map(|(key, value)| (key, value.into_iter().collect()))
            .collect();

        // Partition fares
        let fare_attributes = P::multipartition(
            store.scan::<FareAttribute>().unwrap(),
            num_partitions,
            |fare_attribute| match fare_x_route_keys.get(&fare_attribute.fare_id) {
                Some(value) => value.clone(),
                None => Vec::new(),
            },
        );

        let shapes = if let Some(shapes_table) = store.scan::<Shape>() {
            P::multipartition(shapes_table, num_partitions, |shape| {
                shape_id_x_route_id.get(&shape.shape_id).unwrap().clone()
            })
        } else {
            Box::new(EmptyPartitionedTable::new())
        };

        GtfsPartitioned {
            routes,
            stop_times,
            trips,
            shapes,
            fare_rules,
            fare_attributes,
            stops,
            agencies,
        }
    }

    pub fn iter<'a>(&'a self) -> GtfsIterator<'a> {
        let join = join::join6(
            &self.routes,
            &self.trips,
            &self.stop_times,
            &self.shapes,
            &self.fare_rules,
            &self.fare_attributes,
        )
        .unwrap();

        GtfsIterator {
            join,
            stops: &self.stops,
            agencies: &self.agencies,
        }
    }
}

pub struct GtfsIterator<'r> {
    join: Join6<'r, usize, Route, Trip, StopTime, Shape, FareRule, FareAttribute>,
    stops: &'r HashMap<String, Stop>,
    agencies: &'r HashMap<String, Agency>,
}

pub struct FullTrip {
    pub trip: Trip,
    pub stop_times: Vec<StopTime>,
}

pub struct FullRoute {
    pub agency: Agency,
    pub route: Route,
    pub trips: Vec<FullTrip>, // pub trips: Vec<Trip>,
                              // pub stop_times: Vec<StopTime>,
                              // pub shapes: Vec<Shape>,
                              // pub fare_rules: Vec<FareRule>,
                              // pub fare_attributes: Vec<FareAttribute>,
}

impl<'r> Iterator for GtfsIterator<'r> {
    type Item = FullRoute;

    fn next(&mut self) -> Option<Self::Item> {
        let Some((_key, (routes, trips, stop_times, shapes, fare_rules, fare_attributes))) = self.join.next() else {
            return None
        };

        let route = routes.first().unwrap().clone();
        let agency = self.agencies.get(&route.agency_id).unwrap().clone();

        let mut full_trips = Vec::new();
        let mut stop_times_idx: HashMap<String, Vec<StopTime>> = HashMap::new();

        use std::collections::hash_map::Entry::*;
        for stop_time in stop_times.into_iter() {
            match stop_times_idx.entry(stop_time.trip_id.clone()) {
                Occupied(mut entry) => {
                    entry.get_mut().push(stop_time);
                }
                Vacant(entry) => {
                    entry.insert(vec![stop_time]);
                }
            }
        }

        // Create trips
        for trip in trips.into_iter() {
            let trip_stop_times = stop_times_idx.remove(&trip.trip_id).unwrap();
            full_trips.push(FullTrip {
                trip,
                stop_times: trip_stop_times,
            });
        }

        Some(FullRoute {
            route,
            agency,
            trips: full_trips,
        })
    }
}
