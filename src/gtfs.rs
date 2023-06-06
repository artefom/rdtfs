use std::collections::HashSet;
/// Module for reading gtfs collection
///
use std::{collections::HashMap, hash::Hash};

use serde::{de::DeserializeOwned, Serialize};

mod join;
pub use self::csv_models::{GtfsFile, GtfsFileType};
pub use self::join::PartitionedTable;
use self::{csv_models::Shape, join::Join8};

pub use self::csv_models::{
    Agency, Calendar, CalendarDate, FareAttribute, FareRule, Route, SerivceExceptionType,
    ServiceAvailability, Stop, StopTime, Trip,
};

mod csv_models;

pub trait GtfsStore {
    fn scan<'a, D: DeserializeOwned + GtfsFile + 'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = D> + 'a>;
}

pub trait TablePartitioner {
    fn partition<I, F, K, V>(
        iter: I,
        num_partitions: usize,
        key: F,
    ) -> Box<dyn join::PartitionedTable<K, V>>
    where
        I: Iterator<Item = V>,
        F: FnMut(&V) -> Option<K>,
        K: Hash + Eq + Clone + Serialize + DeserializeOwned + 'static,
        V: Serialize + DeserializeOwned + 'static;

    fn multipartition<I, F, K, V, KI>(
        iter: I,
        num_partitions: usize,
        key: F,
    ) -> Box<dyn join::PartitionedTable<K, V>>
    where
        I: Iterator<Item = V>,
        F: FnMut(&V) -> KI,
        KI: IntoIterator<Item = K>,
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
    calendar: Box<dyn PartitionedTable<usize, Calendar>>,
    calendar_dates: Box<dyn PartitionedTable<usize, CalendarDate>>,
    stops: HashMap<String, Stop>,
    agencies: HashMap<String, Agency>,
}

/// Maps string keys to integer ids
#[derive(Default)]
pub struct KeyStore {
    last_id: usize,
    key_x_id: HashMap<String, usize>,
}

impl KeyStore {
    pub fn map_id(&mut self, key: String) -> usize {
        use std::collections::hash_map::Entry::*;

        match self.key_x_id.entry(key) {
            Occupied(entry) => *entry.get(),
            Vacant(entry) => {
                self.last_id += 1;
                *entry.insert(self.last_id)
            }
        }
    }

    pub fn get_id(&self, key: &str) -> Option<&usize> {
        self.key_x_id.get(key)
    }
}

/// Report about errors during gtfs read
#[derive(Default, Debug)]
pub struct GtfsErrors {
    pub num_missing_trips: usize,
    pub num_missing_fare_zone_id: usize,
    pub num_missing_fare_route_id: usize,
    pub num_missing_service_ids: usize,
}

impl GtfsErrors {
    fn stop_time_unknown_trip_id(&mut self, _trip_id: &str) {
        self.num_missing_trips += 1;
    }
    fn fare_missing_zone_id(&mut self, _zone_id: &str) {
        self.num_missing_fare_zone_id += 1;
    }
    fn fare_missing_route_id(&mut self, _route_id: &str) {
        self.num_missing_fare_route_id += 1;
    }
    fn calendar_missing_service_id(&mut self, _service_id: &str) {
        self.num_missing_service_ids += 1;
    }
}

impl GtfsPartitioned {
    pub fn from_store<S: GtfsStore, P: TablePartitioner>(store: &mut S) -> (Self, GtfsErrors) {
        let mut errors = GtfsErrors::default();

        let num_partitions: usize = 10;

        // Storage of all rotue keys
        let mut route_keys = KeyStore::default();
        let mut trip_id_x_route_id: HashMap<String, usize> = HashMap::new();
        let mut service_id_x_route_id: HashMap<String, HashSet<usize>> = HashMap::new();
        let mut shape_id_x_route_id: HashMap<String, HashSet<usize>> = HashMap::new();
        // This is used to map fare classes to route keys
        let mut zone_id_x_route_key: HashMap<String, usize> = HashMap::new();
        let mut fare_x_route_keys: HashMap<String, HashSet<usize>> = HashMap::new();

        // Scan agencies
        let mut agencies: HashMap<String, Agency> = HashMap::new();
        for agency in store.scan::<Agency>() {
            agencies.insert(agency.agency_id.clone(), agency);
        }

        // This mapping is used to attach fare attributes and rules to trips
        let mut stops: HashMap<String, Stop> = HashMap::new();
        for stop in store.scan::<Stop>() {
            stops.insert(stop.stop_id.clone(), stop);
        }

        let routes = P::partition(store.scan::<Route>(), num_partitions, |route| {
            Some(route_keys.map_id(route.route_id.clone()))
        });

        let trips = P::partition(store.scan::<Trip>(), num_partitions, |trip| {
            let route_id = route_keys.map_id(trip.route_id.clone());
            trip_id_x_route_id.insert(trip.trip_id.clone(), route_id);

            match service_id_x_route_id.entry(trip.service_id.clone()) {
                Occupied(mut entry) => {
                    entry.get_mut().insert(route_id);
                }
                Vacant(vacant) => {
                    vacant.insert(HashSet::from([route_id]));
                }
            }

            // Record shape id mapping
            use std::collections::hash_map::Entry::*;
            if let Some(shape_id) = &trip.shape_id {
                match shape_id_x_route_id.entry(shape_id.clone()) {
                    Occupied(mut entry) => {
                        entry.get_mut().insert(route_id);
                    }
                    Vacant(entry) => {
                        entry.insert(HashSet::from([route_id]));
                    }
                }
            }

            Some(route_id)
        });

        let stop_times = P::partition(
            store.scan::<StopTime>().map(|stop_time| {
                if let Some(stop) = stops.get(&stop_time.stop_id) {
                    if let Some(route_id) = trip_id_x_route_id.get(&stop_time.trip_id) {
                        if let Some(zone_id) = &stop.zone_id {
                            zone_id_x_route_key.insert(zone_id.clone(), route_id.clone());
                        }
                    }
                };

                stop_time
            }),
            num_partitions,
            |stop_time| {
                let Some(route_id) = trip_id_x_route_id.get(&stop_time.trip_id) else {
                    errors.stop_time_unknown_trip_id(&stop_time.trip_id);
                    return None
                };
                Some(route_id.clone())
            },
        );

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
        let fare_rules = P::multipartition(store.scan::<FareRule>(), num_partitions, |farerule| {
            let mut keys: HashSet<usize> = HashSet::with_capacity(4);

            if let Some(origin) = &farerule.origin_id {
                if let Some(route_key) = zone_id_x_route_key.get(origin) {
                    keys.insert(route_key.clone());
                } else {
                    errors.fare_missing_zone_id(origin);
                }
            }

            if let Some(destination) = &farerule.destination_id {
                if let Some(route_key) = zone_id_x_route_key.get(destination) {
                    keys.insert(route_key.clone());
                } else {
                    errors.fare_missing_zone_id(destination);
                }
            }

            if let Some(contains_id) = &farerule.contains_id {
                if let Some(route_key) = zone_id_x_route_key.get(contains_id) {
                    keys.insert(route_key.clone());
                } else {
                    errors.fare_missing_zone_id(contains_id);
                }
            }

            if let Some(route_id) = &farerule.route_id {
                if let Some(route_key) = route_keys.get_id(route_id) {
                    keys.insert(route_key.clone());
                } else {
                    errors.fare_missing_route_id(route_id);
                }
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
        });

        // Partition fares
        let fare_attributes = P::multipartition(
            store.scan::<FareAttribute>(),
            num_partitions,
            |fare_attribute| match fare_x_route_keys.get(&fare_attribute.fare_id) {
                Some(value) => value.clone(),
                None => HashSet::new(),
            },
        );

        // Calendar
        let calendar = P::multipartition(store.scan::<Calendar>(), num_partitions, |calendar| {
            let Some(route_ids) = service_id_x_route_id.get(&calendar.service_id) else {
                errors.calendar_missing_service_id(&calendar.service_id);
                return HashSet::new();
            };
            route_ids.clone()
        });

        // Calendar dates
        let calendar_dates = P::multipartition(
            store.scan::<CalendarDate>(),
            num_partitions,
            |calendar_date| {
                let Some(route_ids) = service_id_x_route_id.get(&calendar_date.service_id) else {
                    errors.calendar_missing_service_id(&calendar_date.service_id);
                    return HashSet::new();
                };
                route_ids.clone()
            },
        );

        let shapes = P::multipartition(store.scan::<Shape>(), num_partitions, |shape| {
            let Some(route_ids) = shape_id_x_route_id.get(&shape.shape_id) else {
                todo!()
            };

            route_ids.clone()
        });

        (
            GtfsPartitioned {
                routes,
                stop_times,
                trips,
                shapes,
                fare_rules,
                fare_attributes,
                stops,
                agencies,
                calendar,
                calendar_dates,
            },
            errors,
        )
    }

    pub fn iter<'a>(&'a self) -> GtfsIterator<'a> {
        let join = join::join8(
            &self.routes,
            &self.trips,
            &self.stop_times,
            &self.shapes,
            &self.fare_rules,
            &self.fare_attributes,
            &self.calendar,
            &self.calendar_dates,
        );

        GtfsIterator {
            join,
            stops: &self.stops,
            agencies: &self.agencies,
        }
    }
}

pub struct GtfsIterator<'r> {
    join: Join8<
        'r,
        usize,
        Route,
        Trip,
        StopTime,
        Shape,
        FareRule,
        FareAttribute,
        Calendar,
        CalendarDate,
    >,
    stops: &'r HashMap<String, Stop>,
    agencies: &'r HashMap<String, Agency>,
}

pub struct FullTrip {
    pub trip: Trip,
    pub stop_times: Vec<StopTime>,
    pub calendar: Option<Calendar>,
    pub calendar_dates: Vec<CalendarDate>,
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
        let Some((_key, data)) = self.join.next() else {
            return None
        };

        // Unpack data
        let (
            routes,
            trips,
            stop_times,
            shapes,
            fare_rules,
            fare_attributes,
            calendar,
            calendar_dates,
        ) = data;

        let route = routes.first().unwrap().clone();
        let agency = self.agencies.get(&route.agency_id).unwrap().clone();

        let mut full_trips = Vec::new();
        let mut stop_times_idx: HashMap<String, Vec<StopTime>> = HashMap::new();
        let mut calendar_idx: HashMap<String, Calendar> = HashMap::new();
        let mut calendar_dates_idx: HashMap<String, Vec<CalendarDate>> = HashMap::new();

        use std::collections::hash_map::Entry::*;

        // Index stop times
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

        // Index calendar
        for calendar in calendar.into_iter() {
            if calendar_idx
                .insert(calendar.service_id.clone(), calendar)
                .is_some()
            {
                todo!()
            }
        }

        // Index calednar dates
        for calendar_date in calendar_dates.into_iter() {
            match calendar_dates_idx.entry(calendar_date.service_id.clone()) {
                Occupied(mut entry) => {
                    entry.get_mut().push(calendar_date);
                }
                Vacant(entry) => {
                    entry.insert(vec![calendar_date]);
                }
            }
        }

        // Create trips
        for trip in trips.into_iter() {
            let trip_stop_times = stop_times_idx.remove(&trip.trip_id).unwrap();
            let calendar = calendar_idx.get(&trip.service_id).map(|x| x.clone());
            let calendar_dates = calendar_dates_idx
                .get(&trip.service_id)
                .map(|x| x.clone())
                .unwrap_or_else(|| Vec::new());

            full_trips.push(FullTrip {
                trip,
                stop_times: trip_stop_times,
                calendar,
                calendar_dates,
            });
        }

        Some(FullRoute {
            route,
            agency,
            trips: full_trips,
        })
    }
}
