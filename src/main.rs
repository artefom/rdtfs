// #![allow(unused_imports)]
// #![allow(dead_code)]
// #![allow(unused_variables)]

use std::{fs::OpenOptions, hash::Hash, io::BufReader, path::Path};

use binarystore::Partitionable;

use gtfs::{to_midnights, FullRoute, GtfsPartitioned, KeyStore, StopTime, TablePartitioner};

use anyhow::{Context, Result};
use indicatif::ProgressIterator;
use rides::Ride;
use serde::{de::DeserializeOwned, Serialize};

use progress::ProgressReader;

use csv::CsvTableReader;

use crate::{progress::progress_style_count, rides::TimetableGrouper};

mod gtfs;

mod csv;

mod binarystore;

mod progress;

mod store;

mod rides;

impl<T> gtfs::GtfsStore for T
where
    T: store::FileCollection,
{
    fn scan<'a, D: DeserializeOwned + gtfs::GtfsFile + 'a>(
        &'a mut self,
    ) -> Box<dyn Iterator<Item = D> + 'a> {
        let file_type = D::get_file_type();

        let Some((reader, total_size)) = self
            .open_by_predicate(|filename| {
                let file_stem = &Path::new(&filename).file_stem().unwrap().to_string_lossy();
                gtfs::GtfsFileType::from_filename(&file_stem) == Some(file_type)
            }) else {
                // When file not found, return empty iterator
                return Box::new(Vec::new().into_iter())
            };
        let reader = ProgressReader::new(BufReader::new(reader), total_size);
        let reader = CsvTableReader::<_, D>::new(reader).map(|x| x.unwrap());
        Box::new(reader)
    }
}

impl<K, V> gtfs::PartitionedTable<K, V> for binarystore::PartitionedReader<K, V>
where
    K: DeserializeOwned + 'static,
    V: DeserializeOwned + 'static,
{
    fn get_partition(&self, index: usize) -> Option<Box<dyn Iterator<Item = (K, V)>>> {
        let Some(value) = binarystore::PartitionedReader::get_partition(self, index) else {
            return None
        };

        Some(Box::new(value.map(|x| x.unwrap())))
    }

    fn len(&self) -> usize {
        binarystore::PartitionedReader::len(&self)
    }
}

struct BinaryPartitioner;

impl TablePartitioner for BinaryPartitioner {
    fn partition<I, F, K, V>(
        iter: I,
        num_partitions: usize,
        key: F,
    ) -> Box<dyn gtfs::PartitionedTable<K, V>>
    where
        I: Iterator<Item = V>,
        F: FnMut(&V) -> Option<K>,
        V: Serialize + DeserializeOwned + 'static,
        K: Serialize + DeserializeOwned + Hash + Eq + Clone + 'static,
    {
        let partitioned = iter.disk_partition(num_partitions, key).unwrap();
        Box::new(partitioned)
    }

    fn multipartition<I, F, K, V, KI>(
        iter: I,
        num_partitions: usize,
        key: F,
    ) -> Box<dyn gtfs::PartitionedTable<K, V>>
    where
        I: Iterator<Item = V>,
        F: FnMut(&V) -> KI,
        KI: IntoIterator<Item = K>,
        K: Hash + Eq + Clone + Serialize + DeserializeOwned + 'static,
        V: Serialize + DeserializeOwned + 'static,
    {
        let partitioned = iter.disk_multipartition(num_partitions, key).unwrap();
        Box::new(partitioned)
    }
}

/// Convert a gtfs trip into rides
fn to_rides(
    station_ids: &mut KeyStore,
    agency: &gtfs::Agency,
    stop_times: Vec<StopTime>,
    calendar: Option<gtfs::Calendar>,
    calendar_dates: Vec<gtfs::CalendarDate>,
) -> Vec<Result<Ride>> {
    let mut rides: Vec<Result<Ride>> = Vec::new();
    let timezone: chrono_tz::Tz = agency.agency_timezone.parse().unwrap();

    for date in to_midnights(calendar, calendar_dates) {
        let mut stops = Vec::new();
        for stop_time in &stop_times {
            let station = station_ids.map_id(stop_time.stop_id.clone());

            // Parse arrival time
            let arrival = match stop_time
                .arrival_datetime(date, timezone)
                .context("Could not parse arrival datetime")
            {
                Ok(value) => value,
                Err(err) => {
                    rides.push(Err(err));
                    continue;
                }
            };

            // Parse departure time
            let departure = match stop_time
                .departure_datetime(date, timezone)
                .context("Could not parse departure datetime")
            {
                Ok(value) => value,
                Err(err) => {
                    rides.push(Err(err));
                    continue;
                }
            };

            stops.push(rides::Stop {
                station,
                arrival,
                departure,
            });
        }

        rides.push(Ok(rides::Ride { stops }))
    }
    rides
}

struct RidesIterator<'k, 'r, I>
where
    I: Iterator<Item = FullRoute> + 'r,
{
    station_ids: &'k mut KeyStore,
    routes_iter: &'r mut I,
    ride_batch: Vec<Result<Ride>>,
}

impl<'k, 'r, I> RidesIterator<'k, 'r, I>
where
    I: Iterator<Item = FullRoute> + 'r,
{
    fn next_route(&mut self) -> bool {
        let route = self.routes_iter.next();
        let Some(route) = route else {
            return false;
        };
        for trip in route.trips {
            let mut rides = to_rides(
                self.station_ids,
                &route.agency,
                trip.stop_times,
                trip.calendar,
                trip.calendar_dates,
            );
            self.ride_batch.append(&mut rides);
        }
        return true;
    }
}

impl<'k, 'r, I> Iterator for RidesIterator<'k, 'r, I>
where
    I: Iterator<Item = FullRoute> + 'r,
{
    type Item = Result<Ride>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Pop next ride batch
            match self.ride_batch.pop() {
                Some(value) => return Some(value),
                None => (),
            };

            // Id next route does not exist, return
            if !self.next_route() {
                return None;
            }
        }
    }
}

trait RidesIiter<I>
where
    I: Iterator<Item = FullRoute>,
{
    fn rides<'k, 'r>(&'r mut self, station_ids: &'k mut KeyStore) -> RidesIterator<'k, 'r, I>;
}

impl<I> RidesIiter<I> for I
where
    I: Iterator<Item = FullRoute>,
{
    fn rides<'k, 'r>(&'r mut self, station_ids: &'k mut KeyStore) -> RidesIterator<'k, 'r, I> {
        let route = self.next();

        let mut ride_batch = Vec::new();

        if let Some(route) = route {
            for trip in route.trips {
                let mut rides = to_rides(
                    station_ids,
                    &route.agency,
                    trip.stop_times,
                    trip.calendar,
                    trip.calendar_dates,
                );
                ride_batch.append(&mut rides);
            }
        };

        RidesIterator {
            station_ids,
            routes_iter: self,
            ride_batch,
        }
    }
}

fn main() -> Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    // let path = "/Users/artef/Downloads/ntra_import_latest_ntra-in.gtfs.txt.zip";
    let path = "/Users/artef/dev/dtfs/local/CATA.gtfs.txt.zip";
    // let path = "/Users/artef/Downloads/AMAU.zip";

    let file = OpenOptions::new().read(true).open(path).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();

    let (gtfs_partitioned, read_errors) =
        GtfsPartitioned::from_store::<_, BinaryPartitioner>(&mut archive);

    println!("Errors while reading gtfs: {read_errors:?}");

    let mut station_ids = KeyStore::default();

    let grouper = TimetableGrouper {};

    let iter = gtfs_partitioned.iter();
    println!("Total number of routes: {}", iter.len());

    let mut total_number_rides: usize = 0;
    let mut error_rides: usize = 0;

    // Group rides
    for ride in gtfs_partitioned
        .iter()
        .progress_with_style(progress_style_count())
        .rides(&mut station_ids)
    {
        total_number_rides += 1;
        match ride {
            Ok(value) => (),
            Err(err) => {
                println!("{:?}", err);
                error_rides += 1;
            }
        }
    }

    println!("Total number of rides: {total_number_rides}, {error_rides} errors");

    // Partition rides

    // // Group rides
    // for route in gtfs_partitioned.iter() {
    //     for trip in route.trips {
    //         let rides = to_rides(
    //             &mut station_ids,
    //             &route.agency,
    //             trip.stop_times,
    //             trip.calendar,
    //             trip.calendar_dates,
    //         );
    //         for ride in rides {
    //             grouper.add_ride(ride)
    //         }
    //     }
    //     break;
    // }

    Ok(())
}
