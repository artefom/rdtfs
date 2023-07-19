#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    hash::Hash,
    io::BufReader,
    path::Path,
};

use binarystore::Partitionable;

use gtfs::{to_midnights, FullRoute, GtfsPartitioned, KeyStore, StopTime, TablePartitioner};

use anyhow::{Context, Result};
use indicatif::ProgressIterator;
use itertools::Itertools;
use rides::Ride;
use serde::{de::DeserializeOwned, Serialize};

use progress::ProgressReader;

use csv::CsvTableReader;

use crate::{
    poa::align,
    poa::print_alignment,
    progress::progress_style_count,
    rides::{group_stop_sequences, StopSequence, TimetableGrouper},
};

mod gtfs;

mod csv;

mod binarystore;

mod progress;

mod store;

mod rides;

mod poa;

mod clustering;

mod sequence_index;

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
        stops.sort_by_key(|x| x.departure);
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
    // let path = "/Users/artef/dev/dtfs/local/MEGB.zip";
    let path = "/Users/artef/dev/dtfs/local/CATA.gtfs.txt.zip";
    // let path = "/Users/artef/Downloads/AMAU.zip";

    let file = OpenOptions::new().read(true).open(path).unwrap();
    let mut archive = zip::ZipArchive::new(file).unwrap();

    let (gtfs_partitioned, read_errors) =
        GtfsPartitioned::from_store::<_, BinaryPartitioner>(&mut archive);

    println!("Errors while reading gtfs: {read_errors:?}");

    let mut station_ids = KeyStore::default();

    // let mut grouper = TimetableGrouper::new();

    let iter = gtfs_partitioned.iter();
    println!("Total number of routes: {}", iter.len());

    let mut total_number_rides: usize = 0;
    let mut error_rides: usize = 0;

    let mut stop_sequences: HashSet<StopSequence> = HashSet::new();

    for ride in gtfs_partitioned
        .iter()
        .progress_with_style(progress_style_count())
        .rides(&mut station_ids)
    {
        total_number_rides += 1;

        let ride = match ride {
            Ok(value) => value,
            Err(err) => {
                println!("{:?}", err);
                error_rides += 1;
                continue;
            }
        };

        stop_sequences.insert((&ride).into());
    }

    println!("Total number of rides: {total_number_rides}, {error_rides} errors");
    println!("Total number of stop sequences: {}", stop_sequences.len());

    let stop_sequences = stop_sequences.into_iter().collect_vec();

    println!("Grouping stop sequences");
    let assigned_clusters = group_stop_sequences(&stop_sequences);

    let mut cluster_x_stop_sequences: HashMap<usize, Vec<StopSequence>> = HashMap::new();

    for (stop_sequence, assigned_cluster) in stop_sequences.into_iter().zip(assigned_clusters) {
        match cluster_x_stop_sequences.entry(assigned_cluster) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().push(stop_sequence);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(vec![stop_sequence]);
            }
        }
    }

    let cluster_x_stop_sequences = cluster_x_stop_sequences
        .values()
        .sorted_by_key(|x| x.len())
        .collect_vec();

    for i in 0..cluster_x_stop_sequences.len() {
        let cluster = cluster_x_stop_sequences[i];
        // println!();
        // println!("Example {i}");
        // println!("------------------------------");
        // println!("Cluster {:?}", cluster);
        // println!();

        let seq_inner = cluster
            .iter()
            .map(|x| x.stop_sequence.as_ref())
            .collect_vec();

        println!("Aligning cluster of size {}", cluster.len());
        let (consensus, alignments) = align(&seq_inner);

        poa::print_alignment(seq_inner.as_slice(), consensus, alignments);
    }

    // // Group rides
    // for ride in gtfs_partitioned
    //     .iter()
    //     .progress_with_style(progress_style_count())
    //     .rides(&mut station_ids)
    // {
    //     total_number_rides += 1;
    //     let ride = match ride {
    //         Ok(value) => value,
    //         Err(err) => {
    //             println!("{:?}", err);
    //             error_rides += 1;
    //             continue;
    //         }
    //     };

    //     // Add ride to grouper
    //     grouper.add_ride(ride);
    // }

    // println!("Total number of rides: {total_number_rides}, {error_rides} errors");

    // let grouped = grouper.finalize();

    // for stop_seqs in grouped.mapping {
    //     // if stop_seqs.len() > 200 {
    //     //     println!("Cluster length is too big: {}", stop_seqs.len());
    //     //     println!("{:?}", stop_seqs);
    //     //     continue;
    //     // }
    //     // println!("Cluster {:?}", stop_seqs);

    //     let seq_inner = stop_seqs.iter().map(|x| x.as_ref()).collect_vec();

    //     println!("Aligning cluster of size {}", stop_seqs.len());
    //     let (consensus, alignments) = align(&seq_inner);

    //     poa::print_alignment(seq_inner.as_slice(), consensus, alignments);
    // }

    Ok(())
}
