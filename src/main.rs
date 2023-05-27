// #![allow(unused_imports)]
// #![allow(dead_code)]
// #![allow(unused_variables)]

use std::{
    collections::HashMap,
    hash::Hash,
    io::{BufRead, Read},
    time::Instant,
};

use bigasstable::BigAssTable;

use binarystore::{join, PartitionedReader, PartitionedStoreWriter};
use csv::CsvTableReader;
use gtfs::{GtfsFile, GtfsStore, GtfsZipStore, Pushable, StopTime, TableFacory, Trip};

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};

use crate::binarystore::Partitionable;

mod gtfs;

mod csv;

mod bigasstable;
mod binarystore;

struct BigAssTableFactory {}

impl<I> Pushable<I> for BigAssTable<I> {
    fn push(&mut self, item: I) {
        BigAssTable::push(self, item);
    }

    fn length(&self) -> usize {
        BigAssTable::length(self)
    }
}

impl TableFacory for BigAssTableFactory {
    fn new<I: 'static>() -> Box<dyn gtfs::Pushable<I>> {
        Box::new(BigAssTable::<I>::new())
    }
}

fn main() -> Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let filename = "/Users/artef/Downloads/ntra_import_latest_ntra-in.gtfs.txt.zip";
    // let filename = "/Users/artef/dev/dtfs/local/CATA.gtfs.txt.zip";

    let mut gtfs_store = GtfsZipStore::from_file(filename);

    let trip_partition_start = Instant::now();

    let trips = gtfs_store
        .get_table_reader()?
        .map(|x| x.unwrap())
        .disk_partition(10, |x: &Trip| x.route_id.clone())?;

    let trip_partition_end = Instant::now();

    println!("Number of trips: {}", trips.len());

    let mut trip_id_x_route_id: HashMap<String, String> = HashMap::new();

    println!("Iterating trips");

    let trip_iteration_start = Instant::now();

    for trip in trips.iter() {
        let trip = trip.unwrap();

        trip_id_x_route_id.insert(trip.trip_id, trip.route_id);
    }

    let trip_iteration_end = Instant::now();

    println!("Number of trips: {}", trip_id_x_route_id.len());

    println!("Partitioning stop times");

    let stop_times_partition_start = Instant::now();

    let stop_times = gtfs_store
        .get_table_reader()?
        .map(|x| x.unwrap())
        .disk_partition(10, |x: &StopTime| {
            trip_id_x_route_id.get(&x.trip_id).unwrap().clone()
        })?;

    let stop_times_partition_end = Instant::now();

    println!("Number of stop times: {}", stop_times.len());

    let join_start = Instant::now();

    let joined = join(&stop_times, &trips)?;

    let mut count = 0;
    for _ in joined {
        count += 1;
    }
    println!("Total joined: {}", count);

    let join_end = Instant::now();

    println!(
        "Partition trips took {:?}",
        trip_partition_end - trip_partition_start
    );
    println!(
        "Trips indexing took {:?}",
        trip_iteration_end - trip_iteration_start
    );
    println!(
        "Stop times partitioning took {:?}",
        stop_times_partition_end - stop_times_partition_start
    );
    println!("Join took {:?}", join_end - join_start);
    println!("Total time: {:?}", join_end - trip_partition_start);

    println!("Done");

    // For CATA
    // Number of trips: 4177
    // Iterating trips
    // Number of trips: 4177
    // Partitioning stop times
    // Number of stop times: 19419
    // Total joined: 2047
    // Partition trips took 12.234875ms
    // Trips indexing took 2.882292ms
    // Stop times partitioning took 40.768125ms
    // Join took 10.867459ms
    // Total time: 66.766709ms

    // Number of trips: 800752
    // Iterating trips
    // Number of trips: 800752
    // Partitioning stop times
    // Number of stop times: 15377055
    // Total joined: 2558
    // Partition trips took 1.218865333s
    // Trips indexing took 351.40575ms
    // Stop times partitioning took 19.062396834s
    // Join took 7.349318416s
    // Total time: 27.982005208s

    Ok(())
}
