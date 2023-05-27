// #![allow(unused_imports)]
// #![allow(dead_code)]
// #![allow(unused_variables)]

use std::{collections::HashMap, hash::Hash, time::Instant};

use bigasstable::BigAssTable;

use binarystore::{join, PartitionedReader, PartitionedStoreWriter};
use gtfs::{GtfsFile, GtfsStore, GtfsZipStore, Pushable, StopTime, TableFacory, Trip};

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};

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

fn partition<'a, G, D, K, H>(
    store: &mut G,
    num_partitions: usize,
    key: K,
) -> Result<PartitionedReader<D>>
where
    G: GtfsStore,
    D: Serialize + DeserializeOwned + GtfsFile,
    K: Fn(&D) -> H,
    H: Hash + Eq + Clone,
{
    let reader = store.get_table_reader::<D>()?;

    let mut table = PartitionedStoreWriter::new(num_partitions, key)?;

    for item in reader {
        let item = item.context("Could not read item")?;
        table.write(&item)?;
    }

    table.into_reader()
}

fn main() -> Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    // let filename = "/Users/artef/Downloads/ntra_import_latest_ntra-in.gtfs.txt.zip";
    let filename = "/Users/artef/dev/dtfs/local/CATA.gtfs.txt.zip";

    let mut gtfs_store = GtfsZipStore::from_file(filename);

    let trip_partition_start = Instant::now();

    let trips = partition(&mut gtfs_store, 10, |x: &Trip| x.route_id.clone())?;

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

    let stop_times = partition(&mut gtfs_store, 10, |x: &StopTime| {
        trip_id_x_route_id.get(&x.trip_id).unwrap().clone()
    })?;

    let stop_times_partition_end = Instant::now();

    println!("Number of stop times: {}", stop_times.len());

    let join_start = Instant::now();

    let joined = join(
        &stop_times,
        &trips,
        |x| trip_id_x_route_id.get(&x.trip_id).unwrap().clone(),
        |x| x.route_id.clone(),
    )?;

    for item in joined {
        println!("{:?}", item);
        break;
    }

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

    Ok(())
}
