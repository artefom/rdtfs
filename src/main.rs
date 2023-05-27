// #![allow(unused_imports)]
// #![allow(dead_code)]
// #![allow(unused_variables)]

use std::{
    collections::HashMap,
    hash::Hash,
    time::{Duration, Instant},
};

use bigasstable::BigAssTable;

use binarystore::{join, PartitionReader, PartitionedReader};
use gtfs::{GtfsFile, GtfsStore, GtfsZipStore, Pushable, StopTime, TableFacory, Trip};

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};

use crate::{binarystore::PartitionedStoreWriter, gtfs::Stop};

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

fn partition<G, D, K, H>(
    store: &mut G,
    num_partitions: usize,
    key: K,
) -> Result<PartitionedReader<D, H, K>>
where
    G: GtfsStore,
    D: Serialize + DeserializeOwned + GtfsFile,
    K: for<'a> Fn(&'a D) -> &'a H + Clone,
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

    let filename = "/Users/artef/Downloads/ntra_import_latest_ntra-in.gtfs.txt.zip";
    // let filename = "/Users/artef/dev/dtfs/local/CATA.gtfs.txt.zip";

    let mut gtfs_store = GtfsZipStore::from_file(filename);

    let trips = partition(&mut gtfs_store, 10, |x: &Trip| &x.route_id)?;

    println!("Number of trips: {}", trips.len());

    let mut trip_id_x_route_id: HashMap<String, String> = HashMap::new();

    println!("Iterating trips");

    for trip in trips.iter() {
        let trip = trip.unwrap();

        trip_id_x_route_id.insert(trip.trip_id, trip.route_id);
    }

    println!("Number of trips: {}", trip_id_x_route_id.len());

    let stop_times = partition(&mut gtfs_store, 10, |x: &StopTime| {
        trip_id_x_route_id.get(&x.trip_id).unwrap()
    })?;

    // let joined = join(stop_times, trips)?;

    // for item in joined {
    //     println!("{:?}", item);
    //     break;
    // }

    // let stop_times: PartitionedReader<StopTime> = (&mut gtfs_store).into();
    // let trips: PartitionedReader<Trip> = (&mut gtfs_store).into();

    // for item in joined {
    //     println!("Item: {:?}", item);
    //     break;
    // }

    // for item in gtfs_store.get::<Stop>()? {
    //     println!("item: {:?}", item);
    // }

    // join2(gtfs_store.get::<StopTime>()?, gtfs_store.get::<Trip>()?);

    // let join_reader = join(gtfs_store.get::<StopTime>(), |x| x.stop, todo!(), todo!());

    // let mut stop_times = PartitionReader::<Stop>::new(100)?;

    // // Write into partitioned writer
    // gtfs_store
    //     .scan(|item| {
    //         match partitioned_stop_times.write(&item, |x| &x.stop_code) {
    //             Ok(_) => (),
    //             Err(err) => println!("Error during write: {:?}", err),
    //         };
    //     })
    //     .context("Could not scan")?;

    // let mut reader = partitioned_stop_times.into_reader()?;

    // println!("Partition 1");
    // for item in reader.next_partition()?.unwrap() {
    //     println!("{:?}", item)
    // }

    // let _gtfs_collection = GtfsCollection::from_store::<_, BigAssTableFactory>(&mut gtfs_store);
    Ok(())
}
