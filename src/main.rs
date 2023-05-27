// #![allow(unused_imports)]
// #![allow(dead_code)]
// #![allow(unused_variables)]

use std::{collections::HashMap, hash::Hash, time::Instant};

use binarystore::{Partitionable, PartitionedReader};

use gtfs::{
    GtfsPartitioned, GtfsStore, GtfsZipStore, PartitionedTable, StopTime, TablePartitioner, Trip,
};

use anyhow::Result;
use serde::{de::DeserializeOwned, Serialize};

mod gtfs;

mod csv;

mod binarystore;

impl<K, V> gtfs::PartitionedTable<K, V> for binarystore::PartitionedReader<K, V>
where
    K: DeserializeOwned + 'static,
    V: DeserializeOwned + 'static,
{
    fn get_partition(&self, index: usize) -> Option<Box<dyn Iterator<Item = (K, V)>>> {
        let Some(value) = binarystore::PartitionedReader::get_partition2(&self, index) else {
            return None
        };

        Some(Box::new(value.map(|x| x.unwrap())))
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
        F: Fn(&V) -> K,
        V: Serialize + DeserializeOwned + 'static,
        K: Serialize + DeserializeOwned + Hash + Eq + Clone + 'static,
    {
        let partitioned = iter.disk_partition(num_partitions, key).unwrap();
        Box::new(partitioned)
    }
}

fn main() -> Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let filename = "/Users/artef/Downloads/ntra_import_latest_ntra-in.gtfs.txt.zip";
    // let filename = "/Users/artef/dev/dtfs/local/CATA.gtfs.txt.zip";

    let mut gtfs_store = GtfsZipStore::from_file(filename);

    let gtfs_partitioned = GtfsPartitioned::from_store::<_, BinaryPartitioner>(&mut gtfs_store);

    println!("Iterating");

    for route in gtfs_partitioned.iter() {
        if route.stop_times.len() > 10 {
            for trip in route.trips {
                println!("{:?}", trip);
            }
            for stop_time in route.stop_times {
                println!("{:?}", stop_time);
            }
            break;
        }
    }

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
