// #![allow(unused_imports)]
// #![allow(dead_code)]
// #![allow(unused_variables)]

use std::{fs::OpenOptions, hash::Hash, io::BufReader, path::Path};

use binarystore::Partitionable;

use gtfs::{to_midnights, to_stop_time, GtfsPartitioned, KeyStore, StopTime, TablePartitioner};

use anyhow::Result;
use rides::Ride;
use serde::{de::DeserializeOwned, Serialize};

use progress::ProgressReader;

use csv::CsvTableReader;

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
) -> Vec<Ride> {
    let mut rides = Vec::new();
    let timezone: chrono_tz::Tz = agency.agency_timezone.parse().unwrap();

    for date in to_midnights(calendar, calendar_dates) {
        let mut stops = Vec::new();

        for stop_time in &stop_times {
            let station = station_ids.map_id(stop_time.stop_id.clone());
            let (arrival, departure) = to_stop_time(timezone, date, stop_time);
            stops.push(rides::Stop {
                station,
                arrival,
                departure,
            });
        }

        rides.push(rides::Ride { stops })
    }

    rides
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

    for route in gtfs_partitioned.iter() {
        let mut enough_stop_times: bool = false;
        if route.trips.len() <= 1 || route.trips.len() > 3 {
            continue;
        };
        for trip in &route.trips {
            if trip.stop_times.len() > 3 {
                enough_stop_times = true;
                break;
            }
        }

        if !enough_stop_times {
            continue;
        }

        println!("{}", route.agency);
        println!("{}", route.route);

        for trip in route.trips {
            let rides = to_rides(
                &mut station_ids,
                &route.agency,
                trip.stop_times,
                trip.calendar,
                trip.calendar_dates,
            );

            for ride in rides {
                println!("{:?}", ride);
            }
        }

        break;

        // if route.stop_times.len() > 3 {
        //     println!("{:?}", route.agency);
        //     println!("{:?}", route.route);

        //     for trip in route.trips {
        //         println!("{:?}", trip);
        //     }
        //     for stop_time in route.stop_times {
        //         println!("{:?}", stop_time);
        //     }
        //     for stop_time in route.fare_rules {
        //         println!("{:?}", stop_time);
        //     }
        //     for stop_time in route.fare_attributes {
        //         println!("{:?}", stop_time);
        //     }
        //     // for shape in route.shapes {
        //     //     println!("{:?}", shape);
        //     // }
        //     break;
        // }
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
