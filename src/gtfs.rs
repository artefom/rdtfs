/// Module for reading gtfs collection
///
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    hash::Hash,
    io::{BufRead, BufReader, Read, Seek},
    path::Path,
};

use anyhow::{bail, Result};

use indicatif::{ProgressBar, ProgressStyle};

use serde::{de::DeserializeOwned, Deserialize, Serialize};

use serde_repr::{Deserialize_repr, Serialize_repr};
use zip::ZipArchive;

use crate::csv::CsvTableReader;

use join::JoinReader;

mod join;
pub use join::PartitionedTable;

use self::csv_models::{GtfsFileType, GtfsFile, StopTime, Trip, Route};

mod csv_models;

pub trait GtfsStore {
    fn get_readable<'a>(&'a mut self, file_type: GtfsFileType) -> Option<Box<dyn BufRead + 'a>>;

    fn get_table_reader<'a, D: DeserializeOwned + GtfsFile>(
        &'a mut self,
    ) -> Result<CsvTableReader<Box<dyn BufRead + 'a>, D>> {
        let file_type = D::get_file_type();
        let read = self.get_readable(file_type);
        let Some(read) = read else {
                bail!("File {} not found", file_type.file_name())
            };
        let reader = CsvTableReader::<_, D>::new(read);
        Ok(reader)
    }
}

pub struct GtfsZipStore {
    archive: ZipArchive<File>,
    file_name_mapping: HashMap<GtfsFileType, String>,
}

fn file_name_to_type(name: &str) -> Option<GtfsFileType> {
    // Remove extension
    let file_name: &str = &Path::new(name).file_stem().unwrap().to_string_lossy();
    GtfsFileType::from_filename(file_name)
}

/// Retrieve file intexes for each of the gtfs file types
fn get_file_names<'a, R: Read + Seek>(
    zip: &'a mut ZipArchive<R>,
) -> Result<HashMap<GtfsFileType, String>> {
    let mut mapping: HashMap<GtfsFileType, String> = HashMap::new();

    for file_idx in 0..zip.len() {
        let zipped_file = zip.by_index(file_idx).unwrap();

        let Some(file_type) = file_name_to_type(zipped_file.name()) else {
            continue
        };

        if let Some(_value) = mapping.insert(file_type, zipped_file.name().to_string()) {
            bail!("Duplicate file in zip: {}", zipped_file.name())
        };
    }

    Ok(mapping)
}

/// Reads data and reports progress
struct ProgressReader<F> {
    file: F,
    bar: ProgressBar,
}

impl<F> ProgressReader<F> {
    fn new(file: F, total_size: u64) -> Self {
        let progress = ProgressBar::new(total_size);

        progress.set_style(
            ProgressStyle::with_template(
                "{bar:40.cyan/blue} {bytes:>7}/{total_bytes:7} {binary_bytes_per_sec} [ETA: {eta}] {msg}",
            )
            .unwrap()
            .progress_chars("##-"),
        );

        ProgressReader {
            file,
            bar: progress,
        }
    }
}

impl<F: Read> Read for ProgressReader<F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.file.read(buf)
    }
}

impl<F: BufRead> BufRead for ProgressReader<F> {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.file.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        match TryInto::<u64>::try_into(amt) {
            Ok(value) => self.bar.inc(value),
            Err(_) => self.bar.inc(u64::MAX),
        };

        self.file.consume(amt);
    }
}

impl GtfsZipStore {
    pub fn from_file(path: &str) -> Self {
        let file = OpenOptions::new().read(true).open(path).unwrap();

        let mut archive = zip::ZipArchive::new(file).unwrap();

        let file_name_mapping = get_file_names(&mut archive).unwrap();

        GtfsZipStore {
            archive,
            file_name_mapping,
        }
    }
}

impl GtfsStore for GtfsZipStore {
    fn get_readable<'a>(&'a mut self, file_type: GtfsFileType) -> Option<Box<dyn BufRead + 'a>> {
        let Some(filename) = self.file_name_mapping.get(&file_type) else {
            return None
        };

        let res = self.archive.by_name(filename).unwrap();

        let total_size = res.size();

        let progress_reader = Box::new(ProgressReader::new(BufReader::new(res), total_size));

        Some(progress_reader)
    }
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

        let routes = P::partition(
            store
                .get_table_reader::<Route>()
                .unwrap()
                .map(|x| x.unwrap()),
            num_partitions,
            |route| route_keys.map_id(route.route_id.clone()),
        );

        let trips = P::partition(
            store
                .get_table_reader::<Trip>()
                .unwrap()
                .map(|x| x.unwrap()),
            num_partitions,
            |trip| {
                let route_id = route_keys.map_id(trip.route_id.clone());
                trip_id_x_route_id.insert(trip.trip_id.clone(), route_id);
                route_id
            },
        );

        let stop_times = P::partition(
            store
                .get_table_reader::<StopTime>()
                .unwrap()
                .map(|x| x.unwrap()),
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
