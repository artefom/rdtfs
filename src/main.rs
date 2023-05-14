#![allow(unused_imports)]
#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    collections::HashMap,
    fmt::Debug,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Error, Read, Seek, Write},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use base64::{
    engine::{general_purpose, GeneralPurpose},
    Engine,
};
use bigasstable::BigAssTable;
use clap::builder::OsStr;
use csv::{from_file, CsvTableReader};
use datastore::Table;
use gtfs::{GtfsCollection, GtfsZipStore, Pushable, TableFacory};
use serde::Serialize;
use xbus::{EsTrips, StationTimezoneGetter, TripsHit};

use anyhow::{bail, Context, Result};

use masterdata::Masterdata;
use zip::{read::ZipFile, ZipArchive};

use crate::csv::CsvTableWriter;

mod datastore;

mod gtfs;

mod xbus;

mod masterdata;

mod csv;

mod bigasstable;

impl StationTimezoneGetter for Masterdata {
    fn get_station_timezone(&self, station_code: &str) -> Option<&chrono_tz::Tz> {
        self.get_station_timezone(station_code)
    }
}

fn decode_api_key(api_key: &str) -> anyhow::Result<(String, String)> {
    let bytes = general_purpose::STANDARD.decode(api_key)?;

    let decoded_text = std::str::from_utf8(bytes.as_slice())
        .context("Api key is not base64 encoded utf-8 text")?;

    let splitted: Vec<&str> = decoded_text.split(':').into_iter().collect();

    if splitted.len() != 2 {
        bail!("Api key must be '<id>:<key>' encoded in base64")
    }
    return Ok((
        splitted.first().unwrap().to_string(),
        splitted.get(1).unwrap().to_string(),
    ));
}

struct TripsConsumer {
    total_consumed: u64,
    next_print: u64,
}

impl TripsConsumer {
    fn new() -> Self {
        TripsConsumer {
            total_consumed: 0,
            next_print: 0,
        }
    }
    fn consume_next(&mut self, trip: TripsHit) {
        self.total_consumed += 1;
        if self.total_consumed > self.next_print {
            self.next_print = (self.next_print + 5) + (self.next_print + 5) / 5;
            log::info!("Consumed {} trips", self.total_consumed);
        }
    }
}

async fn download_connections() -> Result<()> {
    let (api_id, api_key) =
        decode_api_key("Rk1Uc2NJRUJ1LXY3Q2FoNFQ0eG06M0VvOWZ5ODdUcUM4X1gtVjNEZU1nUQ==")
            .context("Invalid api key")?;

    let mut masterdata = Masterdata::new("http://master-data.prod.internal.distribusion.com");

    log::info!("Getting station timezones");
    masterdata.update_data().await?;

    let trips = EsTrips::new(
        "https://prod-xbus.es.europe-west3.gcp.cloud.es.io",
        "trips",
        api_id.as_str(),
        api_key.as_str(),
        masterdata,
    )
    .context("Could not connect to elasticsearch")?;

    let mut consumer = TripsConsumer::new();

    trips
        .consume_into("FBRA", |x| consumer.consume_next(x))
        .await?;

    Ok(())
}

fn read_connections() {
    let reader: CsvTableReader<gtfs::Route, _> = from_file("connections.csv");

    for item in reader {
        println!("Connection: {:?}", item)
    }
}

fn write_connections<'a, I: IntoIterator<Item = &'a gtfs::Route>>(routes: I) {
    let mut writer: CsvTableWriter<gtfs::Route> = CsvTableWriter::new("connections.csv");
    for route in routes {
        writer.write_row(route);
    }
}

// #[derive(Eq, Hash, PartialEq)]
// enum GtfsFileType {
//     Agency,
//     FeedInfo,
//     Stops,
//     Routes,
//     Trips,
//     StopTimes,
//     Calendar,
//     CalendarDates,
//     TicketingDeepLinks,
//     TicketingIdentifiers,
//     FareAttributes,
//     FareRules,
// }

// fn file_name_to_type(name: &str) -> Option<GtfsFileType> {
//     // Remove extension
//     let file_name: &str = &Path::new(name).file_stem().unwrap().to_string_lossy();
//     let file_type = match file_name {
//         "agency" => GtfsFileType::Agency,
//         "feed_info" => GtfsFileType::FeedInfo,
//         "stops" => GtfsFileType::Stops,
//         "routes" => GtfsFileType::Routes,
//         "trips" => GtfsFileType::Trips,
//         "stop_times" => GtfsFileType::StopTimes,
//         "calendar" => GtfsFileType::Calendar,
//         "calendar_dates" => GtfsFileType::CalendarDates,
//         "ticketing_deep_links" => GtfsFileType::TicketingDeepLinks,
//         "ticketing_identifiers" => GtfsFileType::TicketingIdentifiers,
//         "fare_attributes" => GtfsFileType::FareAttributes,
//         "fare_rules" => GtfsFileType::FareRules,
//         _ => return None,
//     };
//     Some(file_type)
// }

// /// Retrieve file intexes for each of the gtfs file types
// fn get_file_names<'a, R: Read + Seek>(
//     zip: &'a mut ZipArchive<R>,
// ) -> Result<HashMap<GtfsFileType, String>> {
//     let mut mapping: HashMap<GtfsFileType, String> = HashMap::new();

//     for file_idx in 0..zip.len() {
//         let zipped_file = zip.by_index(file_idx).unwrap();

//         let Some(file_type) = file_name_to_type(zipped_file.name()) else {
//             continue
//         };

//         if let Some(value) = mapping.insert(file_type, zipped_file.name().to_string()) {
//             bail!("Duplicate file in zip: {}", zipped_file.name())
//         };
//     }

//     Ok(mapping)
// }

// fn read_zip<P: AsRef<Path>>(path: P) {
//     log::info!("Reading {}", path.as_ref().to_string_lossy());

//     let file = OpenOptions::new().read(true).open(path).unwrap();
//     let reader = BufReader::new(file);

//     let mut zip = zip::ZipArchive::new(reader).unwrap();

//     let indexes = get_file_names(&mut zip).unwrap();

//     let routes_file = indexes.get(&GtfsFileType::Routes).unwrap();

//     let routes_zip = zip.by_name(routes_file).unwrap();

//     let routes_reader: CsvTableReader<gtfs::Route, _> =
//         CsvTableReader::new(BufReader::new(routes_zip));

//     for route in routes_reader {
//         println!("Route: {:?}", route)
//     }

//     // let mut mapping: HashMap<GtfsFileType, i32> = HashMap::new();

//     // for i in 0..zip.len() {
//     //     let zipped_file = zip.by_index(i).unwrap();

//     //     let Some(file_type) = file_name_to_type(zipped_file.enclosed_name().unwrap()) else {
//     //         continue
//     //     };

//     //     mapping.insert(file_type, 1);

//     //     // let mut zipreader = BufReader::new(zipped_file);

//     //     // let mut buf = String::new();
//     //     // zipreader.read_line(&mut buf).unwrap();
//     //     // log::info!("{}", buf);

//     //     // std::io::copy(&mut file, &mut std::io::stdout());
//     // }

//     // mapping
// }

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

async fn async_main() -> Result<()> {
    let mut gtfs_store =
        GtfsZipStore::from_file("/Users/artef/Downloads/ntra_import_latest_ntra-in.gtfs.txt.zip");
    // let mut gtfs_store = GtfsZipStore::from_file("/Users/artef/dev/dtfs/local/CATA.gtfs.txt.zip");

    let gtfs_collection = GtfsCollection::from_store::<_, BigAssTableFactory>(&mut gtfs_store);

    // read_zip("/Users/artef/dev/dtfs/local/CATA.gtfs.txt.zip");

    // let routes = vec![
    //     gtfs::Route::simple("agency-1", "route-a"),
    //     gtfs::Route::simple("agency-1", "route-b"),
    // ];

    // write_connections(&routes);
    // read_connections();

    // log::info!("Fetching connections");
    // let connections = trips.get_connections("FBRA", None).await?;

    // for connection in connections {
    //     log::info!(
    //         "{}; {}; {}; {}",
    //         connection.departure_station.uid,
    //         connection.arrival_station.uid,
    //         connection.departure_time,
    //         connection.arrival_time,
    //     );
    // }

    anyhow::Result::<()>::Ok(())
}

fn main() -> Result<()> {
    env_logger::init_from_env(
        env_logger::Env::default().filter_or(env_logger::DEFAULT_FILTER_ENV, "info"),
    );

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    runtime.block_on(async { async_main().await })?;

    Ok(())
}
