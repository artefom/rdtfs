use std::{
    borrow::Borrow,
    cell::{Ref, RefCell},
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek},
    marker::PhantomData,
    path::Path,
};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;
use zip::{read::ZipFile, ZipArchive};

use crate::csv::CsvTableReader;

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum RouteType {
    Tram = 0,
    Subway = 1,
    Rail = 2,
    Bus = 3,
    Ferry = 4,
    CableTram = 5,
    AerialLift = 6,
    Funicular = 7,
    Trolleybus = 11,
    Monorail = 12,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum ContinuousPickupType {
    ContinuousStoppingPickup = 0,
    NoContinuousStoppingPickup = 1,
    PhoneAgency = 2,
    AskDriver = 3,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum ContinuousDropOffType {
    ContinuousStoppingDropOff = 0,
    NoContinuousStoppingDropOff = 1,
    PhoneAgency = 2,
    AskDriver = 3,
}

#[derive(Debug)]
pub enum Color {
    Hex(String),
}

impl Serialize for Color {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Color::Hex(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Color {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: &str = Deserialize::deserialize(deserializer)?;
        Ok(Color::Hex(value.to_string()))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Route {
    pub route_id: String,
    pub agency_id: String,
    pub route_short_name: Option<String>,
    pub route_long_name: Option<String>,
    pub route_desc: Option<String>,
    pub route_type: RouteType,
    pub route_url: Option<String>,
    pub route_color: Option<Color>,
    pub route_text_color: Option<Color>,
    pub route_sort_order: Option<u32>,
    pub continuous_pickup: Option<ContinuousPickupType>,
    pub continuous_drop_off: Option<ContinuousDropOffType>,
}

impl Route {
    pub fn simple(agency_id: &str, name: &str) -> Self {
        Route {
            route_id: Uuid::new_v4().to_string(),
            agency_id: agency_id.to_string(),
            route_short_name: Some(name.to_string()),
            route_long_name: None,
            route_desc: None,
            route_type: RouteType::Bus,
            route_url: None,
            route_color: Some(Color::Hex("FFFFFF".to_string())),
            route_text_color: Some(Color::Hex("BBBBBB".to_string())),
            route_sort_order: None,
            continuous_pickup: Some(ContinuousPickupType::NoContinuousStoppingPickup),
            continuous_drop_off: Some(ContinuousDropOffType::NoContinuousStoppingDropOff),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Agency {
    pub agency_id: String,
    pub agency_name: String,
    pub agency_url: String,
    pub agency_timezone: String,
    pub agency_lang: Option<String>,
    pub agency_phone: Option<String>,
    pub agency_fare_url: Option<String>,
    pub agency_email: Option<String>,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum StopLocationType {
    StopOrPlatform = 0,
    Station = 1,
    EntranceOrExit = 2,
    GenericNode = 3,
    BoardingArea = 5,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum WheelChairBoardingType {
    NoInformation = 0,
    WheelchairSupported = 1,
    NoWheelchairSupport = 2,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Stop {
    pub stop_id: String,
    pub stop_code: Option<String>,
    pub stop_name: Option<String>,
    pub stop_desc: Option<String>,
    pub stop_lat: Option<f64>,
    pub stop_lon: Option<f64>,
    pub zone_id: Option<String>,
    pub stop_url: Option<String>,
    pub location_type: Option<StopLocationType>,
    pub parent_station: Option<String>,
    pub stop_timezone: Option<String>,
    pub wheelchair_boarding: Option<WheelChairBoardingType>,
    pub level_id: Option<String>,
    pub platform_code: Option<String>,
}

struct GtfsFullRouteInfo {
    route: Route,
}

struct GtfsWriter {}

impl GtfsWriter {
    /// Add full route info into gtfs collection
    fn write_route(route: GtfsFullRouteInfo) {
        todo!()
    }
}

struct BigAssTable<T> {
    count: usize,
    _phantom: PhantomData<T>,
}

impl<T> BigAssTable<T> {
    pub fn new() -> Self {
        BigAssTable {
            count: 0,
            _phantom: PhantomData,
        }
    }

    pub fn push(&mut self, data: T) {
        self.count += 1
    }

    pub fn length(&self) -> usize {
        self.count
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Copy)]
pub enum GtfsFileType {
    Agency,
    FeedInfo,
    Stops,
    Routes,
    Trips,
    StopTimes,
    Calendar,
    CalendarDates,
    TicketingDeepLinks,
    TicketingIdentifiers,
    FareAttributes,
    FareRules,
}

pub trait GtfsStore {
    fn get_readable<'a>(&'a mut self, file_type: GtfsFileType) -> BufReader<Box<dyn Read + 'a>>;
}

pub struct GtfsZipStore {
    archive: ZipArchive<BufReader<File>>,
    file_name_mapping: HashMap<GtfsFileType, String>,
}

fn file_name_to_type(name: &str) -> Option<GtfsFileType> {
    // Remove extension
    let file_name: &str = &Path::new(name).file_stem().unwrap().to_string_lossy();
    let file_type = match file_name {
        "agency" => GtfsFileType::Agency,
        "feed_info" => GtfsFileType::FeedInfo,
        "stops" => GtfsFileType::Stops,
        "routes" => GtfsFileType::Routes,
        "trips" => GtfsFileType::Trips,
        "stop_times" => GtfsFileType::StopTimes,
        "calendar" => GtfsFileType::Calendar,
        "calendar_dates" => GtfsFileType::CalendarDates,
        "ticketing_deep_links" => GtfsFileType::TicketingDeepLinks,
        "ticketing_identifiers" => GtfsFileType::TicketingIdentifiers,
        "fare_attributes" => GtfsFileType::FareAttributes,
        "fare_rules" => GtfsFileType::FareRules,
        _ => return None,
    };
    Some(file_type)
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

        if let Some(value) = mapping.insert(file_type, zipped_file.name().to_string()) {
            bail!("Duplicate file in zip: {}", zipped_file.name())
        };
    }

    Ok(mapping)
}

impl GtfsZipStore {
    pub fn from_file(path: &str) -> Self {
        let file = OpenOptions::new().read(true).open(path).unwrap();
        let reader = BufReader::new(file);

        let mut archive = zip::ZipArchive::new(reader).unwrap();

        let file_name_mapping = get_file_names(&mut archive).unwrap();

        GtfsZipStore {
            archive,
            file_name_mapping,
        }
    }
}

impl GtfsStore for GtfsZipStore {
    fn get_readable<'a>(&'a mut self, file_type: GtfsFileType) -> BufReader<Box<dyn Read + 'a>> {
        let res = self
            .archive
            .by_name(self.file_name_mapping.get(&file_type).unwrap())
            .unwrap();

        BufReader::new(Box::new(res))
    }
}

pub struct GtfsCollection {
    routes: BigAssTable<Route>,
}

impl GtfsCollection {
    /// Create gtfs collection from a readable store
    pub fn from_store<T: GtfsStore>(store: &mut T) -> Self {
        let mut routes: BigAssTable<Route> = BigAssTable::new();
        let mut agencies: BigAssTable<Agency> = BigAssTable::new();
        let mut stops: BigAssTable<Stop> = BigAssTable::new();

        log::info!("Deserializing routes");
        {
            let file = store.get_readable(GtfsFileType::Routes);
            let reader: CsvTableReader<Route, _> = CsvTableReader::new(file);
            for obj in reader {
                routes.push(obj)
            }
        }

        log::info!("Deserializing agencies");
        {
            let file = store.get_readable(GtfsFileType::Agency);
            let reader: CsvTableReader<Agency, _> = CsvTableReader::new(file);
            for obj in reader {
                agencies.push(obj)
            }
        }

        log::info!("Deserializing stops");
        {
            let file = store.get_readable(GtfsFileType::Stops);
            let reader: CsvTableReader<Stop, _> = CsvTableReader::new(file);
            for obj in reader {
                stops.push(obj)
            }
        }

        log::info!("Number of routes: {}", routes.length());
        log::info!("Number of agencies: {}", agencies.length());
        log::info!("Number of stops: {}", stops.length());

        GtfsCollection { routes: routes }
    }
}
