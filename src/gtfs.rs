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
use chrono::NaiveDate;
use rust_decimal::Decimal;
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
    pub ticketing_deep_link_id: Option<String>,
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
            ticketing_deep_link_id: None,
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
    pub ticketing_deep_link_id: Option<String>,
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

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum TicketingType {
    Available = 0,
    Unavailable = 1,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum TripDirection {
    Outbound = 0,
    Inbound = 1,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum BikesAllowedType {
    NoInformation = 0,
    BikesAllowed = 1,
    NoBikesAllowed = 2,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Trip {
    pub route_id: String,
    pub service_id: String,
    pub trip_id: String,
    pub trip_headsign: Option<String>,
    pub trip_short_name: Option<String>,
    pub direction_id: Option<TripDirection>,
    pub block_id: Option<String>,
    pub shape_id: Option<String>,
    pub wheelchair_accessible: Option<WheelChairBoardingType>,
    pub bikes_allowed: Option<BikesAllowedType>,
    pub trip_ticketing_id: Option<String>,
    pub ticketing_type: Option<TicketingType>,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum StopPickupType {
    RegularPickup = 0,
    NoPickup = 1,
    PhoneAgency = 2,
    AskDriver = 3,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum StopDropOffType {
    RegularDropOff = 0,
    NoDropOff = 1,
    PhoneAgency = 2,
    AskDriver = 3,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum TimePointType {
    Aproximate = 0,
    Exact = 1,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StopTime {
    pub trip_id: String,
    pub arrival_time: Option<String>,
    pub departure_time: Option<String>,
    pub stop_id: String,
    pub stop_sequence: u64,
    pub stop_headsign: Option<String>,
    pub pickup_type: Option<StopPickupType>,
    pub drop_off_type: Option<StopDropOffType>,
    pub continuous_pickup: Option<ContinuousPickupType>,
    pub continuous_drop_off: Option<ContinuousDropOffType>,
    pub shape_dist_traveled: Option<f64>,
    pub timepoint: Option<TimePointType>,
    pub ticketing_type: Option<TicketingType>,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum ServiceAvailability {
    SeriviceAvailable = 1,
    SeriviceNotAvailable = 0,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Calendar {
    service_id: String,
    start_date: String,
    end_date: String,
    monday: ServiceAvailability,
    tuesday: ServiceAvailability,
    wednesday: ServiceAvailability,
    thursday: ServiceAvailability,
    friday: ServiceAvailability,
    saturday: ServiceAvailability,
    sunday: ServiceAvailability,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum SerivceExceptionType {
    Added = 1,
    Removed = 2,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CalendarDate {
    service_id: String,
    date: String,
    exception_type: SerivceExceptionType,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum PaymentMethod {
    PaidOnBoard = 0,
    PaidBeforeBoard = 1,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
pub enum TransfersIncluded {
    NoTransfersPermitted = 0,
    TransferOnce = 1,
    TransferTwice = 2,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FareAttribute {
    fare_id: String,
    price: f64,
    currency_type: String,
    payment_method: PaymentMethod,
    transfers: Option<TransfersIncluded>,
    agency_id: Option<String>,
    transfer_duration: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FareRule {
    fare_id: String,
    route_id: Option<String>,
    origin_id: Option<String>,
    destination_id: Option<String>,
    contains_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Shape {
    shape_id: String,
    shape_pt_lat: f64,
    shape_pt_lon: f64,
    shape_pt_sequence: u64,
    shape_dist_traveled: Option<f64>,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
enum ExactTimesType {
    FrequencyBased = 0,
    ExactSameHeadway = 1,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Frequency {
    trip_id: String,
    start_time: String,
    end_time: String,
    headway_secs: u64,
    exact_times: Option<ExactTimesType>,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
enum TransferType {
    Recommended = 0,
    TimedTransfer = 1,
    WaitForTransfer = 2,
    TransferNotPossible = 3,
    InSeatTransfer = 4,
    ReboardTransfer = 5,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Transfer {
    from_stop_id: String,
    to_stop_id: String,
    transfer_type: TransferType,
    min_transfer_time: Option<u64>,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
enum PathwayMode {
    Walkway = 1,
    Stairs = 2,
    Travelator = 3,
    Escalator = 4,
    Elevator = 5,
    FareGate = 6,
    ExitGate = 7,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
enum BidirectionalType {
    Unidirectional = 0,
    Bidirectional = 1,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PathWay {
    pathway_id: String,
    from_stop_id: String,
    to_stop_id: String,
    pathway_mode: PathwayMode,
    is_bidirectional: BidirectionalType,
    length: Option<f64>,
    traversal_time: Option<u64>,
    stair_count: Option<u64>,
    max_slope: Option<f64>,
    min_width: Option<f64>,
    signposted_as: Option<String>,
    reversed_signposted_as: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Level {
    level_id: String,
    level_index: f64,
    level_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FeedInfo {
    feed_publisher_name: String,
    feed_publisher_url: String,
    feed_lang: String,
    default_lang: Option<String>,
    feed_start_date: Option<String>,
    feed_end_date: Option<String>,
    feed_version: Option<String>,
    feed_contact_email: Option<String>,
    feed_contact_url: Option<String>,
}

#[derive(Debug, Deserialize_repr, Serialize_repr)]
#[repr(u8)]
enum TableName {
    #[serde(rename = "agency")]
    Agency,
    #[serde(rename = "stops")]
    Stops,
    #[serde(rename = "routes")]
    Routes,
    #[serde(rename = "trips")]
    Trips,
    #[serde(rename = "stop_times")]
    StopTimes,
    #[serde(rename = "feed_info")]
    FeedInfo,
    #[serde(rename = "pathways")]
    Pathways,
    #[serde(rename = "levels")]
    Levels,
    #[serde(rename = "attributions")]
    Attributions,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Translation {
    table_name: TableName,
    field_name: String,
    language: String,
    translation: String,
    record_id: Option<String>,
    record_sub_id: Option<String>,
    field_value: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Attribution {
    attribution_id: Option<String>,
    agency_id: Option<String>,
    route_id: Option<String>,
    trip_id: Option<String>,
    organization_name: String,
    is_producer: u8,
    is_operator: u8,
    is_authority: u8,
    attribution_url: Option<String>,
    attribution_email: Option<String>,
    attribution_phone: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TicketingIdentifier {}

#[derive(Debug, Serialize, Deserialize)]
pub struct TicketingDeepLink {}

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
    Shapes,
    Frequencies,
    Transfers,
    Pathways,
    Levels,
    Translations,
    Attributions,
}

pub trait GtfsStore {
    fn get_readable<'a>(
        &'a mut self,
        file_type: GtfsFileType,
    ) -> Option<BufReader<Box<dyn Read + 'a>>>;
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
    fn get_readable<'a>(
        &'a mut self,
        file_type: GtfsFileType,
    ) -> Option<BufReader<Box<dyn Read + 'a>>> {
        let Some(filename) = self.file_name_mapping.get(&file_type) else {
            return None
        };

        let res = self.archive.by_name(filename).unwrap();

        Some(BufReader::new(Box::new(res)))
    }
}

pub struct GtfsCollection {
    agency: BigAssTable<Agency>,
    stops: BigAssTable<Stop>,
    routes: BigAssTable<Route>,
    trips: BigAssTable<Trip>,
    stop_times: BigAssTable<StopTime>,
    calendar: Option<BigAssTable<Calendar>>,
    calendar_dates: Option<BigAssTable<CalendarDate>>,
    fare_attributes: Option<BigAssTable<FareAttribute>>,
    fare_rules: Option<BigAssTable<FareRule>>,
    shapes: Option<BigAssTable<Shape>>,
    frequencies: Option<BigAssTable<Frequency>>,
    transfers: Option<BigAssTable<Transfer>>,
    pathways: Option<BigAssTable<PathWay>>,
    levels: Option<BigAssTable<Level>>,
    feed_info: Option<BigAssTable<FeedInfo>>,
    translations: Option<BigAssTable<Translation>>,
    attributions: Option<BigAssTable<Attribution>>,
    ticketing_identifiers: Option<BigAssTable<TicketingIdentifier>>,
    ticketing_deep_links: Option<BigAssTable<TicketingDeepLink>>,
}

impl GtfsCollection {
    /// Create gtfs collection from a readable store
    pub fn from_store<T: GtfsStore>(store: &mut T) -> Result<Self> {
        let agency = if let Some(file) = store.get_readable(GtfsFileType::Agency) {
            log::info!("Reading agencies");
            let reader: CsvTableReader<Agency, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            table
        } else {
            bail!("Agencies table not found in source")
        };

        let stops = if let Some(file) = store.get_readable(GtfsFileType::Stops) {
            log::info!("Reading stops");
            let reader: CsvTableReader<Stop, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            table
        } else {
            bail!("Stops table not found in source")
        };

        let routes = if let Some(file) = store.get_readable(GtfsFileType::Routes) {
            log::info!("Reading routes");
            let reader: CsvTableReader<Route, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            table
        } else {
            bail!("Routes table not found in source")
        };

        let trips = if let Some(file) = store.get_readable(GtfsFileType::Trips) {
            log::info!("Reading trips");
            let reader: CsvTableReader<Trip, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            table
        } else {
            bail!("Routes table not found in source")
        };

        let stop_times = if let Some(file) = store.get_readable(GtfsFileType::StopTimes) {
            log::info!("Reading stop times");
            let reader: CsvTableReader<StopTime, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            table
        } else {
            bail!("Routes table not found in source")
        };

        let calendar = if let Some(file) = store.get_readable(GtfsFileType::Calendar) {
            log::info!("Reading calendar");
            let reader: CsvTableReader<Calendar, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let calendar_dates = if let Some(file) = store.get_readable(GtfsFileType::CalendarDates) {
            log::info!("Reading calendar dates");
            let reader: CsvTableReader<CalendarDate, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let fare_attributes = if let Some(file) = store.get_readable(GtfsFileType::FareAttributes) {
            log::info!("Reading fare attributes");
            let reader: CsvTableReader<FareAttribute, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let fare_rules = if let Some(file) = store.get_readable(GtfsFileType::FareRules) {
            log::info!("Reading fare rules");
            let reader: CsvTableReader<FareRule, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let shapes = if let Some(file) = store.get_readable(GtfsFileType::Shapes) {
            log::info!("Reading shapes");
            let reader: CsvTableReader<Shape, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let frequencies = if let Some(file) = store.get_readable(GtfsFileType::Frequencies) {
            log::info!("Reading frequencies");
            let reader: CsvTableReader<Frequency, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let transfers = if let Some(file) = store.get_readable(GtfsFileType::Transfers) {
            log::info!("Reading transfers");
            let reader: CsvTableReader<Transfer, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let pathways = if let Some(file) = store.get_readable(GtfsFileType::Pathways) {
            log::info!("Reading pathways");
            let reader: CsvTableReader<PathWay, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let levels = if let Some(file) = store.get_readable(GtfsFileType::Levels) {
            log::info!("Reading levels");
            let reader: CsvTableReader<Level, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let feed_info = if let Some(file) = store.get_readable(GtfsFileType::FeedInfo) {
            log::info!("Reading feed info");
            let reader: CsvTableReader<FeedInfo, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let translations = if let Some(file) = store.get_readable(GtfsFileType::Translations) {
            log::info!("Reading translations");
            let reader: CsvTableReader<Translation, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let attributions = if let Some(file) = store.get_readable(GtfsFileType::Attributions) {
            log::info!("Reading attributions");
            let reader: CsvTableReader<Attribution, _> = CsvTableReader::new(file);
            let mut table = BigAssTable::new();
            for obj in reader {
                table.push(obj)
            }
            Some(table)
        } else {
            None
        };

        let ticketing_identifier =
            if let Some(file) = store.get_readable(GtfsFileType::TicketingIdentifiers) {
                log::info!("Reading ticketing identifiers");
                let reader: CsvTableReader<TicketingIdentifier, _> = CsvTableReader::new(file);
                let mut table = BigAssTable::new();
                for obj in reader {
                    table.push(obj)
                }
                Some(table)
            } else {
                None
            };

        let ticketing_deep_links =
            if let Some(file) = store.get_readable(GtfsFileType::TicketingDeepLinks) {
                log::info!("Reading ticketing deep links");
                let reader: CsvTableReader<TicketingDeepLink, _> = CsvTableReader::new(file);
                let mut table = BigAssTable::new();
                for obj in reader {
                    table.push(obj)
                }
                Some(table)
            } else {
                None
            };

        Ok(GtfsCollection {
            agency: agency,
            stops: stops,
            routes: routes,
            trips: trips,
            stop_times: stop_times,
            calendar: calendar,
            calendar_dates: calendar_dates,
            fare_attributes: fare_attributes,
            fare_rules: fare_rules,
            shapes: shapes,
            frequencies: frequencies,
            transfers: transfers,
            pathways: pathways,
            levels: levels,
            feed_info: feed_info,
            translations: translations,
            attributions: attributions,
            ticketing_identifiers: ticketing_identifier,
            ticketing_deep_links: ticketing_deep_links,
        })
    }
}
