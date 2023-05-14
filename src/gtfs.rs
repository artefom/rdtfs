/// Module for reading gtfs collection
///
use std::{
    borrow::Borrow,
    cell::{Ref, RefCell},
    collections::HashMap,
    fmt::Display,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek},
    marker::PhantomData,
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use chrono::NaiveDate;
use indicatif::{ProgressBar, ProgressStyle};
use rust_decimal::Decimal;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::value;
use serde_repr::{Deserialize_repr, Serialize_repr};
use uuid::Uuid;
use zip::{read::ZipFile, ZipArchive};

use crate::csv::CsvTableReader;

pub trait GtfsFile {
    fn get_file_type() -> GtfsFileType;
}

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

impl GtfsFile for Route {
    fn get_file_type() -> GtfsFileType {
        return GtfsFileType::Routes;
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

impl GtfsFile for Agency {
    fn get_file_type() -> GtfsFileType {
        return GtfsFileType::Agencies;
    }
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

impl GtfsFile for Stop {
    fn get_file_type() -> GtfsFileType {
        return GtfsFileType::Stops;
    }
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

impl GtfsFile for Trip {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Trips
    }
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

impl GtfsFile for StopTime {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::StopTimes
    }
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

impl GtfsFile for Calendar {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Calendars
    }
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

impl GtfsFile for CalendarDate {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::CalendarDates
    }
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

impl GtfsFile for FareAttribute {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::FareAttributes
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FareRule {
    fare_id: String,
    route_id: Option<String>,
    origin_id: Option<String>,
    destination_id: Option<String>,
    contains_id: Option<String>,
}

impl GtfsFile for FareRule {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::FareRules
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Shape {
    shape_id: String,
    shape_pt_lat: f64,
    shape_pt_lon: f64,
    shape_pt_sequence: u64,
    shape_dist_traveled: Option<f64>,
}

impl GtfsFile for Shape {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Shapes
    }
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

impl GtfsFile for Frequency {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Frequencies
    }
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

impl GtfsFile for Transfer {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Transfers
    }
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

impl GtfsFile for PathWay {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Pathways
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Level {
    level_id: String,
    level_index: f64,
    level_name: Option<String>,
}

impl GtfsFile for Level {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Levels
    }
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

impl GtfsFile for FeedInfo {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::FeedInfos
    }
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

impl GtfsFile for Translation {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Translations
    }
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

impl GtfsFile for Attribution {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Attributions
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TicketingIdentifier {
    ticketing_stop_id: String,
    stop_id: String,
    agency_id: String,
}

impl GtfsFile for TicketingIdentifier {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::TicketingIdentifiers
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TicketingDeepLink {
    ticketing_deep_link_id: String,
    web_url: Option<String>,
    android_intent_uri: Option<String>,
    ios_universal_link_url: Option<String>,
}

impl GtfsFile for TicketingDeepLink {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::TicketingDeepLinks
    }
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

#[derive(Eq, Hash, PartialEq, Clone, Copy)]
pub enum GtfsFileType {
    Agencies,
    FeedInfos,
    Stops,
    Routes,
    Trips,
    StopTimes,
    Calendars,
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

impl GtfsFileType {
    fn file_name(&self) -> &str {
        use GtfsFileType::*;
        match self {
            Agencies => "agency",
            FeedInfos => "feed_info",
            Stops => "stops",
            Routes => "routes",
            Trips => "trips",
            StopTimes => "stop_times",
            Calendars => "calendar",
            CalendarDates => "calendar_dates",
            TicketingDeepLinks => "ticketing_deep_links",
            TicketingIdentifiers => "ticketing_identifiers",
            FareAttributes => "fare_attributes",
            FareRules => "fare_rules",
            Shapes => "shapes",
            Frequencies => "frequencies",
            Transfers => "transfers",
            Pathways => "pathways",
            Levels => "levels",
            Translations => "translations",
            Attributions => "attributions",
        }
    }

    fn from_filename(name: &str) -> Option<Self> {
        use GtfsFileType::*;
        Some(match name {
            "agency" => Agencies,
            "feed_info" => FeedInfos,
            "stops" => Stops,
            "routes" => Routes,
            "trips" => Trips,
            "stop_times" => StopTimes,
            "calendar" => Calendars,
            "calendar_dates" => CalendarDates,
            "ticketing_deep_links" => TicketingDeepLinks,
            "ticketing_identifiers" => TicketingIdentifiers,
            "fare_attributes" => FareAttributes,
            "fare_rules" => FareRules,
            "shapes" => Shapes,
            "frequencies" => Frequencies,
            "transfers" => Transfers,
            "pathways" => Pathways,
            "levels" => Levels,
            "translations" => Translations,
            "attributions" => Attributions,
            _ => {
                log::warn!("Unkown filename: {}", name);
                return None;
            }
        })
    }
}

pub trait GtfsStore {
    fn get_readable<'a>(&'a mut self, file_type: GtfsFileType) -> Option<Box<dyn BufRead + 'a>>;

    fn decompress<'a, I: DeserializeOwned + GtfsFile + 'static, F: TableFacory>(
        &mut self,
    ) -> Result<Box<dyn Pushable<I>>> {
        let file_type = I::get_file_type();
        let read = self.get_readable(file_type);

        let Some(read) = read else {
            bail!("File {} not found", file_type.file_name())
        };
        println!("Decompressing {}", file_type.file_name());
        let reader: CsvTableReader<I, _> = CsvTableReader::new(read);
        let mut table = F::new();
        for obj in reader {
            table.push(obj)
        }
        println!("  Found {} items", table.length());
        Ok(table)
    }

    fn try_decompress<'a, I: DeserializeOwned + GtfsFile + 'static, F: TableFacory>(
        &mut self,
    ) -> Option<Box<dyn Pushable<I>>> {
        match self.decompress::<I, F>() {
            Ok(value) => Some(value),
            Err(value) => None,
        }
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

        if let Some(value) = mapping.insert(file_type, zipped_file.name().to_string()) {
            bail!("Duplicate file in zip: {}", zipped_file.name())
        };
    }

    Ok(mapping)
}

/// Reads data and reports progress
struct ProgressReader<F> {
    consumed: u64,
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
            consumed: 0,
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

pub trait Pushable<I> {
    fn push(&mut self, item: I);
    fn length(&self) -> usize;
}

pub trait TableFacory {
    fn new<I: 'static>() -> Box<dyn Pushable<I>>;
}

pub struct GtfsCollection {
    agency: Box<dyn Pushable<Agency>>,
    stops: Box<dyn Pushable<Stop>>,
    routes: Box<dyn Pushable<Route>>,
    trips: Box<dyn Pushable<Trip>>,
    stop_times: Box<dyn Pushable<StopTime>>,
    calendar: Option<Box<dyn Pushable<Calendar>>>,
    calendar_dates: Option<Box<dyn Pushable<CalendarDate>>>,
    fare_attributes: Option<Box<dyn Pushable<FareAttribute>>>,
    fare_rules: Option<Box<dyn Pushable<FareRule>>>,
    shapes: Option<Box<dyn Pushable<Shape>>>,
    frequencies: Option<Box<dyn Pushable<Frequency>>>,
    transfers: Option<Box<dyn Pushable<Transfer>>>,
    pathways: Option<Box<dyn Pushable<PathWay>>>,
    levels: Option<Box<dyn Pushable<Level>>>,
    feed_info: Option<Box<dyn Pushable<FeedInfo>>>,
    translations: Option<Box<dyn Pushable<Translation>>>,
    attributions: Option<Box<dyn Pushable<Attribution>>>,
    ticketing_identifiers: Option<Box<dyn Pushable<TicketingIdentifier>>>,
    ticketing_deep_links: Option<Box<dyn Pushable<TicketingDeepLink>>>,
}

fn decompress<'a, I: DeserializeOwned + 'static, F: TableFacory>(
    read: Option<Box<dyn BufRead + 'a>>,
) -> Result<Box<dyn Pushable<I>>> {
    let Some(read) = read else {
        bail!("File not found")
    };
    log::info!("Decompressing items");
    let reader: CsvTableReader<I, _> = CsvTableReader::new(read);
    let mut table = F::new();
    for obj in reader {
        table.push(obj)
    }
    log::info!("Found {} items", table.length());
    Ok(table)
}

fn try_decompress<'a, I: DeserializeOwned + 'static, F: TableFacory>(
    read: Option<Box<dyn BufRead + 'a>>,
) -> Option<Box<dyn Pushable<I>>> {
    match decompress::<I, F>(read) {
        Ok(value) => Some(value),
        Err(value) => None,
    }
}

impl GtfsCollection {
    /// Create gtfs collection from a readable store
    pub fn from_store<T: GtfsStore, F: TableFacory>(store: &mut T) -> Result<Self> {
        use GtfsFileType::*;

        // let agency = decompress::<Agency, F>(store.get_readable(GtfsFileType::Agencies))?;
        let agency = store.decompress::<Agency, F>()?;
        let stops = store.decompress::<Stop, F>()?;
        let routes = store.decompress::<Route, F>()?;
        let trips = store.decompress::<Trip, F>()?;
        let stop_times = store.decompress::<StopTime, F>()?;
        let calendar = store.try_decompress::<Calendar, F>();
        let calendar_dates = store.try_decompress::<CalendarDate, F>();
        let fare_attributes = store.try_decompress::<FareAttribute, F>();
        let fare_rules = store.try_decompress::<FareRule, F>();
        let shapes = store.try_decompress::<Shape, F>();
        let frequencies = store.try_decompress::<Frequency, F>();
        let transfers = store.try_decompress::<Transfer, F>();
        let pathways = store.try_decompress::<PathWay, F>();
        let levels = store.try_decompress::<Level, F>();
        let feed_info = store.try_decompress::<FeedInfo, F>();
        let translations = store.try_decompress::<Translation, F>();
        let attributions = store.try_decompress::<Attribution, F>();
        let ticketing_identifiers = store.try_decompress::<TicketingIdentifier, F>();
        let ticketing_deep_links = store.try_decompress::<TicketingDeepLink, F>();

        Ok(GtfsCollection {
            agency,
            stops,
            routes,
            trips,
            stop_times,
            calendar,
            calendar_dates,
            fare_attributes,
            fare_rules,
            shapes,
            frequencies,
            transfers,
            pathways,
            levels,
            feed_info,
            translations,
            attributions,
            ticketing_identifiers,
            ticketing_deep_links,
        })
    }
}
