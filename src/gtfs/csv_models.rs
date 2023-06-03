/// Models for csv serialization/deserialization
///
use std::{fmt::Display, hash::Hash};

use anyhow::Result;

use serde::{Deserialize, Serialize};

use serde_repr::{Deserialize_repr, Serialize_repr};

pub trait GtfsFile {
    fn get_file_type() -> GtfsFileType;
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
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

impl Display for RouteType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RouteType::Tram => write!(f, "Tram"),
            RouteType::Subway => write!(f, "Subway"),
            RouteType::Rail => write!(f, "Rail"),
            RouteType::Bus => write!(f, "Bus"),
            RouteType::Ferry => write!(f, "Ferry"),
            RouteType::CableTram => write!(f, "CableTram"),
            RouteType::AerialLift => write!(f, "AerialLift"),
            RouteType::Funicular => write!(f, "Funicular"),
            RouteType::Trolleybus => write!(f, "Trolleybus"),
            RouteType::Monorail => write!(f, "Monorail"),
        }
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum ContinuousPickupType {
    ContinuousStoppingPickup = 0,
    NoContinuousStoppingPickup = 1,
    PhoneAgency = 2,
    AskDriver = 3,
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum ContinuousDropOffType {
    ContinuousStoppingDropOff = 0,
    NoContinuousStoppingDropOff = 1,
    PhoneAgency = 2,
    AskDriver = 3,
}

#[derive(Debug, Clone)]
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
        let value: String = Deserialize::deserialize(deserializer)?;
        Ok(Color::Hex(value.to_string()))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

impl Display for Route {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Route<{}> {}", self.route_id, self.route_type)?;
        if let Some(long_name) = &self.route_long_name {
            write!(f, " {}", long_name)?;
        }

        if let Some(short_name) = &self.route_short_name {
            write!(f, " ({})", short_name)?;
        }
        Ok(())
    }
}

impl GtfsFile for Route {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Routes
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

impl Display for Agency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Agency<{}> {} {}",
            self.agency_id, self.agency_name, self.agency_timezone
        )
    }
}

impl GtfsFile for Agency {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Agencies
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum StopLocationType {
    StopOrPlatform = 0,
    Station = 1,
    EntranceOrExit = 2,
    GenericNode = 3,
    BoardingArea = 5,
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum WheelChairBoardingType {
    NoInformation = 0,
    WheelchairSupported = 1,
    NoWheelchairSupport = 2,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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
        GtfsFileType::Stops
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum TicketingType {
    Available = 0,
    Unavailable = 1,
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum TripDirection {
    Outbound = 0,
    Inbound = 1,
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum BikesAllowedType {
    NoInformation = 0,
    BikesAllowed = 1,
    NoBikesAllowed = 2,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

impl Display for Trip {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Trip<{}>", self.trip_id)?;
        if let Some(headsign) = &self.trip_headsign {
            write!(f, " to {}", headsign)?;
        }

        if let Some(short_name) = &self.trip_short_name {
            write!(f, " ({})", short_name)?;
        }
        if let Some(direction) = &self.direction_id {
            match direction {
                TripDirection::Outbound => write!(f, " outbound"),
                TripDirection::Inbound => write!(f, " inbound"),
            }?;
        }

        if let Some(shape_id) = &self.shape_id {
            write!(f, ", with shape {}", shape_id)?;
        };

        Ok(())
    }
}

impl GtfsFile for Trip {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Trips
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum StopPickupType {
    RegularPickup = 0,
    NoPickup = 1,
    PhoneAgency = 2,
    AskDriver = 3,
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum StopDropOffType {
    RegularDropOff = 0,
    NoDropOff = 1,
    PhoneAgency = 2,
    AskDriver = 3,
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum TimePointType {
    Aproximate = 0,
    Exact = 1,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
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

impl Display for StopTime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.stop_sequence, self.stop_id)?;
        if let Some(arrival_time) = &self.arrival_time {
            write!(f, " {}", arrival_time)?;
        }

        if let Some(departure_time) = &self.departure_time {
            write!(f, "-{}", departure_time)?;
        }

        if let Some(headsign) = &self.stop_headsign {
            write!(f, " to {}", headsign)?;
        }

        Ok(())
    }
}

impl GtfsFile for StopTime {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::StopTimes
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ServiceAvailability {
    SeriviceAvailable = 1,
    SeriviceNotAvailable = 0,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Calendar {
    pub service_id: String,
    pub start_date: String,
    pub end_date: String,
    pub monday: ServiceAvailability,
    pub tuesday: ServiceAvailability,
    pub wednesday: ServiceAvailability,
    pub thursday: ServiceAvailability,
    pub friday: ServiceAvailability,
    pub saturday: ServiceAvailability,
    pub sunday: ServiceAvailability,
}

impl Display for Calendar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} - {}", self.start_date, self.end_date)?;
        if self.monday == ServiceAvailability::SeriviceAvailable {
            write!(f, " monday")?;
        };

        if self.tuesday == ServiceAvailability::SeriviceAvailable {
            write!(f, " tuesday")?;
        };

        if self.wednesday == ServiceAvailability::SeriviceAvailable {
            write!(f, " wednesday")?;
        };

        if self.thursday == ServiceAvailability::SeriviceAvailable {
            write!(f, " thursday")?;
        };

        if self.friday == ServiceAvailability::SeriviceAvailable {
            write!(f, " friday")?;
        };

        if self.saturday == ServiceAvailability::SeriviceAvailable {
            write!(f, " saturday")?;
        };

        if self.sunday == ServiceAvailability::SeriviceAvailable {
            write!(f, " sunday")?;
        };

        Ok(())
    }
}

impl GtfsFile for Calendar {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Calendars
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum SerivceExceptionType {
    Added = 1,
    Removed = 2,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CalendarDate {
    pub service_id: String,
    pub date: String,
    pub exception_type: SerivceExceptionType,
}

impl Display for CalendarDate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.exception_type {
            SerivceExceptionType::Added => write!(f, "+{}", self.date),
            SerivceExceptionType::Removed => write!(f, "-{}", self.date),
        }
    }
}

impl GtfsFile for CalendarDate {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::CalendarDates
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum PaymentMethod {
    PaidOnBoard = 0,
    PaidBeforeBoard = 1,
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum TransfersIncluded {
    NoTransfersPermitted = 0,
    TransferOnce = 1,
    TransferTwice = 2,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FareAttribute {
    pub fare_id: String,
    pub price: f64,
    pub currency_type: String,
    pub payment_method: PaymentMethod,
    pub transfers: Option<TransfersIncluded>,
    pub agency_id: Option<String>,
    pub transfer_duration: Option<u64>,
}

impl GtfsFile for FareAttribute {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::FareAttributes
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct FareRule {
    pub fare_id: String,
    pub route_id: Option<String>,
    pub origin_id: Option<String>,
    pub destination_id: Option<String>,
    pub contains_id: Option<String>,
}

impl GtfsFile for FareRule {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::FareRules
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Shape {
    pub shape_id: String,
    pub shape_pt_lat: f64,
    pub shape_pt_lon: f64,
    pub shape_pt_sequence: u64,
    pub shape_dist_traveled: Option<f64>,
}

impl GtfsFile for Shape {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Shapes
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum ExactTimesType {
    FrequencyBased = 0,
    ExactSameHeadway = 1,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Frequency {
    pub trip_id: String,
    pub start_time: String,
    pub end_time: String,
    pub headway_secs: u64,
    pub exact_times: Option<ExactTimesType>,
}

impl GtfsFile for Frequency {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Frequencies
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum TransferType {
    Recommended = 0,
    TimedTransfer = 1,
    WaitForTransfer = 2,
    TransferNotPossible = 3,
    InSeatTransfer = 4,
    ReboardTransfer = 5,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Transfer {
    pub from_stop_id: String,
    pub to_stop_id: String,
    pub transfer_type: TransferType,
    pub min_transfer_time: Option<u64>,
}

impl GtfsFile for Transfer {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::Transfers
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum PathwayMode {
    Walkway = 1,
    Stairs = 2,
    Travelator = 3,
    Escalator = 4,
    Elevator = 5,
    FareGate = 6,
    ExitGate = 7,
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
#[repr(u8)]
pub enum BidirectionalType {
    Unidirectional = 0,
    Bidirectional = 1,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PathWay {
    pub pathway_id: String,
    pub from_stop_id: String,
    pub to_stop_id: String,
    pub pathway_mode: PathwayMode,
    pub is_bidirectional: BidirectionalType,
    pub length: Option<f64>,
    pub traversal_time: Option<u64>,
    pub stair_count: Option<u64>,
    pub max_slope: Option<f64>,
    pub min_width: Option<f64>,
    pub signposted_as: Option<String>,
    pub reversed_signposted_as: Option<String>,
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
    pub feed_publisher_name: String,
    pub feed_publisher_url: String,
    pub feed_lang: String,
    pub default_lang: Option<String>,
    pub feed_start_date: Option<String>,
    pub feed_end_date: Option<String>,
    pub feed_version: Option<String>,
    pub feed_contact_email: Option<String>,
    pub feed_contact_url: Option<String>,
}

impl GtfsFile for FeedInfo {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::FeedInfos
    }
}

#[derive(Debug, Deserialize_repr, Serialize_repr, Clone, Copy)]
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
    pub attribution_id: Option<String>,
    pub agency_id: Option<String>,
    pub route_id: Option<String>,
    pub trip_id: Option<String>,
    pub organization_name: String,
    pub is_producer: u8,
    pub is_operator: u8,
    pub is_authority: u8,
    pub attribution_url: Option<String>,
    pub attribution_email: Option<String>,
    pub attribution_phone: Option<String>,
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
    pub ticketing_deep_link_id: String,
    pub web_url: Option<String>,
    pub android_intent_uri: Option<String>,
    pub ios_universal_link_url: Option<String>,
}

impl GtfsFile for TicketingDeepLink {
    fn get_file_type() -> GtfsFileType {
        GtfsFileType::TicketingDeepLinks
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Copy, Debug)]
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
    pub fn from_filename(name: &str) -> Option<Self> {
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
