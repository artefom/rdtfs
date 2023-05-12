/// Sending requests and parsing responses of elasticsearch
///
///
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use chrono::TimeZone;
use elasticsearch::auth::Credentials;
use elasticsearch::http::transport::{SingleNodeConnectionPool, TransportBuilder};
use elasticsearch::{Elasticsearch, SearchParts};

use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::json;

fn nullstring() -> Option<String> {
    None
}

fn nullbool() -> Option<bool> {
    None
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Uid {
    pub uid: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MaybeUid {
    #[serde(default = "nullstring")]
    pub uid: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum VehicleType {
    #[serde(rename = "BUS")]
    Bus,
    #[serde(rename = "TRAIN")]
    Train,
    #[serde(rename = "TRAM")]
    Tram,
    #[serde(rename = "FERRY")]
    Ferry,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Vehicle {
    #[serde(rename = "type")]
    pub vehicle_type: VehicleType,
}

#[derive(Debug, Clone)]
pub struct Segment {
    pub line: Option<String>,
    pub departure_time: chrono::DateTime<chrono_tz::Tz>,
    pub arrival_time: chrono::DateTime<chrono_tz::Tz>,
    pub departure_station: Uid,
    pub arrival_station: Uid,
    pub vehicle: Vehicle,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct SegmentRaw {
    pub index: u32,

    pub operating_carrier: Uid,
    #[serde(default = "nullstring")]
    pub line: Option<String>,
    #[serde(default = "nullstring")]
    pub line_prefix: Option<String>,
    pub departure_time: u64,
    pub arrival_time: u64,

    pub departure_station: Uid,
    pub arrival_station: Uid,
    pub vehicle: Vehicle,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct FareRaw {
    pub price: u32,
    pub fare_class: Uid,
}

#[derive(Debug, Clone)]
pub struct Fare {
    pub price: rust_decimal::Decimal,
    pub fare_class: Uid,
    pub currency: String,
}

#[derive(Debug, Clone)]
pub struct TripsHit {
    pub snapshot_id: String,
    pub snapshot_timestamp: chrono::DateTime<chrono::Utc>,
    pub snapshot_uid: String,
    pub departure_time: chrono::DateTime<chrono_tz::Tz>,
    pub arrival_time: chrono::DateTime<chrono_tz::Tz>,
    pub total_price: rust_decimal::Decimal,
    pub currency: String,
    pub booked_out: bool,
    pub electronic_ticket_available: Option<bool>,
    pub departure_date: String,

    // Uids
    pub departure_station: Uid,
    pub arrival_station: Uid,
    pub marketing_carrier: Uid,
    pub departure_city: MaybeUid,
    pub arrival_city: MaybeUid,
    pub departure_area: MaybeUid,
    pub arrival_area: MaybeUid,

    pub segments: Vec<Segment>,
    pub fares: Vec<Fare>,
}

#[derive(Serialize, Deserialize, Debug)]
struct TripsHitRaw {
    pub snapshot_id: String,
    pub snapshot_timestamp: u64,
    pub snapshot_uid: String,
    pub departure_time: u64,
    pub arrival_time: u64,
    pub total_price: u32,
    pub currency: String,
    pub booked_out: bool,
    #[serde(default = "nullbool")]
    pub electronic_ticket_available: Option<bool>,
    pub departure_date: String,

    // Uids
    pub departure_station: Uid,
    pub arrival_station: Uid,
    pub marketing_carrier: Uid,
    pub departure_city: MaybeUid,
    pub arrival_city: MaybeUid,
    pub departure_area: MaybeUid,
    pub arrival_area: MaybeUid,

    // Arrays
    pub segments: Vec<SegmentRaw>,
    pub fares: Option<Vec<FareRaw>>,
}

pub struct EsTrips<G> {
    elastic: Elasticsearch,
    index: String,
    tz_getter: G,
}

fn make_es_client(url: &str, id: &str, api_key: &str) -> anyhow::Result<Elasticsearch> {
    let url = Url::parse(url).with_context(|| format!("Invalid url {url}"))?;
    let conn_pool = SingleNodeConnectionPool::new(url);
    let credentials = Credentials::ApiKey(id.to_string(), api_key.to_string());
    let transport = TransportBuilder::new(conn_pool)
        .auth(credentials)
        .disable_proxy()
        .cert_validation(elasticsearch::cert::CertificateValidation::None)
        .build()?;
    Ok(Elasticsearch::new(transport))
}

#[derive(Deserialize, Debug)]
pub struct IndexInfo {
    #[serde(rename = "docs.count")]
    pub docs_count: String,
    #[serde(rename = "docs.deleted")]
    pub docs_deleted: String,
    #[serde(rename = "health")]
    pub health: String,
    #[serde(rename = "index")]
    pub index: String,
    pub uuid: String,
    #[serde(rename = "store.size")]
    pub store_size: String,
}

#[derive(Deserialize)]
struct ElasticsearchHit {
    #[serde(rename = "_source")]
    pub source: TripsHitRaw,
}

#[derive(Deserialize)]
struct ElasticsearchTotal {
    value: i64,
}

#[derive(Deserialize)]
struct ElasticsearchHits {
    total: ElasticsearchTotal,
    pub hits: Vec<ElasticsearchHit>,
}

#[derive(Deserialize)]
struct ElasticsearchResponse {
    hits: ElasticsearchHits,
}

#[derive(Deserialize)]
struct AggKey {
    value: String,
}

#[derive(Deserialize)]
struct AggBucket {
    key: AggKey,
    // doc_count: u32,
}

#[derive(Deserialize)]
struct AggResult3 {
    // after_key: AggKey,
    buckets: Vec<AggBucket>,
}
#[derive(Deserialize)]
struct AggResult {
    values: AggResult3,
}

#[derive(Deserialize)]
struct AggResponse {
    // took: u32,
    // timed_out: bool,
    aggregations: AggResult,
}

fn convert_line_id(suffix: Option<String>, prefix: Option<String>) -> Option<String> {
    let merged = match (prefix, suffix) {
        (None, None) => None,
        (None, Some(suffix)) => Some(suffix),
        (Some(prefix), None) => Some(prefix),
        (Some(prefix), Some(suffix)) => Some(format!("{prefix} {suffix}")),
    };

    match merged {
        Some(value) => {
            let value = value.trim();

            if value.is_empty() {
                None
            } else {
                Some(value.to_string())
            }
        }
        None => None,
    }
}

fn millis_to_naive_datetime(millis: u64) -> anyhow::Result<chrono::naive::NaiveDateTime> {
    let result = {
        let secs = millis / 1000;
        let nsecs = (millis - secs * 1000) * 1000000;

        match chrono::naive::NaiveDateTime::from_timestamp_opt(secs.try_into()?, nsecs.try_into()?)
        {
            Some(val) => anyhow::Ok(val),
            None => bail!("Time is out of range"),
        }
    }
    .context("Could not convert millis to datetime")?;
    Ok(result)
}

fn convert_tz_naive(
    dt: &chrono::naive::NaiveDateTime,
    tz_from: &chrono_tz::Tz,
    tz_to: &chrono_tz::Tz,
) -> anyhow::Result<chrono::naive::NaiveDateTime> {
    let local_dttm = match tz_from.from_local_datetime(dt) {
        chrono::LocalResult::None => bail!("Could not convert timezones"),
        chrono::LocalResult::Single(val) => val,
        chrono::LocalResult::Ambiguous(_, _) => {
            bail!("Ambiguous timezone conversion to Berlin time")
        }
    };

    Ok(local_dttm.with_timezone(tz_to).naive_local())
}

// use chrono_tz::OffsetName::
fn elastic_naive_to_tz_aware(
    dt: &chrono::naive::NaiveDateTime,
    station_timezone: &chrono_tz::Tz,
) -> anyhow::Result<chrono::DateTime<chrono_tz::Tz>> {
    let step1 = convert_tz_naive(dt, &chrono_tz::UTC, &chrono_tz::Europe::Berlin)?;
    let step2 = match station_timezone.from_local_datetime(&step1) {
        chrono::LocalResult::None => bail!("Could not convert to station timezone"),
        chrono::LocalResult::Single(val) => val,
        chrono::LocalResult::Ambiguous(_, _) => bail!("Ambiguous local timezone"),
    };
    Ok(step2)
}

fn elastic_timestamp_to_datetime(
    ts: u64,
    station_timezone: &chrono_tz::Tz,
) -> anyhow::Result<chrono::DateTime<chrono_tz::Tz>> {
    elastic_naive_to_tz_aware(&millis_to_naive_datetime(ts)?, station_timezone)
}

fn process_segment(
    segment: SegmentRaw,
    departure_station_tz: &chrono_tz::Tz,
    arrival_station_tz: &chrono_tz::Tz,
) -> anyhow::Result<Segment> {
    let departure_dttm_naive =
        elastic_timestamp_to_datetime(segment.departure_time, departure_station_tz)?;
    let arrival_dttm_naive =
        elastic_timestamp_to_datetime(segment.arrival_time, arrival_station_tz)?;

    Ok(Segment {
        line: convert_line_id(segment.line, segment.line_prefix),
        departure_time: departure_dttm_naive,
        arrival_time: arrival_dttm_naive,
        departure_station: segment.departure_station,
        arrival_station: segment.arrival_station,
        vehicle: segment.vehicle,
    })
}

pub trait StationTimezoneGetter {
    fn get_station_timezone(&self, station_code: &str) -> Option<&chrono_tz::Tz>;
}

fn xbus_to_money(price: u32) -> rust_decimal::Decimal {
    let price_converted = price.into();
    rust_decimal::Decimal::new(price_converted, 0) / rust_decimal::Decimal::new(100, 0)
}

fn parse_trip_hit<G>(hit: TripsHitRaw, tz_getter: &G) -> anyhow::Result<TripsHit>
where
    G: StationTimezoneGetter,
{
    let mut segments = Vec::new();
    let mut fares = Vec::new();

    segments.reserve(hit.segments.len());

    let hit_fares = match hit.fares {
        Some(value) => value,
        None => Vec::new(),
    };

    fares.reserve(hit_fares.len());

    for segment in hit.segments {
        let departure_station_tz = tz_getter
            .get_station_timezone(&segment.departure_station.uid)
            .context("Could not get departure station timezone")?;
        let arrival_station_tz = tz_getter
            .get_station_timezone(&segment.arrival_station.uid)
            .context("Could not get arrival station timezone")?;

        segments.push(
            process_segment(segment, &departure_station_tz, &arrival_station_tz)
                .context("Could not understand segment data")?,
        )
    }

    for fare in hit_fares {
        fares.push(Fare {
            price: xbus_to_money(fare.price),
            fare_class: fare.fare_class,
            currency: hit.currency.clone(),
        })
    }

    let timestamp = millis_to_naive_datetime(hit.snapshot_timestamp)
        .context("Snapshot timestamp not understood")?;
    let tz_aware_datetime = match chrono::Utc.from_local_datetime(&timestamp) {
        chrono::LocalResult::Single(value) => value,
        _ => bail!("Could not convert timestamp to UTC"),
    };

    let dep_tz = tz_getter
        .get_station_timezone(&hit.departure_station.uid)
        .context("Could not get departure station timezone")?;

    let arr_tz = tz_getter
        .get_station_timezone(&hit.departure_station.uid)
        .context("Could not get departure station timezone")?;

    Ok(TripsHit {
        snapshot_id: hit.snapshot_id,
        snapshot_timestamp: tz_aware_datetime,
        snapshot_uid: hit.snapshot_uid,
        departure_time: elastic_timestamp_to_datetime(hit.departure_time, &dep_tz)
            .context("Could not parse trip hit departure time")?,
        arrival_time: elastic_timestamp_to_datetime(hit.arrival_time, &arr_tz)
            .context("Could not parse trip arrival time")?,
        total_price: xbus_to_money(hit.total_price),
        currency: hit.currency,
        booked_out: hit.booked_out,
        electronic_ticket_available: hit.electronic_ticket_available,
        departure_date: hit.departure_date,
        departure_station: hit.departure_station,
        arrival_station: hit.arrival_station,
        marketing_carrier: hit.marketing_carrier,
        departure_city: hit.departure_city,
        arrival_city: hit.arrival_city,
        departure_area: hit.departure_area,
        arrival_area: hit.arrival_area,
        segments,
        fares,
    })
}

impl<G> EsTrips<G>
where
    G: StationTimezoneGetter,
{
    pub fn new(
        url: &str,
        index: &str,
        api_id: &str,
        api_key: &str,
        tz_getter: G,
    ) -> anyhow::Result<Self> {
        let elastic = make_es_client(url, api_id, api_key)
            .context("Could not establish connection to Elasticsearch")?;

        Ok(EsTrips {
            elastic,
            index: index.to_string(),
            tz_getter,
        })
    }

    pub async fn index_info(&self) -> anyhow::Result<IndexInfo> {
        let response = self
            .elastic
            .cat()
            .indices(elasticsearch::cat::CatIndicesParts::Index(&[self
                .index
                .as_str()]))
            .format("json")
            .send()
            .await?;

        let index_info = {
            let status_code = response.status_code();
            let response_text = response.text().await?;
            let mut response_body = serde_json::from_str::<Vec<IndexInfo>>(&response_text)
                .with_context(|| {
                    format!(
                        "Index info response is not recognized. Server respondend with status code: {}; Response: {}",
                        status_code, response_text,
                    )
                })?;
            if response_body.len() > 1 {
                Err(anyhow!("Expected array of length 1"))
            } else {
                Ok(response_body.pop().unwrap())
            }
        }
        .with_context(|| format!("/_cat/indices/{} response not understood", self.index))?;

        Ok(index_info)
    }

    /// Get trips with given key
    pub async fn get_connections(
        &self,
        carrier: &str,
        after: Option<&str>,
    ) -> anyhow::Result<Vec<TripsHit>> {
        let es_max: i64 = 100;

        let mut query = json!({
            "query": {
                "bool": {
                    "must": [
                        {"term": {"marketing_carrier.uid": carrier}},
                    ],
                }
            },
            "sort": [
                {"snapshot_id": "asc"},
            ]
        });

        // Add search after if it is present in the request
        if let Some(after) = after {
            query["search_after"] = json!([after]);
        }

        let response = self
            .elastic
            .search(SearchParts::Index(&[self.index.as_str()]))
            .size(es_max) // Maximum 1k records
            .body(query)
            .send()
            .await?;

        let response_text = response
            .text()
            .await
            .context("Could not get body of elasticsearch response")?;

        let response_body: ElasticsearchResponse = match serde_json::from_str(&response_text) {
            Ok(value) => value,
            Err(err) => {
                let column = err.column();
                let line = err.line();

                let error_line = response_text
                    .lines()
                    .into_iter()
                    .nth(line.saturating_sub(1))
                    .context("Could not get error line")?;

                let from = column.saturating_sub(20);
                let to = std::cmp::min(column + 20, error_line.len().saturating_sub(1));

                let problem = &error_line[from..to];

                bail!(
                    "Error parsing elasticsearch response: {} Line: {}",
                    err,
                    problem
                );
            }
        };

        let mut result = Vec::new();

        for hit in response_body.hits.hits {
            result.push(
                parse_trip_hit(hit.source, &self.tz_getter).context("Could not parse trip hit")?,
            );
        }

        Ok(result)
    }

    /// Consume all connections of carrier into a function
    pub async fn consume_into<F: FnMut(TripsHit) -> ()>(
        &self,
        carrier: &str,
        mut target: F,
    ) -> Result<()> {
        let mut after: Option<String> = None;

        loop {
            let hits = self
                .get_connections(carrier, after.as_ref().map(|x| x.as_str()))
                .await?;

            if let Some(last) = hits.last() {
                after = Some(last.snapshot_id.clone())
            } else {
                break;
            }
            for hit in hits {
                target(hit)
            }
        }

        Ok(())
    }
}
