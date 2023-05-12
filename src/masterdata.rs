use std::collections::HashMap;

use serde::Deserialize;

pub struct Masterdata {
    client: reqwest::Client,
    station_timezones: HashMap<String, chrono_tz::Tz>,
    stations_url: String,
}

#[derive(Deserialize)]
struct Station {
    code: String,
    time_zone: String,
}

#[derive(Deserialize)]
struct StationWrapper {
    attributes: Station,
}

#[derive(Deserialize)]

struct MastedataResponse {
    data: Vec<StationWrapper>,
}

impl Masterdata {
    pub fn new(masterdata_url: &str) -> Self {
        Masterdata {
            client: reqwest::Client::new(),
            station_timezones: HashMap::new(),
            stations_url: format!("{masterdata_url}/api/v1/stations"),
        }
    }

    pub async fn update_data(&mut self) -> anyhow::Result<()> {
        let response = self
            .client
            .get(&self.stations_url)
            .send()
            .await?
            .json::<MastedataResponse>()
            .await?;

        for station in response.data {
            let tz_parsed = match station.attributes.time_zone.parse() {
                Ok(val) => val,
                Err(_) => continue,
            };

            self.station_timezones
                .insert(station.attributes.code, tz_parsed);
        }

        Ok(())
    }

    pub fn get_station_timezone(&self, code: &str) -> Option<&chrono_tz::Tz> {
        self.station_timezones.get(code)
    }
}
