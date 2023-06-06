#[derive(Debug)]
pub struct Stop {
    pub station: usize, // Id of the station
    pub arrival: chrono::DateTime<chrono::Utc>,
    pub departure: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug)]
pub struct Ride {
    pub stops: Vec<Stop>, // Ride is defined by a sequence of stops
}

pub struct TimetableGrouper;

pub struct TimetableGrouped;

impl TimetableGrouper {
    pub fn add_ride(&self, ride: Ride) {}

    pub fn finalize(self) -> TimetableGrouped {
        TimetableGrouped {}
    }
}

impl TimetableGrouped {}
