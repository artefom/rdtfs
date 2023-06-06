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
