use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    rc::Rc,
};
use stringmetrics::levenshtein_limit_iter;

type StopId = usize;
type ClusterId = usize;

#[derive(Debug)]
pub struct Stop {
    pub station: StopId, // Id of the station
    pub arrival: chrono::DateTime<chrono::Utc>,
    pub departure: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug)]
pub struct Ride {
    pub stops: Vec<Stop>, // Ride is defined by a sequence of stops
}

type StationSeq = Vec<StopId>;

impl Ride {
    fn station_sequence(&self) -> StationSeq {
        let mut sequence = Vec::with_capacity(self.stops.len());
        for stop in &self.stops {
            sequence.push(stop.station)
        }
        sequence
    }
}

pub struct TimetableGrouper {
    next_cluster_id: ClusterId,
    distance_threshold: f64,

    // We store processed sequences here
    // so we do not process them twice
    seen_sequences: HashSet<Rc<StationSeq>>,

    // We use a vector here so we can refer to it using station_x_seq
    // this way we can calculate sparse distance matrix
    sequences: Vec<(Rc<StationSeq>, ClusterId)>,

    /// Contains pointers to all sequences that include specific stop id
    station_x_seq: HashMap<StopId, HashSet<usize>>,
}

pub struct TimetableGrouped {
    pub mapping: Vec<Vec<StationSeq>>,
}

impl TimetableGrouper {
    pub fn new() -> Self {
        TimetableGrouper {
            next_cluster_id: 0,
            seen_sequences: HashSet::new(),
            sequences: Vec::new(),
            station_x_seq: HashMap::new(),
            distance_threshold: 0.6,
        }
    }

    /// Fetch candidates for calculating sparse distance
    fn distance_candidates(&self, sequence: &StationSeq) -> Vec<&(Rc<StationSeq>, ClusterId)> {
        let mut candidates: HashSet<usize> = HashSet::new();

        for station_id in sequence {
            let Some(id_arr) = self.station_x_seq.get(station_id) else {
                continue;
            };

            candidates.extend(id_arr)
        }

        let mut result = Vec::new();

        for candidate_id in candidates {
            result.push(self.sequences.get(candidate_id).unwrap())
        }

        result
    }

    fn distance(&self, seq1: &StationSeq, seq2: &StationSeq) -> f64 {
        let max_len: u32 = match std::cmp::min(seq1.len(), seq2.len()).try_into() {
            Ok(value) => value,
            Err(_) => u32::MAX,
        };

        let editions = levenshtein_limit_iter(seq1, seq2, max_len);

        let edited_frac = (editions as f64) / (max_len as f64);

        edited_frac
    }

    /// Merge clusters returning new cluster id
    fn merge_clusters(&mut self, clusters: HashSet<ClusterId>) -> ClusterId {
        let new_cluster_id = self.create_cluster();
        for (_, cluster_id) in self.sequences.iter_mut() {
            if clusters.contains(cluster_id) {
                *cluster_id = new_cluster_id;
            }
        }
        new_cluster_id
    }

    fn create_cluster(&mut self) -> ClusterId {
        let next_cluster = self.next_cluster_id;
        self.next_cluster_id += 1;
        next_cluster
    }

    fn add_to_cluster(&mut self, cluster: ClusterId, seq: StationSeq) {
        let seq = Rc::new(seq);
        for station_id in seq.as_ref() {
            use std::collections::hash_map::Entry::*;
            match self.station_x_seq.entry(*station_id) {
                Occupied(mut entry) => {
                    entry.get_mut().insert(self.sequences.len());
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    entry.insert(HashSet::from([self.sequences.len()]));
                }
            }
        }
        self.seen_sequences.insert(seq.clone());
        self.sequences.push((seq, cluster));
    }

    pub fn add_ride(&mut self, ride: Ride) {
        let sequence = ride.station_sequence();

        if self.seen_sequences.contains(&sequence) {
            return;
        }

        let mut related_clusters = HashSet::new();

        for (candidate, cluster_id) in self.distance_candidates(&sequence) {
            // If one of the condidates is exactly equal to our sequence, do nothing
            let dist = self.distance(candidate, &sequence);

            if dist < self.distance_threshold {
                related_clusters.insert(cluster_id.clone());
            }
        }

        let target_cluster: ClusterId = match related_clusters.len() {
            0 => self.create_cluster(),
            1 => related_clusters.iter().next().cloned().unwrap(),
            _ => self.merge_clusters(related_clusters),
        };

        self.add_to_cluster(target_cluster, sequence);
    }

    pub fn finalize(self) -> TimetableGrouped {
        let mut mapping_hm: HashMap<ClusterId, Vec<StationSeq>> = HashMap::new();

        // let mut clusters: HashSet<usize> = HashSet::new();

        // we're guaranteed here that sequences are unique at this point in time
        for (seq, cluster_id) in self.sequences {
            use std::collections::hash_map::Entry::*;
            match mapping_hm.entry(cluster_id) {
                Occupied(mut entry) => {
                    entry.get_mut().push((*seq).clone());
                }
                Vacant(entry) => {
                    entry.insert(vec![(*seq).clone()]);
                }
            }
        }

        let mut mapping = Vec::new();

        // Convert everything to sequences and sort to preserve invariance (for consistent outputs)
        for (_, mut seq) in mapping_hm.into_iter() {
            seq.sort();
            mapping.push(seq);
        }
        mapping.sort();

        TimetableGrouped { mapping }
    }
}

impl TimetableGrouped {}
