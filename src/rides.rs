use indicatif::ProgressIterator;
use ordered_float::OrderedFloat;
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

struct Matrix<T> {
    rows: usize,
    cols: usize,
    data: Vec<T>,
}

impl<T: Copy> Matrix<T> {
    fn new(rows: usize, cols: usize, initial: T) -> Self {
        Matrix {
            rows,
            cols,
            data: vec![initial; rows * cols],
        }
    }

    fn get(&self, row: usize, col: usize) -> T {
        self.data[row * self.cols + col]
    }

    fn set(&mut self, row: usize, col: usize, value: T) {
        self.data[row * self.cols + col] = value;
    }
}

pub fn needleman_wunsch<T: PartialEq + Copy>(
    seq1: &[T],
    seq2: &[T],
    match_score: i32,
    gap_score: i32,
) -> i32 {
    let len1 = seq1.len() + 1;
    let len2 = seq2.len() + 1;

    let mut matrix = Matrix::new(len1, len2, 0);

    for i in 1..len1 {
        matrix.set(i, 0, i as i32 * gap_score);
    }

    for j in 1..len2 {
        matrix.set(0, j, j as i32 * gap_score);
    }

    for i in 1..len1 {
        for j in 1..len2 {
            let score_diag = matrix.get(i - 1, j - 1)
                + if seq1[i - 1] == seq2[j - 1] {
                    match_score
                } else {
                    gap_score
                };
            let score_left = matrix.get(i, j - 1) + gap_score;
            let score_up = matrix.get(i - 1, j) + gap_score;
            matrix.set(
                i,
                j,
                std::cmp::max(std::cmp::max(score_diag, score_left), score_up),
            );
        }
    }

    matrix.get(len1 - 1, len2 - 1)
}

#[test]
fn test_needleman_wunsch() {
    let seq1 = vec!['A', 'C', 'C', 'T', 'G'];
    let seq2 = vec!['A', 'C', 'C', 'B', 'T', 'G'];

    assert_eq!(needleman_wunsch(&seq1, &seq2, 1, -1), 4);
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
        let max_len: u32 = match std::cmp::max(seq1.len(), seq2.len()).try_into() {
            Ok(value) => value,
            Err(_) => u32::MAX,
        };

        let matches = needleman_wunsch(seq1, seq2, 10, -1);

        let matches_frac = (matches as f64) / 10.0 / (max_len as f64);

        matches_frac
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

            if dist > self.distance_threshold {
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

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct StopSequence {
    pub stop_sequence: Vec<StopId>,
}

impl From<&Ride> for StopSequence {
    fn from(value: &Ride) -> Self {
        let mut stop_sequence = Vec::new();

        for stop in &value.stops {
            stop_sequence.push(stop.station)
        }

        StopSequence { stop_sequence }
    }
}

struct MatrixIterator {
    len: usize,
    current_row: usize,
    current_column: usize,
}

impl MatrixIterator {
    fn new(len: usize) -> Self {
        MatrixIterator {
            len,
            current_row: 0,
            current_column: 0,
        }
    }
}

impl ExactSizeIterator for MatrixIterator {
    fn len(&self) -> usize {
        self.len * self.len
    }
}

impl Iterator for MatrixIterator {
    type Item = (usize, usize);

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len(), Some(self.len()))
    }

    fn next(&mut self) -> Option<Self::Item> {
        let result = Some((self.current_row, self.current_column));

        self.current_column += 1;
        if self.current_column == self.len {
            self.current_column = 0;
            self.current_row += 1;
        }

        if self.current_row == self.len {
            return None;
        }

        result
    }
}

pub fn group_stop_sequences(stop_seqs: &Vec<StopSequence>) -> Vec<usize> {
    fn dissimilarity(lhs: &StopSequence, rhs: &StopSequence) -> OrderedFloat<f64> {
        let max_len: u32 =
            match std::cmp::max(lhs.stop_sequence.len(), rhs.stop_sequence.len()).try_into() {
                Ok(value) => value,
                Err(_) => u32::MAX,
            };

        let mut matches = needleman_wunsch(&lhs.stop_sequence, &rhs.stop_sequence, 10, -1);

        if matches < 30 {
            matches = 0;
        };

        // Similarity metric betwen 0 and 1
        let mut similarity = (matches as f64) / 10.0 / (max_len as f64);
        if similarity < 0.0 {
            similarity = 0.0
        };
        assert!(similarity <= 1.0);

        // Dissimilarity as 1 - similarity
        OrderedFloat(1.0 / (similarity + 0.0001))
    }

    // Calculate dissimilarity matrix
    let mut dissimilarity_mat =
        ndarray::Array2::<OrderedFloat<f64>>::ones((stop_seqs.len(), stop_seqs.len()));

    println!("Calulating dissimilarity matrix");

    let mut n_iterations = 0;

    for (from_id, to_id) in MatrixIterator::new(stop_seqs.len()).progress() {
        let from = &stop_seqs[from_id];
        let to = &stop_seqs[to_id];

        dissimilarity_mat[[from_id, to_id]] = dissimilarity(from, to);
        n_iterations += 1;
    }

    println!("{:?}", dissimilarity_mat);

    println!("Total number of iterations: {}", n_iterations);

    let mut meds = kmedoids::random_initialization(stop_seqs.len(), 100, &mut rand::thread_rng());

    let (loss, assigned_clusters, n_iter, n_swap): (f64, _, _, _) =
        kmedoids::fasterpam(&dissimilarity_mat, &mut meds, 50);

    assigned_clusters
}
