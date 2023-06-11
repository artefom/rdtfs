use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    hash::Hash,
    rc::Rc,
};

use itertools::Itertools;
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

    seen_sequences: HashSet<Rc<StationSeq>>,

    sequences: Vec<(Rc<StationSeq>, ClusterId)>,

    /// Contains pointers to all sequences that include specific stop id
    station_x_seq: HashMap<StopId, HashSet<usize>>,
}

pub struct TimetableGrouped {
    pub mapping: HashMap<StationSeq, usize>,
}

// A B C D E D

// A   C   E D
//   B C D
// A     D

// A C E D
// B C D
// A D

#[derive(Debug, Eq, PartialEq, Hash, Clone)]
struct ProfileItem {
    next_positions: Vec<usize>,
}

impl ProfileItem {
    fn with_len(len: usize) -> Self {
        let mut next_positions = Vec::with_capacity(len);
        for _ in 0..len {
            next_positions.push(0);
        }
        ProfileItem { next_positions }
    }

    fn iter_next<'a, T>(&self, seqs: &'a [&'a [T]]) -> Vec<Option<&'a T>> {
        let mut items = Vec::new();
        for (next_pos, seq) in self.next_positions.iter().zip(seqs.iter()) {
            if *next_pos < seq.len() {
                items.push(Some(&seq[*next_pos]));
            } else {
                items.push(None);
            }
        }
        items
    }

    fn is_finished<T>(&self, seqs: &[&[T]]) -> bool {
        for (next_pos, seq) in self.next_positions.iter().zip(seqs.iter()) {
            if *next_pos < seq.len() {
                return false;
            }
        }
        true
    }

    fn all_in_progress<T>(&self, seqs: &[&[T]]) -> HashSet<usize> {
        let mut in_progress: HashSet<usize> = HashSet::new();
        for (idx, (next_pos, seq)) in self.next_positions.iter().zip(seqs.iter()).enumerate() {
            let started = *next_pos > 0;
            let finished = *next_pos < seq.len();
            if started && !finished {
                in_progress.insert(idx);
            }
        }
        in_progress
    }

    // Genreate all possible next profiles from the current one
    fn all_possible_next<T: Hash + Eq + Clone + Debug>(&self, seqs: &[&[T]]) -> Vec<ProfileItem> {
        let mut next_items = Vec::new();
        let mut next_possible_items: HashSet<&T> = HashSet::new();

        for item in self.iter_next(seqs) {
            let Some(item) = item else {
                continue
            };
            next_possible_items.insert(item);
        }

        for next_item in &next_possible_items {
            let next_ids = self
                .iter_next(seqs)
                .into_iter()
                .enumerate()
                .filter(|(_, elem)| *elem == Some(*next_item))
                .map(|(pos, _)| pos);

            for comb in next_ids.powerset() {
                if comb.len() == 0 {
                    continue;
                }

                let mut next_item = ProfileItem {
                    next_positions: self.next_positions.clone(),
                };

                let mut comb_hs = HashSet::new();

                for item in &comb {
                    comb_hs.insert(item.clone());
                }

                for inc_item in &comb {
                    next_item.next_positions[*inc_item] += 1;
                }

                next_items.push(next_item);
            }
        }

        next_items
    }
}

#[derive(Clone, Debug)]
struct ProfilePath {
    cnt: usize,
    source: Option<ProfileItem>,
}

fn get_finished<'a, T>(
    profiles: &'a Vec<HashMap<ProfileItem, ProfilePath>>,
    seqs: &[&[T]],
) -> &'a ProfileItem {
    // Find all finished profiles
    let mut result: Vec<&ProfileItem> = Vec::new();
    for (item, _) in profiles.last().unwrap() {
        if item.is_finished(seqs) {
            result.push(item);
        }
    }

    // Take some element as a final result. We're guaranteed to have at least one
    // result ant this point
    let result: &ProfileItem = *result.first().unwrap();

    result
}

fn recover_full_path<'a, T>(
    profiles: &'a Vec<HashMap<ProfileItem, ProfilePath>>,
    seqs: &[&[T]],
) -> Vec<&'a ProfileItem> {
    let mut result = Vec::new();

    let mut current_item = Some(get_finished(profiles, seqs));

    for profile in profiles.iter().rev() {
        // We're guaranteed to have an item here
        let item = current_item.unwrap();

        result.push(item);

        // Update next current item (Will be None on last iteration)
        current_item = profile.get(item).unwrap().source.as_ref();
    }

    result.reverse();

    result
}

fn recover_letters<'a, T: Debug + Hash + Eq>(
    result: &[&ProfileItem],
    seqs: &[&'a [T]],
) -> Vec<&'a T> {
    let mut letters_result: Vec<&T> = Vec::new();
    for (prev, next) in result.iter().tuple_windows() {
        let mut letter: Option<&T> = None;
        for (seq, (prev_position, next_position)) in seqs
            .iter()
            .zip(prev.next_positions.iter().zip(next.next_positions.iter()))
        {
            if prev_position != next_position {
                let cur_letter = &seq[*prev_position];
                if let Some(letter) = letter {
                    if letter != cur_letter {
                        panic!(
                            "Profile positions delta is invalid. Expected one unique element, got {:?} {:?}",
                            letter, cur_letter
                        )
                    }
                } else {
                    letter = Some(cur_letter);
                }
            }
        }
        letters_result.push(letter.expect("Invalid positions delta. No delta found"));
    }
    letters_result
}

/// Alignes multiple sequences into one
fn sequence_aligner<'a, T: Hash + Eq + Clone + Debug>(seqs: &[&'a [T]]) -> Vec<&'a T> {
    let mut profiles: Vec<HashMap<ProfileItem, ProfilePath>> = Vec::new();
    profiles.push(HashMap::from([(
        ProfileItem::with_len(seqs.len()),
        ProfilePath {
            cnt: 1,
            source: None,
        },
    )]));

    loop {
        let profile = profiles.last().unwrap();
        let mut profile_next: HashMap<ProfileItem, ProfilePath> =
            HashMap::with_capacity(profile.len());
        let mut found_shortest: bool = false;
        for (item, path) in profile.iter() {
            if item.is_finished(seqs) {
                found_shortest = true;
                continue;
            }
            for next_item in item.all_possible_next(seqs) {
                use std::collections::hash_map::Entry::*;
                match profile_next.entry(next_item) {
                    Occupied(mut entry) => {
                        entry.get_mut().cnt += path.cnt;
                    }
                    Vacant(entry) => {
                        let profile_path = ProfilePath {
                            cnt: path.cnt,
                            source: Some(item.clone()),
                        };
                        entry.insert(profile_path);
                    }
                }
            }
        }
        if found_shortest {
            break;
        }
        profiles.push(profile_next)
    }

    let recovered = recover_full_path(&profiles, seqs);

    recover_letters(&recovered, seqs)
}

#[cfg(test)]
mod tests {
    use super::sequence_aligner;

    #[test]
    fn it_works() {
        // A B C D E D

        // A   C E D
        //   B C   D
        // A       D

        //   A C E D
        // B   C   D
        //   A     D

        // A   C   E D
        //   B C D
        // A     D

        // A C E D
        // B C D
        // A D

        let seq1 = vec!["A", "C", "E", "D"];
        let seq2 = vec!["B", "C", "D"];
        let seq3 = vec!["A", "D"];
        // let seq4 = vec!["A", "B", "C"];

        let result = sequence_aligner(&[&seq1, &seq2, &seq3]);
        println!("Result: {:?}", result);
    }
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
        let mut mapping: HashMap<StationSeq, usize> = HashMap::new();

        let mut clusters: HashSet<usize> = HashSet::new();

        for (seq, cluster) in self.sequences {
            if mapping.insert((*seq).clone(), cluster).is_some() {
                unreachable!()
            }
            clusters.insert(cluster);
        }

        println!("Total number of unique stop seqs: {}", mapping.len());
        println!("Total number of clusters: {}", clusters.len());

        TimetableGrouped { mapping }
    }
}

impl TimetableGrouped {}
