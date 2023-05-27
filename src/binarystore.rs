use std::{
    collections::{hash_map::DefaultHasher, HashMap, HashSet},
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{BufReader, Read, Write},
    marker::PhantomData,
    path::PathBuf,
};

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use tempfile::{tempdir, TempDir};

struct BinaryWriter {
    path: PathBuf,
    buf: Vec<u8>,
}

impl BinaryWriter {
    fn new(path: PathBuf) -> Self {
        BinaryWriter {
            path,
            buf: Vec::new(),
        }
    }

    fn into_path(self) -> Result<PathBuf> {
        Ok(self.path)
    }

    pub fn write_one<S: Serialize>(&mut self, data: &S) -> Result<()> {
        bincode::serialize_into(&mut self.buf, data).context("Could not serialize data")?;
        if self.buf.len() > 8000 {
            self.flush()?;
        }
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.buf.len() == 0 {
            return Ok(());
        };
        let mut file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&self.path)?;
        file.write(&self.buf)?;
        self.buf.clear();
        Ok(())
    }
}

struct BinaryReader<R: Read> {
    reader: R,
}

impl<R: Read> BinaryReader<R> {
    fn new(reader: R) -> Self {
        BinaryReader { reader }
    }
}

impl<R: Read> BinaryReader<R> {
    pub fn read_one<D: DeserializeOwned>(&mut self) -> Result<Option<D>> {
        // let num_bytes = bincode::deserialize_from(&mut self.reader)?;

        let object = match bincode::deserialize_from(&mut self.reader) {
            Ok(value) => Ok(value),
            Err(err) => {
                if let bincode::ErrorKind::Io(_) = *err {
                    return Ok(None);
                } else {
                    Err(err)
                }
            }
        }?;

        Ok(Some(object))
    }
}

pub struct PartitionedStoreWriter<S, H, K>
where
    S: Serialize + DeserializeOwned,
    H: Hash + Eq,
    K: Fn(&S) -> H,
{
    dir: TempDir,
    partitions: Vec<BinaryWriter>,
    key: K,
    num_written: usize,
    _phantom: PhantomData<S>,
}

fn calculate_hash<T: Hash>(t: T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

impl<'a, S, H, K> PartitionedStoreWriter<S, H, K>
where
    S: Serialize + DeserializeOwned,
    H: Hash + Eq + Serialize + DeserializeOwned,
    K: Fn(&S) -> H,
{
    pub fn new(n_partitions: usize, key: K) -> Result<Self> {
        let dir = tempdir().context("Could not create temporary directory")?;

        let mut partitions = Vec::new();

        for partition_i in 0..n_partitions {
            partitions.push(BinaryWriter::new(
                dir.path().join(format!("part-{}", partition_i)),
            ))
        }

        Ok(PartitionedStoreWriter {
            partitions,
            dir,
            key,
            num_written: 0,
            _phantom: PhantomData,
        })
    }

    pub fn write(&mut self, obj: &S) -> Result<()> {
        let key_obj = (self.key)(obj);
        let partition_id: usize = (calculate_hash(&key_obj) % (self.partitions.len() as u64))
            .try_into()
            .unwrap();

        let target_partition = &mut self.partitions[partition_id];

        target_partition
            .write_one(&key_obj)
            .context("Could not write key into partition")?;

        target_partition
            .write_one(obj)
            .context("Could not write to partition")?;

        self.num_written += 1;

        Ok(())
    }

    fn flush_all(&mut self) -> Result<()> {
        for partition in &mut self.partitions {
            partition.flush().context("Could not flush partition")?;
        }
        Ok(())
    }
    pub fn into_reader(mut self) -> Result<PartitionedReader<H, S>> {
        // Flush all pending data
        self.flush_all()?;

        let mut partitions = Vec::new();

        for partition in self.partitions {
            let file = partition.into_path()?;
            partitions.push(file)
        }

        Ok(PartitionedReader {
            partitions: partitions,
            total_count: self.num_written,
            _dir: self.dir,
            _phantom: PhantomData,
            _phantom_key: PhantomData,
        })
    }
}

pub struct PartitionedReader<H, D>
where
    H: DeserializeOwned,
    D: DeserializeOwned,
{
    partitions: Vec<PathBuf>,
    total_count: usize,
    _dir: TempDir,
    _phantom: PhantomData<D>,
    _phantom_key: PhantomData<H>,
}

pub struct PartitionedReaderIter<'a, H, D>
where
    H: DeserializeOwned,
    D: DeserializeOwned,
{
    partition_reader: &'a PartitionedReader<H, D>,
    current_partition: usize,
    current_partition_reader: PartitionReader<H, D>,
}

impl<'a, H, D> PartitionedReaderIter<'a, H, D>
where
    H: DeserializeOwned,
    D: DeserializeOwned,
{
    fn open_or_empty(partition: &PathBuf) -> PartitionReader<H, D> {
        let file = match OpenOptions::new().read(true).open(partition) {
            Ok(value) => value,
            Err(_) => {
                return PartitionReader {
                    reader: None,
                    _phantom: PhantomData,
                    _phantom_key: PhantomData,
                }
            }
        };

        PartitionReader {
            reader: Some(BinaryReader::new(BufReader::new(file))),
            _phantom: PhantomData,
            _phantom_key: PhantomData,
        }
    }
}

impl<'a, H, D> Iterator for PartitionedReaderIter<'a, H, D>
where
    H: DeserializeOwned,
    D: DeserializeOwned,
{
    type Item = Result<D>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current_partition_reader.next() {
                Some(value) => match value {
                    Ok((_, value)) => return Some(Ok(value)),
                    Err(err) => return Some(Err(err)),
                },
                None => {
                    self.current_partition += 1;
                    match self.partition_reader.get_partition(self.current_partition) {
                        Some(value) => self.current_partition_reader = value,
                        None => return None, // Run out of partitions, nothing to iterate
                    }
                    continue;
                }
            }
        }
    }
}

impl<H, D> PartitionedReader<H, D>
where
    H: DeserializeOwned,
    D: DeserializeOwned,
{
    pub fn get_partition(&self, index: usize) -> Option<PartitionReader<H, D>> {
        let Some(partition_file) =self.partitions.get(index) else {
            return None
        };

        Some(PartitionedReaderIter::open_or_empty(partition_file))
    }

    pub fn len(&self) -> usize {
        return self.total_count;
    }

    pub fn iter(&self) -> PartitionedReaderIter<H, D> {
        // There should be at least one partition
        let first_parition = self.get_partition(0).unwrap();

        PartitionedReaderIter {
            partition_reader: &self,
            current_partition: 0,
            current_partition_reader: first_parition,
        }
    }
}

pub struct PartitionReader<H, D>
where
    H: DeserializeOwned,
    D: DeserializeOwned,
{
    reader: Option<BinaryReader<BufReader<File>>>,
    _phantom: PhantomData<D>,
    _phantom_key: PhantomData<H>,
}

impl<H, D> Iterator for PartitionReader<H, D>
where
    H: DeserializeOwned,
    D: DeserializeOwned,
{
    type Item = Result<(H, D)>;

    fn next(&mut self) -> Option<Self::Item> {
        let Some(reader) = &mut self.reader else {
            return None
        };

        let key = match reader.read_one::<H>() {
            Ok(value) => match value {
                Some(value) => value,
                None => return None,
            },
            Err(err) => return Some(Err(err)),
        };

        let value: D = match reader.read_one::<D>() {
            Ok(value) => match value {
                Some(value) => value,
                None => return None,
            },
            Err(err) => return Some(Err(err)),
        };

        Some(Ok((key, value)))
    }
}

pub struct JoinReader<'r, H, V1, V2>
where
    H: Hash + DeserializeOwned,
    V1: DeserializeOwned,
    V2: DeserializeOwned,
{
    reader1: &'r PartitionedReader<H, V1>,
    reader2: &'r PartitionedReader<H, V2>,
    hmjoin: HashMapJoin<H, V1, V2>,
    current_partition: usize,
}

struct HashMapJoin<H: Hash, V1, V2> {
    lhs: HashMap<H, Vec<V1>>,
    rhs: HashMap<H, Vec<V2>>,
    keys: Vec<H>,
}

impl<H: Hash + Eq, V1, V2> Iterator for HashMapJoin<H, V1, V2> {
    type Item = (Vec<V1>, Vec<V2>);

    fn next(&mut self) -> Option<Self::Item> {
        let Some(next_key) = self.keys.pop() else {
            return None;
        };

        let lhs = match self.lhs.remove(&next_key) {
            Some(value) => value,
            None => Vec::new(),
        };

        let rhs = match self.rhs.remove(&next_key) {
            Some(value) => value,
            None => Vec::new(),
        };

        Some((lhs, rhs))
    }
}

fn into_hasmap<H, I, V>(collection: I, keyshs: &mut HashSet<H>) -> HashMap<H, Vec<V>>
where
    H: Hash + Eq + Clone,
    I: Iterator<Item = Result<(H, V)>>,
{
    let mut lhs: HashMap<H, Vec<V>> = HashMap::new();

    // Populate lhs
    for value in collection {
        let (key, value) = value.unwrap();

        keyshs.insert(key.clone());

        match lhs.entry(key) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                entry.get_mut().push(value);
            }
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(vec![value]);
            }
        };
    }

    lhs
}

impl<H: Hash + Eq + Clone + DeserializeOwned, V1: DeserializeOwned, V2: DeserializeOwned>
    HashMapJoin<H, V1, V2>
{
    pub fn from_partitions(
        partition1: PartitionReader<H, V1>,
        partition2: PartitionReader<H, V2>,
    ) -> Self {
        let mut keyshs = HashSet::new();
        let lhs = into_hasmap(partition1, &mut keyshs);
        let rhs = into_hasmap(partition2, &mut keyshs);
        HashMapJoin {
            lhs,
            rhs,
            keys: keyshs.into_iter().collect(),
        }
    }
}

/// Join two tables by given key out-of-memory
/// Can be used for extremely large tales
pub fn join<'r, H, V1, V2>(
    reader1: &'r PartitionedReader<H, V1>,
    reader2: &'r PartitionedReader<H, V2>,
) -> Result<JoinReader<'r, H, V1, V2>>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    H: Hash + Eq + Clone + DeserializeOwned,
{
    let partition1 = reader1.get_partition(0).unwrap();
    let partition2 = reader2.get_partition(0).unwrap();

    let hmjoin = HashMapJoin::from_partitions(partition1, partition2);

    Ok(JoinReader {
        reader1: reader1,
        reader2: reader2,
        hmjoin,
        current_partition: 0,
    })
}

impl<'r, H, V1, V2> JoinReader<'r, H, V1, V2>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    H: Hash + Eq + Clone + DeserializeOwned,
{
    fn next_hmjoin(&mut self) -> Option<HashMapJoin<H, V1, V2>> {
        self.current_partition += 1;
        // Hm join is drained, create a new one if possible
        let partition1 = match self.reader1.get_partition(self.current_partition) {
            Some(value) => value,
            None => return None, // No next partition
        };

        let partition2 = match self.reader2.get_partition(self.current_partition) {
            Some(value) => value,
            None => unreachable!(), // This hould not exist since we're guranteed to have next partition
        };

        Some(HashMapJoin::from_partitions(partition1, partition2))
    }
}

impl<'r, H, V1, V2> Iterator for JoinReader<'r, H, V1, V2>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    H: Hash + Eq + Clone + DeserializeOwned,
{
    type Item = (Vec<V1>, Vec<V2>);

    fn next(&mut self) -> Option<Self::Item> {
        // Get next value and return if it exists
        match self.hmjoin.next() {
            Some(value) => return Some(value),
            None => (),
        };

        loop {
            let Some(mut next_hm) = self.next_hmjoin() else {
                // Return none immedietelly if we cannot get next hashmap join
                return None
            };
            let Some(next_value) = next_hm.next() else {
                continue;
            };

            self.hmjoin = next_hm;
            return Some(next_value);
        }
    }
}

pub trait Partitionable<T>
where
    T: Serialize + DeserializeOwned,
{
    fn disk_partition<H, K>(self, num_partitions: usize, key: K) -> Result<PartitionedReader<H, T>>
    where
        K: Fn(&T) -> H,
        H: Hash + Eq + Clone + DeserializeOwned + Serialize;
}

impl<I, T> Partitionable<T> for I
where
    I: Iterator<Item = T>,
    T: Serialize + DeserializeOwned,
{
    fn disk_partition<H, K>(self, num_partitions: usize, key: K) -> Result<PartitionedReader<H, T>>
    where
        K: Fn(&T) -> H,
        H: Hash + Eq + Clone + DeserializeOwned + Serialize,
    {
        let mut table = PartitionedStoreWriter::new(num_partitions, key)?;

        for item in self {
            table.write(&item)?;
        }

        table.into_reader()
    }
}
