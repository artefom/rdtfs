use std::{
    collections::hash_map::{self, DefaultHasher},
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::BufReader,
    marker::PhantomData,
    path::PathBuf,
};

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};
use tempfile::{tempdir, TempDir};

use binarystore::BinaryWriter;

use self::{binarystore::BinaryReader, hasmap_join::hashmap_join};

mod binarystore;
mod hasmap_join;

struct PartitionedWriter<V, K>
where
    V: Serialize + DeserializeOwned,
    K: Hash + Eq,
{
    dir: TempDir,
    partitions: Vec<BinaryWriter>,
    num_written: usize,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

fn calculate_hash<T: Hash>(t: T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

impl<'a, V, K> PartitionedWriter<V, K>
where
    V: Serialize + DeserializeOwned,
    K: Hash + Eq + Serialize + DeserializeOwned,
{
    fn new(n_partitions: usize) -> Result<Self> {
        let dir = tempdir().context("Could not create temporary directory")?;

        let mut partitions = Vec::new();

        for partition_i in 0..n_partitions {
            partitions.push(BinaryWriter::new(
                dir.path().join(format!("part-{}", partition_i)),
            ))
        }

        Ok(PartitionedWriter {
            partitions,
            dir,
            num_written: 0,
            _key: PhantomData,
            _value: PhantomData,
        })
    }

    fn write<F>(&mut self, obj: &V, key: F) -> Result<()>
    where
        F: Fn(&V) -> K,
    {
        let key_obj = key(obj);
        let partition_id: usize = (calculate_hash(&key_obj) % (self.partitions.len() as u64))
            .try_into()
            .unwrap();

        let target_partition = &mut self.partitions[partition_id];

        target_partition
            .write_one(&(key_obj, obj))
            .context("Could not write key into partition")?;

        self.num_written += 1;

        Ok(())
    }

    fn flush_all(&mut self) -> Result<()> {
        for partition in &mut self.partitions {
            partition.flush().context("Could not flush partition")?;
        }
        Ok(())
    }
    fn into_reader(mut self) -> Result<PartitionedReader<K, V>> {
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

pub struct PartitionedReader<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    partitions: Vec<PathBuf>,
    total_count: usize,
    _dir: TempDir,
    _phantom: PhantomData<V>,
    _phantom_key: PhantomData<K>,
}

pub struct PartitionedReaderIter<'a, K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    partition_reader: &'a PartitionedReader<K, V>,
    current_partition: usize,
    current_partition_reader: BinaryReader<BufReader<File>, (K, V)>,
}

impl<'a, K, V> Iterator for PartitionedReaderIter<'a, K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    type Item = Result<V>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current_partition_reader.next() {
                Some(value) => {
                    let (_, value) = value.unwrap();
                    return Some(Ok(value));
                }
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

impl<K, V> PartitionedReader<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    fn get_partition(&self, index: usize) -> Option<BinaryReader<BufReader<File>, (K, V)>> {
        let Some(partition_file) =self.partitions.get(index) else {
            return None
        };

        // File must exist, if it does not - some error with disk occured
        // or bug in the partitioned writer
        let file = OpenOptions::new().read(true).open(partition_file).unwrap();

        Some(BinaryReader::new(BufReader::new(file)))
    }

    pub fn len(&self) -> usize {
        return self.total_count;
    }

    pub fn iter(&self) -> PartitionedReaderIter<K, V> {
        // There should be at least one partition
        let first_parition = self.get_partition(0).unwrap();

        PartitionedReaderIter {
            partition_reader: &self,
            current_partition: 0,
            current_partition_reader: first_parition,
        }
    }
}

pub struct JoinReader<'r, K, V1, V2>
where
    K: Hash + DeserializeOwned,
    V1: DeserializeOwned,
    V2: DeserializeOwned,
{
    reader1: &'r PartitionedReader<K, V1>,
    reader2: &'r PartitionedReader<K, V2>,
    current_data: hash_map::IntoIter<K, (Vec<V1>, Vec<V2>)>,
    current_partition: usize,
}

/// Join two tables by given key out-of-memory
/// Can be used for extremely large tales
pub fn join<'r, K, V1, V2>(
    reader1: &'r PartitionedReader<K, V1>,
    reader2: &'r PartitionedReader<K, V2>,
) -> Result<JoinReader<'r, K, V1, V2>>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    K: Hash + Eq + Clone + DeserializeOwned,
{
    let partition1 = reader1.get_partition(0).unwrap().map(|x| x.unwrap());
    let partition2 = reader2.get_partition(0).unwrap().map(|x| x.unwrap());

    let joined = hashmap_join(partition1, partition2).into_iter();

    Ok(JoinReader {
        reader1: reader1,
        reader2: reader2,
        current_data: joined,
        current_partition: 0,
    })
}

impl<'r, K, V1, V2> Iterator for JoinReader<'r, K, V1, V2>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    K: Hash + Eq + Clone + DeserializeOwned,
{
    type Item = (K, (Vec<V1>, Vec<V2>));

    fn next(&mut self) -> Option<Self::Item> {
        // Get next value and return if it exists
        match self.current_data.next() {
            Some(value) => return Some(value),
            None => (),
        };

        loop {
            self.current_partition += 1;

            let Some(partition1) = self
                .reader1
                .get_partition(self.current_partition) else {
                    return None
                };

            let Some(partition2) = self
                .reader2
                .get_partition(self.current_partition) else {
                    return None
                };

            let mut joined = hashmap_join(
                partition1.map(|x| x.unwrap()),
                partition2.map(|x| x.unwrap()),
            )
            .into_iter();

            let Some(next_value) = joined.next() else {
                continue;
            };

            self.current_data = joined;
            return Some(next_value);
        }
    }
}

pub trait Partitionable<V>
where
    V: Serialize + DeserializeOwned,
{
    fn disk_partition<K, F>(self, num_partitions: usize, key: F) -> Result<PartitionedReader<K, V>>
    where
        F: Fn(&V) -> K,
        K: Hash + Eq + Clone + DeserializeOwned + Serialize;
}

impl<I, V> Partitionable<V> for I
where
    I: Iterator<Item = V>,
    V: Serialize + DeserializeOwned,
{
    fn disk_partition<K, F>(self, num_partitions: usize, key: F) -> Result<PartitionedReader<K, V>>
    where
        F: Fn(&V) -> K,
        K: Hash + Eq + Clone + DeserializeOwned + Serialize,
    {
        let mut table = PartitionedWriter::new(num_partitions)?;

        for item in self {
            table.write(&item, &key)?;
        }

        table.into_reader()
    }
}
