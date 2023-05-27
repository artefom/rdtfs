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

struct PartitionedWriter<S, H, K>
where
    S: Serialize + DeserializeOwned,
    H: Hash + Eq,
    K: Fn(&S) -> &H,
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

impl<'a, S, H, K> PartitionedWriter<S, H, K>
where
    S: Serialize + DeserializeOwned,
    H: Hash + Eq + Clone,
    K: for<'b> Fn(&'b S) -> &'b H,
{
    fn new(n_partitions: usize, key: K) -> Result<Self> {
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
            key,
            num_written: 0,
            _phantom: PhantomData,
        })
    }

    fn write(&mut self, obj: &S) -> Result<()> {
        let key_obj = (self.key)(obj);
        let partition_id: usize = (calculate_hash(&key_obj) % (self.partitions.len() as u64))
            .try_into()
            .unwrap();

        let target_partition = &mut self.partitions[partition_id];

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
    fn into_reader(mut self) -> Result<PartitionedReader<S, H, K>> {
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
            _hash: PhantomData,
            key: self.key,
        })
    }
}

pub struct PartitionedReader<D, H, K>
where
    D: DeserializeOwned,
    H: Hash + Eq + Clone,
    K: Fn(&D) -> &H,
{
    partitions: Vec<PathBuf>,
    total_count: usize,
    _dir: TempDir,
    _phantom: PhantomData<D>,
    _hash: PhantomData<H>,
    key: K,
}

pub struct PartitionedReaderIter<'a, D, H, K>
where
    D: DeserializeOwned,
    H: Hash + Eq + Clone,
    K: for<'b> Fn(&'b D) -> &'b H,
{
    partition_reader: &'a PartitionedReader<D, H, K>,
    current_partition: usize,
    current_partition_reader: BinaryReader<BufReader<File>, D>,
    _hash: PhantomData<H>,
    _key: PhantomData<K>,
}

impl<'a, D, H, K> Iterator for PartitionedReaderIter<'a, D, H, K>
where
    D: DeserializeOwned,
    H: Hash + Eq + Clone,
    K: for<'b> Fn(&'b D) -> &'b H,
{
    type Item = Result<D>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.current_partition_reader.next() {
                Some(value) => {
                    let value = value.unwrap();
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

impl<D, H, K> PartitionedReader<D, H, K>
where
    D: DeserializeOwned,
    H: Hash + Eq + Clone,
    K: Fn(&D) -> &H,
{
    fn get_partition(&self, index: usize) -> Option<BinaryReader<BufReader<File>, D>> {
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

    pub fn iter(&self) -> PartitionedReaderIter<D, H, K> {
        // There should be at least one partition
        let first_parition = self.get_partition(0).unwrap();

        PartitionedReaderIter {
            partition_reader: &self,
            current_partition: 0,
            current_partition_reader: first_parition,
            _hash: PhantomData,
            _key: PhantomData,
        }
    }
}

pub struct JoinReader<'r, H, V1, V2, K1, K2>
where
    H: Hash + DeserializeOwned,
    V1: DeserializeOwned,
    V2: DeserializeOwned,
    H: Hash + Eq + Clone,
    K1: Fn(&V1) -> &H,
    K2: Fn(&V2) -> &H,
{
    reader1: &'r PartitionedReader<V1, H, K1>,
    reader2: &'r PartitionedReader<V2, H, K2>,
    current_data: hash_map::IntoIter<H, (Vec<V1>, Vec<V2>)>,
    current_partition: usize,
    _k1: PhantomData<K1>,
    _k2: PhantomData<K2>,
}

/// Join two tables by given key out-of-memory
/// Can be used for extremely large tales
pub fn join<'r, H, V1, V2, K1, K2>(
    reader1: &'r PartitionedReader<V1, H, K1>,
    reader2: &'r PartitionedReader<V2, H, K2>,
) -> Result<JoinReader<'r, H, V1, V2, K1, K2>>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    H: Hash + Eq + Clone + DeserializeOwned,
    K1: Fn(&V1) -> &H,
    K2: Fn(&V2) -> &H,
{
    let partition1 = reader1.get_partition(0).unwrap().map(|x| x.unwrap());
    let partition2 = reader2.get_partition(0).unwrap().map(|x| x.unwrap());

    let joined = hashmap_join(partition1, &reader1.key, partition2, &reader2.key).into_iter();

    Ok(JoinReader {
        reader1: reader1,
        reader2: reader2,
        current_data: joined,
        current_partition: 0,
        _k1: PhantomData,
        _k2: PhantomData,
    })
}

impl<'r, H, V1, V2, K1, K2> Iterator for JoinReader<'r, H, V1, V2, K1, K2>
where
    V1: Serialize + DeserializeOwned,
    V2: Serialize + DeserializeOwned,
    H: Hash + Eq + Clone + DeserializeOwned,
    K1: Fn(&V1) -> &H,
    K2: Fn(&V2) -> &H,
{
    type Item = (H, (Vec<V1>, Vec<V2>));

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
                &self.reader1.key,
                partition2.map(|x| x.unwrap()),
                &self.reader2.key,
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

pub trait Partitionable<T>
where
    T: Serialize + DeserializeOwned,
{
    fn disk_partition<H, K>(
        self,
        num_partitions: usize,
        key: K,
    ) -> Result<PartitionedReader<T, H, K>>
    where
        K: Fn(&T) -> &H,
        H: Hash + Eq + Clone + DeserializeOwned + Serialize;
}

impl<I, T> Partitionable<T> for I
where
    I: Iterator<Item = T>,
    T: Serialize + DeserializeOwned,
{
    fn disk_partition<H, K>(
        self,
        num_partitions: usize,
        key: K,
    ) -> Result<PartitionedReader<T, H, K>>
    where
        K: Fn(&T) -> &H,
        H: Hash + Eq + Clone + DeserializeOwned + Serialize,
    {
        let mut table = PartitionedWriter::new(num_partitions, key)?;

        for item in self {
            table.write(&item)?;
        }

        table.into_reader()
    }
}
