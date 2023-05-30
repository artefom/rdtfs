use std::{
    collections::hash_map::DefaultHasher,
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

use self::binarystore::BinaryReader;

mod binarystore;

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

    fn write(&mut self, obj: &V, key_obj: K) -> Result<()> {
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
            partitions,
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
    _dir: TempDir,
    _phantom: PhantomData<V>,
    _phantom_key: PhantomData<K>,
}

impl<K, V> PartitionedReader<K, V>
where
    K: DeserializeOwned,
    V: DeserializeOwned,
{
    pub fn get_partition(&self, index: usize) -> Option<BinaryReader<BufReader<File>, (K, V)>> {
        let Some(partition_file) =self.partitions.get(index) else {
            return None
        };

        // File must exist, if it does not - some error with disk occured
        // or bug in the partitioned writer
        let file = OpenOptions::new().read(true).open(partition_file).unwrap();

        Some(BinaryReader::new(BufReader::new(file)))
    }
}

pub trait Partitionable<V>
where
    V: Serialize + DeserializeOwned,
{
    fn disk_partition<K, F>(self, num_partitions: usize, key: F) -> Result<PartitionedReader<K, V>>
    where
        F: FnMut(&V) -> K,
        K: Hash + Eq + Clone + DeserializeOwned + Serialize;

    fn disk_multipartition<K, F>(
        self,
        num_partitions: usize,
        key: F,
    ) -> Result<PartitionedReader<K, V>>
    where
        F: FnMut(&V) -> Vec<K>,
        K: Hash + Eq + Clone + DeserializeOwned + Serialize;
}

impl<I, V> Partitionable<V> for I
where
    I: Iterator<Item = V>,
    V: Serialize + DeserializeOwned,
{
    fn disk_partition<K, F>(
        self,
        num_partitions: usize,
        mut key: F,
    ) -> Result<PartitionedReader<K, V>>
    where
        F: FnMut(&V) -> K,
        K: Hash + Eq + Clone + DeserializeOwned + Serialize,
    {
        let mut table = PartitionedWriter::new(num_partitions)?;

        for item in self {
            let key_obj = key(&item);
            table.write(&item, key_obj)?;
        }

        table.into_reader()
    }

    fn disk_multipartition<K, F>(
        self,
        num_partitions: usize,
        mut key: F,
    ) -> Result<PartitionedReader<K, V>>
    where
        F: FnMut(&V) -> Vec<K>,
        K: Hash + Eq + Clone + DeserializeOwned + Serialize,
    {
        let mut table = PartitionedWriter::new(num_partitions)?;
        for item in self {
            let keys = key(&item);
            for key in keys {
                table.write(&item, key)?;
            }
        }
        table.into_reader()
    }
}
