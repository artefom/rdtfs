use std::{
    collections::hash_map::DefaultHasher,
    fmt::Debug,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{Read, Write},
    marker::PhantomData,
    path::PathBuf,
};

use anyhow::{bail, Context, Result};
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

pub struct PartitionedStoreWriter<S: Serialize> {
    dir: TempDir,
    partitions: Vec<BinaryWriter>,
    _phantom: PhantomData<S>,
}

fn calculate_hash<T: Hash>(t: T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

impl<S: Serialize + DeserializeOwned> PartitionedStoreWriter<S> {
    pub fn new(n_partitions: usize) -> Result<Self> {
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
            _phantom: PhantomData,
        })
    }

    pub fn write<F, H>(&mut self, obj: &S, key: F) -> Result<()>
    where
        F: for<'a> Fn(&'a S) -> &'a H,
        H: Hash + Debug,
    {
        let partition_id: usize = (calculate_hash(key(obj)) % (self.partitions.len() as u64))
            .try_into()
            .unwrap();

        let target_partition = &mut self.partitions[partition_id];

        target_partition
            .write_one(obj)
            .context("Could not write to partition")?;

        Ok(())
    }

    fn flush_all(&mut self) -> Result<()> {
        for partition in &mut self.partitions {
            partition.flush().context("Could not flush partition")?;
        }
        Ok(())
    }
    pub fn into_reader(mut self) -> Result<PartitionedReader<S>> {
        // Flush all pending data
        self.flush_all()?;

        let mut partitions = Vec::new();

        for partition in self.partitions {
            let file = partition.into_path()?;
            partitions.push(Some(file))
        }

        Ok(PartitionedReader {
            partitions: partitions,
            _dir: self.dir,
            _phantom: PhantomData,
        })
    }
}

pub struct PartitionedReader<D: DeserializeOwned> {
    partitions: Vec<Option<PathBuf>>,
    _dir: TempDir,
    _phantom: PhantomData<D>,
}

impl<D: DeserializeOwned> PartitionedReader<D> {
    pub fn read_partition(&mut self, partition_i: usize) -> Result<Option<PartitionReader<D>>> {
        let Some(partition) = self.partitions.get_mut(partition_i) else {
            return Ok(None)
        };

        let Some(partition) = partition.take() else {
            bail!("Partition was already read")
        };

        // Try opening file or return empty reader if file does not exist
        let file = match OpenOptions::new().read(true).open(partition) {
            Ok(value) => value,
            Err(_) => {
                return Ok(Some(PartitionReader {
                    reader: None,
                    _phantom: PhantomData,
                }))
            }
        };

        Ok(Some(PartitionReader {
            reader: Some(BinaryReader::new(file)),
            _phantom: PhantomData,
        }))
    }
}

pub struct PartitionReader<D: DeserializeOwned> {
    reader: Option<BinaryReader<File>>,
    _phantom: PhantomData<D>,
}

impl<D: DeserializeOwned> Iterator for PartitionReader<D> {
    type Item = Result<D>;

    fn next(&mut self) -> Option<Self::Item> {
        let Some(reader) = &mut self.reader else {
            return None
        };

        match reader.read_one::<D>() {
            Ok(value) => match value {
                Some(value) => Some(Ok(value)),
                None => None,
            },
            Err(err) => Some(Err(err)),
        }
    }
}
