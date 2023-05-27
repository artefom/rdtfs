use std::{
    fs::OpenOptions,
    io::{Read, Write},
    marker::PhantomData,
    path::PathBuf,
};

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};

pub struct BinaryWriter {
    path: PathBuf,
    buf: Vec<u8>,
}

impl BinaryWriter {
    pub fn new(path: PathBuf) -> Self {
        BinaryWriter {
            path,
            buf: Vec::new(),
        }
    }

    pub fn into_path(self) -> Result<PathBuf> {
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

pub struct BinaryReader<R: Read, T: DeserializeOwned> {
    reader: R,
    _phantom: PhantomData<T>,
}

impl<R: Read, T: DeserializeOwned> BinaryReader<R, T> {
    pub fn new(reader: R) -> Self {
        BinaryReader {
            reader,
            _phantom: PhantomData,
        }
    }
}

impl<R: Read, T: DeserializeOwned> Iterator for BinaryReader<R, T> {
    type Item = bincode::Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let object = bincode::deserialize_from::<_, T>(&mut self.reader);

        match object {
            Ok(value) => Some(Ok(value)),
            Err(err) => match *err {
                bincode::ErrorKind::Io(_) => None, // Finished reading file
                _ => Some(Err(err)),
            },
        }
    }
}
