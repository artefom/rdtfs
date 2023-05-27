use std::{
    fs::OpenOptions,
    io::{Read, Write},
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

pub struct BinaryReader<R: Read> {
    reader: R,
}

impl<R: Read> BinaryReader<R> {
    pub fn new(reader: R) -> Self {
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
