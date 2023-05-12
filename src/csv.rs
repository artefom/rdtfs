use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    marker::PhantomData,
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Serialize};

use header::get_columns;
use row::{parse_csv_line, serialize_to_csv, to_csv_row};

use crate::csv::rowread::deserialize_item;
mod header;
mod row;
mod rowread;

/// A variant of `Arc` that delegates IO traits if available on `&T`.
#[derive(Debug)]
pub struct IoArc<T>(Arc<T>);

impl<T> IoArc<T> {
    /// Create a new instance of IoArc.
    pub fn new(data: T) -> Self {
        Self(Arc::new(data))
    }
}

impl<T> Read for IoArc<T>
where
    for<'a> &'a T: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        (&mut &*self.0).read(buf)
    }
}

impl<T> Write for IoArc<T>
where
    for<'a> &'a T: Write,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        (&mut &*self.0).write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        (&mut &*self.0).flush()
    }
}

pub struct CsvTableWriter<S: Serialize> {
    writer: BufWriter<File>,
    _phantom: PhantomData<S>,
    headers: Option<Vec<String>>,
}

impl<S: Serialize> CsvTableWriter<S> {
    pub fn new<P: AsRef<Path>>(path: P) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(path)
            .unwrap();

        // File already has some data inside, get the headers
        let headers = if file.metadata().unwrap().len() > 0 {
            let mut reader = BufReader::new(&file);

            let mut first_line = String::new();
            reader.read_line(&mut first_line).unwrap();

            Some(parse_csv_line(&first_line))
        } else {
            None
        };

        CsvTableWriter {
            writer: BufWriter::new(file),
            headers: headers,
            _phantom: PhantomData,
        }
    }

    /// Write header to file and set internal header storage
    fn write_header(&mut self, headers: Vec<String>) -> &Vec<String> {
        self.writer.write(to_csv_row(&headers).as_bytes()).unwrap();
        self.writer.write("\n".as_bytes()).unwrap();
        self.headers = Some(headers);
        self.headers.as_ref().unwrap()
    }

    /// Writes row to the end of the file
    pub fn write_row(&mut self, item: &S) {
        let headers = match &self.headers {
            Some(value) => value,

            None => self.write_header(get_columns(&item).iter().map(|x| x.to_string()).collect()),
        };

        let serialized = serialize_to_csv(headers, item);

        self.writer.write(serialized.as_bytes()).unwrap();
        self.writer.write("\n".as_bytes()).unwrap();
    }
}

pub struct CsvTableReader<S: DeserializeOwned, R: Read> {
    reader: R,
    _phantom: PhantomData<S>,
    headers: Vec<String>,
}

pub fn from_file<D: DeserializeOwned, P: AsRef<Path>>(
    path: P,
) -> CsvTableReader<D, BufReader<File>> {
    let file = OpenOptions::new().read(true).open(path).unwrap();

    let mut reader = BufReader::new(file);

    // File already has some data inside, get the headers
    let mut first_line = String::new();
    reader.read_line(&mut first_line).unwrap();
    let headers = parse_csv_line(&first_line);

    CsvTableReader {
        reader,
        headers,
        _phantom: PhantomData,
    }
}

impl<D: DeserializeOwned, R: Read + BufRead> CsvTableReader<D, R> {
    pub fn new(mut reader: R) -> Self {
        // File already has some data inside, get the headers
        let mut first_line = String::new();
        reader.read_line(&mut first_line).unwrap();
        let headers = parse_csv_line(&first_line);

        CsvTableReader {
            reader,
            headers,
            _phantom: PhantomData,
        }
    }

    /// Reads one row from the file
    pub fn read_row(&mut self) -> Result<Option<D>> {
        let mut line = String::new();

        // Read line into buffer and return if 0 bytes were read
        let num_read = self.reader.read_line(&mut line).unwrap();
        if num_read == 0 {
            return Ok(None);
        };

        let records = parse_csv_line(&line);

        let item: HashMap<&String, &String> = records.iter().zip(self.headers.iter()).collect();

        let deserialized: D =
            deserialize_item(&self.headers, &records).context("Could not deserialize item")?;

        Ok(Some(deserialized))
    }
}

impl<D: DeserializeOwned, R: Read + BufRead> Iterator for CsvTableReader<D, R> {
    type Item = D;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let value = match self.read_row() {
                Ok(value) => value,
                Err(err) => {
                    log::error!("{:#}", err);
                    continue;
                }
            };
            return value;
        }
    }
}
