use std::{
    any::type_name,
    collections::HashMap,
    error,
    fs::{File, OpenOptions},
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    marker::PhantomData,
    path::Path,
    sync::Arc,
};

use anyhow::{Context, Result};
use itertools::Itertools;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use header::get_columns;
use row::{parse_csv_line, serialize_to_csv, to_csv_row};

use rowread::deserialize_item;

use self::row::{FieldReference, FieldReferenceCollection};
pub mod header;
pub mod row;
pub mod rowread;

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
            let reader = BufReader::new(&file);

            let first_line = String::new();
            todo!()
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

pub struct CsvTableReader<R: Read> {
    reader: R,
    headers: HashMap<String, usize>,
}

pub fn from_file<'a, P: AsRef<Path>>(path: P) -> CsvTableReader<BufReader<File>> {
    let file = OpenOptions::new().read(true).open(path).unwrap();
    let reader = BufReader::new(file);
    CsvTableReader::new(reader)
}

impl<R: Read + BufRead> CsvTableReader<R> {
    pub fn new(mut reader: R) -> Self {
        // File already has some data inside, get the headers
        // let mut first_line = String::new();

        let mut line_buf = String::new();
        let mut field_buf = Vec::new();

        reader.read_line(&mut line_buf).unwrap();

        parse_csv_line(line_buf.as_str(), &mut field_buf);

        let mut headers = HashMap::new();

        for (col_i, col) in field_buf.into_str_vec(&line_buf).iter().enumerate() {
            headers.insert(col.to_string(), col_i);
        }

        CsvTableReader { reader, headers }
    }

    /// Deserialize one using buffer as intermediate storage
    pub fn read<'de, D>(
        &mut self,
        field_buf: &'de mut Vec<FieldReference>,
        line_buf: &'de mut String,
    ) -> Result<Option<D>>
    where
        D: Deserialize<'de>,
    {
        line_buf.clear();
        let num_read = self.reader.read_line(line_buf).unwrap();

        if num_read == 0 {
            return Ok(None);
        };

        parse_csv_line(&line_buf, field_buf);

        let deserialized = deserialize_item::<D>(&self.headers, field_buf, line_buf)
            .with_context(|| format!("Could not deserialize {}", type_name::<D>()))?;

        Ok(Some(deserialized))
    }
}
