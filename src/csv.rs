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

use crate::csv::rowread::deserialize_item;

use self::row::{FieldReference, FieldReferenceCollection};
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
            let reader = BufReader::new(&file);

            let first_line = String::new();
            todo!()
            // Some(
            //     parse_csv_line(&first_line)
            //         .iter()
            //         .map(|x| x.to_string())
            //         .collect(),
            // )
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

    // fn deserialize_one<'de, F: FnMut(D) -> (), D: Deserialize<'de>>(
    //     headers: &HashMap<String, usize>,
    //     data: &'de str,
    //     divisions: &Vec<FieldReference>,
    //     mut callable: F,
    // ) {

    //     let deserialized: D = deserialize_item(headers, divisions, data)
    //         .with_context(|| format!("Could not deserialize {}", type_name::<D>()))
    //         .unwrap();
    //     callable(deserialized)
    // }

    /// Map all rows from the file
    pub fn map<F: FnMut(D) -> (), D: DeserializeOwned>(&mut self, mut callable: F) -> Result<()> {
        // Read line into buffer and return if 0 bytes were read
        // todo!();

        let mut line_buf = String::new();
        let mut field_buf: Vec<FieldReference> = Vec::new();

        loop {
            line_buf.clear();
            let num_read = self.reader.read_line(&mut line_buf).unwrap();

            if num_read == 0 {
                return Ok(());
            };

            parse_csv_line(&mut line_buf, &mut field_buf);

            let deserialized = deserialize_item::<D>(&self.headers, &mut field_buf, &mut line_buf)
                .with_context(|| format!("Could not deserialize {}", type_name::<D>()));

            match deserialized {
                Ok(value) => callable(value),
                Err(error) => log::error!("Error deserializing: {error}"),
            }
        }

        // loop {
        //     self.line_buf.clear();
        //     // let num_read = self.reader.read_line((*self).line_buf).unwrap();
        //     // if num_read == 0 {
        //     //     return Ok(());
        //     // };

        //     {
        //         parse_csv_line(self.line_buf, self.field_buf);
        //         Self::deserialize_to_function(
        //             &self.headers,
        //             &self.line_buf,
        //             &self.field_buf,
        //             &mut callable,
        //         );

        //         // let deserialized: D =
        //         //     deserialize_item(&self.headers, self.field_buf, &self.line_buf)
        //         //         .with_context(|| format!("Could not deserialize {}", type_name::<D>()))?;

        //         // callable(deserialized);
        //     }
        // }

        // Ok(())
    }
}
