use std::{
    any::type_name,
    collections::HashMap,
    io::{BufRead, Read},
};

use anyhow::{Context, Result};

use serde::Deserialize;

use rowread::{deserialize_item, parse_csv_line, Divisions};

pub mod rowread;

pub struct CsvTableReader<R: Read> {
    reader: R,
    headers: HashMap<String, usize>,
    buf: CsvRowBuf,
}

struct CsvRowBuf {
    divisions: Vec<Divisions>,
    data: String,
}

impl Default for CsvRowBuf {
    fn default() -> Self {
        Self {
            divisions: Default::default(),
            data: Default::default(),
        }
    }
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

        for col_i in 0..field_buf.len() {
            let col = field_buf.get(col_i).unwrap().get(&line_buf);
            headers.insert(col.to_string(), col_i);
        }

        CsvTableReader {
            reader,
            headers,
            buf: CsvRowBuf::default(),
        }
    }

    /// Deserialize one using buffer as intermediate storage
    pub fn read<'de, D>(&'de mut self) -> Result<Option<D>>
    where
        D: Deserialize<'de>,
    {
        self.buf.data.clear();
        let num_read = self.reader.read_line(&mut self.buf.data).unwrap();

        if num_read == 0 {
            return Ok(None);
        };

        parse_csv_line(&self.buf.data, &mut self.buf.divisions);

        let deserialized =
            deserialize_item::<D>(&self.headers, &self.buf.divisions, &self.buf.data)
                .with_context(|| format!("Could not deserialize {}", type_name::<D>()))?;

        Ok(Some(deserialized))
    }
}
