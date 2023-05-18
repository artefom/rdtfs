use itertools::join;
use serde::{
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
    Serialize,
};
use std::{
    collections::HashMap,
    error,
    fmt::{self},
};

use serde::{de, ser};

struct RowSerializer<'a, H: AsRef<str>> {
    headers: &'a [H],
    current_item: HashMap<&'static str, String>,
}

#[derive(Debug)]
enum Error {
    Message(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Message(message) => write!(f, "{}", message),
        }
    }
}

impl error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Message(msg.to_string())
    }
}

impl<'a, 'b, H: AsRef<str>> serde::Serializer for &'a mut RowSerializer<'b, H> {
    type Ok = String;
    type Error = Error;

    type SerializeSeq = Self;

    type SerializeTuple = Self;

    type SerializeTupleStruct = Self;

    type SerializeTupleVariant = Self;

    type SerializeMap = Self;

    type SerializeStruct = Self;

    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_i8(self, v: i8) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_i16(self, v: i16) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_i32(self, v: i32) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_i64(self, v: i64) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_u8(self, v: u8) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_u16(self, v: u16) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_u32(self, v: u32) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_u64(self, v: u64) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_f32(self, v: f32) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_f64(self, v: f64) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_char(self, v: char) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Self::Error> {
        Ok(format!("{}", v))
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        Ok("".to_string())
    }

    fn serialize_some<T: ?Sized>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(variant)
    }

    fn serialize_newtype_struct<T: ?Sized>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        todo!()
    }

    fn serialize_newtype_variant<T: ?Sized>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: Serialize,
    {
        todo!()
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        todo!()
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        todo!()
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        todo!()
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        Ok(self)
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        todo!()
    }
}

impl<'a, 'b, H: AsRef<str>> SerializeSeq for &'a mut RowSerializer<'b, H> {
    type Ok = String;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl<'a, 'b, H: AsRef<str>> SerializeTuple for &'a mut RowSerializer<'b, H> {
    type Ok = String;
    type Error = Error;

    fn serialize_element<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl<'a, 'b, H: AsRef<str>> SerializeTupleStruct for &'a mut RowSerializer<'b, H> {
    type Ok = String;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl<'a, 'b, H: AsRef<str>> SerializeTupleVariant for &'a mut RowSerializer<'b, H> {
    type Ok = String;
    type Error = Error;

    fn serialize_field<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl<'a, 'b, H: AsRef<str>> SerializeMap for &'a mut RowSerializer<'b, H> {
    type Ok = String;
    type Error = Error;

    fn serialize_key<T: ?Sized>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        todo!()
    }

    fn serialize_value<T: ?Sized>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl<'a, 'b, H: AsRef<str>> SerializeStruct for &'a mut RowSerializer<'b, H> {
    type Ok = String;
    type Error = Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        let value_str = value.serialize(&mut **self)?;
        self.current_item.insert(key, value_str);
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        let mut items: Vec<&String> = Vec::new();
        let empty_string = String::new();
        let row = {
            for header in self.headers {
                if let Some(value) = self.current_item.get(header.as_ref()) {
                    items.push(value);
                } else {
                    items.push(&empty_string);
                }
            }
            to_csv_row(&items)
        };
        self.current_item.clear();
        Ok(row)
    }
}

impl<'a, 'b, H: AsRef<str>> SerializeStructVariant for &'a mut RowSerializer<'b, H> {
    type Ok = String;
    type Error = Error;

    fn serialize_field<T: ?Sized>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), Self::Error>
    where
        T: Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

pub trait FieldReferenceCollection {
    fn into_str_vec<'a>(&self, data: &'a str) -> Vec<&'a str>;
}
pub struct FieldReference {
    field_start: usize,
    field_end: usize,
}

impl FieldReference {
    pub fn get<'a>(&self, data: &'a str) -> &'a str {
        &data[self.field_start..self.field_end]
    }
}

impl FieldReferenceCollection for Vec<FieldReference> {
    fn into_str_vec<'a>(&self, data: &'a str) -> Vec<&'a str> {
        let mut result = Vec::new();

        for item in self {
            result.push(&data[item.field_start..item.field_end])
        }

        result
    }
}

/// Read csv line with trimming
/// No-copy deserialisation
pub fn parse_csv_line<'a, 'b>(line: &'a str, out: &'b mut Vec<FieldReference>) {
    // let mut fields: Vec<&str> = Vec::new();
    // let mut current = String::new();
    let mut in_quotes = false;
    let mut just_hit_quote = false;

    let mut field_start: usize = 0;
    let mut field_end: usize = 0;

    let mut current_field: usize = 0;

    for (c_i, c) in line.bytes().enumerate() {
        match c {
            b'"' if !in_quotes && just_hit_quote => {
                just_hit_quote = false;
                in_quotes = true;
                field_end = c_i + 1;
            }
            b'"' if in_quotes => {
                just_hit_quote = true;
                in_quotes = false;
            }
            b'"' => {
                in_quotes = true;
                if field_end == field_start {
                    field_start = c_i + 1;
                }
                field_end = c_i + 1;
            }
            b',' if !in_quotes => {
                if out.len() <= current_field {
                    out.push(FieldReference {
                        field_start,
                        field_end,
                    });
                } else {
                    out[current_field] = FieldReference {
                        field_start,
                        field_end,
                    };
                };
                current_field += 1;
                field_start = c_i + 1;
                field_end = field_start;
            }
            _ => {
                field_end = c_i + 1;
                just_hit_quote = false;
            }
        }
    }

    if field_end > 0 && &line[field_end - 1..field_end] == "\n" && field_end > field_start {
        field_end = field_end - 1
    };

    if out.len() <= current_field {
        out.push(FieldReference {
            field_start,
            field_end,
        })
    } else {
        out[current_field] = FieldReference {
            field_start,
            field_end,
        };
    }

    current_field += 1;

    out.truncate(current_field);
}

#[cfg(test)]
mod test_csv_line {
    use crate::csv::row::FieldReferenceCollection;

    use super::parse_csv_line;

    #[test]
    fn test_iteration() {
        let line = "a,b,c";
        let mut out = Vec::new();
        parse_csv_line(line, &mut out);
        assert_eq!(out.into_str_vec(line), vec!["a", "b", "c"]);

        let line = "a,b,c,,,";
        parse_csv_line(line, &mut out);
        assert_eq!(out.into_str_vec(line), vec!["a", "b", "c", "", "", ""]);

        // parse_csv_line("Hello,World!", &mut out);
        // assert_eq!(out, vec!["Hello", "World!"]);
        // parse_csv_line("message,\"Hello,World!\"", &mut out);
        // assert_eq!(out, vec!["message", "Hello,World!"]);
        // parse_csv_line("a,b", &mut out);
        // assert_eq!(out, vec!["a", "b"]);
        // parse_csv_line("a,", &mut out);
        // assert_eq!(out, vec!["a", ""]);
        // parse_csv_line("a,\"\"", &mut out);
        // assert_eq!(out, vec!["a", ""]);
        // parse_csv_line("a,\"\",c", &mut out);
        // assert_eq!(out, vec!["a", "", "c"]);
        // parse_csv_line("a ,b ,c ", &mut out);
        // assert_eq!(out, vec!["a ", "b ", "c "]);
        // parse_csv_line("a ,b ,c \n", &mut out);
        // assert_eq!(out, vec!["a ", "b ", "c "]);
        // parse_csv_line("\n", &mut out);
        // assert_eq!(out, vec![""]);
        // parse_csv_line(",\n", &mut out);
        // assert_eq!(out, vec!["", ""]);
        // parse_csv_line("a \n", &mut out);
        // assert_eq!(out, vec!["a "]);
        // parse_csv_line("a\n", &mut out);
        // assert_eq!(out, vec!["a"]);
        // parse_csv_line("", &mut out);
        // assert_eq!(out, vec![""]);

        // // // Escaped quotes
        // parse_csv_line("a,\"\"\"\",c", &mut out);
        // assert_eq!(out, vec!["a", "\"\"", "c"]);

        // parse_csv_line("a,\"\"\"\"\"\",c", &mut out);
        // assert_eq!(out, vec!["a", "\"\"\"\"", "c"]);

        // parse_csv_line("message,\"Hello,\"\"\"\"World\"\"!\"", &mut out);
        // assert_eq!(out, vec!["message", "Hello,\"\"\"\"World\"\"!"]);

        // // // Invalid quotation
        // parse_csv_line("aaa,b\"c,\"d,eee", &mut out);
        // assert_eq!(out, vec!["aaa", "b\"c,\"d", "eee"]);
    }
}

/// Convert fields to a csv row
pub fn to_csv_row<S: AsRef<str>>(fields: &[S]) -> String {
    let mut row = String::new();

    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            row.push(',');
        }

        // Check if the field contains a quote or comma
        if field.as_ref().contains('"') || field.as_ref().contains(',') {
            // If so, surround the field with quotes and escape internal quotes
            row.push('"');
            for c in field.as_ref().chars() {
                if c == '"' {
                    row.push_str("\"\"");
                } else {
                    row.push(c);
                }
            }
            row.push('"');
        } else {
            // If not, simply add the field to the row
            row.push_str(field.as_ref());
        }
    }

    row
}

/// Get column names from serialisable
pub fn serialize_to_columns<S: Serialize, H: AsRef<str>>(headers: &[H], value: S) -> Vec<String> {
    todo!()
}

pub fn serialize_to_csv<S: Serialize, H: AsRef<str>>(headers: &[H], value: S) -> String {
    let mut my_serializer = RowSerializer {
        headers: headers,
        current_item: HashMap::new(),
    };

    value.serialize(&mut my_serializer).unwrap()
}
