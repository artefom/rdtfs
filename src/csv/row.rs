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

/// Read csv line with trimming
pub fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut just_hit_quote = false;

    for c in line.chars() {
        match c {
            '"' if !in_quotes && just_hit_quote => {
                just_hit_quote = false;
                in_quotes = true;
                current.push(c)
            }
            '"' if in_quotes => {
                just_hit_quote = true;
                in_quotes = false;
            }
            '"' => {
                in_quotes = true;
            }
            ',' if !in_quotes => {
                fields.push(current.to_string());
                current = String::new();
            }
            _ => {
                current.push(c);
                just_hit_quote = false;
            }
        }
    }

    // Remove trailing new line
    if current.ends_with("\n") {
        fields.push(current[..current.len() - 1].to_string());
    } else {
        fields.push(current.to_string());
    }

    fields
}

#[cfg(test)]
mod test_csv_line {
    use super::parse_csv_line;

    #[test]
    fn test_iteration() {
        assert_eq!(parse_csv_line("a,b,c"), vec!["a", "b", "c"]);
        assert_eq!(parse_csv_line("a,b,c,,,"), vec!["a", "b", "c", "", "", ""]);
        assert_eq!(parse_csv_line("Hello,World!"), vec!["Hello", "World!"]);
        assert_eq!(
            parse_csv_line("message,\"Hello,World!\""),
            vec!["message", "Hello,World!"]
        );
        assert_eq!(
            parse_csv_line("message,\"Hello,\"\"\"\"World\"\"!\""),
            vec!["message", "Hello,\"\"World\"!"]
        );
        assert_eq!(parse_csv_line("a,b"), vec!["a", "b"]);
        assert_eq!(parse_csv_line("a,"), vec!["a", ""]);
        assert_eq!(parse_csv_line("a,\"\""), vec!["a", ""]);
        assert_eq!(parse_csv_line("a,\"\"\"\""), vec!["a", "\""]);
        assert_eq!(parse_csv_line("a,\"\",c"), vec!["a", "", "c"]);
        assert_eq!(parse_csv_line("a,\"\"\"\",c"), vec!["a", "\"", "c"]);
        assert_eq!(parse_csv_line("a ,b ,c "), vec!["a ", "b ", "c "]);
        assert_eq!(parse_csv_line("a ,b ,c \n"), vec!["a ", "b ", "c "]);
        assert_eq!(parse_csv_line("a \n"), vec!["a "]);
        assert_eq!(parse_csv_line("a\n"), vec!["a"]);
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
