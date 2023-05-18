use std::any::type_name;
use std::collections::HashMap;
use std::error;
use std::fmt;
use std::ops::{AddAssign, MulAssign, Neg};

use serde::de::{
    self, DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess, VariantAccess,
    Visitor,
};
use serde::Deserialize;

use super::row::FieldReference;

#[derive(Debug)]
pub enum Error {
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

impl de::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Self::Message(msg.to_string())
    }
}

/// Lifetime 'de is the lifetime for references for the data that is being deserialized
/// Lifetime 'a is for the reference to headers and divisions
struct CsvRowDeserializer<'a, 'de> {
    item: CsvRow<'a, 'de>,
    next_header: Option<&'static str>,
}

impl<'de> CsvRowDeserializer<'_, 'de> {
    fn set_header(&mut self, header: &'static str) {
        self.next_header = Some(header)
    }

    fn get_maybe_value(&self) -> Option<&'de str> {
        let Some(next_header) = self.next_header else {
            unreachable!()
        };
        let Some(value) = self.item.get(next_header) else {
            return None
        };
        if value.len() == 0 {
            return None;
        };
        Some(value)
    }

    fn get_value(&self) -> Result<&'de str, Error> {
        let Some(next_header) = self.next_header else {
            unreachable!()
        };
        let Some(value) = self.item.get(next_header) else {
            return Err(Error::Message(format!("Expected value, column {} not found", next_header)));
        };
        if value.len() == 0 {
            return Err(Error::Message(format!(
                "Expected value for column {} got empty string",
                next_header
            )));
        }
        Ok(value)
    }
}

impl<'a, 'de> de::Deserializer<'de> for &'a mut CsvRowDeserializer<'_, 'de> {
    type Error = Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        return Err(Error::Message(
            "Deserializing any is not supported".to_string(),
        ));
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let Ok(parsed) = self.get_value()?.parse::<i8>() else {
            return Err(Error::Message("Could not parse value as i8".to_string()))
        };
        visitor.visit_i8(parsed)
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let Ok(parsed) = self.get_value()?.parse::<i16>() else {
            return Err(Error::Message("Could not parse value as i16".to_string()))
        };
        visitor.visit_i16(parsed)
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let Ok(parsed) = self.get_value()?.parse::<i32>() else {
            return Err(Error::Message("Could not parse value as i32".to_string()))
        };
        visitor.visit_i32(parsed)
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let Ok(parsed) = self.get_value()?.parse::<i64>() else {
            return Err(Error::Message("Could not parse value as i64".to_string()))
        };
        visitor.visit_i64(parsed)
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let Ok(parsed) = self.get_value()?.parse::<u8>() else {
            return Err(Error::Message("Could not parse value as u8".to_string()))
        };
        visitor.visit_u8(parsed)
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let Ok(parsed) = self.get_value()?.parse::<u16>() else {
            return Err(Error::Message("Could not parse value as u16".to_string()))
        };
        visitor.visit_u16(parsed)
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let value = self.get_value()?;
        let Ok(parsed) = value.parse::<u32>() else {
            return Err(Error::Message(format!("Could not parse '{value}' as u32")))
        };
        visitor.visit_u32(parsed)
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let value = self.get_value()?;

        let Ok(parsed) = value.parse::<u64>() else {
            return Err(Error::Message("Could not parse value as u64".to_string()))
        };

        visitor.visit_u64(parsed)
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let value = self.get_value()?;

        let Ok(parsed) = value.parse::<f32>() else {
            return Err(Error::Message("Could not parse value as f32".to_string()))
        };

        visitor.visit_f32(parsed)
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let value = self.get_value()?;

        let Ok(parsed) = value.parse::<f64>() else {
            return Err(Error::Message(format!("Could not parse value {value} as f64")))
        };

        visitor.visit_f64(parsed)
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let value = self.get_value()?;
        visitor.visit_borrowed_str(value)
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let value = self.get_value()?;
        visitor.visit_borrowed_str(value)
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        match self.get_maybe_value() {
            Some(value) => visitor.visit_some(self),
            None => visitor.visit_none(),
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        let rec_visitor = RecordVisitor {
            de: &mut *self,
            fields: fields,
            current_field: 0,
        };

        let value = visitor.visit_map(rec_visitor)?;

        Ok(value)
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        // let value = self.header.get(self.cur_header_pos).unwrap();
        // self.cur_header_pos += 1;
        // visitor.visit_borrowed_str(value.as_ref())
        let Some(value) = self.next_header else {
            unreachable!()
        };
        visitor.visit_borrowed_str(value)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        todo!()
    }
}

// Lifetime 'a is for headers and divisions
// Lifetime 'de is for data that is being deserialized
struct RecordVisitor<'a, 'b, 'de> {
    de: &'b mut CsvRowDeserializer<'a, 'de>,
    fields: &'static [&'static str],
    current_field: usize,
}

impl<'a, 'b, 'de> MapAccess<'de> for RecordVisitor<'a, 'b, 'de> {
    type Error = Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: DeserializeSeed<'de>,
    {
        let Some(&current_field) = self.fields.get(self.current_field) else {
            return Ok(None)
        };
        self.current_field += 1;

        self.de.next_header = Some(current_field);

        // This will call deserialize identifier
        seed.deserialize(&mut *self.de).map(Some)
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: DeserializeSeed<'de>,
    {
        seed.deserialize(&mut *self.de)
    }
}

/// Lifetime 'de is for the data that is beinf deserialized
/// Lifetime 'a is for reference to parent element
struct CsvRow<'a, 'de> {
    header: &'a HashMap<String, usize>,
    divisions: &'a Vec<FieldReference>,
    data: &'de str,
}

impl<'a, 'de> CsvRow<'a, 'de> {
    fn get(&self, key: &str) -> Option<&'de str> {
        let Some(col_i) = self.header.get(key) else {
            return None
        };

        let Some(division) = self.divisions.get(*col_i) else {
            return None
        };

        Some(division.get(self.data.as_ref()))
    }
}

pub fn deserialize_item<'a, 'de, D: Deserialize<'de>>(
    header: &'a HashMap<String, usize>,
    record: &'a Vec<FieldReference>,
    data: &'de str,
) -> Result<D, Error> {
    let item = CsvRow::<'a, 'de> {
        header: header,
        divisions: record,
        data: data,
    };

    let mut deserializer = CsvRowDeserializer {
        item,
        next_header: None,
    };

    D::deserialize(&mut deserializer)
}
