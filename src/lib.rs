use std::borrow::Cow;
use std::collections::HashMap;
use std::f32;
use std::fmt;
use std::ops::Index;
use std::str;
use std::fs::File;
use std::io::prelude::*;

pub struct ByteBuffer<'a> {
  data: &'a [u8],
  index: usize,
}

impl<'a> ByteBuffer<'a> {
  pub fn new(data: &[u8]) -> ByteBuffer {
    ByteBuffer{data, index: 0}
  }

  pub fn data(&self) -> &'a [u8] {
    self.data
  }

  pub fn index(&self) -> usize {
    self.index
  }

  pub fn read_bool(&mut self) -> Result<bool, ()> {
    match self.read_byte() {
      Ok(0) => Ok(false),
      Ok(1) => Ok(true),
      _ => Err(()),
    }
  }

  pub fn read_byte(&mut self) -> Result<u8, ()> {
    if self.index >= self.data.len() {
      Err(())
    } else {
      let value = self.data[self.index];
      self.index = self.index + 1;
      Ok(value)
    }
  }

  pub fn read_bytes(&mut self, len: usize) -> Result<&'a [u8], ()> {
    if self.index + len > self.data.len() {
      Err(())
    } else {
      let value = &self.data[self.index..self.index + len];
      self.index = self.index + len;
      Ok(value)
    }
  }

  pub fn read_var_int(&mut self) -> Result<i32, ()> {
    let value = self.read_var_uint()?;
    Ok((if (value & 1) != 0 { !(value >> 1) } else { value >> 1 }) as i32)
  }

  pub fn read_var_uint(&mut self) -> Result<u32, ()> {
    let mut shift: u8 = 0;
    let mut result: u32 = 0;

    loop {
      let byte = self.read_byte()?;
      result |= ((byte & 127) as u32) << shift;
      shift += 7;

      if (byte & 128) == 0 || shift >= 35 {
        break;
      }
    }

    Ok(result)
  }

  pub fn read_var_float(&mut self) -> Result<f32, ()> {
    let first = self.read_byte()?;

    if first == 0 {
      Ok(0.0)
    }

    else if self.index + 3 > self.data.len() {
      Err(())
    }

    else {
      let mut bits: u32 =
        first as u32 |
        ((self.data[self.index] as u32) << 8) |
        ((self.data[self.index + 1] as u32) << 16) |
        ((self.data[self.index + 2] as u32) << 24);
      self.index += 3;

      bits = (bits << 23) | (bits >> 9);

      Ok(f32::from_bits(bits))
    }
  }

  pub fn read_string(&mut self) -> Result<Cow<'a, str>, ()> {
    let start = self.index;

    while self.index < self.data.len() {
      if self.data[self.index] == 0 {
        self.index += 1;
        return Ok(String::from_utf8_lossy(&self.data[start..self.index - 1]));
      }

      self.index += 1;
    }

    Err(())
  }
}

pub struct ByteBufferMut {
  data: Vec<u8>,
}

impl ByteBufferMut {
  pub fn new() -> ByteBufferMut {
    ByteBufferMut{data: vec![]}
  }

  pub fn data(self) -> Vec<u8> {
    self.data
  }

  pub fn len(&self) -> usize {
    self.data.len()
  }

  pub fn write_bool(&mut self, value: bool) {
    self.data.push(if value { 1 } else { 0 });
  }

  pub fn write_byte(&mut self, value: u8) {
    self.data.push(value);
  }

  pub fn write_bytes(&mut self, value: &[u8]) {
    self.data.extend_from_slice(value);
  }

  pub fn write_var_int(&mut self, value: i32) {
    self.write_var_uint(((value << 1) ^ (value >> 31)) as u32);
  }

  pub fn write_var_uint(&mut self, mut value: u32) {
    loop {
      let byte = value as u8 & 127;
      value >>= 7;

      if value == 0 {
        self.write_byte(byte);
        return;
      }

      self.write_byte(byte | 128);
    }
  }

  pub fn write_var_float(&mut self, value: f32) {
    let mut bits = value.to_bits();

    bits = (bits >> 23) | (bits << 9);

    if (bits & 255) == 0 {
      self.data.push(0);
      return;
    }

    self.data.extend_from_slice(&[
      bits as u8,
      (bits >> 8) as u8,
      (bits >> 16) as u8,
      (bits >> 24) as u8,
    ]);
  }

  pub fn write_string(&mut self, value: &str) {
    self.data.extend_from_slice(value.as_bytes());
    self.data.push(0);
  }
}

pub const TYPE_BOOL: i32 = -1;
pub const TYPE_BYTE: i32 = -2;
pub const TYPE_INT: i32 = -3;
pub const TYPE_UINT: i32 = -4;
pub const TYPE_FLOAT: i32 = -5;
pub const TYPE_STRING: i32 = -6;

#[derive(Debug, PartialEq)]
pub struct Field {
  pub name: String,

  pub type_id: i32,

  pub is_array: bool,

  pub value: u32,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DefKind {
  Enum,
  Struct,
  Message,
}

pub const DEF_ENUM: u8 = 0;
pub const DEF_STRUCT: u8 = 1;
pub const DEF_MESSAGE: u8 = 2;

#[derive(Debug, PartialEq)]
pub struct Def {
  pub name: String,

  pub index: i32,

  pub kind: DefKind,

  pub fields: Vec<Field>,

  pub field_value_to_index: HashMap<u32, usize>,
  pub field_name_to_index: HashMap<String, usize>,
}

impl Def {
  pub fn new(name: String, kind: DefKind, fields: Vec<Field>) -> Def {
    let mut field_value_to_index = HashMap::new();
    let mut field_name_to_index = HashMap::new();
    for (i, field) in fields.iter().enumerate() {
      field_value_to_index.insert(field.value, i);
      field_name_to_index.insert(field.name.clone(), i);
    }
    Def {name, index: 0, kind, fields, field_value_to_index, field_name_to_index}
  }

  pub fn field(&self, name: &str) -> Option<&Field> {
    self.field_name_to_index.get(name).map(|i| &self.fields[*i])
  }
}

#[derive(Debug, PartialEq)]
pub struct Schema {
  pub name : String,

  pub defs: Vec<Def>,

  pub def_name_to_index: HashMap<String, usize>,

}

impl Schema {
  pub fn new(name: String, mut defs: Vec<Def>) -> Schema {
    let mut def_name_to_index = HashMap::new();
    for (i, def) in defs.iter_mut().enumerate() {
      def.index = i as i32;
      def_name_to_index.insert(def.name.clone(), i);
    }
    Schema {name, defs, def_name_to_index}
  }

  pub fn decode(name: String, bytes: &[u8]) -> Result<Schema, ()> {
    let mut defs = Vec::new();
    let mut bb = ByteBuffer::new(bytes);
    let definition_count = bb.read_var_uint()?;

    for _ in 0..definition_count {
      let name = bb.read_string()?.into_owned();
      let kind = match bb.read_byte()? {
        DEF_ENUM => DefKind::Enum,
        DEF_STRUCT => DefKind::Struct,
        DEF_MESSAGE => DefKind::Message,
        _ => return Err(()),
      };
      let field_count = bb.read_var_uint()?;
      let mut fields = Vec::new();

      for _ in 0..field_count {
        let name = bb.read_string()?.into_owned();
        let type_id = bb.read_var_int()?;
        let is_array = bb.read_bool()?;
        let value = bb.read_var_uint()?;
        fields.push(Field {name, type_id, is_array, value});
      }

      defs.push(Def::new(name, kind, fields));
    }

    Ok(Schema::new(name, defs))
  }

  pub fn save_to(target: String, bytes: Vec<u8>) -> std::io::Result<()> {
    let mut pos = 0;
    let mut buffer = File::create(target)?;
    while pos < bytes.len() {
        let bytes_written = buffer.write(&bytes[pos..])?;
        pos += bytes_written;
    }
    Ok(())
  }

  pub fn encode(&self) -> Vec<u8> {
    let mut bb = ByteBufferMut::new();
    bb.write_var_uint(self.defs.len() as u32);

    for def in &self.defs {
      bb.write_string(def.name.as_str());
      bb.write_byte(match def.kind {
        DefKind::Enum => DEF_ENUM,
        DefKind::Struct => DEF_STRUCT,
        DefKind::Message => DEF_MESSAGE,
      });
      bb.write_var_uint(def.fields.len() as u32);

      for field in &def.fields {
        bb.write_string(field.name.as_str());
        bb.write_var_int(field.type_id);
        bb.write_bool(field.is_array);
        bb.write_var_uint(field.value);
      }
    }

    bb.data()
  }

  pub fn def(&self, name: &str) -> Option<&Def> {
    self.def_name_to_index.get(name).map(|i| &self.defs[*i])
  }

  pub fn skip(&self, bb: &mut ByteBuffer, type_id: i32) -> Result<(), ()> {
    match type_id {
      TYPE_BOOL => { bb.read_bool()?; },
      TYPE_BYTE => { bb.read_byte()?; },
      TYPE_INT => { bb.read_var_int()?; },
      TYPE_UINT => { bb.read_var_uint()?; },
      TYPE_FLOAT => { bb.read_var_float()?; },
      TYPE_STRING => { bb.read_string()?; },

      _ => {
        let def = &self.defs[type_id as usize];

        match def.kind {
          DefKind::Enum => {
            if !def.field_value_to_index.contains_key(&bb.read_var_uint()?) {
              return Err(());
            }
          },

          DefKind::Struct => {
            for field in &def.fields {
              self.skip_field(bb, field)?;
            }
          },

          DefKind::Message => {
            loop {
              let value = bb.read_var_uint()?;
              if value == 0 {
                break;
              }
              if let Some(index) = def.field_value_to_index.get(&value) {
                self.skip_field(bb, &def.fields[*index])?;
              } else {
                return Err(());
              }
            }
          },
        }
      },
    }

    Ok(())
  }

  pub fn skip_field(&self, bb: &mut ByteBuffer, field: &Field) -> Result<(), ()> {
    if field.is_array {
      let len = bb.read_var_uint()? as usize;
      for _ in 0..len {
        self.skip(bb, field.type_id)?;
      }
    } else {
      self.skip(bb, field.type_id)?;
    }
    Ok(())
  }
}

#[derive(Clone, PartialEq)]
pub enum Value<'a> {
  Bool(bool),
  Byte(u8),
  Int(i32),
  UInt(u32),
  Float(f32),
  String(String),
  Array(Vec<Value<'a>>),
  Enum(&'a str, &'a str),
  Object(&'a str, HashMap<&'a str, Value<'a>>),
}

impl<'a> Value<'a> {
  pub fn as_bool(&self) -> bool {
    match *self {
      Value::Bool(value) => value,
      _ => false,
    }
  }

  pub fn as_byte(&self) -> u8 {
    match *self {
      Value::Byte(value) => value,
      _ => 0,
    }
  }

  pub fn as_int(&self) -> i32 {
    match *self {
      Value::Int(value) => value,
      _ => 0,
    }
  }

  pub fn as_uint(&self) -> u32 {
    match *self {
      Value::UInt(value) => value,
      _ => 0,
    }
  }

  pub fn as_float(&self) -> f32 {
    match *self {
      Value::Float(value) => value,
      _ => 0.0,
    }
  }

  pub fn as_string(&self) -> &str {
    match *self {
      Value::String(ref value) => value.as_str(),
      _ => "",
    }
  }

  pub fn len(&self) -> usize {
    match *self {
      Value::Array(ref values) => values.len(),
      _ => 0,
    }
  }

  pub fn push(&mut self, value: Value<'a>) {
    if let Value::Array(ref mut values) = *self {
      values.push(value);
    }
  }

  pub fn get(&self, name: &str) -> Option<&Value<'a>> {
    match *self {
      Value::Object(_, ref fields) => fields.get(name),
      _ => None,
    }
  }

  pub fn set(&mut self, name: &'a str, value: Value<'a>) {
    if let Value::Object(_, ref mut fields) = *self {
      fields.insert(name, value);
    }
  }

  pub fn remove(&mut self, name: &'a str) {
    if let Value::Object(_, ref mut fields) = *self {
      fields.remove(name);
    }
  }

  pub fn decode(schema: &'a Schema, type_id: i32, bytes: &[u8]) -> Result<Value<'a>, ()> {
    Value::decode_bb(schema, type_id, &mut ByteBuffer::new(bytes))
  }

  pub fn encode(&self, schema: &Schema) -> Vec<u8> {
    let mut bb = ByteBufferMut::new();
    self.encode_bb(schema, &mut bb);
    bb.data()
  }

  pub fn decode_bb(schema: &'a Schema, type_id: i32, bb: &mut ByteBuffer) -> Result<Value<'a>, ()> {
    match type_id {
      TYPE_BOOL => { Ok(Value::Bool(bb.read_bool()?)) },
      TYPE_BYTE => { Ok(Value::Byte(bb.read_byte()?)) },
      TYPE_INT => { Ok(Value::Int(bb.read_var_int()?)) },
      TYPE_UINT => { Ok(Value::UInt(bb.read_var_uint()?)) },
      TYPE_FLOAT => { Ok(Value::Float(bb.read_var_float()?)) },
      TYPE_STRING => { Ok(Value::String(bb.read_string()?.into_owned())) },

      _ => {
        let def = &schema.defs[type_id as usize];

        match def.kind {
          DefKind::Enum => {
            if let Some(index) = def.field_value_to_index.get(&bb.read_var_uint()?) {
              Ok(Value::Enum(def.name.as_str(), def.fields[*index].name.as_str()))
            } else {
              Err(())
            }
          },

          DefKind::Struct => {
            let mut fields = HashMap::new();
            for field in &def.fields {
              fields.insert(field.name.as_str(), Value::decode_field_bb(schema, field, bb)?);
            }
            Ok(Value::Object(def.name.as_str(), fields))
          },

          DefKind::Message => {
            let mut fields = HashMap::new();
            loop {
              let value = bb.read_var_uint()?;
              if value == 0 {
                return Ok(Value::Object(def.name.as_str(), fields));
              }
              if let Some(index) = def.field_value_to_index.get(&value) {
                let field = &def.fields[*index];
                fields.insert(field.name.as_str(), Value::decode_field_bb(schema, field, bb)?);
              } else {
                return Err(());
              }
            }
          },
        }
      },
    }
  }

  pub fn decode_field_bb(schema: &'a Schema, field: &Field, bb: &mut ByteBuffer) -> Result<Value<'a>, ()> {
    if field.is_array {
      let len = bb.read_var_uint()? as usize;
      let mut array = Vec::with_capacity(len);
      for _ in 0..len {
        array.push(Value::decode_bb(schema, field.type_id, bb)?);
      }
      Ok(Value::Array(array))
    } else {
      Value::decode_bb(schema, field.type_id, bb)
    }
  }

  pub fn encode_bb(&self, schema: &Schema, bb: &mut ByteBufferMut) {
    match *self {
      Value::Bool(value) => bb.write_byte(if value { 1 } else { 0 }),
      Value::Byte(value) => bb.write_byte(value),
      Value::Int(value) => bb.write_var_int(value),
      Value::UInt(value) => bb.write_var_uint(value),
      Value::Float(value) => bb.write_var_float(value),
      Value::String(ref value) => bb.write_string(value.as_str()),

      Value::Array(ref values) => {
        bb.write_var_uint(values.len() as u32);
        for value in values {
          value.encode_bb(schema, bb);
        }
        return;
      },

      Value::Enum(name, value) => {
        let def = &schema.defs[*schema.def_name_to_index.get(name).unwrap()];
        let index = *def.field_name_to_index.get(value).unwrap();
        bb.write_var_uint(def.fields[index].value);
      },

      Value::Object(name, ref fields) => {
        let def = &schema.defs[*schema.def_name_to_index.get(name).unwrap()];
        match def.kind {
          DefKind::Enum => panic!(),
          DefKind::Struct => {
            for field in &def.fields {
              fields.get(field.name.as_str()).unwrap().encode_bb(schema, bb);
            }
          },
          DefKind::Message => {
            for field in &def.fields {
              if let Some(value) = fields.get(field.name.as_str()) {
                bb.write_var_uint(field.value);
                value.encode_bb(schema, bb);
              }
            }
            bb.write_byte(0);
          },
        }
      },
    }
  }
}

impl<'a> Index<usize> for Value<'a> {
  type Output = Value<'a>;

  fn index(&self, index: usize) -> &Value<'a> {
    match *self {
      Value::Array(ref values) => &values[index],
      _ => panic!(),
    }
  }
}

impl<'a> fmt::Debug for Value<'a> {
  fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    match *self {
      Value::Bool(value) => value.fmt(f),
      Value::Byte(value) => value.fmt(f),
      Value::Int(value) => value.fmt(f),
      Value::UInt(value) => value.fmt(f),
      Value::Float(value) => value.fmt(f),
      Value::String(ref value) => value.fmt(f),
      Value::Array(ref values) => values.fmt(f),
      Value::Enum(name, ref value) => write!(f, "{}::{}", name, value),

      Value::Object(name, ref fields) => {
        let mut keys: Vec<_> = fields.keys().collect();
        let mut first = true;
        keys.sort();
        write!(f, "{} {{", name)?;

        for key in keys {
          if first {
            first = false;
          } else {
            write!(f, ", ")?;
          }
          write!(f, "{}: {:?}", key, fields[key])?;
        }

        write!(f, "}}")
      },
    }
  }
}