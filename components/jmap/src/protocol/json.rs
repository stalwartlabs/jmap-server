use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
#[serde(untagged)]
pub enum JSONValue {
    Null,
    Bool(bool),
    String(String),
    Number(JSONNumber),
    Array(Vec<JSONValue>),
    Object(HashMap<String, JSONValue>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone)]
#[serde(untagged)]
pub enum JSONNumber {
    PosInt(u64),
    NegInt(i64),
    Float(f64),
}

impl Eq for JSONNumber {}

impl JSONNumber {
    pub fn to_unsigned_int(&self) -> u64 {
        match self {
            JSONNumber::PosInt(i) => *i,
            JSONNumber::NegInt(i) => {
                if *i > 0 {
                    *i as u64
                } else {
                    0
                }
            }
            JSONNumber::Float(f) => {
                if *f > 0.0 {
                    *f as u64
                } else {
                    0
                }
            }
        }
    }

    pub fn to_int(&self) -> i64 {
        match self {
            JSONNumber::PosInt(i) => *i as i64,
            JSONNumber::NegInt(i) => *i,
            JSONNumber::Float(f) => *f as i64,
        }
    }
}

impl Default for JSONValue {
    fn default() -> Self {
        JSONValue::Null
    }
}

impl From<HashMap<String, JSONValue>> for JSONValue {
    fn from(o: HashMap<String, JSONValue>) -> Self {
        JSONValue::Object(o)
    }
}

impl From<Vec<JSONValue>> for JSONValue {
    fn from(a: Vec<JSONValue>) -> Self {
        JSONValue::Array(a)
    }
}

impl From<String> for JSONValue {
    fn from(s: String) -> Self {
        JSONValue::String(s)
    }
}

impl From<bool> for JSONValue {
    fn from(b: bool) -> Self {
        JSONValue::Bool(b)
    }
}

impl From<i64> for JSONValue {
    fn from(i: i64) -> Self {
        JSONValue::Number(JSONNumber::NegInt(i))
    }
}

impl From<u64> for JSONValue {
    fn from(i: u64) -> Self {
        JSONValue::Number(JSONNumber::PosInt(i))
    }
}

impl From<u32> for JSONValue {
    fn from(i: u32) -> Self {
        JSONValue::Number(JSONNumber::PosInt(i as u64))
    }
}

impl From<usize> for JSONValue {
    fn from(i: usize) -> Self {
        JSONValue::Number(JSONNumber::PosInt(i as u64))
    }
}

impl From<()> for JSONValue {
    fn from(_: ()) -> Self {
        JSONValue::Null
    }
}

impl<T> From<Option<T>> for JSONValue
where
    JSONValue: From<T>,
{
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => JSONValue::Null,
        }
    }
}

impl JSONValue {
    pub fn is_null(&self) -> bool {
        matches!(self, JSONValue::Null)
    }

    pub fn to_array(&self) -> Option<&Vec<JSONValue>> {
        match self {
            JSONValue::Array(array) => Some(array),
            _ => None,
        }
    }

    pub fn to_object(&self) -> Option<&HashMap<String, JSONValue>> {
        match self {
            JSONValue::Object(object) => Some(object),
            _ => None,
        }
    }

    pub fn to_object_mut(&mut self) -> Option<&mut HashMap<String, JSONValue>> {
        match self {
            JSONValue::Object(object) => Some(object),
            _ => None,
        }
    }

    pub fn as_object_mut(&mut self) -> &mut HashMap<String, JSONValue> {
        match self {
            JSONValue::Object(object) => object,
            _ => unreachable!(),
        }
    }

    pub fn as_array_mut(&mut self) -> &mut Vec<JSONValue> {
        match self {
            JSONValue::Array(array) => array,
            _ => unreachable!(),
        }
    }

    pub fn to_string(&self) -> Option<&str> {
        match self {
            JSONValue::String(string) => Some(string),
            _ => None,
        }
    }

    pub fn to_unsigned_int(&self) -> Option<u64> {
        match self {
            JSONValue::Number(number) => Some(number.to_unsigned_int()),
            _ => None,
        }
    }

    pub fn to_int(&self) -> Option<i64> {
        match self {
            JSONValue::Number(number) => Some(number.to_int()),
            _ => None,
        }
    }

    pub fn to_bool(&self) -> Option<bool> {
        match self {
            JSONValue::Bool(bool) => Some(*bool),
            _ => None,
        }
    }

    pub fn unwrap_array(self) -> Option<Vec<JSONValue>> {
        match self {
            JSONValue::Array(array) => array.into(),
            _ => None,
        }
    }

    pub fn unwrap_object(self) -> Option<HashMap<String, JSONValue>> {
        match self {
            JSONValue::Object(object) => object.into(),
            _ => None,
        }
    }

    pub fn unwrap_string(self) -> Option<String> {
        match self {
            JSONValue::String(string) => Some(string),
            _ => None,
        }
    }

    pub fn unwrap_unsigned_int(self) -> Option<u64> {
        self.to_unsigned_int()
    }

    pub fn unwrap_bool(self) -> Option<bool> {
        match self {
            JSONValue::Bool(bool) => Some(bool),
            _ => None,
        }
    }
}
