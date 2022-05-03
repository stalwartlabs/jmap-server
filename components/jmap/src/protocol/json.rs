use std::{collections::HashMap, vec::IntoIter};

use serde::{Deserialize, Serialize};
use store::{
    chrono::{LocalResult, SecondsFormat, TimeZone, Utc},
    core::number::Number,
};

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

impl From<&JSONNumber> for Number {
    fn from(value: &JSONNumber) -> Self {
        match value {
            JSONNumber::PosInt(i) => Number::LongInteger(*i),
            JSONNumber::NegInt(i) => Number::LongInteger(*i as u64),
            JSONNumber::Float(f) => Number::Float(*f),
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

    pub fn is_empty(&self) -> bool {
        match self {
            JSONValue::Null => true,
            JSONValue::String(string) => string.is_empty(),
            JSONValue::Array(array) => array.is_empty(),
            JSONValue::Object(obj) => obj.is_empty(),
            JSONValue::Bool(_) | JSONValue::Number(_) => false,
        }
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

    pub fn into_utc_date(self) -> JSONValue {
        match self {
            JSONValue::Number(timestamp) => {
                if let LocalResult::Single(timestamp) =
                    Utc.timestamp_opt(timestamp.to_unsigned_int() as i64, 0)
                {
                    timestamp.to_rfc3339_opts(SecondsFormat::Secs, true).into()
                } else {
                    JSONValue::Null
                }
            }
            JSONValue::Array(items) => items
                .into_iter()
                .filter_map(|item| {
                    if let LocalResult::Single(timestamp) =
                        Utc.timestamp_opt(item.unwrap_unsigned_int()? as i64, 0)
                    {
                        Some(timestamp.to_rfc3339_opts(SecondsFormat::Secs, true).into())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .into(),
            _ => JSONValue::Null,
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

    pub fn unwrap_values(self, mut visitor: impl FnMut(JSONValue) -> bool) {
        let mut it: Option<IntoIter<JSONValue>> = None;
        let mut it_stack: Vec<IntoIter<JSONValue>> = Vec::new();
        let mut item = self;

        loop {
            if let Some(cur_it) = it.as_mut() {
                if let Some(next) = cur_it.next() {
                    item = next;
                } else if let Some(prev_it) = it_stack.pop() {
                    it = Some(prev_it);
                    continue;
                } else {
                    break;
                }
            }

            match item {
                JSONValue::Array(array) => {
                    if let Some(it) = it {
                        it_stack.push(it);
                    }
                    it = Some(array.into_iter());
                }
                JSONValue::String(_) | JSONValue::Number(_) => {
                    if !visitor(item) {
                        return;
                    }
                }
                _ => {}
            }

            if it.is_none() {
                break;
            } else {
                item = JSONValue::Null;
            }
        }
    }

    pub fn unwrap_object_properties(
        self,
        properties: &[&str],
        mut visitor: impl FnMut(usize, JSONValue) -> bool,
    ) {
        let mut it: Option<IntoIter<JSONValue>> = None;
        let mut it_stack: Vec<IntoIter<JSONValue>> = Vec::new();
        let mut item = self;

        loop {
            if let Some(cur_it) = it.as_mut() {
                if let Some(next) = cur_it.next() {
                    item = next;
                } else if let Some(prev_it) = it_stack.pop() {
                    it = Some(prev_it);
                    continue;
                } else {
                    break;
                }
            }

            match item {
                JSONValue::Array(array) => {
                    if let Some(it) = it {
                        it_stack.push(it);
                    }
                    it = Some(array.into_iter());
                }
                JSONValue::Object(mut obj) => {
                    for (property_idx, property) in properties.iter().enumerate() {
                        if let Some(value) = obj.remove(*property) {
                            if !visitor(property_idx, value) {
                                return;
                            }
                        }
                    }
                    for (_, value) in obj {
                        if let JSONValue::Array(array) = value {
                            if let Some(it) = it {
                                it_stack.push(it);
                            }
                            it = Some(array.into_iter());
                        }
                    }
                }
                _ => {}
            }

            if it.is_none() {
                break;
            } else {
                item = JSONValue::Null;
            }
        }
    }
}
