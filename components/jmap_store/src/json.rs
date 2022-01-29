use std::{collections::HashMap, fmt::Display};

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum JSONPointer<T> {
    Wildcard,
    Property(T),
    String(String),
    Number(u64),
    Path(Vec<JSONPointer<T>>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum JSONValue {
    Null,
    Bool(bool),
    String(String),
    Number(i64),
    Array(Vec<JSONValue>),
    Object(HashMap<String, JSONValue>),
}

impl Default for JSONValue {
    fn default() -> Self {
        JSONValue::Null
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
        JSONValue::Number(i)
    }
}

impl From<usize> for JSONValue {
    fn from(i: usize) -> Self {
        JSONValue::Number(i as i64)
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

    pub fn to_string(&self) -> Option<&str> {
        match self {
            JSONValue::String(string) => Some(string),
            _ => None,
        }
    }

    pub fn to_number(&self) -> Option<i64> {
        match self {
            JSONValue::Number(number) => Some(*number),
            _ => None,
        }
    }

    pub fn to_bool(&self) -> Option<bool> {
        match self {
            JSONValue::Bool(bool) => Some(*bool),
            _ => None,
        }
    }

    pub fn unwrap_array(self) -> Vec<JSONValue> {
        match self {
            JSONValue::Array(array) => array,
            _ => panic!("Expected array"),
        }
    }

    pub fn unwrap_object(self) -> HashMap<String, JSONValue> {
        match self {
            JSONValue::Object(object) => object,
            _ => panic!("Expected object"),
        }
    }
}

impl<T> JSONPointer<T> {
    pub fn as_string(&self) -> String {
        "".to_string()
    }
}

impl<T> Display for JSONPointer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_string())
    }
}
