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

impl JSONValue {
    pub fn is_null(&self) -> bool {
        matches!(self, JSONValue::Null)
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
