use std::{borrow::Cow, collections::HashMap, fmt::Display, hash::Hash};

use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub enum JSONPointer<'x, T> {
    Wildcard,
    Property(T),
    String(Cow<'x, str>),
    Number(u64),
    Path(Vec<JSONPointer<'x, T>>),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum JSONValue<'x, T>
where
    T: Hash + Eq + PartialEq,
{
    Null,
    Bool(bool),
    String(Cow<'x, str>),
    Number(i64),
    Array(Vec<JSONValue<'x, T>>),
    Object(HashMap<T, JSONValue<'x, T>>),
}

impl<'x, 'y, T> From<&JSONValue<'y, T>> for JSONValue<'x, Cow<'x, str>>
where
    T: Hash + Eq + PartialEq + std::fmt::Display,
{
    fn from(value: &JSONValue<'y, T>) -> Self {
        match value {
            JSONValue::Null => JSONValue::Null,
            JSONValue::Bool(value) => JSONValue::Bool(*value),
            JSONValue::String(string) => JSONValue::String(string.clone().into_owned().into()),
            JSONValue::Number(value) => JSONValue::Number(*value),
            JSONValue::Array(list) => JSONValue::Array(list.iter().map(JSONValue::from).collect()),
            JSONValue::Object(map) => JSONValue::Object(
                map.iter()
                    .map(|(key, value)| (key.to_string().into(), JSONValue::from(value)))
                    .collect(),
            ),
        }
    }
}

impl<'x, 'y, T> JSONValue<'y, T>
where
    T: Hash + Eq + PartialEq + std::fmt::Display,
{
    pub fn into_string(self) -> JSONValue<'x, Cow<'x, str>> {
        match self {
            JSONValue::Null => JSONValue::Null,
            JSONValue::Bool(value) => JSONValue::Bool(value),
            JSONValue::String(string) => JSONValue::String(string.into_owned().into()),
            JSONValue::Number(value) => JSONValue::Number(value),
            JSONValue::Array(list) => {
                JSONValue::Array(list.into_iter().map(JSONValue::into_string).collect())
            }
            JSONValue::Object(map) => JSONValue::Object(
                map.into_iter()
                    .map(|(key, value)| (key.to_string().into(), JSONValue::into_string(value)))
                    .collect(),
            ),
        }
    }
}

impl<'x, T> Default for JSONValue<'x, T>
where
    T: Hash + Eq + PartialEq,
{
    fn default() -> Self {
        JSONValue::Null
    }
}

impl<'x, T> JSONPointer<'x, T> {
    pub fn as_string(&self) -> String {
        "".to_string()
    }
}

impl<'x, T> Display for JSONPointer<'x, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_string())
    }
}
