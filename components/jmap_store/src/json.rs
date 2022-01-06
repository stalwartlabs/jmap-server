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

#[derive(Debug, Serialize, Deserialize)]
pub enum JSONValue<'x, T>
where
    T: Hash + Eq + PartialEq,
{
    Null,
    Bool(bool),
    String(Cow<'x, str>),
    Number(f64),
    Array(Vec<JSONValue<'x, T>>),
    Object(HashMap<Cow<'x, str>, JSONValue<'x, T>>),
    Properties(HashMap<T, JSONValue<'x, T>>),
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
