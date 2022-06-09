pub mod acl;
pub mod merge;
pub mod serialize;
pub mod tags;
pub mod update;

use store::core::acl::Permission;
use store::core::tag::Tag;
use store::{Integer, LongInteger};

use crate::jmap_store::Object;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct TinyORM<T>
where
    T: Object,
{
    #[serde(bound(
        serialize = "HashMap<T::Property, T::Value>: serde::Serialize",
        deserialize = "HashMap<T::Property, T::Value>: serde::Deserialize<'de>"
    ))]
    properties: HashMap<T::Property, T::Value>,
    #[serde(bound(
        serialize = "HashMap<T::Property, HashSet<Tag>>: serde::Serialize",
        deserialize = "HashMap<T::Property, HashSet<Tag>>: serde::Deserialize<'de>"
    ))]
    tags: HashMap<T::Property, HashSet<Tag>>,
    acls: Vec<Permission>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub enum Index {
    Text(String),
    TextList(Vec<String>),
    Integer(Integer),
    IntegerList(Vec<Integer>),
    LongInteger(LongInteger),
    Null,
}

impl From<String> for Index {
    fn from(value: String) -> Self {
        Index::Text(value)
    }
}

impl From<Vec<String>> for Index {
    fn from(value: Vec<String>) -> Self {
        Index::TextList(value)
    }
}

impl From<Integer> for Index {
    fn from(value: Integer) -> Self {
        Index::Integer(value)
    }
}

impl From<Vec<Integer>> for Index {
    fn from(value: Vec<Integer>) -> Self {
        Index::IntegerList(value)
    }
}

impl From<LongInteger> for Index {
    fn from(value: LongInteger) -> Self {
        Index::LongInteger(value)
    }
}

pub trait Value
where
    Self: Sized
        + Sync
        + Send
        + Eq
        + PartialEq
        + Default
        + std::fmt::Debug
        + serde::Serialize
        + for<'de> serde::Deserialize<'de>,
{
    fn index_as(&self) -> Index;
    fn is_empty(&self) -> bool;
}

impl Value for () {
    fn index_as(&self) -> Index {
        Index::Null
    }

    fn is_empty(&self) -> bool {
        true
    }
}

impl<T> Default for TinyORM<T>
where
    T: Object,
{
    fn default() -> Self {
        Self {
            properties: HashMap::new(),
            tags: HashMap::new(),
            acls: Vec::new(),
        }
    }
}

impl<T> TinyORM<T>
where
    T: Object + 'static,
{
    pub const FIELD_ID: u8 = u8::MAX;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn track_changes(source: &TinyORM<T>) -> TinyORM<T> {
        TinyORM {
            properties: HashMap::new(),
            tags: source.tags.clone(),
            acls: source.acls.clone(),
        }
    }

    pub fn get(&self, property: &T::Property) -> Option<&T::Value> {
        self.properties.get(property)
    }

    pub fn get_mut(&mut self, property: &T::Property) -> Option<&mut T::Value> {
        self.properties.get_mut(property)
    }

    pub fn entry(&mut self, property: T::Property) -> Entry<'_, T::Property, T::Value> {
        self.properties.entry(property)
    }

    pub fn set(&mut self, property: T::Property, value: impl Into<T::Value>) {
        self.properties.insert(property, value.into());
    }

    pub fn remove(&mut self, property: &T::Property) -> Option<T::Value> {
        self.properties.remove(property)
    }

    pub fn has_property(&self, property: &T::Property) -> bool {
        self.properties.contains_key(property)
    }
}
