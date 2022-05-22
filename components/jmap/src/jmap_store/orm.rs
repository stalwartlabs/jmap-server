use store::core::document::Document;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::nlp::Language;
use store::serialize::{StoreDeserialize, StoreSerialize};
use store::write::options::{IndexOptions, Options};
use store::{AccountId, DocumentId, JMAPStore, Store};

use crate::error::set::SetError;
use std::collections::{HashMap, HashSet};

use super::Object;

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub struct TinyORM<T>
where
    T: Object,
{
    #[serde(bound(
        serialize = "HashMap<T::Property, Value<T::Value>>: serde::Serialize",
        deserialize = "HashMap<T::Property, Value<T::Value>>: serde::Deserialize<'de>"
    ))]
    properties: HashMap<T::Property, Value<T::Value>>,
    #[serde(bound(
        serialize = "HashMap<T::Property, HashSet<Tag>>: serde::Serialize",
        deserialize = "HashMap<T::Property, HashSet<Tag>>: serde::Deserialize<'de>"
    ))]
    tags: HashMap<T::Property, HashSet<Tag>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub enum Value<T>
where
    T: Sync + Send + Eq + PartialEq + std::fmt::Debug,
{
    String(String),
    Number(Number),
    Bool(bool),
    Object(T),
    Null,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Debug, Clone)]
pub enum Number {
    PosInt(u64),
    NegInt(i64),
    Float(f64),
}

impl<T> From<String> for Value<T>
where
    T: Sync + Send + Eq + PartialEq + std::fmt::Debug,
{
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl<T> From<u64> for Value<T>
where
    T: Sync + Send + Eq + PartialEq + std::fmt::Debug,
{
    fn from(s: u64) -> Self {
        Value::Number(Number::PosInt(s))
    }
}

impl<T> From<u32> for Value<T>
where
    T: Sync + Send + Eq + PartialEq + std::fmt::Debug,
{
    fn from(s: u32) -> Self {
        Value::Number(Number::PosInt(s as u64))
    }
}

impl<T> From<i64> for Value<T>
where
    T: Sync + Send + Eq + PartialEq + std::fmt::Debug,
{
    fn from(s: i64) -> Self {
        Value::Number(Number::NegInt(s))
    }
}

impl<T> From<f64> for Value<T>
where
    T: Sync + Send + Eq + PartialEq + std::fmt::Debug,
{
    fn from(s: f64) -> Self {
        Value::Number(Number::Float(s))
    }
}

impl<T> From<bool> for Value<T>
where
    T: Sync + Send + Eq + PartialEq + std::fmt::Debug,
{
    fn from(s: bool) -> Self {
        Value::Bool(s)
    }
}

impl<T> From<Option<T>> for Value<T>
where
    T: Sync + Send + Eq + PartialEq + std::fmt::Debug + Into<Value<T>>,
{
    fn from(s: Option<T>) -> Self {
        match s {
            Some(value) => value.into(),
            None => Value::Null,
        }
    }
}

impl Eq for Number {}

impl<T> Default for TinyORM<T>
where
    T: Object,
{
    fn default() -> Self {
        Self {
            properties: HashMap::new(),
            tags: HashMap::new(),
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
        }
    }

    pub fn set(&mut self, property: T::Property, value: impl Into<Value<T::Value>>) {
        self.properties.insert(property, value.into());
    }

    pub fn remove(&mut self, property: &T::Property) -> Option<Value<T::Value>> {
        self.properties.remove(property)
    }

    pub fn has_property(&self, property: &T::Property) -> bool {
        self.properties.contains_key(property)
    }

    pub fn tag(&mut self, property: T::Property, tag: Tag) {
        self.tags
            .entry(property)
            .or_insert_with(HashSet::new)
            .insert(tag);
    }

    pub fn untag(&mut self, property: &T::Property, tag: &Tag) {
        self.tags.get_mut(property).map(|set| set.remove(tag));
    }

    pub fn untag_all(&mut self, property: &T::Property) {
        if let Some(set) = self.tags.get_mut(property) {
            set.clear()
        }
    }

    pub fn get_tags(&self, property: &T::Property) -> Option<&HashSet<Tag>> {
        self.tags.get(property)
    }

    pub fn has_tags(&self, property: &T::Property) -> bool {
        self.tags
            .get(property)
            .map(|set| !set.is_empty())
            .unwrap_or(false)
    }

    pub fn get_unsigned_int(&self, property: &T::Property) -> Option<u64> {
        self.properties
            .get(property)
            .and_then(|value| value.to_unsigned_int())
    }

    pub fn get_string(&self, property: &T::Property) -> Option<&str> {
        self.properties
            .get(property)
            .and_then(|value| value.to_string())
    }

    pub fn remove_string(&mut self, property: &T::Property) -> Option<String> {
        self.properties
            .remove(property)
            .and_then(|value| value.unwrap_string())
    }

    pub fn insert_validate(
        self,
        document: &mut Document,
    ) -> crate::error::set::Result<(), T::Property> {
        for property in T::required() {
            if self
                .properties
                .get(property)
                .map(|v| v.is_empty())
                .unwrap_or(true)
            {
                return Err(SetError::invalid_property(
                    property.clone(),
                    "Property cannot be empty.".to_string(),
                ));
            }
        }
        self.insert(document).map_err(|err| err.into())
    }

    pub fn insert(self, document: &mut Document) -> store::Result<()> {
        self.insert_orm(document)?;
        self.update_document(document, false);
        Ok(())
    }

    pub fn merge_validate(
        self,
        document: &mut Document,
        changes: TinyORM<T>,
    ) -> crate::error::set::Result<bool, T::Property> {
        for property in T::required() {
            if changes
                .properties
                .get(property)
                .map(|v| v.is_empty())
                .unwrap_or_else(|| self.properties.get(property).is_none())
            {
                return Err(SetError::invalid_property(
                    property.clone(),
                    "Property cannot be empty.".to_string(),
                ));
            }
        }
        self.merge(document, changes).map_err(|err| err.into())
    }

    pub fn get_changed_tags(&self, changes: &Self, property: &T::Property) -> HashSet<Tag> {
        match (self.tags.get(property), changes.tags.get(property)) {
            (Some(this), Some(changes)) if this != changes => {
                let mut tag_diff = HashSet::new();
                for tag in this {
                    if !changes.contains(tag) {
                        tag_diff.insert(tag.clone());
                    }
                }
                for tag in changes {
                    if !this.contains(tag) {
                        tag_diff.insert(tag.clone());
                    }
                }
                tag_diff
            }
            (Some(this), None) => this.clone(),
            (None, Some(changes)) => changes.clone(),
            _ => HashSet::with_capacity(0),
        }
    }

    pub fn merge(mut self, document: &mut Document, changes: Self) -> store::Result<bool> {
        let indexed = T::indexed();
        let mut has_changes = false;

        for (property, value) in changes.properties {
            let (is_indexed, index_options) = indexed
                .iter()
                .filter_map(|(p, index_type)| {
                    if p == &property {
                        Some((true, index_type))
                    } else {
                        None
                    }
                })
                .next()
                .unwrap_or((false, &0));

            if let Some(current_value) = self.properties.get(&property) {
                if current_value == &value {
                    continue;
                } else if is_indexed {
                    match &current_value {
                        Value::String(text) => {
                            document.text(
                                property.clone(),
                                text.clone(),
                                Language::Unknown,
                                (*index_options).clear(),
                            );
                        }
                        Value::Number(number) => {
                            document.number(property.clone(), number, (*index_options).clear());
                        }
                        value => {
                            debug_assert!(false, "ORM unsupported type: {:?}", value);
                        }
                    }
                }
            }

            match &value {
                Value::String(text) => {
                    if is_indexed {
                        document.text(
                            property.clone(),
                            text.clone(),
                            Language::Unknown,
                            *index_options,
                        );
                    }

                    self.properties.insert(property, value);
                }
                Value::Number(number) => {
                    if is_indexed {
                        document.number(property.clone(), number, *index_options);
                    }
                    self.properties.insert(property, value);
                }
                Value::Null => {
                    self.properties.remove(&property);
                }
                _ => {
                    self.properties.insert(property, value);
                }
            }

            if !has_changes {
                has_changes = true;
            }
        }

        if self.tags != changes.tags {
            for (property, tags) in &self.tags {
                if let Some(changed_tags) = changes.tags.get(property) {
                    if tags != changed_tags {
                        for tag in tags {
                            if !changed_tags.contains(tag) {
                                document.tag(
                                    property.clone(),
                                    tag.clone(),
                                    IndexOptions::new().clear(),
                                );
                            }
                        }
                    }
                }
            }

            for (property, changed_tags) in &changes.tags {
                if let Some(tags) = self.tags.get(property) {
                    if changed_tags != tags {
                        for changed_tag in changed_tags {
                            if !tags.contains(changed_tag) {
                                document.tag(
                                    property.clone(),
                                    changed_tag.clone(),
                                    IndexOptions::new(),
                                );
                            }
                        }
                    }
                } else {
                    for changed_tag in changed_tags {
                        document.tag(property.clone(), changed_tag.clone(), IndexOptions::new());
                    }
                }
            }

            self.tags = changes.tags;

            if !has_changes {
                has_changes = true;
            }
        }

        if has_changes {
            self.insert_orm(document)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn delete(self, document: &mut Document) {
        TinyORM::<T>::delete_orm(document);
        self.update_document(document, true);
    }

    fn update_document(self, document: &mut Document, is_delete: bool) {
        let indexed = T::indexed();
        if indexed.is_empty() && self.tags.is_empty() {
            return;
        }

        for (property, value) in self.properties {
            let (is_indexed, index_options) = indexed
                .iter()
                .filter_map(|(p, index_options)| {
                    if p == &property {
                        Some((
                            true,
                            if !is_delete {
                                *index_options
                            } else {
                                (*index_options).clear()
                            },
                        ))
                    } else {
                        None
                    }
                })
                .next()
                .unwrap_or((false, 0));
            if is_indexed {
                match value {
                    Value::String(text) => {
                        document.text(property, text, Language::Unknown, index_options);
                    }
                    Value::Number(number) => {
                        document.number(property, &number, index_options);
                    }
                    Value::Null => (),
                    value => {
                        debug_assert!(false, "ORM unsupported type: {:?}", value);
                    }
                }
            }
        }

        let index_options = if !is_delete {
            IndexOptions::new()
        } else {
            IndexOptions::new().clear()
        };
        for (property, tags) in self.tags {
            for tag in tags {
                document.tag(property.clone(), tag, index_options);
            }
        }
    }

    pub fn insert_orm(&self, document: &mut Document) -> store::Result<()> {
        document.binary(
            Self::FIELD_ID,
            self.serialize().ok_or_else(|| {
                StoreError::SerializeError("Failed to serialize ORM object.".to_string())
            })?,
            IndexOptions::new().store(),
        );
        Ok(())
    }

    pub fn delete_orm(document: &mut Document) {
        document.binary(
            Self::FIELD_ID,
            Vec::with_capacity(0),
            IndexOptions::new().clear(),
        );
    }
}

impl<T> Value<T>
where
    T: Sync + Send + Eq + PartialEq + std::fmt::Debug,
{
    pub fn is_empty(&self) -> bool {
        match self {
            Value::String(string) => string.is_empty(),
            Value::Number(_) | Value::Bool(_) | Value::Object(_) => false,
            Value::Null => true,
        }
    }

    pub fn to_string(&self) -> Option<&str> {
        match self {
            Value::String(string) => Some(string.as_str()),
            _ => None,
        }
    }

    pub fn unwrap_string(self) -> Option<String> {
        match self {
            Value::String(string) => Some(string),
            _ => None,
        }
    }

    pub fn to_unsigned_int(&self) -> Option<u64> {
        match self {
            Value::Number(number) => number.to_unsigned_int().into(),
            _ => None,
        }
    }

    pub fn unwrap_object(self) -> Option<T> {
        match self {
            Value::Object(object) => object.into(),
            _ => None,
        }
    }
}

impl Number {
    pub fn to_unsigned_int(&self) -> u64 {
        match self {
            Number::PosInt(i) => *i,
            Number::NegInt(i) => {
                if *i > 0 {
                    *i as u64
                } else {
                    0
                }
            }
            Number::Float(f) => {
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
            Number::PosInt(i) => *i as i64,
            Number::NegInt(i) => *i,
            Number::Float(f) => *f as i64,
        }
    }
}

impl From<&Number> for store::core::number::Number {
    fn from(value: &Number) -> Self {
        match value {
            Number::PosInt(i) => store::core::number::Number::LongInteger(*i),
            Number::NegInt(i) => store::core::number::Number::LongInteger(*i as u64),
            Number::Float(f) => store::core::number::Number::Float(*f),
        }
    }
}

impl<T> StoreSerialize for TinyORM<T>
where
    T: Object,
{
    fn serialize(&self) -> Option<Vec<u8>> {
        store::bincode::serialize(self).ok()
    }
}

impl<T> StoreDeserialize for TinyORM<T>
where
    T: Object,
{
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        store::bincode::deserialize(bytes).ok()
    }
}

pub trait JMAPOrm {
    fn get_orm<U>(
        &self,
        account: AccountId,
        document: DocumentId,
    ) -> store::Result<Option<TinyORM<U>>>
    where
        U: Object + 'static;
}

impl<T> JMAPOrm for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_orm<U>(
        &self,
        account: AccountId,
        document: DocumentId,
    ) -> store::Result<Option<TinyORM<U>>>
    where
        U: Object + 'static,
    {
        self.get_document_value::<TinyORM<U>>(
            account,
            U::collection(),
            document,
            TinyORM::<U>::FIELD_ID,
        )
    }
}
