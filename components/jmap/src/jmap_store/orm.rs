use store::core::document::Document;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::nlp::Language;
use store::serialize::{StoreDeserialize, StoreSerialize};
use store::write::options::{IndexOptions, Options};
use store::{AccountId, DocumentId, Integer, JMAPStore, LongInteger, Store};

use crate::error::set::SetError;
use std::collections::{HashMap, HashSet};

use super::Object;

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
}

#[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq)]
pub enum IndexableValue {
    String(String),
    Integer(Integer),
    LongInteger(LongInteger),
    Null,
}

impl From<String> for IndexableValue {
    fn from(value: String) -> Self {
        IndexableValue::String(value)
    }
}

impl From<Integer> for IndexableValue {
    fn from(value: Integer) -> Self {
        IndexableValue::Integer(value)
    }
}

impl From<LongInteger> for IndexableValue {
    fn from(value: LongInteger) -> Self {
        IndexableValue::LongInteger(value)
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
    fn index_as(&self) -> IndexableValue;
    fn is_empty(&self) -> bool;
}

impl Value for () {
    fn index_as(&self) -> IndexableValue {
        IndexableValue::Null
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

    pub fn get(&self, property: &T::Property) -> Option<&T::Value> {
        self.properties.get(property)
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
                    match current_value.index_as() {
                        IndexableValue::String(value) => {
                            document.text(
                                property.clone(),
                                value,
                                Language::Unknown,
                                (*index_options).clear(),
                            );
                        }
                        IndexableValue::Integer(value) => {
                            document.number(property.clone(), value, (*index_options).clear());
                        }
                        IndexableValue::LongInteger(value) => {
                            document.number(property.clone(), value, (*index_options).clear());
                        }
                        IndexableValue::Null => (),
                    }
                }
            }

            let do_insert = if is_indexed {
                match value.index_as() {
                    IndexableValue::String(value) => {
                        document.text(property.clone(), value, Language::Unknown, *index_options);
                        true
                    }
                    IndexableValue::Integer(value) => {
                        document.number(property.clone(), value, *index_options);
                        true
                    }
                    IndexableValue::LongInteger(value) => {
                        document.number(property.clone(), value, *index_options);
                        true
                    }
                    IndexableValue::Null => false,
                }
            } else {
                !value.is_empty()
            };

            if do_insert {
                self.properties.insert(property, value);
            } else {
                self.properties.remove(&property);
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
                match value.index_as() {
                    IndexableValue::String(value) => {
                        document.text(property, value, Language::Unknown, index_options);
                    }
                    IndexableValue::Integer(value) => {
                        document.number(property, value, index_options);
                    }
                    IndexableValue::LongInteger(value) => {
                        document.number(property, value, index_options);
                    }
                    IndexableValue::Null => (),
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
