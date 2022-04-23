use store::batch::Document;
use store::field::{DefaultOptions, Options, Text};
use store::serialize::{StoreDeserialize, StoreSerialize};
use store::{AccountId, DocumentId, JMAPStore, Store, StoreError, Tag};

use crate::error::set::SetError;
use crate::{protocol::json::JSONValue, Property};
use std::collections::HashMap;
use std::hash::Hash;

pub trait PropertySchema:
    Property
    + Eq
    + Hash
    + Copy
    + Into<u8>
    + serde::Serialize
    + serde::de::DeserializeOwned
    + Sync
    + Send
{
    fn required() -> &'static [Self];
    fn sorted() -> &'static [Self];
    fn tokenized() -> &'static [Self];
    fn keywords() -> &'static [Self];
    fn tags() -> &'static [Self];
}

pub struct TinyORM<T>
where
    T: PropertySchema,
{
    properties: HashMap<T, JSONValue>,
}

impl<T> Default for TinyORM<T>
where
    T: PropertySchema,
{
    fn default() -> Self {
        Self {
            properties: Default::default(),
        }
    }
}

impl<T> TinyORM<T>
where
    T: PropertySchema + 'static,
{
    pub const FIELD_ID: u8 = u8::MAX;

    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, property: T, value: JSONValue) {
        self.properties.insert(property, value);
    }

    pub fn remove(&mut self, property: &T) -> Option<JSONValue> {
        self.properties.remove(property)
    }

    pub fn has_property(&self, property: &T) -> bool {
        self.properties.contains_key(property)
    }

    pub fn get_unsigned_int(&self, property: &T) -> Option<u64> {
        self.properties
            .get(property)
            .and_then(|value| value.to_unsigned_int())
    }

    pub fn get_string(&self, property: &T) -> Option<&str> {
        self.properties
            .get(property)
            .and_then(|value| value.to_string())
    }

    pub fn validate(&self, changes: &TinyORM<T>) -> crate::error::set::Result<()> {
        for property in T::required() {
            let is_null = if let Some(value) = changes.properties.get(property) {
                match value {
                    JSONValue::Null => true,
                    JSONValue::String(string) => string.is_empty(),
                    JSONValue::Array(array) => array.is_empty(),
                    JSONValue::Object(obj) => obj.is_empty(),
                    JSONValue::Bool(_) | JSONValue::Number(_) => false,
                }
            } else {
                self.properties.get(property).is_none()
            };
            if is_null {
                return Err(SetError::invalid_property(
                    property.to_string(),
                    "Property cannot be empty.".to_string(),
                ));
            }
        }

        Ok(())
    }

    pub fn merge(mut self, document: &mut Document, changes: TinyORM<T>) -> store::Result<bool> {
        let sorted = T::sorted();
        let tokenized = T::tokenized();
        let keywords = T::keywords();
        let tags = T::tags();

        for (property, value) in changes.properties {
            let is_sorted = sorted.contains(&property);
            let is_tokenized = tokenized.contains(&property);
            let is_keyword = keywords.contains(&property);
            let is_tagged = tags.contains(&property);

            if let Some(current_value) = self.properties.get(&property) {
                if current_value == &value {
                    continue;
                }

                if is_sorted || is_tokenized || is_keyword {
                    match &current_value {
                        JSONValue::String(text) => {
                            let options = if is_sorted {
                                DefaultOptions::new().clear().sort()
                            } else {
                                DefaultOptions::new().clear()
                            };
                            if is_tokenized {
                                document.text(property, Text::tokenized(text.clone()), options);
                            } else if is_keyword {
                                document.text(property, Text::keyword(text.clone()), options);
                            }
                        }
                        JSONValue::Number(number) => {
                            if is_sorted {
                                document.number(
                                    property,
                                    number,
                                    DefaultOptions::new().sort().clear(),
                                );
                            }
                        }
                        value => {
                            debug_assert!(false, "ORM unsupported type: {:?}", value);
                        }
                    }
                }
            }

            match &value {
                JSONValue::String(text) => {
                    if is_tokenized {
                        document.text(
                            property,
                            Text::tokenized(text.clone()),
                            if is_sorted {
                                DefaultOptions::new().sort()
                            } else {
                                DefaultOptions::new()
                            },
                        );
                    } else if is_keyword {
                        document.text(
                            property,
                            Text::keyword(text.clone()),
                            if is_sorted {
                                DefaultOptions::new().sort()
                            } else {
                                DefaultOptions::new()
                            },
                        );
                    }
                    if is_tagged && !self.properties.contains_key(&property) {
                        document.tag(property, Tag::Default, DefaultOptions::new());
                    }
                    self.properties.insert(property, value);
                }
                JSONValue::Number(number) => {
                    if is_sorted {
                        document.number(property, number, DefaultOptions::new().sort());
                    }
                    self.properties.insert(property, value);
                }
                JSONValue::Null => {
                    let had_value = self.properties.remove(&property).is_some();
                    if had_value && is_tagged {
                        document.tag(property, Tag::Default, DefaultOptions::new().clear());
                    }
                }
                _ => {
                    self.properties.insert(property, value);
                }
            }
        }

        if !document.is_empty() {
            document.binary(
                Self::FIELD_ID,
                rmp_serde::encode::to_vec(&self.properties).map_err(|_| {
                    StoreError::SerializeError("Failed to serialize ORM object.".to_string())
                })?,
                DefaultOptions::new().store(),
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl<T> From<HashMap<T, JSONValue>> for TinyORM<T>
where
    T: PropertySchema,
{
    fn from(properties: HashMap<T, JSONValue>) -> Self {
        TinyORM { properties }
    }
}

impl<T> StoreSerialize for TinyORM<T>
where
    T: PropertySchema,
{
    fn serialize(&self) -> Option<Vec<u8>> {
        rmp_serde::encode::to_vec(&self.properties).ok()
    }
}

impl<T> StoreDeserialize for TinyORM<T>
where
    T: PropertySchema,
{
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        Some(TinyORM {
            properties: rmp_serde::decode::from_slice(bytes).ok()?,
        })
    }
}

pub trait JMAPOrm {
    fn get_orm<U>(
        &self,
        account: AccountId,
        document: DocumentId,
    ) -> store::Result<Option<TinyORM<U>>>
    where
        U: PropertySchema + 'static;
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
        U: PropertySchema + 'static,
    {
        self.get_document_value::<TinyORM<U>>(
            account,
            U::collection(),
            document,
            TinyORM::<U>::FIELD_ID,
        )
    }
}
