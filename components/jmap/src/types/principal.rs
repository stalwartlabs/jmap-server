use std::{
    borrow::Cow,
    fmt::{self, Display},
};

use serde::{de::IgnoredAny, ser::SerializeMap, Deserialize, Serialize};
use store::{
    core::{acl::ACL, collection::Collection, vec_map::VecMap},
    read::{
        filter::{self, Query},
        FilterMapper,
    },
    write::options::Options,
    AccountId, FieldId, JMAPStore, Store,
};

use crate::{
    error::set::{SetError, SetErrorType},
    jmap_store::{get::GetObject, query::QueryObject, set::SetObject, Object},
    orm::{self, acl::ACLUpdate, serialize::JMAPOrm},
    request::ResultReference,
    sanitize_email, SUPERUSER_ID,
};

use super::{blob::JMAPBlob, jmap::JMAPId, json_pointer::JSONPointer};

#[derive(Debug, Clone, Default)]
pub struct Principal {
    pub properties: VecMap<Property, Value>,
}

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone)]
#[repr(u8)]
pub enum Property {
    Id = 0,
    Type = 1,
    Name = 2,
    Description = 3,
    Email = 4,
    Timezone = 5,
    Capabilities = 6,
    Aliases = 7,
    Secret = 8,
    DKIM = 9,
    Quota = 10,
    Picture = 11,
    Members = 12,
    ACL = 13,
    Invalid = 14,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Type {
    #[serde(rename = "individual")]
    Individual,
    #[serde(rename = "group")]
    Group,
    #[serde(rename = "resource")]
    Resource,
    #[serde(rename = "location")]
    Location,
    #[serde(rename = "domain")]
    Domain,
    #[serde(rename = "list")]
    List,
    #[serde(rename = "other")]
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DKIM {
    #[serde(rename = "dkimSelector")]
    pub dkim_selector: Option<String>,
    #[serde(rename = "dkimExpiration")]
    pub dkim_expiration: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Id { value: JMAPId },
    Blob { value: JMAPBlob },
    Text { value: String },
    TextList { value: Vec<String> },
    Number { value: i64 },
    Type { value: Type },
    DKIM { value: DKIM },
    Members { value: Vec<JMAPId> },
    ACLSet(Vec<ACLUpdate>),
    ACLGet(VecMap<String, Vec<ACL>>),
    Null,
}

pub trait JMAPPrincipals<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_to_email(&self, id: AccountId) -> crate::Result<Option<String>>;
    fn principal_to_id<U>(&self, email: &str) -> crate::error::set::Result<AccountId, U>;
}

impl<T> JMAPPrincipals<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_to_email(&self, id: AccountId) -> crate::Result<Option<String>> {
        Ok(self
            .get_orm::<Principal>(SUPERUSER_ID, id)?
            .and_then(|mut p| p.remove(&Property::Email))
            .and_then(|p| {
                if let Value::Text { value } = p {
                    Some(value)
                } else {
                    None
                }
            }))
    }

    fn principal_to_id<U>(&self, email: &str) -> crate::error::set::Result<AccountId, U> {
        let email_clean = sanitize_email(email).ok_or_else(|| {
            SetError::new(
                SetErrorType::InvalidProperties,
                format!("E-mail {:?} is invalid.", email),
            )
        })?;
        self.query_store::<FilterMapper>(
            SUPERUSER_ID,
            Collection::Principal,
            filter::Filter::or(vec![
                filter::Filter::eq(
                    Property::Email.into(),
                    Query::Index(email_clean.to_string()),
                ),
                filter::Filter::eq(Property::Aliases.into(), Query::Index(email_clean)),
            ]),
            store::read::comparator::Comparator::None,
        )
        .map_err(SetError::from)?
        .get_min()
        .ok_or_else(|| {
            SetError::new(
                SetErrorType::InvalidProperties,
                format!("E-mail {:?} does not exist.", email),
            )
        })
    }
}

impl orm::Value for Value {
    fn index_as(&self) -> orm::Index {
        match self {
            Value::Text { value } => value.to_string().into(),
            Value::TextList { value } => {
                if !value.is_empty() {
                    value.to_vec().into()
                } else {
                    orm::Index::Null
                }
            }
            Value::Number { value } => (*value as u64).into(),
            Value::Type { value } => match value {
                Type::Individual => "i".to_string().into(),
                Type::Group => "g".to_string().into(),
                Type::Resource => "r".to_string().into(),
                Type::Location => "l".to_string().into(),
                Type::Domain => "d".to_string().into(),
                Type::List => "t".to_string().into(),
                Type::Other => "o".to_string().into(),
            },
            Value::Members { value } => {
                if !value.is_empty() {
                    value
                        .iter()
                        .map(|id| id.get_document_id())
                        .collect::<Vec<_>>()
                        .into()
                } else {
                    orm::Index::Null
                }
            }
            _ => orm::Index::Null,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Value::Text { value } => value.is_empty(),
            Value::Null => true,
            _ => false,
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl Object for Principal {
    type Property = Property;

    type Value = Value;

    fn new(id: JMAPId) -> Self {
        let mut item = Principal::default();
        item.properties
            .append(Property::Id, Value::Id { value: id });
        item
    }

    fn id(&self) -> Option<&JMAPId> {
        self.properties.get(&Property::Id).and_then(|id| match id {
            Value::Id { value } => Some(value),
            _ => None,
        })
    }

    fn required() -> &'static [Self::Property] {
        &[]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[
            (
                Property::Type,
                <u64 as Options>::F_KEYWORD | <u64 as Options>::F_INDEX,
            ),
            (
                Property::Name,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_INDEX,
            ),
            (
                Property::Email,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_INDEX,
            ),
            (
                Property::Aliases,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_INDEX,
            ),
            (Property::Members, <u64 as Options>::F_INDEX),
            (Property::Description, <u64 as Options>::F_TOKENIZE),
            (Property::Timezone, <u64 as Options>::F_TOKENIZE),
            (Property::Quota, <u64 as Options>::F_INDEX),
        ]
    }

    fn collection() -> Collection {
        Collection::Principal
    }
}

impl GetObject for Principal {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::Name,
            Property::Email,
            Property::Type,
            Property::Description,
        ]
    }

    fn get_as_id(&self, _property: &Self::Property) -> Option<Vec<JMAPId>> {
        None
    }
}

impl SetObject for Principal {
    type SetArguments = ();

    type NextCall = ();

    fn eval_id_references(&mut self, _fnc: impl FnMut(&str) -> Option<JMAPId>) {}
    fn eval_result_references(&mut self, _fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>) {}
}

impl QueryObject for Principal {
    type QueryArguments = ();

    type Filter = Filter;

    type Comparator = Comparator;
}

#[derive(Clone, Debug)]
pub enum Filter {
    Email { value: String },
    Name { value: String },
    Text { value: String },
    Type { value: Type },
    Timezone { value: String },
    Members { value: JMAPId },
    QuotaLt { value: u64 },
    QuotaGt { value: u64 },
    Unsupported { value: String },
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "property")]
pub enum Comparator {
    #[serde(rename = "type")]
    Type,
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "email")]
    Email,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => f.write_str("id"),
            Property::Type => f.write_str("type"),
            Property::Name => f.write_str("name"),
            Property::Description => f.write_str("description"),
            Property::Email => f.write_str("email"),
            Property::Timezone => f.write_str("timezone"),
            Property::Capabilities => f.write_str("capabilities"),
            Property::Secret => f.write_str("secret"),
            Property::DKIM => f.write_str("dkim"),
            Property::Quota => f.write_str("quota"),
            Property::Picture => f.write_str("picture"),
            Property::Members => f.write_str("members"),
            Property::Aliases => f.write_str("aliases"),
            Property::ACL => f.write_str("acl"),
            Property::Invalid => Ok(()),
        }
    }
}

impl From<Property> for FieldId {
    fn from(property: Property) -> Self {
        property as FieldId
    }
}

impl From<FieldId> for Property {
    fn from(field: FieldId) -> Self {
        match field {
            0 => Property::Id,
            1 => Property::Type,
            2 => Property::Name,
            3 => Property::Description,
            4 => Property::Email,
            5 => Property::Timezone,
            6 => Property::Capabilities,
            7 => Property::Aliases,
            8 => Property::Secret,
            9 => Property::DKIM,
            10 => Property::Quota,
            11 => Property::Picture,
            12 => Property::Members,
            13 => Property::ACL,
            _ => Property::Invalid,
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match Property::parse(value) {
            Property::Invalid => Err(()),
            property => Ok(property),
        }
    }
}

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "type" => Property::Type,
            "name" => Property::Name,
            "description" => Property::Description,
            "email" => Property::Email,
            "timezone" => Property::Timezone,
            "capabilities" => Property::Capabilities,
            "secret" => Property::Secret,
            "aliases" => Property::Aliases,
            "dkim" => Property::DKIM,
            "quota" => Property::Quota,
            "picture" => Property::Picture,
            "members" => Property::Members,
            "acl" => Property::ACL,
            _ => Property::Invalid,
        }
    }
}

// Principal de/serialization
impl Serialize for Principal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(self.properties.len().into())?;

        for (name, value) in &self.properties {
            match value {
                Value::Id { value } => map.serialize_entry(name, value)?,
                Value::Text { value } => map.serialize_entry(name, value)?,
                Value::Null => map.serialize_entry(name, &None::<&str>)?,
                Value::TextList { value } => map.serialize_entry(name, value)?,
                Value::Number { value } => map.serialize_entry(name, value)?,
                Value::Type { value } => map.serialize_entry(name, value)?,
                Value::Members { value } => map.serialize_entry(name, value)?,
                Value::Blob { value } => map.serialize_entry(name, value)?,
                Value::DKIM { value } => map.serialize_entry(name, value)?,
                Value::ACLGet(value) => map.serialize_entry(name, value)?,
                Value::ACLSet(_) => (),
            }
        }

        map.end()
    }
}

struct PrincipalVisitor;

impl<'de> serde::de::Visitor<'de> for PrincipalVisitor {
    type Value = Principal;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP Principal object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut properties: VecMap<Property, Value> = VecMap::new();
        let mut acls = Vec::new();

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "name" => {
                    properties.append(
                        Property::Name,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "description" => {
                    properties.append(
                        Property::Description,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "timezone" => {
                    properties.append(
                        Property::Timezone,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "email" => {
                    properties.append(
                        Property::Email,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "aliases" => {
                    properties.append(
                        Property::Aliases,
                        if let Some(value) = map.next_value::<Option<Vec<String>>>()? {
                            Value::TextList { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "capabilities" => {
                    properties.append(
                        Property::Capabilities,
                        if let Some(value) = map.next_value::<Option<Vec<String>>>()? {
                            Value::TextList { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "type" => {
                    properties.append(
                        Property::Type,
                        Value::Type {
                            value: map.next_value::<Type>()?,
                        },
                    );
                }
                "secret" => {
                    properties.append(
                        Property::Secret,
                        if let Some(value) = map.next_value::<Option<String>>()? {
                            Value::Text { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "dkim" => {
                    properties.append(
                        Property::DKIM,
                        if let Some(value) = map.next_value::<Option<DKIM>>()? {
                            Value::DKIM { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "members" => {
                    properties.append(
                        Property::Members,
                        if let Some(value) = map.next_value::<Option<Vec<JMAPId>>>()? {
                            Value::Members { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "quota" => {
                    properties.append(
                        Property::Quota,
                        if let Some(value) = map.next_value::<Option<u64>>()? {
                            Value::Number {
                                value: value as i64,
                            }
                        } else {
                            Value::Null
                        },
                    );
                }
                "picture" => {
                    properties.append(
                        Property::Picture,
                        if let Some(value) = map.next_value::<Option<JMAPBlob>>()? {
                            Value::Blob { value }
                        } else {
                            Value::Null
                        },
                    );
                }
                "acl" => {
                    acls.push(ACLUpdate::Replace {
                        acls: map
                            .next_value::<Option<VecMap<String, Vec<ACL>>>>()?
                            .unwrap_or_default(),
                    });
                }
                key => match JSONPointer::parse(key) {
                    Some(JSONPointer::Path(path))
                        if path.len() >= 2
                            && path
                                .get(0)
                                .and_then(|p| p.to_string())
                                .map(Property::parse)
                                .unwrap_or(Property::Invalid)
                                == Property::ACL =>
                    {
                        if let Some(account_id) = path
                            .get(1)
                            .and_then(|p| p.to_string())
                            .map(|p| p.to_string())
                        {
                            if path.len() > 2 {
                                if let Some(acl) =
                                    path.get(2).and_then(|p| p.to_string()).map(ACL::parse)
                                {
                                    if acl != ACL::None_ {
                                        acls.push(ACLUpdate::Set {
                                            account_id,
                                            acl,
                                            is_set: map
                                                .next_value::<Option<bool>>()?
                                                .unwrap_or(false),
                                        });
                                    }
                                }
                            } else {
                                acls.push(ACLUpdate::Update {
                                    account_id,
                                    acls: map.next_value::<Option<Vec<ACL>>>()?.unwrap_or_default(),
                                });
                            }
                        }
                    }
                    _ => {
                        map.next_value::<IgnoredAny>()?;
                    }
                },
            }
        }

        if !acls.is_empty() {
            properties.append(Property::ACL, Value::ACLSet(acls));
        }

        Ok(Principal { properties })
    }
}

impl<'de> Deserialize<'de> for Principal {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(PrincipalVisitor)
    }
}

// Property de/serialization
impl Serialize for Property {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}
struct PropertyVisitor;

impl<'de> serde::de::Visitor<'de> for PropertyVisitor {
    type Value = Property;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid Principal property")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(Property::parse(v))
    }
}

impl<'de> Deserialize<'de> for Property {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(PropertyVisitor)
    }
}

// Filter deserializer
struct FilterVisitor;

impl<'de> serde::de::Visitor<'de> for FilterVisitor {
    type Value = Filter;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid Principal filter")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        Ok(
            match map
                .next_key::<&str>()?
                .ok_or_else(|| serde::de::Error::custom("Missing filter property"))?
            {
                "email" => Filter::Email {
                    value: map.next_value()?,
                },
                "name" => Filter::Name {
                    value: map.next_value()?,
                },
                "text" => Filter::Text {
                    value: map.next_value()?,
                },
                "type" => Filter::Type {
                    value: map.next_value()?,
                },
                "timezone" => Filter::Timezone {
                    value: map.next_value()?,
                },
                "members" => Filter::Members {
                    value: map.next_value()?,
                },
                "quotaLowerThan" => Filter::QuotaLt {
                    value: map.next_value()?,
                },
                "quotaGreaterThan" => Filter::QuotaGt {
                    value: map.next_value()?,
                },
                unsupported => Filter::Unsupported {
                    value: unsupported.to_string(),
                },
            },
        )
    }
}

impl<'de> Deserialize<'de> for Filter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(FilterVisitor)
    }
}
