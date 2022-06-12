pub mod changes;
pub mod copy;
pub mod get;
pub mod query;
pub mod query_changes;
pub mod set;

use std::sync::Arc;

use store::{
    core::{acl::ACLToken, collection::Collection},
    AccountId,
};

use crate::{
    error::method::MethodError, types::jmap::JMAPId, types::json_pointer::JSONPointer, SUPERUSER_ID,
};

pub trait ACLEnforce: Sized {
    fn has_access(&self, to_account_id: AccountId, to_collection: Collection) -> bool;
    fn is_member(&self, account_id: AccountId) -> bool;
    fn is_shared(&self, account_id: AccountId) -> bool;
    fn assert_has_access(
        self,
        to_account_id: AccountId,
        to_collection: Collection,
    ) -> crate::Result<Self>;
    fn assert_is_member(self, account_id: AccountId) -> crate::Result<Self>;
}

impl ACLEnforce for Arc<ACLToken> {
    fn has_access(&self, to_account_id: AccountId, to_collection: Collection) -> bool {
        self.member_of.contains(&to_account_id)
            || self.access_to.iter().any(|(id, collections)| {
                *id == to_account_id && collections.contains(to_collection)
            })
            || self.member_of.contains(&SUPERUSER_ID)
    }

    fn is_member(&self, account_id: AccountId) -> bool {
        self.member_of.contains(&account_id) || self.member_of.contains(&SUPERUSER_ID)
    }

    fn is_shared(&self, account_id: AccountId) -> bool {
        !self.is_member(account_id) && self.access_to.iter().any(|(id, _)| *id == account_id)
    }

    fn assert_has_access(
        self,
        to_account_id: AccountId,
        to_collection: Collection,
    ) -> crate::Result<Self> {
        if self.has_access(to_account_id, to_collection) {
            Ok(self)
        } else {
            Err(MethodError::Forbidden)
        }
    }

    fn assert_is_member(self, account_id: AccountId) -> crate::Result<Self> {
        if self.is_member(account_id) {
            Ok(self)
        } else {
            Err(MethodError::Forbidden)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct ResultReference {
    #[serde(rename = "resultOf")]
    pub result_of: String,
    pub name: Method,
    pub path: JSONPointer,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum MaybeResultReference<T> {
    Value(T),
    Reference(ResultReference),
}

impl<T> MaybeResultReference<T> {
    pub fn unwrap_value(self) -> Option<T> {
        match self {
            MaybeResultReference::Value(value) => Some(value),
            _ => None,
        }
    }

    pub fn value(&self) -> Option<&T> {
        match self {
            MaybeResultReference::Value(value) => Some(value),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MaybeIdReference {
    Value(JMAPId),
    Reference(String),
}

impl MaybeIdReference {
    pub fn unwrap_value(self) -> Option<JMAPId> {
        match self {
            MaybeIdReference::Value(id) => id.into(),
            _ => None,
        }
    }

    pub fn value(&self) -> Option<&JMAPId> {
        match self {
            MaybeIdReference::Value(id) => id.into(),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Method {
    #[serde(rename = "Core/echo")]
    Echo,
    #[serde(rename = "Blob/copy")]
    CopyBlob,
    #[serde(rename = "PushSubscription/get")]
    GetPushSubscription,
    #[serde(rename = "PushSubscription/set")]
    SetPushSubscription,
    #[serde(rename = "Mailbox/get")]
    GetMailbox,
    #[serde(rename = "Mailbox/changes")]
    ChangesMailbox,
    #[serde(rename = "Mailbox/query")]
    QueryMailbox,
    #[serde(rename = "Mailbox/queryChanges")]
    QueryChangesMailbox,
    #[serde(rename = "Mailbox/set")]
    SetMailbox,
    #[serde(rename = "Thread/get")]
    GetThread,
    #[serde(rename = "Thread/changes")]
    ChangesThread,
    #[serde(rename = "Email/get")]
    GetEmail,
    #[serde(rename = "Email/changes")]
    ChangesEmail,
    #[serde(rename = "Email/query")]
    QueryEmail,
    #[serde(rename = "Email/queryChanges")]
    QueryChangesEmail,
    #[serde(rename = "Email/set")]
    SetEmail,
    #[serde(rename = "Email/copy")]
    CopyEmail,
    #[serde(rename = "Email/import")]
    ImportEmail,
    #[serde(rename = "Email/parse")]
    ParseEmail,
    #[serde(rename = "SearchSnippet/get")]
    GetSearchSnippet,
    #[serde(rename = "Identity/get")]
    GetIdentity,
    #[serde(rename = "Identity/changes")]
    ChangesIdentity,
    #[serde(rename = "Identity/set")]
    SetIdentity,
    #[serde(rename = "EmailSubmission/get")]
    GetEmailSubmission,
    #[serde(rename = "EmailSubmission/changes")]
    ChangesEmailSubmission,
    #[serde(rename = "EmailSubmission/query")]
    QueryEmailSubmission,
    #[serde(rename = "EmailSubmission/queryChanges")]
    QueryChangesEmailSubmission,
    #[serde(rename = "EmailSubmission/set")]
    SetEmailSubmission,
    #[serde(rename = "VacationResponse/get")]
    GetVacationResponse,
    #[serde(rename = "VacationResponse/set")]
    SetVacationResponse,
    #[serde(rename = "error")]
    Error,
}

// MaybeIdReference de/serialization
impl serde::Serialize for MaybeIdReference {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            MaybeIdReference::Value(id) => id.serialize(serializer),
            MaybeIdReference::Reference(str) => serializer.serialize_str(&format!("#{}", str)),
        }
    }
}

struct MaybeIdReferenceVisitor;

impl<'de> serde::de::Visitor<'de> for MaybeIdReferenceVisitor {
    type Value = MaybeIdReference;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid JMAP state")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(if !v.starts_with('#') {
            MaybeIdReference::Value(JMAPId::parse(v).ok_or_else(|| {
                serde::de::Error::custom(format!("Failed to parse JMAP id '{}'", v))
            })?)
        } else {
            MaybeIdReference::Reference(
                v.get(1..)
                    .ok_or_else(|| {
                        serde::de::Error::custom(format!("Failed to parse JMAP id '{}'", v))
                    })?
                    .to_string(),
            )
        })
    }
}

impl<'de> serde::Deserialize<'de> for MaybeIdReference {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(MaybeIdReferenceVisitor)
    }
}

pub trait ArgumentSerializer {
    fn deserialize<'x: 'y, 'y>(
        &'y mut self,
        property: &'x str,
        value: &mut impl serde::de::MapAccess<'x>,
    ) -> Result<(), String>;
}

impl ArgumentSerializer for () {
    fn deserialize<'x: 'y, 'y>(
        &'y mut self,
        _property: &'x str,
        _value: &mut impl serde::de::MapAccess<'x>,
    ) -> Result<(), String> {
        Ok(())
    }
}
