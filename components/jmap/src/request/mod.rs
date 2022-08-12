pub mod blob;
pub mod changes;
pub mod copy;
pub mod get;
pub mod query;
pub mod query_changes;
pub mod set;

use std::{borrow::Cow, sync::Arc};

use serde::de::IgnoredAny;
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
    fn primary_id(&self) -> AccountId;
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

    fn primary_id(&self) -> AccountId {
        *self.member_of.first().unwrap()
    }

    fn assert_has_access(
        self,
        to_account_id: AccountId,
        to_collection: Collection,
    ) -> crate::Result<Self> {
        if self.has_access(to_account_id, to_collection) {
            Ok(self)
        } else {
            Err(MethodError::Forbidden(format!(
                "You do not have access to account {}",
                JMAPId::from(to_account_id)
            )))
        }
    }

    fn assert_is_member(self, account_id: AccountId) -> crate::Result<Self> {
        if self.is_member(account_id) {
            Ok(self)
        } else {
            Err(MethodError::Forbidden(format!(
                "You are not an owner of account {}",
                JMAPId::from(account_id)
            )))
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

// Todo remove all untagged and HashMaps
#[derive(Debug, Clone, serde::Deserialize)]
pub enum MaybeResultReference<T> {
    Value(T),
    Reference(ResultReference),
    Error(Cow<'static, str>),
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

    pub fn result_reference(&self) -> crate::Result<Option<&ResultReference>> {
        match self {
            MaybeResultReference::Reference(rr) => Ok(Some(rr)),
            MaybeResultReference::Value(_) => Ok(None),
            MaybeResultReference::Error(err) => Err(MethodError::InvalidArguments(err.to_string())),
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Echo,
    CopyBlob,
    GetPushSubscription,
    SetPushSubscription,
    GetMailbox,
    ChangesMailbox,
    QueryMailbox,
    QueryChangesMailbox,
    SetMailbox,
    GetThread,
    ChangesThread,
    GetEmail,
    ChangesEmail,
    QueryEmail,
    QueryChangesEmail,
    SetEmail,
    CopyEmail,
    ImportEmail,
    ParseEmail,
    GetSearchSnippet,
    GetIdentity,
    ChangesIdentity,
    SetIdentity,
    GetEmailSubmission,
    ChangesEmailSubmission,
    QueryEmailSubmission,
    QueryChangesEmailSubmission,
    SetEmailSubmission,
    GetVacationResponse,
    SetVacationResponse,
    GetPrincipal,
    SetPrincipal,
    QueryPrincipal,
    Error,
}

// Method de/serialization
impl serde::Serialize for Method {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(match self {
            Method::Echo => "Core/echo",
            Method::CopyBlob => "Blob/copy",
            Method::GetPushSubscription => "PushSubscription/get",
            Method::SetPushSubscription => "PushSubscription/set",
            Method::GetMailbox => "Mailbox/get",
            Method::ChangesMailbox => "Mailbox/changes",
            Method::QueryMailbox => "Mailbox/query",
            Method::QueryChangesMailbox => "Mailbox/queryChanges",
            Method::SetMailbox => "Mailbox/set",
            Method::GetThread => "Thread/get",
            Method::ChangesThread => "Thread/changes",
            Method::GetEmail => "Email/get",
            Method::ChangesEmail => "Email/changes",
            Method::QueryEmail => "Email/query",
            Method::QueryChangesEmail => "Email/queryChanges",
            Method::SetEmail => "Email/set",
            Method::CopyEmail => "Email/copy",
            Method::ImportEmail => "Email/import",
            Method::ParseEmail => "Email/parse",
            Method::GetSearchSnippet => "SearchSnippet/get",
            Method::GetIdentity => "Identity/get",
            Method::ChangesIdentity => "Identity/changes",
            Method::SetIdentity => "Identity/set",
            Method::GetEmailSubmission => "EmailSubmission/get",
            Method::ChangesEmailSubmission => "EmailSubmission/changes",
            Method::QueryEmailSubmission => "EmailSubmission/query",
            Method::QueryChangesEmailSubmission => "EmailSubmission/queryChanges",
            Method::SetEmailSubmission => "EmailSubmission/set",
            Method::GetVacationResponse => "VacationResponse/get",
            Method::SetVacationResponse => "VacationResponse/set",
            Method::GetPrincipal => "Principal/get",
            Method::SetPrincipal => "Principal/set",
            Method::QueryPrincipal => "Principal/query",
            Method::Error => "error",
        })
    }
}

struct MethodVisitor;

impl<'de> serde::de::Visitor<'de> for MethodVisitor {
    type Value = Method;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid JMAP state")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        Ok(match v {
            "Core/echo" => Method::Echo,
            "Blob/copy" => Method::CopyBlob,
            "PushSubscription/get" => Method::GetPushSubscription,
            "PushSubscription/set" => Method::SetPushSubscription,
            "Mailbox/get" => Method::GetMailbox,
            "Mailbox/changes" => Method::ChangesMailbox,
            "Mailbox/query" => Method::QueryMailbox,
            "Mailbox/queryChanges" => Method::QueryChangesMailbox,
            "Mailbox/set" => Method::SetMailbox,
            "Thread/get" => Method::GetThread,
            "Thread/changes" => Method::ChangesThread,
            "Email/get" => Method::GetEmail,
            "Email/changes" => Method::ChangesEmail,
            "Email/query" => Method::QueryEmail,
            "Email/queryChanges" => Method::QueryChangesEmail,
            "Email/set" => Method::SetEmail,
            "Email/copy" => Method::CopyEmail,
            "Email/import" => Method::ImportEmail,
            "Email/parse" => Method::ParseEmail,
            "SearchSnippet/get" => Method::GetSearchSnippet,
            "Identity/get" => Method::GetIdentity,
            "Identity/changes" => Method::ChangesIdentity,
            "Identity/set" => Method::SetIdentity,
            "EmailSubmission/get" => Method::GetEmailSubmission,
            "EmailSubmission/changes" => Method::ChangesEmailSubmission,
            "EmailSubmission/query" => Method::QueryEmailSubmission,
            "EmailSubmission/queryChanges" => Method::QueryChangesEmailSubmission,
            "EmailSubmission/set" => Method::SetEmailSubmission,
            "VacationResponse/get" => Method::GetVacationResponse,
            "VacationResponse/set" => Method::SetVacationResponse,
            "Principal/get" => Method::GetPrincipal,
            "Principal/set" => Method::SetPrincipal,
            "Principal/query" => Method::QueryPrincipal,
            _ => Method::Error,
        })
    }
}

impl<'de> serde::Deserialize<'de> for Method {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(MethodVisitor)
    }
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

pub trait ArgumentDeserializer {
    fn deserialize<'x: 'y, 'y, 'z>(
        &'y mut self,
        property: &'z str,
        value: &mut impl serde::de::MapAccess<'x>,
    ) -> Result<(), String>;
}

impl ArgumentDeserializer for () {
    fn deserialize<'x: 'y, 'y, 'z>(
        &'y mut self,
        _property: &'z str,
        value: &mut impl serde::de::MapAccess<'x>,
    ) -> Result<(), String> {
        value
            .next_value::<IgnoredAny>()
            .map_err(|err| err.to_string())?;
        Ok(())
    }
}
