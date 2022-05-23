use std::{collections::HashMap, fmt::Display};

use jmap::{id::jmap::JMAPId, request::ResultReference};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default)]
pub struct Mailbox {
    pub properties: HashMap<Property, Value>,
}

#[derive(Debug, Clone)]
pub enum Value {
    Id { value: JMAPId },
    Text { value: String },
    Bool { value: bool },
    Number { value: u32 },
    MailboxRights { value: MailboxRights },
    ResultReference { value: ResultReference },
    IdReference { value: String },
    Null,
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct MailboxRights {
    #[serde(rename = "mayReadItems")]
    may_read_items: bool,

    #[serde(rename = "mayAddItems")]
    may_add_items: bool,

    #[serde(rename = "mayRemoveItems")]
    may_remove_items: bool,

    #[serde(rename = "maySetSeen")]
    may_set_seen: bool,

    #[serde(rename = "maySetKeywords")]
    may_set_keywords: bool,

    #[serde(rename = "mayCreateChild")]
    may_create_child: bool,

    #[serde(rename = "mayRename")]
    may_rename: bool,

    #[serde(rename = "mayDelete")]
    may_delete: bool,

    #[serde(rename = "maySubmit")]
    may_submit: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Copy)]
#[repr(u8)]
pub enum Property {
    Id = 0,
    Name = 1,
    ParentId = 2,
    Role = 3,
    SortOrder = 4,
    TotalEmails = 5,
    UnreadEmails = 6,
    TotalThreads = 7,
    UnreadThreads = 8,
    MyRights = 9,
    IsSubscribed = 10,
    Invalid = 11,
}

impl Display for Property {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Property::Id => write!(f, "id"),
            Property::Name => write!(f, "name"),
            Property::ParentId => write!(f, "parentId"),
            Property::Role => write!(f, "role"),
            Property::SortOrder => write!(f, "sortOrder"),
            Property::TotalEmails => write!(f, "totalEmails"),
            Property::UnreadEmails => write!(f, "unreadEmails"),
            Property::TotalThreads => write!(f, "totalThreads"),
            Property::UnreadThreads => write!(f, "unreadThreads"),
            Property::MyRights => write!(f, "myRights"),
            Property::IsSubscribed => write!(f, "isSubscribed"),
            Property::Invalid => Ok(()),
        }
    }
}

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "name" => Property::Name,
            "parentId" => Property::ParentId,
            "role" => Property::Role,
            "sortOrder" => Property::SortOrder,
            "isSubscribed" => Property::IsSubscribed,
            "totalEmails" => Property::TotalEmails,
            "unreadEmails" => Property::UnreadEmails,
            "totalThreads" => Property::TotalThreads,
            "unreadThreads" => Property::UnreadThreads,
            "myRights" => Property::MyRights,
            _ => Property::Invalid,
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
#[serde(untagged)]
pub enum Filter {
    ParentId {
        #[serde(rename = "parentId")]
        value: Option<JMAPId>,
    },
    Name {
        #[serde(rename = "name")]
        value: String,
    },
    Role {
        #[serde(rename = "role")]
        value: Option<String>,
    },
    HasAnyRole {
        #[serde(rename = "hasAnyRole")]
        value: bool,
    },
    IsSubscribed {
        #[serde(rename = "isSubscribed")]
        value: bool,
    },
}

#[derive(Deserialize, Debug, Clone)]
#[serde(tag = "property")]
pub enum Comparator {
    #[serde(rename = "name")]
    Name,
    #[serde(rename = "sortOrder")]
    SortOrder,
    #[serde(rename = "parentId")]
    ParentId,
}
