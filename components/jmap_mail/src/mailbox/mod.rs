pub mod changes;
pub mod get;
pub mod query;
pub mod raft;
pub mod set;

use std::collections::HashMap;
use std::fmt::Display;

use jmap::error::method::MethodError;
use jmap::jmap_store::orm::PropertySchema;
use jmap::protocol::json::JSONValue;
use jmap::request::JSONArgumentParser;
use jmap::Property;

use store::core::collection::Collection;
use store::write::options::Options;
use store::FieldId;

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum MailboxProperty {
    Id = 0,
    Name = 1,
    ParentId = 2,
    Role = 3,
    SortOrder = 4,
    IsSubscribed = 5,
    TotalEmails = 6,
    UnreadEmails = 7,
    TotalThreads = 8,
    UnreadThreads = 9,
    MyRights = 10,
}

#[derive(Debug, Default)]
pub struct MailboxRights {
    may_read_items: bool,
    may_add_items: bool,
    may_remove_items: bool,
    may_set_seen: bool,
    may_set_keywords: bool,
    may_create_child: bool,
    may_rename: bool,
    may_delete: bool,
    may_submit: bool,
}

impl Property for MailboxProperty {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "id" => Some(MailboxProperty::Id),
            "name" => Some(MailboxProperty::Name),
            "parentId" => Some(MailboxProperty::ParentId),
            "role" => Some(MailboxProperty::Role),
            "sortOrder" => Some(MailboxProperty::SortOrder),
            "isSubscribed" => Some(MailboxProperty::IsSubscribed),
            "totalEmails" => Some(MailboxProperty::TotalEmails),
            "unreadEmails" => Some(MailboxProperty::UnreadEmails),
            "totalThreads" => Some(MailboxProperty::TotalThreads),
            "unreadThreads" => Some(MailboxProperty::UnreadThreads),
            "myRights" => Some(MailboxProperty::MyRights),
            _ => None,
        }
    }

    fn collection() -> Collection {
        Collection::Mailbox
    }
}

impl Display for MailboxProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MailboxProperty::Id => write!(f, "id"),
            MailboxProperty::Name => write!(f, "name"),
            MailboxProperty::ParentId => write!(f, "parentId"),
            MailboxProperty::Role => write!(f, "role"),
            MailboxProperty::SortOrder => write!(f, "sortOrder"),
            MailboxProperty::IsSubscribed => write!(f, "isSubscribed"),
            MailboxProperty::TotalEmails => write!(f, "totalEmails"),
            MailboxProperty::UnreadEmails => write!(f, "unreadEmails"),
            MailboxProperty::TotalThreads => write!(f, "totalThreads"),
            MailboxProperty::UnreadThreads => write!(f, "unreadThreads"),
            MailboxProperty::MyRights => write!(f, "myRights"),
        }
    }
}

impl PropertySchema for MailboxProperty {
    fn required() -> &'static [Self] {
        &[MailboxProperty::Name]
    }

    fn indexed() -> &'static [(Self, u64)] {
        &[
            (
                MailboxProperty::Name,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_SORT,
            ),
            (MailboxProperty::Role, <u64 as Options>::F_KEYWORD),
            (MailboxProperty::ParentId, <u64 as Options>::F_SORT),
            (MailboxProperty::SortOrder, <u64 as Options>::F_SORT),
        ]
    }
}

impl From<MailboxProperty> for FieldId {
    fn from(field: MailboxProperty) -> Self {
        field as FieldId
    }
}

impl From<MailboxProperty> for JSONValue {
    fn from(value: MailboxProperty) -> Self {
        JSONValue::String(value.to_string())
    }
}

impl From<MailboxRights> for JSONValue {
    fn from(value: MailboxRights) -> Self {
        let mut map = HashMap::new();
        map.insert("mayReadItems".to_string(), value.may_read_items.into());
        map.insert("mayAddItems".to_string(), value.may_add_items.into());
        map.insert("mayRemoveItems".to_string(), value.may_remove_items.into());
        map.insert("maySetSeen".to_string(), value.may_set_seen.into());
        map.insert("maySetKeywords".to_string(), value.may_set_keywords.into());
        map.insert("mayCreateChild".to_string(), value.may_create_child.into());
        map.insert("mayRename".to_string(), value.may_rename.into());
        map.insert("mayDelete".to_string(), value.may_delete.into());
        map.insert("maySubmit".to_string(), value.may_submit.into());
        map.into()
    }
}

impl JSONArgumentParser for MailboxProperty {
    fn parse_argument(argument: JSONValue) -> jmap::Result<Self> {
        let argument = argument.unwrap_string().ok_or_else(|| {
            MethodError::InvalidArguments("Expected string argument.".to_string())
        })?;
        MailboxProperty::parse(&argument).ok_or_else(|| {
            MethodError::InvalidArguments(format!("Unknown mailbox property: '{}'.", argument))
        })
    }
}
