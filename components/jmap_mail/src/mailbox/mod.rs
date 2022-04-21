pub mod changes;
pub mod get;
pub mod query;
pub mod set;

use std::fmt::Display;

use jmap::error::method::MethodError;
use jmap::protocol::json::JSONValue;
use jmap::request::JSONArgumentParser;

use store::serialize::{StoreDeserialize, StoreSerialize};

use store::{bincode, FieldId, JMAPId};

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct Mailbox {
    pub name: String,
    pub parent_id: JMAPId,
    pub role: Option<String>,
    pub sort_order: u32,
}

impl StoreSerialize for Mailbox {
    fn serialize(&self) -> Option<Vec<u8>> {
        bincode::serialize(self).ok()
    }
}

impl StoreDeserialize for Mailbox {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}

#[derive(Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MailboxProperties {
    Id = 0,
    Name = 1,
    ParentId = 2,
    Role = 3,
    HasRole = 4,
    SortOrder = 5,
    IsSubscribed = 6,
    TotalEmails = 7,
    UnreadEmails = 8,
    TotalThreads = 9,
    UnreadThreads = 10,
    MyRights = 11,
}

impl MailboxProperties {
    pub fn as_str(&self) -> &'static str {
        match self {
            MailboxProperties::Id => "id",
            MailboxProperties::Name => "name",
            MailboxProperties::ParentId => "parentId",
            MailboxProperties::Role => "role",
            MailboxProperties::HasRole => "hasRole",
            MailboxProperties::SortOrder => "sortOrder",
            MailboxProperties::IsSubscribed => "isSubscribed",
            MailboxProperties::TotalEmails => "totalEmails",
            MailboxProperties::UnreadEmails => "unreadEmails",
            MailboxProperties::TotalThreads => "totalThreads",
            MailboxProperties::UnreadThreads => "unreadThreads",
            MailboxProperties::MyRights => "myRights",
        }
    }
}

impl Display for MailboxProperties {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<MailboxProperties> for FieldId {
    fn from(field: MailboxProperties) -> Self {
        field as FieldId
    }
}

impl From<MailboxProperties> for JSONValue {
    fn from(value: MailboxProperties) -> Self {
        JSONValue::String(value.as_str().to_string())
    }
}

impl MailboxProperties {
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "id" => Some(MailboxProperties::Id),
            "name" => Some(MailboxProperties::Name),
            "parentId" => Some(MailboxProperties::ParentId),
            "role" => Some(MailboxProperties::Role),
            "sortOrder" => Some(MailboxProperties::SortOrder),
            "isSubscribed" => Some(MailboxProperties::IsSubscribed),
            "totalEmails" => Some(MailboxProperties::TotalEmails),
            "unreadEmails" => Some(MailboxProperties::UnreadEmails),
            "totalThreads" => Some(MailboxProperties::TotalThreads),
            "unreadThreads" => Some(MailboxProperties::UnreadThreads),
            "myRights" => Some(MailboxProperties::MyRights),
            _ => None,
        }
    }
}

impl JSONArgumentParser for MailboxProperties {
    fn parse_argument(argument: JSONValue) -> jmap::Result<Self> {
        let argument = argument.unwrap_string().ok_or_else(|| {
            MethodError::InvalidArguments("Expected string argument.".to_string())
        })?;
        MailboxProperties::parse(&argument).ok_or_else(|| {
            MethodError::InvalidArguments(format!("Unknown mailbox property: '{}'.", argument))
        })
    }
}
