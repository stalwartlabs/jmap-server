use std::fmt::Display;

use jmap::{jmap_store::orm::PropertySchema, Property};
use store::{core::collection::Collection, write::options::Options};

pub mod changes;
pub mod get;
pub mod query;
pub mod raft;
pub mod set;

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum EmailSubmissionProperty {
    Id = 0,
    IdentityId = 1,
    EmailId = 2,
    ThreadId = 3,
    Envelope = 4,
    SendAt = 5,
    UndoStatus = 6,
    DeliveryStatus = 7,
    DsnBlobIds = 8,
    MdnBlobIds = 9,
}

impl Property for EmailSubmissionProperty {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "id" => Some(EmailSubmissionProperty::Id),
            "identityId" => Some(EmailSubmissionProperty::IdentityId),
            "emailId" => Some(EmailSubmissionProperty::EmailId),
            "threadId" => Some(EmailSubmissionProperty::ThreadId),
            "envelope" => Some(EmailSubmissionProperty::Envelope),
            "sendAt" => Some(EmailSubmissionProperty::SendAt),
            "undoStatus" => Some(EmailSubmissionProperty::UndoStatus),
            "deliveryStatus" => Some(EmailSubmissionProperty::DeliveryStatus),
            "dsnBlobIds" => Some(EmailSubmissionProperty::DsnBlobIds),
            "mdnBlobIds" => Some(EmailSubmissionProperty::MdnBlobIds),
            _ => None,
        }
    }

    fn collection() -> Collection {
        Collection::EmailSubmission
    }
}

impl PropertySchema for EmailSubmissionProperty {
    fn required() -> &'static [Self] {
        &[
            EmailSubmissionProperty::IdentityId,
            EmailSubmissionProperty::EmailId,
        ]
    }

    fn indexed() -> &'static [(Self, u64)] {
        &[
            (
                EmailSubmissionProperty::UndoStatus,
                <u64 as Options>::F_KEYWORD,
            ),
            (EmailSubmissionProperty::EmailId, <u64 as Options>::F_SORT),
            (
                EmailSubmissionProperty::IdentityId,
                <u64 as Options>::F_SORT,
            ),
            (EmailSubmissionProperty::ThreadId, <u64 as Options>::F_SORT),
            (EmailSubmissionProperty::SendAt, <u64 as Options>::F_SORT),
        ]
    }
}

impl Display for EmailSubmissionProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EmailSubmissionProperty::Id => write!(f, "id"),
            EmailSubmissionProperty::IdentityId => write!(f, "identityId"),
            EmailSubmissionProperty::EmailId => write!(f, "emailId"),
            EmailSubmissionProperty::ThreadId => write!(f, "threadId"),
            EmailSubmissionProperty::Envelope => write!(f, "envelope"),
            EmailSubmissionProperty::SendAt => write!(f, "sendAt"),
            EmailSubmissionProperty::UndoStatus => write!(f, "undoStatus"),
            EmailSubmissionProperty::DeliveryStatus => write!(f, "deliveryStatus"),
            EmailSubmissionProperty::DsnBlobIds => write!(f, "dsnBlobIds"),
            EmailSubmissionProperty::MdnBlobIds => write!(f, "mdnBlobIds"),
        }
    }
}

impl From<EmailSubmissionProperty> for u8 {
    fn from(property: EmailSubmissionProperty) -> Self {
        property as u8
    }
}
