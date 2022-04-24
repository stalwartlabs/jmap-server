use std::fmt::Display;

use jmap::{jmap_store::orm::PropertySchema, Property};
use store::{field::TextIndex, Collection};

pub mod changes;
pub mod get;
pub mod set;

#[derive(Debug, PartialEq, Eq, Hash, Copy, Clone, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum IdentityProperty {
    Id = 0,
    Name = 1,
    Email = 2,
    ReplyTo = 3,
    Bcc = 4,
    TextSignature = 5,
    HtmlSignature = 6,
    MayDelete = 7,
}

impl Property for IdentityProperty {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "id" => Some(IdentityProperty::Id),
            "name" => Some(IdentityProperty::Name),
            "email" => Some(IdentityProperty::Email),
            "replyTo" => Some(IdentityProperty::ReplyTo),
            "bcc" => Some(IdentityProperty::Bcc),
            "textSignature" => Some(IdentityProperty::TextSignature),
            "htmlSignature" => Some(IdentityProperty::HtmlSignature),
            "mayDelete" => Some(IdentityProperty::MayDelete),
            _ => None,
        }
    }

    fn collection() -> store::Collection {
        Collection::Identity
    }
}

impl PropertySchema for IdentityProperty {
    fn required() -> &'static [Self] {
        &[IdentityProperty::Email]
    }

    fn sorted() -> &'static [Self] {
        &[]
    }

    fn indexed() -> &'static [(Self, TextIndex)] {
        &[]
    }

    fn tags() -> &'static [Self] {
        &[]
    }
}

impl Display for IdentityProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdentityProperty::Id => write!(f, "id"),
            IdentityProperty::Name => write!(f, "name"),
            IdentityProperty::Email => write!(f, "email"),
            IdentityProperty::ReplyTo => write!(f, "replyTo"),
            IdentityProperty::Bcc => write!(f, "bcc"),
            IdentityProperty::TextSignature => write!(f, "textSignature"),
            IdentityProperty::HtmlSignature => write!(f, "htmlSignature"),
            IdentityProperty::MayDelete => write!(f, "mayDelete"),
        }
    }
}

impl From<IdentityProperty> for u8 {
    fn from(property: IdentityProperty) -> Self {
        property as u8
    }
}
