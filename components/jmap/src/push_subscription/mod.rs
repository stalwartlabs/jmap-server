pub mod get;
pub mod set;

use std::fmt::Display;

use crate::{jmap_store::orm::PropertySchema, protocol::json::JSONValue, Property};
use store::{core::collection::Collection, FieldId};

#[derive(Debug, PartialEq, Eq, Hash, Clone, Copy, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum PushSubscriptionProperty {
    Id = 0,
    DeviceClientId = 1,
    Url = 2,
    Keys = 3,
    VerificationCode = 4,
    Expires = 5,
    Types = 6,
    VerificationCode_ = 7,
}

impl Property for PushSubscriptionProperty {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "id" => Some(PushSubscriptionProperty::Id),
            "deviceClientId" => Some(PushSubscriptionProperty::DeviceClientId),
            "url" => Some(PushSubscriptionProperty::Url),
            "keys" => Some(PushSubscriptionProperty::Keys),
            "verificationCode" => Some(PushSubscriptionProperty::VerificationCode),
            "expires" => Some(PushSubscriptionProperty::Expires),
            "types" => Some(PushSubscriptionProperty::Types),
            _ => None,
        }
    }

    fn collection() -> store::core::collection::Collection {
        Collection::PushSubscription
    }
}

impl Display for PushSubscriptionProperty {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PushSubscriptionProperty::Id => write!(f, "id"),
            PushSubscriptionProperty::DeviceClientId => write!(f, "deviceClientId"),
            PushSubscriptionProperty::Url => write!(f, "url"),
            PushSubscriptionProperty::Keys => write!(f, "keys"),
            PushSubscriptionProperty::VerificationCode => write!(f, "verificationCode"),
            PushSubscriptionProperty::Expires => write!(f, "expires"),
            PushSubscriptionProperty::Types => write!(f, "types"),
            PushSubscriptionProperty::VerificationCode_ => Ok(()),
        }
    }
}

impl PropertySchema for PushSubscriptionProperty {
    fn required() -> &'static [Self] {
        &[
            PushSubscriptionProperty::DeviceClientId,
            PushSubscriptionProperty::Url,
        ]
    }

    fn indexed() -> &'static [(Self, u64)] {
        &[]
    }
}

impl From<PushSubscriptionProperty> for FieldId {
    fn from(field: PushSubscriptionProperty) -> Self {
        field as FieldId
    }
}

impl From<PushSubscriptionProperty> for JSONValue {
    fn from(value: PushSubscriptionProperty) -> Self {
        JSONValue::String(value.to_string())
    }
}
