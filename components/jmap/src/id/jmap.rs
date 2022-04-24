use store::JMAPId;

use crate::{error::method::MethodError, protocol::json::JSONValue};

use super::JMAPIdSerialize;

impl JMAPIdSerialize for JMAPId {
    fn from_jmap_string(id: &str) -> Option<Self>
    where
        Self: Sized,
    {
        if id.as_bytes().get(0)? == &b'i' {
            JMAPId::from_str_radix(id.get(1..)?, 16).ok()?.into()
        } else if id == "singleton" {
            0.into()
        } else {
            None
        }
    }

    fn to_jmap_string(&self) -> String {
        format!("i{:02x}", self)
    }
}

impl JSONValue {
    pub fn to_jmap_id(&self) -> Option<JMAPId> {
        match self {
            JSONValue::String(string) => JMAPId::from_jmap_string(string),
            _ => None,
        }
    }

    pub fn parse_jmap_id(self, optional: bool) -> crate::Result<Option<JMAPId>> {
        match self {
            JSONValue::String(string) => Ok(Some(JMAPId::from_jmap_string(&string).ok_or_else(
                || MethodError::InvalidArguments("Failed to parse JMAP Id.".to_string()),
            )?)),
            JSONValue::Null if optional => Ok(None),
            _ => Err(MethodError::InvalidArguments(
                "Expected string.".to_string(),
            )),
        }
    }
}
