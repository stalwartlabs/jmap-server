use std::collections::HashMap;

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
        } else {
            None
        }
    }

    fn to_jmap_string(&self) -> String {
        format!("i{:02x}", self)
    }
}

pub trait JMAPIdReference {
    fn from_jmap_ref(id: &str, created_ids: &HashMap<String, JSONValue>) -> crate::Result<Self>
    where
        Self: Sized;
}

impl JMAPIdReference for JMAPId {
    fn from_jmap_ref(id: &str, created_ids: &HashMap<String, JSONValue>) -> crate::Result<Self>
    where
        Self: Sized,
    {
        if !id.starts_with('#') {
            JMAPId::from_jmap_string(id)
                .ok_or_else(|| MethodError::InvalidArguments(format!("Invalid JMAP Id: {}", id)))
        } else {
            let id_ref = id.get(1..).ok_or_else(|| {
                MethodError::InvalidArguments(format!("Invalid reference to JMAP Id: {}", id))
            })?;

            if let Some(created_id) = created_ids.get(id_ref) {
                let created_id = created_id
                    .to_object()
                    .unwrap()
                    .get("id")
                    .unwrap()
                    .to_string()
                    .unwrap();
                JMAPId::from_jmap_string(created_id).ok_or_else(|| {
                    MethodError::InvalidArguments(format!(
                        "Invalid referenced JMAP Id: {} ({})",
                        id_ref, created_id
                    ))
                })
            } else {
                Err(MethodError::InvalidArguments(format!(
                    "Reference '{}' not found in createdIds.",
                    id_ref
                )))
            }
        }
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
