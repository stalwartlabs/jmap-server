use jmap::id::jmap::JMAPId;
use serde::{Deserialize, Serialize};
use store::FieldId;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Thread {
    pub id: JMAPId,
    #[serde(rename = "emailIds")]
    pub email_ids: Vec<JMAPId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Copy)]
#[repr(u8)]
pub enum Property {
    #[serde(rename = "id")]
    Id = 0,
    #[serde(rename = "emailIds")]
    EmailIds = 1,
}

impl Property {
    pub fn parse(value: &str) -> Self {
        match value {
            "id" => Property::Id,
            "emailIds" => Property::EmailIds,
            _ => Property::Id,
        }
    }
}

impl From<Property> for FieldId {
    fn from(property: Property) -> Self {
        property as FieldId
    }
}
impl From<FieldId> for Property {
    fn from(field: FieldId) -> Self {
        match field {
            0 => Property::Id,
            _ => Property::EmailIds,
        }
    }
}

impl TryFrom<&str> for Property {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Ok(Property::parse(value))
    }
}
