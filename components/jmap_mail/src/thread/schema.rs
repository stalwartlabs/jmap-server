use jmap::{
    id::jmap::JMAPId,
    jmap_store::{orm::EmptyValue, Object},
};
use serde::{Deserialize, Serialize};
use store::{core::collection::Collection, FieldId};

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

impl From<Property> for FieldId {
    fn from(property: Property) -> Self {
        property as FieldId
    }
}

impl Object for Thread {
    type Property = Property;

    type Value = EmptyValue;

    fn new(id: JMAPId) -> Self {
        Thread {
            id,
            email_ids: Vec::new(),
        }
    }

    fn id(&self) -> Option<&JMAPId> {
        Some(&self.id)
    }

    fn required() -> &'static [Self::Property] {
        &[]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[]
    }

    fn collection() -> store::core::collection::Collection {
        Collection::Thread
    }
}
