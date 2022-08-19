use jmap::{jmap_store::Object, types::jmap::JMAPId};
use store::core::collection::Collection;

use self::schema::{Property, VacationResponse, Value};

pub mod get;
pub mod raft;
pub mod schema;
pub mod serialize;
pub mod set;

impl Object for VacationResponse {
    type Property = Property;

    type Value = Value;

    fn new(id: JMAPId) -> Self {
        let mut item = VacationResponse::default();
        item.properties
            .append(Property::Id, Value::Id { value: id });
        item
    }

    fn id(&self) -> Option<&JMAPId> {
        self.properties.get(&Property::Id).and_then(|id| match id {
            Value::Id { value } => Some(value),
            _ => None,
        })
    }

    fn required() -> &'static [Self::Property] {
        &[]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[]
    }

    fn max_len() -> &'static [(Self::Property, usize)] {
        &[
            (Property::Subject, 512),
            (Property::HtmlBody, 4096),
            (Property::TextBody, 4096),
        ]
    }

    fn collection() -> store::core::collection::Collection {
        Collection::VacationResponse
    }
}
