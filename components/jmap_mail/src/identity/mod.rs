use jmap::{id::jmap::JMAPId, jmap_store::Object};
use store::core::collection::Collection;

use self::schema::{Identity, Property, Value};

pub mod changes;
pub mod get;
pub mod serialize;
pub mod set;

pub mod schema;

impl Object for Identity {
    type Property = Property;

    type Value = Value;

    fn new(id: JMAPId) -> Self {
        let mut item = Identity::default();
        item.properties
            .insert(Property::Id, Value::Id { value: id });
        item
    }

    fn id(&self) -> Option<&JMAPId> {
        self.properties.get(&Property::Id).and_then(|id| match id {
            Value::Id { value } => Some(value),
            _ => None,
        })
    }

    fn required() -> &'static [Self::Property] {
        &[Property::Email]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[]
    }

    fn collection() -> Collection {
        Collection::Identity
    }

    fn hide_account() -> bool {
        false
    }
}
