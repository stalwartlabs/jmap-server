pub mod get;
pub mod raft;
pub mod schema;
pub mod serialize;
pub mod set;

use crate::{jmap_store::Object, types::jmap::JMAPId};
use store::core::collection::Collection;

use self::schema::{Property, PushSubscription, Value};

impl Object for PushSubscription {
    type Property = Property;

    type Value = Value;

    fn new(id: JMAPId) -> Self {
        let mut item = PushSubscription::default();
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
        &[Property::DeviceClientId, Property::Url]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[]
    }

    fn collection() -> Collection {
        Collection::PushSubscription
    }
}
