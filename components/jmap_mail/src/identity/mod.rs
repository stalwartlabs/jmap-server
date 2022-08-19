use jmap::{jmap_store::Object, types::jmap::JMAPId};
use store::core::collection::Collection;

use self::schema::{Identity, Property, Value};

pub mod changes;
pub mod get;
pub mod raft;
pub mod schema;
pub mod serialize;
pub mod set;

impl Object for Identity {
    type Property = Property;

    type Value = Value;

    fn new(id: JMAPId) -> Self {
        let mut item = Identity::default();
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
        &[Property::Email]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[]
    }

    fn max_len() -> &'static [(Self::Property, usize)] {
        &[
            (Property::Name, 255),
            (Property::Email, 255),
            (Property::TextSignature, 2048),
            (Property::HtmlSignature, 2048),
            (Property::ReplyTo, 1024),
            (Property::Bcc, 1024),
        ]
    }

    fn collection() -> Collection {
        Collection::Identity
    }
}
