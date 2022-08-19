pub mod changes;
pub mod get;
pub mod query;
pub mod raft;
pub mod schema;
pub mod serialize;
pub mod set;

use jmap::{jmap_store::Object, types::jmap::JMAPId};
use store::{core::collection::Collection, write::options::Options};

use self::schema::{EmailSubmission, Property, Value};

impl Object for EmailSubmission {
    type Property = Property;

    type Value = Value;

    fn new(id: JMAPId) -> Self {
        let mut item = EmailSubmission::default();
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
        &[Property::IdentityId, Property::EmailId]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[
            (Property::UndoStatus, <u64 as Options>::F_KEYWORD),
            (Property::EmailId, <u64 as Options>::F_INDEX),
            (Property::IdentityId, <u64 as Options>::F_INDEX),
            (Property::ThreadId, <u64 as Options>::F_INDEX),
            (Property::SendAt, <u64 as Options>::F_INDEX),
        ]
    }

    fn max_len() -> &'static [(Self::Property, usize)] {
        &[(Property::Envelope, 2048)]
    }

    fn collection() -> Collection {
        Collection::EmailSubmission
    }
}
