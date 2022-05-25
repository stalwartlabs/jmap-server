pub mod changes;
pub mod get;
pub mod query;
pub mod raft;
pub mod schema;
pub mod serialize;
pub mod set;

use jmap::id::jmap::JMAPId;
use jmap::jmap_store::Object;

use store::core::collection::Collection;
use store::write::options::Options;

use self::schema::{Mailbox, Property, Value};

impl Object for Mailbox {
    type Property = Property;

    type Value = Value;

    fn id(&self) -> Option<&JMAPId> {
        self.properties.get(&Property::Id).and_then(|id| match id {
            Value::Id { value } => Some(value),
            _ => None,
        })
    }

    fn required() -> &'static [Self::Property] {
        &[Property::Name]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[
            (
                Property::Name,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_SORT,
            ),
            (Property::Role, <u64 as Options>::F_KEYWORD),
            (Property::ParentId, <u64 as Options>::F_SORT),
            (Property::SortOrder, <u64 as Options>::F_SORT),
        ]
    }

    fn collection() -> Collection {
        Collection::Mailbox
    }

    fn new(id: JMAPId) -> Self {
        let mut item = Mailbox::default();
        item.properties
            .insert(Property::Id, Value::Id { value: id });
        item
    }
}
