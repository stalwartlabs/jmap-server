use store::{core::collection::Collection, write::options::Options};

use crate::jmap_store::Object;

use self::schema::{Principal, Property, Value};

pub mod get;
pub mod query;
pub mod schema;
pub mod serialize;
pub mod set;

impl Object for Principal {
    type Property = Property;

    type Value = Value;

    fn new(id: crate::types::jmap::JMAPId) -> Self {
        let mut item = Principal::default();
        item.properties
            .insert(Property::Id, Value::Id { value: id });
        item
    }

    fn id(&self) -> Option<&crate::types::jmap::JMAPId> {
        self.properties.get(&Property::Id).and_then(|id| match id {
            Value::Id { value } => Some(value),
            _ => None,
        })
    }

    fn required() -> &'static [Self::Property] {
        &[]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[
            (
                Property::Type,
                <u64 as Options>::F_KEYWORD | <u64 as Options>::F_INDEX,
            ),
            (
                Property::Name,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_INDEX,
            ),
            (
                Property::Email,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_INDEX,
            ),
            (
                Property::Aliases,
                <u64 as Options>::F_TOKENIZE | <u64 as Options>::F_INDEX,
            ),
            (Property::Description, <u64 as Options>::F_TOKENIZE),
            (Property::Timezone, <u64 as Options>::F_TOKENIZE),
            (Property::Quota, <u64 as Options>::F_INDEX),
        ]
    }

    fn collection() -> Collection {
        Collection::Principal
    }
}
