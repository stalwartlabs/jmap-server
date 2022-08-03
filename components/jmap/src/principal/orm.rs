use store::{core::collection::Collection, write::options::Options};

use crate::{
    jmap_store::{get::GetObject, query::QueryObject, set::SetObject, Object},
    orm,
    request::ResultReference,
    types::jmap::JMAPId,
};

use super::schema::{Comparator, Filter, Principal, Property, Type, Value};

impl orm::Value for Value {
    fn index_as(&self) -> orm::Index {
        match self {
            Value::Text { value } => value.to_string().into(),
            Value::TextList { value } => {
                if !value.is_empty() {
                    value.to_vec().into()
                } else {
                    orm::Index::Null
                }
            }
            Value::Number { value } => (*value as u64).into(),
            Value::Type { value } => match value {
                Type::Individual => "i".to_string().into(),
                Type::Group => "g".to_string().into(),
                Type::Resource => "r".to_string().into(),
                Type::Location => "l".to_string().into(),
                Type::Domain => "d".to_string().into(),
                Type::List => "t".to_string().into(),
                Type::Other => "o".to_string().into(),
            },
            Value::Members { value } => {
                if !value.is_empty() {
                    value
                        .iter()
                        .map(|id| id.get_document_id())
                        .collect::<Vec<_>>()
                        .into()
                } else {
                    orm::Index::Null
                }
            }
            _ => orm::Index::Null,
        }
    }

    fn is_empty(&self) -> bool {
        match self {
            Value::Text { value } => value.is_empty(),
            Value::Null => true,
            _ => false,
        }
    }
}

impl Default for Value {
    fn default() -> Self {
        Value::Null
    }
}

impl Object for Principal {
    type Property = Property;

    type Value = Value;

    fn new(id: JMAPId) -> Self {
        let mut item = Principal::default();
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
            (Property::Members, <u64 as Options>::F_INDEX),
            (Property::Description, <u64 as Options>::F_TOKENIZE),
            (Property::Timezone, <u64 as Options>::F_TOKENIZE),
            (Property::Quota, <u64 as Options>::F_INDEX),
        ]
    }

    fn collection() -> Collection {
        Collection::Principal
    }
}

impl GetObject for Principal {
    type GetArguments = ();

    fn default_properties() -> Vec<Self::Property> {
        vec![
            Property::Id,
            Property::Name,
            Property::Email,
            Property::Type,
            Property::Description,
        ]
    }

    fn get_as_id(&self, _property: &Self::Property) -> Option<Vec<JMAPId>> {
        None
    }
}

impl SetObject for Principal {
    type SetArguments = ();

    type NextCall = ();

    fn eval_id_references(&mut self, _fnc: impl FnMut(&str) -> Option<JMAPId>) {}
    fn eval_result_references(&mut self, _fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>) {}
}

impl QueryObject for Principal {
    type QueryArguments = ();

    type Filter = Filter;

    type Comparator = Comparator;
}
