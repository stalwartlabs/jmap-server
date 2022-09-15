/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use store::{
    core::{acl::ACL, collection::Collection},
    write::options::Options,
};

use crate::{
    jmap_store::{get::GetObject, query::QueryObject, set::SetObject, Object},
    orm,
    request::ResultReference,
    types::{blob::JMAPBlob, jmap::JMAPId},
};

use super::schema::{Comparator, Filter, Patch, Principal, Property, Type, Value};

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

    fn len(&self) -> usize {
        match self {
            Value::Id { .. } => std::mem::size_of::<JMAPId>(),
            Value::Blob { .. } => std::mem::size_of::<JMAPBlob>(),
            Value::Text { value } => value.len(),
            Value::TextList { value } => value.iter().fold(0, |acc, item| acc + item.len()),
            Value::Number { .. } => std::mem::size_of::<i64>(),
            Value::Type { .. } => std::mem::size_of::<Type>(),
            Value::DKIM { value } => {
                value.dkim_selector.as_ref().map(|s| s.len()).unwrap_or(0)
                    + std::mem::size_of::<i64>()
            }
            Value::Members { value } => value.len() * std::mem::size_of::<JMAPId>(),
            Value::ACL(value) => value.iter().fold(0, |acc, (k, v)| {
                acc + k.len() + v.len() * std::mem::size_of::<ACL>()
            }),
            Value::Patch(_) => std::mem::size_of::<Patch>(),
            Value::Null => 0,
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

    fn max_len() -> &'static [(Self::Property, usize)] {
        &[
            (Property::Name, 255),
            (Property::Email, 255),
            (Property::Aliases, 255 * 1000),
            (Property::Capabilities, 100 * 10),
            (Property::Description, 512),
            (Property::Timezone, 100),
            (Property::Secret, 2048),
            (Property::DKIM, 100),
        ]
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
