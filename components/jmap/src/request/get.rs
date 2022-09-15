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

use std::{borrow::Cow, fmt, sync::Arc};

use serde::{de::IgnoredAny, Deserialize};
use store::{ahash::AHashSet, core::acl::ACLToken};

use crate::{
    error::method::MethodError,
    jmap_store::get::GetObject,
    types::json_pointer::{JSONPointer, JSONPointerEval},
    types::{jmap::JMAPId, state::JMAPState},
};

use super::{ArgumentDeserializer, MaybeResultReference, ResultReference};

#[derive(Debug, Clone, Default)]
pub struct GetRequest<O: GetObject> {
    pub acl: Option<Arc<ACLToken>>,
    pub account_id: JMAPId,
    pub ids: Option<MaybeResultReference<Vec<JMAPId>>>,
    pub properties: Option<MaybeResultReference<Vec<O::Property>>>,
    pub arguments: O::GetArguments,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct GetResponse<O: GetObject> {
    #[serde(rename = "accountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<JMAPId>,

    pub state: JMAPState,

    pub list: Vec<O>,

    #[serde(rename = "notFound")]
    pub not_found: Vec<JMAPId>,
}

impl<O: GetObject> GetRequest<O> {
    pub fn eval_result_references(
        &mut self,
        mut fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>,
    ) -> crate::Result<()> {
        if let Some(items) = self.ids.as_mut() {
            if let Some(rr) = items.result_reference()? {
                if let Some(ids) = fnc(rr) {
                    *items = MaybeResultReference::Value(ids.into_iter().map(Into::into).collect());
                } else {
                    return Err(MethodError::InvalidResultReference(
                        "Failed to evaluate #ids result reference.".to_string(),
                    ));
                }
            }
        }

        if let Some(items) = self.properties.as_mut() {
            if let Some(rr) = items.result_reference()? {
                if let Some(property_ids) = fnc(rr) {
                    *items = MaybeResultReference::Value(
                        property_ids
                            .into_iter()
                            .map(|property_id| O::Property::from(property_id as u8))
                            .collect(),
                    );
                } else {
                    return Err(MethodError::InvalidResultReference(
                        "Failed to evaluate #properties result reference.".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

impl<O: GetObject> JSONPointerEval for GetResponse<O> {
    fn eval_json_pointer(&self, ptr: &JSONPointer) -> Option<Vec<u64>> {
        match ptr {
            JSONPointer::Path(path) if path.len() == 3 => {
                match (path.get(0)?, path.get(1)?, path.get(2)?) {
                    (
                        JSONPointer::String(root),
                        JSONPointer::Wildcard,
                        JSONPointer::String(property),
                    ) if root == "list" => {
                        let property = O::Property::try_from(property).ok()?;

                        Some(
                            self.list
                                .iter()
                                .filter_map(|item| item.get_as_id(&property))
                                .flat_map(|v| v.into_iter().map(Into::into))
                                .collect::<Vec<u64>>(),
                        )
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}

// Deserialize
struct GetRequestVisitor<O: GetObject> {
    phantom: std::marker::PhantomData<O>,
}

impl<'de, O: GetObject> serde::de::Visitor<'de> for GetRequestVisitor<O> {
    type Value = GetRequest<O>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP get request")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut request = GetRequest {
            acl: None,
            account_id: JMAPId::default(),
            ids: None,
            properties: None,
            arguments: O::GetArguments::default(),
        };

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "accountId" => {
                    request.account_id = map.next_value()?;
                }
                "ids" => {
                    request.ids = if request.ids.is_none() {
                        map.next_value::<Option<Vec<JMAPId>>>()?
                            .map(MaybeResultReference::Value)
                    } else {
                        map.next_value::<IgnoredAny>()?;
                        MaybeResultReference::Error("Duplicate 'ids' property.".into()).into()
                    };
                }
                "#ids" => {
                    request.ids = if request.ids.is_none() {
                        MaybeResultReference::Reference(map.next_value()?)
                    } else {
                        map.next_value::<IgnoredAny>()?;
                        MaybeResultReference::Error("Duplicate 'ids' property.".into())
                    }
                    .into();
                }
                "properties" => {
                    request.properties = if request.properties.is_none() {
                        map.next_value::<Option<AHashSet<O::Property>>>()?
                            .map(|p| MaybeResultReference::Value(p.into_iter().collect()))
                    } else {
                        map.next_value::<IgnoredAny>()?;
                        MaybeResultReference::Error("Duplicate 'properties' property.".into())
                            .into()
                    }
                }
                "#properties" => {
                    request.properties = if request.properties.is_none() {
                        MaybeResultReference::Reference(map.next_value()?)
                    } else {
                        map.next_value::<IgnoredAny>()?;
                        MaybeResultReference::Error("Duplicate 'properties' property.".into())
                    }
                    .into();
                }
                _ => {
                    if let Err(err) =
                        O::GetArguments::deserialize(&mut request.arguments, key.as_ref(), &mut map)
                    {
                        return Err(serde::de::Error::custom(err));
                    }
                }
            }
        }

        Ok(request)
    }
}

impl<'de, O: GetObject> Deserialize<'de> for GetRequest<O> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(GetRequestVisitor {
            phantom: std::marker::PhantomData,
        })
    }
}
