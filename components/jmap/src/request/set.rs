use crate::error::method::MethodError;
use crate::error::set::SetError;
use crate::jmap_store::set::SetObject;
use crate::request::ArgumentSerializer;
use crate::types::jmap::JMAPId;
use crate::types::state::JMAPState;
use crate::types::type_state::TypeState;
use serde::de::IgnoredAny;
use serde::Deserialize;
use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use store::ahash::AHashMap;
use store::core::ahash_is_empty;
use store::core::vec_map::VecMap;
use store::AccountId;
use store::{core::acl::ACLToken, log::changes::ChangeId};

use super::{MaybeResultReference, ResultReference};

#[derive(Debug, Clone, Default)]
pub struct SetRequest<O: SetObject> {
    pub acl: Option<Arc<ACLToken>>,
    pub account_id: JMAPId,
    pub if_in_state: Option<JMAPState>,
    pub create: Option<VecMap<String, O>>,
    pub update: Option<VecMap<JMAPId, O>>,
    pub destroy: Option<MaybeResultReference<Vec<JMAPId>>>,
    pub arguments: O::SetArguments,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct SetResponse<O: SetObject> {
    #[serde(rename = "accountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<JMAPId>,

    #[serde(rename = "oldState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub old_state: Option<JMAPState>,

    #[serde(rename = "newState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_state: Option<JMAPState>,

    #[serde(rename = "created")]
    #[serde(skip_serializing_if = "ahash_is_empty")]
    pub created: AHashMap<String, O>,

    #[serde(rename = "updated")]
    #[serde(skip_serializing_if = "VecMap::is_empty")]
    pub updated: VecMap<JMAPId, Option<O>>,

    #[serde(rename = "destroyed")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub destroyed: Vec<JMAPId>,

    #[serde(rename = "notCreated")]
    #[serde(skip_serializing_if = "VecMap::is_empty")]
    pub not_created: VecMap<String, SetError<O::Property>>,

    #[serde(rename = "notUpdated")]
    #[serde(skip_serializing_if = "VecMap::is_empty")]
    pub not_updated: VecMap<JMAPId, SetError<O::Property>>,

    #[serde(rename = "notDestroyed")]
    #[serde(skip_serializing_if = "VecMap::is_empty")]
    pub not_destroyed: VecMap<JMAPId, SetError<O::Property>>,

    #[serde(skip)]
    pub change_id: Option<ChangeId>,

    #[serde(skip)]
    pub state_changes: Option<Vec<(TypeState, ChangeId)>>,

    #[serde(skip)]
    pub next_call: Option<O::NextCall>,
}

impl<O: SetObject> SetRequest<O> {
    pub fn eval_references(
        &mut self,
        mut result_map_fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>,
        created_ids: &AHashMap<String, JMAPId>,
    ) -> crate::Result<()> {
        if let Some(mut objects) = self.create.take() {
            let mut create = VecMap::with_capacity(objects.len());
            let mut graph = AHashMap::with_capacity(objects.len());

            for (child_id, object) in objects.iter_mut() {
                object.eval_result_references(&mut result_map_fnc);
                object.eval_id_references(|parent_id| {
                    if let Some(id) = created_ids.get(parent_id) {
                        Some(*id)
                    } else {
                        graph
                            .entry(child_id.to_string())
                            .or_insert_with(Vec::new)
                            .push(parent_id.to_string());
                        None
                    }
                });
            }

            // Topological sort
            if !graph.is_empty() {
                let mut it_stack = Vec::new();
                let keys = graph.keys().cloned().collect::<Vec<_>>();
                let mut it = keys.iter();

                'main: loop {
                    while let Some(from_id) = it.next() {
                        if let Some(to_ids) = graph.get(from_id) {
                            it_stack.push((it, from_id));
                            if it_stack.len() > 1000 {
                                return Err(MethodError::InvalidArguments(
                                    "Cyclical references are not allowed.".to_string(),
                                ));
                            }
                            it = to_ids.iter();
                            continue;
                        } else if let Some((id, value)) = objects.remove_entry(from_id) {
                            create.append(id, value);
                            if objects.is_empty() {
                                break 'main;
                            }
                        }
                    }

                    if let Some((prev_it, from_id)) = it_stack.pop() {
                        it = prev_it;
                        if let Some((id, value)) = objects.remove_entry(from_id) {
                            create.append(id, value);
                            if objects.is_empty() {
                                break 'main;
                            }
                        }
                    } else {
                        break;
                    }
                }
            }

            for (user_id, object) in objects {
                create.append(user_id, object);
            }

            self.create = create.into();
        }

        if let Some(objects) = self.update.as_mut() {
            for (_, object) in objects.iter_mut() {
                object.eval_id_references(|parent_id| created_ids.get(parent_id).copied());
                object.eval_result_references(&mut result_map_fnc);
            }
        }

        if let Some(items) = self.destroy.as_mut() {
            if let Some(rr) = items.result_reference()? {
                if let Some(ids) = result_map_fnc(rr) {
                    *items = MaybeResultReference::Value(ids.into_iter().map(Into::into).collect());
                } else {
                    return Err(MethodError::InvalidResultReference(
                        "Failed to evaluate result reference.".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }
}

impl<O: SetObject> SetResponse<O> {
    pub fn created_ids(&self) -> Option<AHashMap<String, JMAPId>> {
        if !self.created.is_empty() {
            let mut created_ids = AHashMap::with_capacity(self.created.len());
            for (create_id, item) in &self.created {
                created_ids.insert(create_id.to_string(), *item.id().unwrap());
            }
            created_ids.into()
        } else {
            None
        }
    }

    pub fn account_id(&self) -> AccountId {
        self.account_id.as_ref().unwrap().get_document_id()
    }

    pub fn has_changes(&self) -> Option<ChangeId> {
        self.change_id
    }

    pub fn state_changes(&mut self) -> Option<Vec<(TypeState, ChangeId)>> {
        self.state_changes.take()
    }

    pub fn next_call(&mut self) -> Option<O::NextCall> {
        self.next_call.take()
    }
}

// Deserialize
struct SetRequestVisitor<O: SetObject> {
    phantom: std::marker::PhantomData<O>,
}

impl<'de, O: SetObject> serde::de::Visitor<'de> for SetRequestVisitor<O> {
    type Value = SetRequest<O>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP set request")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut request = SetRequest {
            acl: None,
            account_id: JMAPId::default(),
            if_in_state: None,
            create: None,
            update: None,
            destroy: None,
            arguments: O::SetArguments::default(),
        };

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "accountId" => {
                    request.account_id = map.next_value()?;
                }
                "ifInState" => {
                    request.if_in_state = map.next_value()?;
                }
                "update" => {
                    request.update = map.next_value()?;
                }
                "create" => {
                    request.create = map.next_value()?;
                }
                "destroy" => {
                    request.destroy = if request.destroy.is_none() {
                        map.next_value::<Option<Vec<JMAPId>>>()?
                            .map(MaybeResultReference::Value)
                    } else {
                        map.next_value::<IgnoredAny>()?;
                        MaybeResultReference::Error("Duplicate 'destroy' property.".into()).into()
                    };
                }
                "#destroy" => {
                    request.destroy = if request.destroy.is_none() {
                        MaybeResultReference::Reference(map.next_value()?)
                    } else {
                        map.next_value::<IgnoredAny>()?;
                        MaybeResultReference::Error("Duplicate 'destroy' property.".into())
                    }
                    .into();
                }
                key => {
                    if let Err(err) =
                        O::SetArguments::deserialize(&mut request.arguments, key, &mut map)
                    {
                        return Err(serde::de::Error::custom(err));
                    }
                }
            }
        }

        Ok(request)
    }
}

impl<'de, O: SetObject> Deserialize<'de> for SetRequest<O> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(SetRequestVisitor {
            phantom: std::marker::PhantomData,
        })
    }
}
