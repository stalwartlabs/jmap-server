use store::log::changes::ChangeId;
use store::AccountId;

use crate::error::method::MethodError;
use crate::error::set::SetError;
use crate::id::jmap::JMAPId;
use crate::id::state::JMAPState;
use crate::jmap_store::set::SetObject;
use crate::protocol::type_state::TypeState;
use std::collections::HashMap;

use super::{MaybeResultReference, ResultReference};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct SetRequest<O: SetObject> {
    #[serde(rename = "accountId", skip_serializing_if = "Option::is_none")]
    pub account_id: Option<JMAPId>,

    #[serde(rename = "ifInState", skip_serializing_if = "Option::is_none")]
    pub if_in_state: Option<JMAPState>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(deserialize_with = "deserialize_map_as_vec")]
    #[serde(bound(deserialize = "Option<Vec<(String, O)>>: serde::Deserialize<'de>"))]
    pub create: Option<Vec<(String, O)>>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(bound(deserialize = "Option<HashMap<JMAPId, O>>: serde::Deserialize<'de>"))]
    pub update: Option<HashMap<JMAPId, O>>,

    #[serde(alias = "#destroy")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destroy: Option<MaybeResultReference<Vec<JMAPId>>>,

    #[serde(flatten)]
    pub arguments: O::SetArguments,
}

fn deserialize_map_as_vec<'de, D, T>(deserializer: D) -> Result<Option<Vec<(String, T)>>, D::Error>
where
    D: serde::de::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    let map: Option<HashMap<String, T>> = serde::de::Deserialize::deserialize(deserializer)?;
    Ok(map.map(|m| m.into_iter().collect()))
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
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub created: HashMap<String, O>,

    #[serde(rename = "updated")]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub updated: HashMap<JMAPId, Option<O>>,

    #[serde(rename = "destroyed")]
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub destroyed: Vec<JMAPId>,

    #[serde(rename = "notCreated")]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub not_created: HashMap<String, SetError<O::Property>>,

    #[serde(rename = "notUpdated")]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub not_updated: HashMap<JMAPId, SetError<O::Property>>,

    #[serde(rename = "notDestroyed")]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub not_destroyed: HashMap<JMAPId, SetError<O::Property>>,

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
        created_ids: &HashMap<String, JMAPId>,
    ) -> crate::Result<()> {
        if let Some(mut objects) = self.create.take() {
            let mut create = Vec::with_capacity(objects.len());
            let mut graph = HashMap::with_capacity(objects.len());

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
                        } else if let Some(object_pos) =
                            objects.iter().position(|(id, _)| id == from_id)
                        {
                            create.push((from_id.to_string(), objects.swap_remove(object_pos).1));
                            if objects.is_empty() {
                                break 'main;
                            }
                        }
                    }

                    if let Some((prev_it, from_id)) = it_stack.pop() {
                        it = prev_it;
                        if let Some(object_pos) = objects.iter().position(|(id, _)| id == from_id) {
                            create.push((from_id.to_string(), objects.swap_remove(object_pos).1));
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
                create.push((user_id, object));
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
            if let MaybeResultReference::Reference(rr) = items {
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
    pub fn created_ids(&self) -> Option<HashMap<String, JMAPId>> {
        if !self.created.is_empty() {
            let mut created_ids = HashMap::with_capacity(self.created.len());
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
