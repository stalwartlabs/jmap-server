use store::log::changes::ChangeId;
use store::AccountId;

use crate::error::set::SetError;
use crate::id::jmap::JMAPId;
use crate::id::state::JMAPState;
use crate::jmap_store::set::SetObject;
use crate::protocol::type_state::TypeState;
use std::collections::HashMap;

use super::ResultReference;

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

    #[serde(skip_serializing_if = "Option::is_none")]
    pub destroy: Option<Vec<JMAPId>>,

    #[serde(rename = "#destroy")]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destroy_ref: Option<ResultReference>,

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
    pub state_changes: Option<HashMap<TypeState, ChangeId>>,

    #[serde(skip)]
    pub next_call: Option<O::NextCall>,
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

    pub fn state_changes(&mut self) -> Option<HashMap<TypeState, ChangeId>> {
        self.state_changes.take()
    }

    pub fn next_call(&mut self) -> Option<O::NextCall> {
        self.next_call.take()
    }
}
