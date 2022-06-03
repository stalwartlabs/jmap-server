use std::collections::HashMap;

use store::{log::changes::ChangeId, AccountId};

use crate::{
    error::set::SetError,
    jmap_store::set::SetObject,
    types::{jmap::JMAPId, state::JMAPState, type_state::TypeState},
};

use super::{MaybeIdReference, ResultReference};

//TODO implement + searchsnippet
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CopyRequest<T: SetObject> {
    #[serde(rename = "fromAccountId")]
    pub from_account_id: JMAPId,

    #[serde(rename = "ifFromInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_from_in_state: Option<JMAPState>,

    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "ifInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub if_in_state: Option<JMAPState>,

    #[serde(rename = "create")]
    #[serde(bound(deserialize = "HashMap<MaybeIdReference, T>: serde::Deserialize<'de>"))]
    pub create: HashMap<MaybeIdReference, T>,

    #[serde(rename = "onSuccessDestroyOriginal")]
    pub on_success_destroy_original: Option<bool>,

    #[serde(rename = "destroyFromIfInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destroy_from_if_in_state: Option<JMAPState>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CopyResponse<O: SetObject> {
    #[serde(rename = "fromAccountId")]
    pub from_account_id: JMAPId,

    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "oldState")]
    pub old_state: JMAPState,

    #[serde(rename = "newState")]
    pub new_state: JMAPState,

    #[serde(rename = "created")]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub created: HashMap<JMAPId, O>,

    #[serde(rename = "notCreated")]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub not_created: HashMap<JMAPId, SetError<O::Property>>,

    #[serde(skip)]
    pub change_id: Option<ChangeId>,

    #[serde(skip)]
    pub state_changes: Option<Vec<(TypeState, ChangeId)>>,

    #[serde(skip)]
    pub next_call: Option<O::NextCall>,
}

impl<O: SetObject> CopyRequest<O> {
    pub fn eval_references(
        &mut self,
        mut result_map_fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>,
        created_ids: &HashMap<String, JMAPId>,
    ) -> crate::Result<()> {
        let mut create = HashMap::with_capacity(self.create.len());

        for (id, mut object) in std::mem::take(&mut self.create) {
            object.eval_result_references(&mut result_map_fnc);
            object.eval_id_references(|parent_id| created_ids.get(parent_id).copied());
            create.insert(
                match id {
                    MaybeIdReference::Reference(id_ref) => {
                        if let Some(id) = created_ids.get(&id_ref) {
                            MaybeIdReference::Value(*id)
                        } else {
                            return Err(crate::MethodError::InvalidResultReference(format!(
                                "Reference '{}' not  found.",
                                id_ref
                            )));
                        }
                    }
                    id => id,
                },
                object,
            );
        }
        self.create = create;

        Ok(())
    }
}

impl<O: SetObject> CopyResponse<O> {
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
        self.account_id.get_document_id()
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
