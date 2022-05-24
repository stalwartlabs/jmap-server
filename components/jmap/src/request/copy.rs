use std::collections::HashMap;

use crate::{
    error::set::SetError,
    id::{jmap::JMAPId, state::JMAPState},
    jmap_store::Object,
};

//TODO implement + searchsnippet
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CopyRequest<T> {
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
    pub create: HashMap<String, T>,

    #[serde(rename = "onSuccessDestroyOriginal")]
    pub on_success_destroy_original: Option<bool>,

    #[serde(rename = "destroyFromIfInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destroy_from_if_in_state: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CopyResponse<T: Object> {
    #[serde(rename = "fromAccountId")]
    pub from_account_id: JMAPId,

    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "oldState")]
    pub old_state: Option<JMAPState>,

    #[serde(rename = "newState")]
    pub new_state: JMAPState,

    #[serde(rename = "created")]
    pub created: Option<HashMap<String, T>>,

    #[serde(rename = "notCreated")]
    pub not_created: Option<HashMap<String, SetError<T::Property>>>,
}
