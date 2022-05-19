use std::collections::HashMap;

use crate::{
    error::set::SetError,
    id::{jmap::JMAPId, state::JMAPState},
};

//TODO implement + searchsnippet
#[derive(Debug, Clone, serde::Deserialize)]
pub struct CopyRequest<T> {
    #[serde(rename = "fromAccountId")]
    from_account_id: JMAPId,

    #[serde(rename = "ifFromInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    if_from_in_state: Option<JMAPState>,

    #[serde(rename = "accountId")]
    account_id: JMAPId,

    #[serde(rename = "ifInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    if_in_state: Option<JMAPState>,

    #[serde(rename = "create")]
    create: HashMap<String, T>,

    #[serde(rename = "onSuccessDestroyOriginal")]
    on_success_destroy_original: Option<bool>,

    #[serde(rename = "destroyFromIfInState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    destroy_from_if_in_state: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CopyResponse<T, U> {
    #[serde(rename = "fromAccountId")]
    from_account_id: JMAPId,

    #[serde(rename = "accountId")]
    account_id: JMAPId,

    #[serde(rename = "oldState")]
    old_state: Option<JMAPState>,

    #[serde(rename = "newState")]
    new_state: JMAPState,

    #[serde(rename = "created")]
    created: Option<HashMap<String, T>>,

    #[serde(rename = "notCreated")]
    not_created: Option<HashMap<String, SetError<U>>>,
}
