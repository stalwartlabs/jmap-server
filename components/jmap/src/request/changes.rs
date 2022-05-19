use crate::{
    id::{jmap::JMAPId, state::JMAPState},
    jmap_store::changes::ChangesObject,
};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ChangesRequest {
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "sinceState")]
    pub since_state: JMAPState,

    #[serde(rename = "maxChanges")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_changes: Option<usize>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ChangesResponse<O: ChangesObject> {
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "oldState")]
    pub old_state: JMAPState,

    #[serde(rename = "newState")]
    pub new_state: JMAPState,

    #[serde(rename = "hasMoreChanges")]
    pub has_more_changes: bool,

    pub created: Vec<JMAPId>,

    pub updated: Vec<JMAPId>,

    pub destroyed: Vec<JMAPId>,

    #[serde(flatten)]
    pub arguments: O::ChangesResponse,

    #[serde(skip)]
    pub total_changes: usize,
    #[serde(skip)]
    pub has_children_changes: bool,
}

impl<O: ChangesObject> ChangesResponse<O> {
    pub fn empty(account_id: JMAPId) -> Self {
        Self {
            account_id,
            old_state: JMAPState::default(),
            new_state: JMAPState::default(),
            has_more_changes: false,
            created: Vec::with_capacity(0),
            updated: Vec::with_capacity(0),
            destroyed: Vec::with_capacity(0),
            arguments: O::ChangesResponse::default(),
            total_changes: 0,
            has_children_changes: false,
        }
    }
}
