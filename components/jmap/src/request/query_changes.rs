use std::collections::HashMap;

use store::{AccountId, Store};

use crate::{
    id::{jmap::JMAPId, state::JMAPState},
    jmap_store::query::QueryObject,
    protocol::{json::JSONValue, response::Response},
};

use super::query::{Comparator, Filter};
#[derive(Debug, Clone, serde::Deserialize)]
pub struct QueryChangesRequest<O: QueryObject> {
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "filter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter<O::Filter>>,

    #[serde(rename = "sort")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<Comparator<O::Comparator>>>,

    #[serde(rename = "sinceQueryState")]
    pub since_query_state: JMAPState,

    #[serde(rename = "maxChanges")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_changes: Option<usize>,

    #[serde(rename = "upToId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub up_to_id: Option<JMAPId>,

    #[serde(rename = "calculateTotal")]
    pub calculate_total: Option<bool>,

    #[serde(flatten)]
    pub arguments: O::QueryArguments,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryChangesResponse {
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "oldQueryState")]
    pub old_query_state: JMAPState,

    #[serde(rename = "newQueryState")]
    pub new_query_state: JMAPState,

    #[serde(rename = "total")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,

    #[serde(rename = "removed")]
    pub removed: Vec<JMAPId>,

    #[serde(rename = "added")]
    pub added: Vec<AddedItem>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AddedItem {
    id: JMAPId,
    index: usize,
}

impl AddedItem {
    pub fn new(id: JMAPId, index: usize) -> Self {
        Self { id, index }
    }
}
