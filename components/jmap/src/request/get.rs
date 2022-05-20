use std::collections::HashMap;

use store::{AccountId, Store};

use crate::{
    id::{jmap::JMAPId, state::JMAPState},
    jmap_store::{get::GetObject, Object},
    protocol::{json::JSONValue, response::Response},
};

use super::{MaybeResultReference, ResultReference};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct GetRequest<O: GetObject> {
    #[serde(rename = "accountId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account_id: Option<JMAPId>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids: Option<Vec<JMAPId>>,

    #[serde(rename = "#ids")]
    #[serde(skip_deserializing)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ids_ref: Option<ResultReference>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub properties: Option<Vec<O::Property>>,

    #[serde(flatten)]
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
