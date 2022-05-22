use crate::{
    id::{jmap::JMAPId, state::JMAPState},
    jmap_store::query::QueryObject,
};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct QueryRequest<O: QueryObject> {
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "filter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter<O::Filter>>,

    #[serde(rename = "sort")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<Comparator<O::Comparator>>>,

    #[serde(rename = "position")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position: Option<i32>,

    #[serde(rename = "anchor")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anchor: Option<JMAPId>,

    #[serde(rename = "anchorOffset")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub anchor_offset: Option<i32>,

    #[serde(rename = "limit")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,

    #[serde(rename = "calculateTotal")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub calculate_total: Option<bool>,

    #[serde(flatten)]
    pub arguments: O::QueryArguments,
}

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(untagged)]
pub enum Filter<T> {
    FilterOperator(FilterOperator<T>),
    FilterCondition(T),
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct FilterOperator<T> {
    pub operator: Operator,
    pub conditions: Vec<Filter<T>>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub enum Operator {
    #[serde(rename = "AND")]
    And,
    #[serde(rename = "OR")]
    Or,
    #[serde(rename = "NOT")]
    Not,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Comparator<A> {
    #[serde(rename = "isAscending")]
    pub is_ascending: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,

    #[serde(flatten)]
    pub property: A,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryResponse {
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "queryState")]
    pub query_state: JMAPState,

    #[serde(rename = "canCalculateChanges")]
    pub can_calculate_changes: bool,

    #[serde(rename = "position")]
    pub position: i32,

    #[serde(rename = "ids")]
    pub ids: Vec<JMAPId>,

    #[serde(rename = "total")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,

    #[serde(rename = "limit")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,

    #[serde(skip)]
    pub is_immutable: bool,
}
