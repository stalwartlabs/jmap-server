use std::{
    borrow::Cow,
    fmt::{self, Debug},
    sync::Arc,
};

use serde::Deserialize;
use store::core::acl::ACLToken;

use crate::{
    jmap_store::query::QueryObject,
    types::json_pointer::{JSONPointer, JSONPointerEval},
    types::{jmap::JMAPId, state::JMAPState},
};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct QueryRequest<O: QueryObject> {
    #[serde(skip)]
    pub acl: Option<Arc<ACLToken>>,

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

#[derive(Debug, Clone)]
pub enum Filter<T: FilterDeserializer> {
    FilterOperator(FilterOperator<T>),
    FilterCondition(T),
    Empty,
}

#[derive(Debug, Clone)]
pub struct FilterOperator<T: FilterDeserializer> {
    pub operator: Operator,
    pub conditions: Vec<Filter<T>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    And,
    Or,
    Not,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Comparator<A> {
    #[serde(rename = "isAscending")]
    #[serde(default = "is_true")]
    pub is_ascending: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub collation: Option<String>,

    #[serde(flatten)]
    pub property: A,
}

fn is_true() -> bool {
    true
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

impl JSONPointerEval for QueryResponse {
    fn eval_json_pointer(&self, ptr: &JSONPointer) -> Option<Vec<u64>> {
        if ptr.is_item_query("ids") {
            Some(self.ids.iter().map(Into::into).collect())
        } else {
            None
        }
    }
}

// Filter deserializer
struct FilterVisitor<T> {
    phantom: std::marker::PhantomData<T>,
}

pub trait FilterDeserializer: Sized + Debug {
    fn deserialize<'x>(property: &str, map: &mut impl serde::de::MapAccess<'x>) -> Option<Self>;
}

impl<'de, T: FilterDeserializer> serde::de::Visitor<'de> for FilterVisitor<T> {
    type Value = Filter<T>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JMAP e-mail filter")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let mut operator = Operator::And;
        let mut conditions = Vec::new();

        while let Some(key) = map.next_key::<Cow<str>>()? {
            match key.as_ref() {
                "operator" => {
                    operator = match map.next_value::<&str>()? {
                        "AND" => Operator::And,
                        "OR" => Operator::Or,
                        "NOT" => Operator::Not,
                        _ => Operator::And,
                    };
                }
                "conditions" => {
                    conditions = map.next_value()?;
                }
                property => {
                    if let Some(value) = T::deserialize(property, &mut map) {
                        conditions.push(Filter::FilterCondition(value));
                    }
                }
            }
        }

        Ok(match conditions.len() {
            1 if operator != Operator::Not => conditions.pop().unwrap(),
            0 => Filter::Empty,
            _ => Filter::FilterOperator(FilterOperator {
                operator,
                conditions,
            }),
        })
    }
}

impl<'de, T: FilterDeserializer> Deserialize<'de> for Filter<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_map(FilterVisitor {
            phantom: std::marker::PhantomData,
        })
    }
}
