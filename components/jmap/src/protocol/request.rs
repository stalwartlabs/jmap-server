use std::collections::HashMap;

use super::json::JSONValue;

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
pub struct Request {
    pub using: Vec<String>,
    #[serde(rename(deserialize = "methodCalls"))]
    pub method_calls: Vec<(String, JSONValue, String)>,
    #[serde(rename(deserialize = "createdIds"))]
    pub created_ids: Option<HashMap<String, String>>,
}
