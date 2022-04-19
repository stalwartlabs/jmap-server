use std::collections::HashMap;

use serde::Serialize;

use crate::error::method::MethodError;

use super::json::JSONValue;

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
pub struct Response {
    #[serde(rename(serialize = "methodResponses"))]
    pub method_responses: Vec<(String, JSONValue, String)>,
    #[serde(rename(serialize = "sessionState"))]
    #[serde(serialize_with = "serialize_hex")]
    pub session_state: u64,
    #[serde(rename(deserialize = "createdIds"))]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub created_ids: HashMap<String, String>,
}

impl Response {
    pub fn new(session_state: u64, created_ids: HashMap<String, String>, capacity: usize) -> Self {
        Response {
            session_state,
            created_ids,
            method_responses: Vec::with_capacity(capacity),
        }
    }

    pub fn push_response(
        &mut self,
        name: String,
        call_id: String,
        response: JSONValue,
        add_created_ids: bool,
    ) {
        if add_created_ids {
            if let Some(obj) = response
                .to_object()
                .and_then(|o| o.get("created"))
                .and_then(|o| o.to_object())
            {
                for (user_id, obj) in obj {
                    if let Some(id) = obj
                        .to_object()
                        .and_then(|o| o.get("id"))
                        .and_then(|id| id.to_string())
                    {
                        self.created_ids.insert(user_id.to_string(), id.to_string());
                    }
                }
            }
        }

        self.method_responses.push((name, response, call_id));
    }

    pub fn push_error(&mut self, call_id: String, error: MethodError) {
        self.method_responses
            .push(("error".to_string(), error.into(), call_id));
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(&self).unwrap()
    }
}

pub fn serialize_hex<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    format!("{:x}", value).serialize(serializer)
}
