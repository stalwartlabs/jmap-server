use std::collections::HashMap;

use serde::Serialize;

use jmap::{error::method::MethodError, id::jmap::JMAPId};

use super::method;

#[derive(Debug, serde::Serialize)]
pub struct Response {
    #[serde(rename = "methodResponses")]
    pub method_responses: Vec<method::Call<method::Response>>,

    #[serde(rename = "sessionState")]
    #[serde(serialize_with = "serialize_hex")]
    pub session_state: u64,

    #[serde(rename(deserialize = "createdIds"))]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    pub created_ids: HashMap<String, JMAPId>,
}

impl Response {
    pub fn new(session_state: u64, created_ids: HashMap<String, JMAPId>, capacity: usize) -> Self {
        Response {
            session_state,
            created_ids,
            method_responses: Vec::with_capacity(capacity),
        }
    }

    pub fn push_response(&mut self, id: String, method: method::Response) {
        self.method_responses.push(method::Call { id, method });
    }

    pub fn push_created_id(&mut self, create_id: String, id: JMAPId) {
        self.created_ids.insert(create_id, id);
    }

    pub fn push_error(&mut self, id: String, error: MethodError) {
        self.method_responses.push(method::Call {
            id,
            method: method::Response::Error(error),
        });
    }
}

pub fn serialize_hex<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    format!("{:x}", value).serialize(serializer)
}
