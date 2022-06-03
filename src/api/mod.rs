use std::collections::HashMap;

use jmap::types::{jmap::JMAPId, state::JMAPState, type_state::TypeState};

pub mod ingest;
pub mod invocation;
pub mod method;
pub mod request;
pub mod response;
pub mod session;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum StateChangeType {
    StateChange,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct StateChangeResponse {
    #[serde(rename = "@type")]
    pub type_: StateChangeType,
    pub changed: HashMap<JMAPId, HashMap<TypeState, JMAPState>>,
}

impl StateChangeResponse {
    pub fn new() -> Self {
        Self {
            type_: StateChangeType::StateChange,
            changed: HashMap::new(),
        }
    }
}

impl Default for StateChangeResponse {
    fn default() -> Self {
        Self::new()
    }
}
