use std::collections::HashMap;

use jmap::protocol::invocation::Object;
use store::{
    core::collection::{Collection, Collections},
    log::changes::ChangeId,
    AccountId,
};
use tokio::sync::mpsc;

pub mod event_source;
pub mod manager;

#[derive(Clone, Debug)]
pub struct StateChange {
    pub account_id: AccountId,
    pub collection: Collection,
    pub id: ChangeId,
}

#[derive(Debug)]
pub enum Event {
    Start,
    Stop,
    Subscribe {
        subscriber_id: AccountId,
        account_ids: Vec<AccountId>,
        collections: Collections,
        tx: mpsc::Sender<StateChange>,
    },
    Publish {
        state_change: StateChange,
    },
}

#[derive(serde::Serialize)]
pub enum StateChangeType {
    StateChange,
}

#[derive(serde::Serialize)]
pub struct StateChangeResponse {
    #[serde(rename(serialize = "@type"))]
    pub type_: StateChangeType,
    pub changed: HashMap<String, HashMap<Object, String>>,
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
