use std::collections::HashMap;

use jmap::protocol::invocation::Object;
use store::{
    core::collection::{Collection, Collections},
    log::changes::ChangeId,
    AccountId, DocumentId,
};
use tokio::sync::mpsc;

pub mod event_source;
pub mod manager;
pub mod push;

#[cfg(test)]
pub const THROTTLE_MS: u64 = 500;

#[cfg(not(test))]
pub const THROTTLE_MS: u64 = 1000;
pub const LONG_SLUMBER_MS: u64 = 60 * 60 * 24 * 1000;

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
        id: DocumentId,
        account_id: AccountId,
        collections: Collections,
        tx: mpsc::Sender<StateChange>,
    },
    Publish {
        state_change: StateChange,
    },
    UpdateSharedAccounts {
        owner_account_id: AccountId,
        shared_account_ids: Vec<AccountId>,
    },
    UpdateSubscriptions {
        account_id: AccountId,
        subscriptions: Vec<UpdateSubscription>,
    },
}

#[derive(Debug)]
pub enum UpdateSubscription {
    Unverified {
        id: DocumentId,
        url: String,
        code: String,
        keys: Option<EncriptionKeys>,
    },
    Verified(PushSubscription),
}

#[derive(Debug)]
pub struct PushSubscription {
    pub id: DocumentId,
    pub url: String,
    pub expires: u64,
    pub collections: Collections,
    pub keys: Option<EncriptionKeys>,
}

#[derive(Debug, Clone)]
pub struct EncriptionKeys {
    pub p256dh: Vec<u8>,
    pub auth: Vec<u8>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum StateChangeType {
    StateChange,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct StateChangeResponse {
    #[serde(rename = "@type")]
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