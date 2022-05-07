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
    pub p256dh: String,
    pub auth: String,
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
