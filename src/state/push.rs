use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    time::{Duration, Instant, SystemTime},
};

use jmap::{
    base64,
    id::{state::JMAPState, JMAPIdSerialize},
    jmap_store::orm::JMAPOrm,
    protocol::{invocation::Object, json::JSONValue},
    push_subscription::PushSubscriptionProperty,
};
use reqwest::header::{CONTENT_ENCODING, CONTENT_TYPE};
use store::{
    core::{
        collection::{Collection, Collections},
        error::StoreError,
    },
    tracing::debug,
    AccountId, DocumentId, JMAPId, Store,
};
use tokio::{sync::mpsc, time};

use crate::{cluster::IPC_CHANNEL_BUFFER, JMAPServer};

use super::{EncriptionKeys, StateChange, StateChangeResponse, UpdateSubscription};

#[derive(Debug)]
pub enum Event {
    Update {
        updates: Vec<PushUpdate>,
    },
    Push {
        ids: Vec<JMAPId>,
        state_change: StateChange,
    },
    DeliverySuccess {
        id: JMAPId,
    },
    DeliveryFailure {
        id: JMAPId,
        state_changes: Vec<StateChange>,
    },
    Reset,
}

#[derive(Debug)]
pub enum PushUpdate {
    Verify {
        id: DocumentId,
        account_id: AccountId,
        url: String,
        code: String,
        keys: Option<EncriptionKeys>,
    },
    Register {
        id: JMAPId,
        url: String,
        keys: Option<EncriptionKeys>,
    },
    Unregister {
        id: JMAPId,
    },
}

#[derive(Debug)]
pub struct PushSubscription {
    url: String,
    keys: Option<EncriptionKeys>,
    num_attempts: u32,
    last_request: Instant,
    state_changes: Vec<StateChange>,
    in_flight: bool,
}

#[cfg(test)]
const PUSH_ATTEMPT_INTERVAL_MS: u64 = 500;
#[cfg(test)]
const PUSH_THROTTLE_MS: u64 = 500;

#[cfg(not(test))]
const PUSH_ATTEMPT_INTERVAL_MS: u64 = 60 * 1000;
#[cfg(not(test))]
const PUSH_THROTTLE_MS: u64 = 1000;

const PUSH_MAX_ATTEMPTS: u32 = 3;
const PUSH_TIMEOUT_MS: u64 = 10 * 1000;
const RETRY_MS: u64 = 1000;
const VERIFY_WAIT_SECS: u64 = 60;
const LONG_SLUMBER_SECS: u64 = 60 * 60 * 24;

pub fn spawn_push_manager() -> mpsc::Sender<Event> {
    let (push_tx_, mut push_rx) = mpsc::channel::<Event>(IPC_CHANNEL_BUFFER);
    let push_tx = push_tx_.clone();

    tokio::spawn(async move {
        let mut subscriptions = HashMap::new();
        let mut last_verify: HashMap<AccountId, u64> = HashMap::new();
        let mut last_retry = Instant::now();
        let mut retry_timeout = Duration::from_secs(LONG_SLUMBER_SECS);
        let mut retry_ids = HashSet::new();

        loop {
            match time::timeout(retry_timeout, push_rx.recv()).await {
                Ok(Some(event)) => {
                    //println!("Push: {:?}", event);

                    match event {
                        Event::Update { updates } => {
                            for update in updates {
                                match update {
                                    PushUpdate::Verify {
                                        id,
                                        account_id,
                                        url,
                                        code,
                                        keys,
                                    } => {
                                        let current_time = SystemTime::now()
                                            .duration_since(SystemTime::UNIX_EPOCH)
                                            .map(|d| d.as_secs())
                                            .unwrap_or(0);

                                        #[cfg(test)]
                                        if url.contains("skip_checks") {
                                            last_verify.insert(
                                                account_id,
                                                current_time - (VERIFY_WAIT_SECS + 1),
                                            );
                                        }

                                        if last_verify
                                            .get(&account_id)
                                            .map(|last_verify| {
                                                current_time - *last_verify > VERIFY_WAIT_SECS
                                            })
                                            .unwrap_or(true)
                                        {
                                            tokio::spawn(async move {
                                                http_request(
                                                    url,
                                                    format!(
                                                        concat!(
                                                            "{{\"@type\":\"PushVerification\",",
                                                            "\"pushSubscriptionId\":\"{}\",",
                                                            "\"verificationCode\":\"{}\"}}"
                                                        ),
                                                        (id as JMAPId).to_jmap_string(),
                                                        code
                                                    ),
                                                    keys,
                                                )
                                                .await;
                                            });

                                            last_verify.insert(account_id, current_time);
                                        } else {
                                            debug!(
                                                concat!(
                                                    "Failed to verify push subscription: ",
                                                    "Too many requests for from accountId {}."
                                                ),
                                                account_id
                                            );
                                            continue;
                                        }
                                    }
                                    PushUpdate::Register { id, url, keys } => {
                                        if let Entry::Vacant(entry) = subscriptions.entry(id) {
                                            entry.insert(PushSubscription {
                                                url,
                                                keys,
                                                num_attempts: 0,
                                                last_request: Instant::now()
                                                    - Duration::from_millis(PUSH_THROTTLE_MS + 1),
                                                state_changes: Vec::new(),
                                                in_flight: false,
                                            });
                                        }
                                    }
                                    PushUpdate::Unregister { id } => {
                                        subscriptions.remove(&id);
                                    }
                                }
                            }
                        }
                        Event::Push { ids, state_change } => {
                            for id in ids {
                                if let Some(subscription) = subscriptions.get_mut(&id) {
                                    subscription.state_changes.push(state_change.clone());
                                    let last_request =
                                        subscription.last_request.elapsed().as_millis() as u64;

                                    if !subscription.in_flight
                                        && ((subscription.num_attempts == 0
                                            && last_request > PUSH_THROTTLE_MS)
                                            || ((1..PUSH_MAX_ATTEMPTS)
                                                .contains(&subscription.num_attempts)
                                                && last_request > PUSH_ATTEMPT_INTERVAL_MS))
                                    {
                                        subscription.send(id, push_tx.clone());
                                        retry_ids.remove(&id);
                                    } else {
                                        retry_ids.insert(id);
                                    }
                                } else {
                                    debug!("No push subscription found for id: {}", id);
                                }
                            }
                        }
                        Event::Reset => {
                            subscriptions.clear();
                        }
                        Event::DeliverySuccess { id } => {
                            if let Some(subscription) = subscriptions.get_mut(&id) {
                                subscription.num_attempts = 0;
                                subscription.in_flight = false;
                                retry_ids.remove(&id);
                            }
                        }
                        Event::DeliveryFailure { id, state_changes } => {
                            if let Some(subscription) = subscriptions.get_mut(&id) {
                                subscription.last_request = Instant::now();
                                subscription.num_attempts += 1;
                                subscription.state_changes.extend(state_changes);
                                subscription.in_flight = false;
                                retry_ids.insert(id);
                            }
                        }
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(_) => (),
            }

            retry_timeout = if !retry_ids.is_empty() {
                let last_retry_elapsed = last_retry.elapsed().as_millis() as u64;

                if last_retry_elapsed >= RETRY_MS {
                    let mut remove_ids = Vec::with_capacity(retry_ids.len());

                    for retry_id in &retry_ids {
                        if let Some(subscription) = subscriptions.get_mut(retry_id) {
                            let last_request =
                                subscription.last_request.elapsed().as_millis() as u64;

                            if !subscription.in_flight
                                && ((subscription.num_attempts == 0
                                    && last_request >= PUSH_THROTTLE_MS)
                                    || (subscription.num_attempts > 0
                                        && last_request >= PUSH_ATTEMPT_INTERVAL_MS))
                            {
                                if subscription.num_attempts < PUSH_MAX_ATTEMPTS {
                                    subscription.send(*retry_id, push_tx.clone());
                                } else {
                                    debug!(
                                        concat!(
                                            "Failed to deliver push subscription: ",
                                            "Too many attempts for url {}."
                                        ),
                                        subscription.url
                                    );
                                    subscription.state_changes.clear();
                                    subscription.num_attempts = 0;
                                }
                                remove_ids.push(*retry_id);
                            }
                        }
                    }

                    if remove_ids.len() < retry_ids.len() {
                        for remove_id in remove_ids {
                            retry_ids.remove(&remove_id);
                        }
                        last_retry = Instant::now();
                        Duration::from_millis(RETRY_MS)
                    } else {
                        retry_ids.clear();
                        Duration::from_secs(LONG_SLUMBER_SECS)
                    }
                } else {
                    Duration::from_millis(RETRY_MS - last_retry_elapsed)
                }
            } else {
                Duration::from_secs(LONG_SLUMBER_SECS)
            };
            //println!("Retry ids {:?} in {:?}", retry_ids, retry_timeout);
        }
    });

    push_tx_
}

impl PushSubscription {
    fn send(&mut self, id: JMAPId, push_tx: mpsc::Sender<Event>) {
        let url = self.url.clone();
        let keys = self.keys.clone();
        let state_changes = std::mem::take(&mut self.state_changes);

        self.in_flight = true;
        self.last_request = Instant::now();

        tokio::spawn(async move {
            let mut response = StateChangeResponse::new();
            for state_change in &state_changes {
                response
                    .changed
                    .entry((state_change.account_id as JMAPId).to_jmap_string())
                    .or_insert_with(HashMap::new)
                    .insert(
                        state_change.collection.into(),
                        JMAPState::from(state_change.id).to_jmap_string(),
                    );
            }

            //println!("Posting to {}: {:?}", url, response);

            push_tx
                .send(
                    if http_request(url, serde_json::to_string(&response).unwrap(), keys).await {
                        Event::DeliverySuccess { id }
                    } else {
                        Event::DeliveryFailure { id, state_changes }
                    },
                )
                .await
                .ok();
        });
    }
}

async fn http_request(url: String, mut body: String, keys: Option<EncriptionKeys>) -> bool {
    let client_builder = reqwest::Client::builder().timeout(Duration::from_millis(PUSH_TIMEOUT_MS));

    #[cfg(test)]
    let client_builder = client_builder.danger_accept_invalid_certs(true);

    let mut client = client_builder
        .build()
        .unwrap_or_default()
        .post(&url)
        .header(CONTENT_TYPE, "application/json")
        .header("TTL", "86400");

    if let Some(keys) = keys {
        match ece::encrypt(&keys.p256dh, &keys.auth, body.as_bytes())
            .map(|b| base64::encode_config(b, base64::URL_SAFE))
        {
            Ok(body_) => {
                body = body_;
                client = client.header(CONTENT_ENCODING, "aes128gcm");
            }
            Err(err) => {
                // Do not reattempt if encryption fails.
                debug!("Failed to encrypt push subscription to {}: {}", url, err);
                return true;
            }
        }
    }

    match client.body(body).send().await {
        Ok(response) => response.status().is_success(),
        Err(err) => {
            debug!("HTTP post to {} failed with: {}", url, err);
            false
        }
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn fetch_push_subscriptions(
        &self,
        account_id: AccountId,
    ) -> jmap::Result<super::Event> {
        let store = self.store.clone();

        self.spawn_jmap_request(move || {
            let mut subscriptions = Vec::new();
            let document_ids = store
                .get_document_ids(account_id, Collection::PushSubscription)?
                .unwrap_or_default();
            let current_time = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);

            for document_id in document_ids {
                let mut subscription = store
                    .get_orm::<PushSubscriptionProperty>(account_id, document_id)?
                    .ok_or_else(|| {
                        StoreError::InternalError(format!(
                            "Could not find ORM for push subscription {}",
                            document_id
                        ))
                    })?;
                let expires = subscription
                    .get_unsigned_int(&PushSubscriptionProperty::Expires)
                    .ok_or_else(|| {
                        StoreError::InternalError(format!(
                            "Missing expires property for push subscription {}",
                            document_id
                        ))
                    })?;
                if expires > current_time {
                    let keys = if let Some(JSONValue::Object(mut keys)) =
                        subscription.remove(&PushSubscriptionProperty::Keys)
                    {
                        EncriptionKeys {
                            p256dh: keys
                                .remove("p256dh")
                                .and_then(|v| v.unwrap_string())
                                .and_then(|v| base64::decode_config(v, base64::URL_SAFE).ok())
                                .unwrap_or_default(),
                            auth: keys
                                .remove("auth")
                                .and_then(|v| v.unwrap_string())
                                .and_then(|v| base64::decode_config(v, base64::URL_SAFE).ok())
                                .unwrap_or_default(),
                        }
                        .into()
                    } else {
                        None
                    };
                    let verification_code = subscription
                        .remove_string(&PushSubscriptionProperty::VerificationCode_)
                        .ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Missing verificationCode property for push subscription {}",
                                document_id
                            ))
                        })?;
                    let url = subscription
                        .remove_string(&PushSubscriptionProperty::Url)
                        .ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Missing Url property for push subscription {}",
                                document_id
                            ))
                        })?;

                    if subscription
                        .get_string(&PushSubscriptionProperty::VerificationCode)
                        .map_or(false, |v| v == verification_code)
                    {
                        let mut collections = Collections::default();
                        if let Some(types) =
                            subscription.get_array(&PushSubscriptionProperty::Types)
                        {
                            for obj_type in types {
                                if let Some(obj_type) = obj_type.to_string().and_then(Object::parse)
                                {
                                    collections.insert(obj_type.into());
                                }
                            }
                        }
                        if collections.is_empty() {
                            collections = Collections::all();
                        }

                        // Add verified subscription
                        subscriptions.push(UpdateSubscription::Verified(super::PushSubscription {
                            id: document_id,
                            url,
                            expires,
                            collections,
                            keys,
                        }));
                    } else {
                        // Add unverified subscription
                        subscriptions.push(UpdateSubscription::Unverified {
                            id: document_id,
                            url,
                            code: verification_code,
                            keys,
                        });
                    }
                }
            }

            Ok(super::Event::UpdateSubscriptions {
                account_id,
                subscriptions,
            })
        })
        .await
    }
}
