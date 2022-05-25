use jmap::protocol::type_state::TypeState;
use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant, SystemTime},
};
use store::{
    core::bitmap::Bitmap,
    tracing::{debug, error},
    AccountId, JMAPId, Store,
};
use store::{core::JMAPIdPrefix, DocumentId};
use tokio::sync::mpsc;

use crate::{cluster::IPC_CHANNEL_BUFFER, JMAPServer};

use super::{push::spawn_push_manager, Event, StateChange, UpdateSubscription};

#[derive(Debug)]
struct Subscriber {
    types: Bitmap<TypeState>,
    subscription: SubscriberType,
}

#[derive(Debug)]
pub enum SubscriberType {
    Ipc { tx: mpsc::Sender<StateChange> },
    Push { expires: u64 },
}

impl Subscriber {
    fn is_valid(&self, current_time: u64) -> bool {
        match &self.subscription {
            SubscriberType::Ipc { tx } => !tx.is_closed(),
            SubscriberType::Push { expires } => expires > &current_time,
        }
    }
}

const PURGE_EVERY_SECS: u64 = 3600;
const SEND_TIMEOUT_MS: u64 = 500;

//TODO: emailDelivery type
pub fn spawn_state_manager(mut started: bool) -> mpsc::Sender<Event> {
    let (change_tx, mut change_rx) = mpsc::channel::<Event>(IPC_CHANNEL_BUFFER);
    let push_tx = spawn_push_manager();

    tokio::spawn(async move {
        let mut subscribers: HashMap<AccountId, HashMap<DocumentId, Subscriber>> = HashMap::new();
        let mut shared_accounts: HashMap<AccountId, Vec<AccountId>> = HashMap::new();
        let mut shared_accounts_map: HashMap<AccountId, HashSet<AccountId>> = HashMap::new();

        let mut last_purge = Instant::now();

        while let Some(event) = change_rx.recv().await {
            let mut purge_needed = last_purge.elapsed() >= Duration::from_secs(PURGE_EVERY_SECS);

            //println!("Manager: {:?}", event);

            match event {
                Event::Start => {
                    started = true;
                }
                Event::Stop => {
                    started = false;

                    subscribers.clear();
                    shared_accounts.clear();
                    shared_accounts_map.clear();

                    if let Err(err) = push_tx.send(super::push::Event::Reset).await {
                        debug!("Error sending push reset: {}", err);
                    }
                }
                Event::UpdateSharedAccounts {
                    owner_account_id,
                    shared_account_ids,
                } => {
                    // Delete any removed sharings
                    if let Some(current_shared_account_ids) = shared_accounts.get(&owner_account_id)
                    {
                        for current_shared_account_id in current_shared_account_ids {
                            if !shared_account_ids.contains(current_shared_account_id) {
                                if let Some(shared_accounts_map) =
                                    shared_accounts_map.get_mut(current_shared_account_id)
                                {
                                    shared_accounts_map.remove(&owner_account_id);
                                }
                            }
                        }
                    }

                    // Link account owner
                    shared_accounts_map
                        .entry(owner_account_id)
                        .or_insert_with(HashSet::new)
                        .insert(owner_account_id);

                    // Link shared accounts
                    for shared_account_id in shared_account_ids {
                        shared_accounts_map
                            .entry(shared_account_id)
                            .or_insert_with(HashSet::new)
                            .insert(owner_account_id);
                    }
                }
                Event::Subscribe {
                    id,
                    account_id,
                    types,
                    tx,
                } if started => {
                    subscribers
                        .entry(account_id)
                        .or_insert_with(HashMap::new)
                        .insert(
                            DocumentId::MAX - id,
                            Subscriber {
                                types,
                                subscription: SubscriberType::Ipc { tx },
                            },
                        );
                }
                Event::Publish { state_change } if started => {
                    //println!("{:?}\n{:?}", shared_accounts_map, subscribers);

                    if let Some(shared_accounts) = shared_accounts_map.get(&state_change.account_id)
                    {
                        let current_time = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let mut push_ids = Vec::new();

                        for owner_account_id in shared_accounts {
                            if let Some(subscribers) = subscribers.get(owner_account_id) {
                                for (subscriber_id, subscriber) in subscribers {
                                    let mut types = Vec::with_capacity(state_change.types.len());
                                    for (state_type, change_id) in &state_change.types {
                                        if subscriber.types.contains(state_type.clone()) {
                                            types.push((state_type.clone(), *change_id));
                                        }
                                    }
                                    if !types.is_empty() {
                                        match &subscriber.subscription {
                                            SubscriberType::Ipc { tx } if !tx.is_closed() => {
                                                let subscriber_tx = tx.clone();
                                                let state_change = state_change.clone();

                                                tokio::spawn(async move {
                                                    // Timeout after 500ms in case there is a blocked client
                                                    if let Err(err) = subscriber_tx
                                                        .send_timeout(
                                                            StateChange {
                                                                account_id: state_change.account_id,
                                                                types,
                                                            },
                                                            Duration::from_millis(SEND_TIMEOUT_MS),
                                                        )
                                                        .await
                                                    {
                                                        debug!(
                                                        "Error sending state change to subscriber: {}",
                                                        err
                                                    );
                                                    }
                                                });
                                            }
                                            SubscriberType::Push { expires }
                                                if expires > &current_time =>
                                            {
                                                push_ids.push(JMAPId::from_parts(
                                                    *owner_account_id,
                                                    *subscriber_id,
                                                ));
                                            }
                                            _ => {
                                                purge_needed = true;
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if !push_ids.is_empty() {
                            if let Err(err) = push_tx
                                .send(super::push::Event::Push {
                                    ids: push_ids,
                                    state_change,
                                })
                                .await
                            {
                                debug!("Error sending push updates: {}", err);
                            }
                        }
                    }
                }
                Event::UpdateSubscriptions {
                    account_id,
                    subscriptions,
                } if started => {
                    let mut updated_ids = Vec::with_capacity(subscriptions.len());
                    let mut push_updates = Vec::with_capacity(subscriptions.len());

                    if let Some(subscribers) = subscribers.get_mut(&account_id) {
                        let mut remove_ids = Vec::new();

                        for subscriber_id in subscribers.keys() {
                            #[allow(clippy::match_like_matches_macro)]
                            if (*subscriber_id < DocumentId::MAX / 2)
                                && !subscriptions.iter().any(|s| match s {
                                    UpdateSubscription::Verified(super::PushSubscription {
                                        id,
                                        ..
                                    }) if id == subscriber_id => true,
                                    _ => false,
                                })
                            {
                                remove_ids.push(*subscriber_id);
                            }
                        }

                        for remove_id in remove_ids {
                            push_updates.push(super::push::PushUpdate::Unregister {
                                id: JMAPId::from_parts(account_id, remove_id),
                            });
                            subscribers.remove(&remove_id);
                        }
                    }

                    for subscription in subscriptions {
                        match subscription {
                            UpdateSubscription::Unverified {
                                id,
                                url,
                                code,
                                keys,
                            } => {
                                push_updates.push(super::push::PushUpdate::Verify {
                                    id,
                                    account_id,
                                    url,
                                    code,
                                    keys,
                                });
                            }
                            UpdateSubscription::Verified(verified) => {
                                updated_ids.push(verified.id);
                                subscribers
                                    .entry(account_id)
                                    .or_insert_with(HashMap::new)
                                    .insert(
                                        verified.id,
                                        Subscriber {
                                            types: verified.types,
                                            subscription: SubscriberType::Push {
                                                expires: verified.expires,
                                            },
                                        },
                                    );

                                push_updates.push(super::push::PushUpdate::Register {
                                    id: JMAPId::from_parts(account_id, verified.id),
                                    url: verified.url,
                                    keys: verified.keys,
                                });
                            }
                        }
                    }

                    if !push_updates.is_empty() {
                        if let Err(err) = push_tx
                            .send(super::push::Event::Update {
                                updates: push_updates,
                            })
                            .await
                        {
                            debug!("Error sending push updates: {}", err);
                        }
                    }
                }
                _ => {
                    debug!("Ignoring state event {:?}", event);
                }
            }

            if purge_needed {
                let mut remove_account_ids = Vec::new();
                let current_time = SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .map(|d| d.as_secs())
                    .unwrap_or(0);

                for (account_id, subscriber_map) in &mut subscribers {
                    let mut remove_subscription_ids = Vec::new();
                    for (id, subscriber) in subscriber_map.iter() {
                        if !subscriber.is_valid(current_time) {
                            remove_subscription_ids.push(*id);
                        }
                    }
                    if !remove_subscription_ids.is_empty() {
                        if remove_subscription_ids.len() < subscriber_map.len() {
                            for remove_subscription_id in remove_subscription_ids {
                                subscriber_map.remove(&remove_subscription_id);
                            }
                        } else {
                            remove_account_ids.push(*account_id);
                        }
                    }
                }

                for remove_account_id in remove_account_ids {
                    subscribers.remove(&remove_account_id);
                }

                last_purge = Instant::now();
            }
        }
    });
    change_tx
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn subscribe_state_manager(
        &self,
        id: DocumentId,
        owner_account_id: DocumentId,
        types: Bitmap<TypeState>,
    ) -> Option<mpsc::Receiver<StateChange>> {
        let (change_tx, change_rx) = mpsc::channel::<StateChange>(IPC_CHANNEL_BUFFER);
        let state_tx = self.state_change.clone();

        for event in [
            Event::UpdateSharedAccounts {
                owner_account_id,
                shared_account_ids: vec![], //TODO: shared accounts
            },
            Event::Subscribe {
                id,
                account_id: owner_account_id,
                types,
                tx: change_tx,
            },
        ] {
            if let Err(err) = state_tx.send(event).await {
                error!(
                    "Channel failure while subscribing to state manager: {}",
                    err
                );
                return None;
            }
        }

        change_rx.into()
    }

    pub async fn publish_state_change(&self, state_change: StateChange) -> jmap::Result<()> {
        let state_tx = self.state_change.clone();
        if let Err(err) = state_tx.clone().send(Event::Publish { state_change }).await {
            error!("Channel failure while publishing state change: {}", err);
        }
        Ok(())
    }

    pub async fn update_push_subscriptions(&self, account_id: AccountId) -> jmap::Result<()> {
        let state_tx = self.state_change.clone();
        for event in [
            Event::UpdateSharedAccounts {
                owner_account_id: account_id,
                shared_account_ids: vec![], //TODO: shared accounts
            },
            self.fetch_push_subscriptions(account_id).await?,
        ] {
            if let Err(err) = state_tx.send(event).await {
                error!("Channel failure while publishing state change: {}", err);
                break;
            }
        }

        Ok(())
    }
}
