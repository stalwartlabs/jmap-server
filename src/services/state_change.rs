/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use actix_web::web;
use jmap::types::type_state::TypeState;
use jmap_sharing::principal::account::JMAPAccountStore;
use std::time::{Duration, Instant, SystemTime};
use store::{
    ahash::AHashMap,
    config::env_settings::EnvSettings,
    core::bitmap::Bitmap,
    log::changes::ChangeId,
    tracing::{debug, error},
    AccountId, JMAPId, Store,
};
use store::{core::JMAPIdPrefix, DocumentId};
use tokio::sync::mpsc;

use crate::{cluster::IPC_CHANNEL_BUFFER, JMAPServer};

use super::push_subscription::{spawn_push_manager, UpdateSubscription};

#[derive(Debug)]
pub enum Event {
    Start,
    Stop,
    Subscribe {
        id: DocumentId,
        account_id: AccountId,
        types: Bitmap<TypeState>,
        tx: mpsc::Sender<StateChange>,
    },
    Publish {
        state_change: StateChange,
    },
    UpdateSharedAccounts {
        account_id: AccountId,
    },
    UpdateSubscriptions {
        account_id: AccountId,
        subscriptions: Vec<UpdateSubscription>,
    },
}

#[derive(Clone, Debug)]
pub struct StateChange {
    pub account_id: AccountId,
    pub types: Vec<(TypeState, ChangeId)>,
}

impl StateChange {
    pub fn new(account_id: AccountId, types: Vec<(TypeState, ChangeId)>) -> Self {
        Self { account_id, types }
    }
}

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

pub fn init_state_manager() -> (mpsc::Sender<Event>, mpsc::Receiver<Event>) {
    mpsc::channel::<Event>(IPC_CHANNEL_BUFFER)
}

pub fn spawn_state_manager<T>(
    core: web::Data<JMAPServer<T>>,
    settings: &EnvSettings,
    mut started: bool,
    mut change_rx: mpsc::Receiver<Event>,
) where
    T: for<'x> Store<'x> + 'static,
{
    let push_tx = spawn_push_manager(settings);

    tokio::spawn(async move {
        let mut subscribers: AHashMap<AccountId, AHashMap<DocumentId, Subscriber>> =
            AHashMap::default();
        let mut shared_accounts: AHashMap<AccountId, Vec<AccountId>> = AHashMap::default();
        let mut shared_accounts_map: AHashMap<AccountId, Vec<(AccountId, Bitmap<TypeState>)>> =
            AHashMap::default();

        let mut last_purge = Instant::now();

        while let Some(event) = change_rx.recv().await {
            let mut purge_needed = last_purge.elapsed() >= Duration::from_secs(PURGE_EVERY_SECS);

            match event {
                Event::Start => {
                    started = true;
                }
                Event::Stop => {
                    started = false;

                    subscribers.clear();
                    shared_accounts.clear();
                    shared_accounts_map.clear();

                    if let Err(err) = push_tx.send(super::push_subscription::Event::Reset).await {
                        debug!("Error sending push reset: {}", err);
                    }
                }
                Event::UpdateSharedAccounts { account_id } => {
                    // Obtain account membership and shared mailboxes
                    let store = core.store.clone();
                    let acl = match core
                        .spawn_worker(move || store.get_acl_token(account_id))
                        .await
                    {
                        Ok(result) => result,
                        Err(err) => {
                            error!("Error updating shared accounts: {}", err);
                            continue;
                        }
                    };

                    // Delete any removed sharings
                    if let Some(shared_account_ids) = shared_accounts.get(&account_id) {
                        for shared_account_id in shared_account_ids {
                            if !acl.member_of.contains(shared_account_id)
                                && !acl
                                    .access_to
                                    .iter()
                                    .any(|(id, _)| *id == *shared_account_id)
                            {
                                if let Some(shared_list) =
                                    shared_accounts_map.get_mut(shared_account_id)
                                {
                                    if let Some(pos) =
                                        shared_list.iter().position(|(id, _)| *id == account_id)
                                    {
                                        if shared_list.len() > 1 {
                                            shared_list.swap_remove(pos);
                                        } else {
                                            shared_accounts_map.remove(shared_account_id);
                                        }
                                    }
                                }
                            }
                        }
                    }

                    // Update lists
                    let mut shared_account_ids =
                        Vec::with_capacity(acl.member_of.len() + acl.access_to.len());
                    for member_id in acl.member_of.iter() {
                        shared_account_ids.push(*member_id);
                        shared_accounts_map
                            .entry(*member_id)
                            .or_insert_with(Vec::new)
                            .push((account_id, Bitmap::all()));
                    }
                    for (shared_account_id, shared_collections) in acl.access_to.iter() {
                        let mut types: Bitmap<TypeState> = Bitmap::new();
                        for collection in shared_collections.clone() {
                            if let Ok(type_state) = TypeState::try_from(collection) {
                                types.insert(type_state);
                                if type_state == TypeState::Email {
                                    types.insert(TypeState::EmailDelivery);
                                    types.insert(TypeState::Thread);
                                }
                            }
                        }
                        if !types.is_empty() {
                            shared_account_ids.push(*shared_account_id);
                            shared_accounts_map
                                .entry(*shared_account_id)
                                .or_insert_with(Vec::new)
                                .push((account_id, types.clone()));
                        }
                    }
                    shared_accounts.insert(account_id, shared_account_ids);
                }
                Event::Subscribe {
                    id,
                    account_id,
                    types,
                    tx,
                } if started => {
                    subscribers
                        .entry(account_id)
                        .or_insert_with(AHashMap::default)
                        .insert(
                            DocumentId::MAX - id,
                            Subscriber {
                                types,
                                subscription: SubscriberType::Ipc { tx },
                            },
                        );
                }
                Event::Publish { state_change } if started => {
                    if let Some(shared_accounts) = shared_accounts_map.get(&state_change.account_id)
                    {
                        let current_time = SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .map(|d| d.as_secs())
                            .unwrap_or(0);
                        let mut push_ids = Vec::new();

                        for (owner_account_id, allowed_types) in shared_accounts {
                            if let Some(subscribers) = subscribers.get(owner_account_id) {
                                for (subscriber_id, subscriber) in subscribers {
                                    let mut types = Vec::with_capacity(state_change.types.len());
                                    for (state_type, change_id) in &state_change.types {
                                        if subscriber.types.contains(*state_type)
                                            && allowed_types.contains(*state_type)
                                        {
                                            types.push((*state_type, *change_id));
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
                                .send(super::push_subscription::Event::Push {
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
                                    UpdateSubscription::Verified(
                                        super::push_subscription::PushSubscription { id, .. },
                                    ) if id == subscriber_id => true,
                                    _ => false,
                                })
                            {
                                remove_ids.push(*subscriber_id);
                            }
                        }

                        for remove_id in remove_ids {
                            push_updates.push(super::push_subscription::PushUpdate::Unregister {
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
                                push_updates.push(super::push_subscription::PushUpdate::Verify {
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
                                    .or_insert_with(AHashMap::default)
                                    .insert(
                                        verified.id,
                                        Subscriber {
                                            types: verified.types,
                                            subscription: SubscriberType::Push {
                                                expires: verified.expires,
                                            },
                                        },
                                    );

                                push_updates.push(super::push_subscription::PushUpdate::Register {
                                    id: JMAPId::from_parts(account_id, verified.id),
                                    url: verified.url,
                                    keys: verified.keys,
                                });
                            }
                        }
                    }

                    if !push_updates.is_empty() {
                        if let Err(err) = push_tx
                            .send(super::push_subscription::Event::Update {
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
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn subscribe_state_manager(
        &self,
        id: DocumentId,
        account_id: DocumentId,
        types: Bitmap<TypeState>,
    ) -> Option<mpsc::Receiver<StateChange>> {
        let (change_tx, change_rx) = mpsc::channel::<StateChange>(IPC_CHANNEL_BUFFER);
        let state_tx = self.state_change.clone();

        for event in [
            Event::UpdateSharedAccounts { account_id },
            Event::Subscribe {
                id,
                account_id,
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
            Event::UpdateSharedAccounts { account_id },
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
