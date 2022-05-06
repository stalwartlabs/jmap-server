use actix_web::web;
use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    time::{Duration, Instant},
};
use store::{
    core::collection::Collections,
    tracing::{debug, error},
    AccountId, Store,
};
use tokio::sync::mpsc;

use crate::{cluster::IPC_CHANNEL_BUFFER, JMAPServer};

use super::{Event, StateChange};

struct Subscriber {
    account_ids: Vec<AccountId>,
    collections: Collections,
    tx: mpsc::Sender<StateChange>,
}

const PURGE_EVERY_SECS: u64 = 60;
const SENT_TIMEOUT_MS: u64 = 500;

pub fn spawn_state_manager(mut started: bool) -> mpsc::Sender<Event> {
    let (change_tx, mut change_rx) = mpsc::channel::<Event>(IPC_CHANNEL_BUFFER);

    tokio::spawn(async move {
        let mut subscribers: HashMap<AccountId, Subscriber> = HashMap::new();
        let mut subscriber_map: HashMap<AccountId, HashSet<AccountId>> = HashMap::new();
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
                    subscriber_map.clear();
                }
                Event::Subscribe {
                    subscriber_id,
                    account_ids,
                    collections,
                    tx,
                } if started => match subscribers.entry(subscriber_id) {
                    Entry::Occupied(mut entry) => {
                        let subscriber = entry.get();
                        for account_id in &subscriber.account_ids {
                            if let Some(map) = subscriber_map.get_mut(account_id) {
                                map.remove(&subscriber_id);
                            }
                        }
                        for account_id in &account_ids {
                            subscriber_map
                                .entry(*account_id)
                                .or_insert_with(HashSet::new)
                                .insert(subscriber_id);
                        }
                        entry.insert(Subscriber {
                            account_ids,
                            collections,
                            tx,
                        });
                    }
                    Entry::Vacant(entry) => {
                        for account_id in &account_ids {
                            subscriber_map
                                .entry(*account_id)
                                .or_insert_with(HashSet::new)
                                .insert(subscriber_id);
                        }
                        entry.insert(Subscriber {
                            account_ids,
                            collections,
                            tx,
                        });
                    }
                },
                Event::Publish { state_change } if started => {
                    if let Some(account_subscribers) = subscriber_map.get(&state_change.account_id)
                    {
                        for subscriber_id in account_subscribers {
                            if let Some(subscriber) = subscribers.get(subscriber_id) {
                                if subscriber.collections.contains(state_change.collection) {
                                    if !subscriber.tx.is_closed() {
                                        let subscriber_tx = subscriber.tx.clone();
                                        let state_change = state_change.clone();

                                        tokio::spawn(async move {
                                            // Timeout after 500ms in case there is a blocked client
                                            if let Err(err) = subscriber_tx
                                                .send_timeout(
                                                    state_change,
                                                    Duration::from_millis(SENT_TIMEOUT_MS),
                                                )
                                                .await
                                            {
                                                debug!(
                                                    "Error sending state change to subscriber: {}",
                                                    err
                                                );
                                            }
                                        });
                                    } else {
                                        purge_needed = true;
                                    }
                                }
                            }
                        }
                    }
                }
                _ => {
                    debug!("Ignoring state event {:?}", event);
                }
            }

            if purge_needed {
                let mut remove_ids = Vec::new();
                for (subscriber_id, subscriber) in &subscribers {
                    if subscriber.tx.is_closed() {
                        for account_id in &subscriber.account_ids {
                            if let Some(map) = subscriber_map.get_mut(account_id) {
                                map.remove(subscriber_id);
                            }
                        }
                        remove_ids.push(*subscriber_id);
                    }
                }
                for remove_id in remove_ids {
                    subscribers.remove(&remove_id);
                }

                last_purge = Instant::now();
            }
        }
    });
    change_tx
}

pub async fn subscribe_state_manager<T>(
    core: web::Data<JMAPServer<T>>,
    subscriber_id: AccountId,
    account_ids: Vec<AccountId>,
    collections: Collections,
) -> Option<mpsc::Receiver<StateChange>>
where
    T: for<'x> Store<'x> + 'static,
{
    let (change_tx, change_rx) = mpsc::channel::<StateChange>(IPC_CHANNEL_BUFFER);
    //let (change_tx, change_rx) = watch::channel::<StateChange>(IPC_CHANNEL_BUFFER);
    if let Err(err) = core
        .state_change
        .clone()
        .send(Event::Subscribe {
            subscriber_id,
            account_ids,
            collections,
            tx: change_tx,
        })
        .await
    {
        error!(
            "Channel failure while trying to subscribe to state manager: {}",
            err
        );
        None
    } else {
        change_rx.into()
    }
}
