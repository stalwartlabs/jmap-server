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

pub mod event_source;
pub mod http;
pub mod websocket;

use crate::services::{email_delivery, housekeeper, state_change};
use crate::{cluster, JMAPServer};
use store::core::error::StoreError;
use store::tracing::{debug, error};
use store::ColumnFamily;
use store::{
    serialize::{StoreDeserialize, StoreSerialize},
    Store,
};
use tokio::sync::oneshot;

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn get_key<U>(&self, key: &'static str) -> store::Result<Option<U>>
    where
        U: StoreDeserialize + Send + Sync + 'static,
    {
        let store = self.store.clone();
        self.spawn_worker(move || store.db.get(ColumnFamily::Values, key.as_bytes()))
            .await
    }

    pub async fn set_key<U>(&self, key: &'static str, value: U) -> store::Result<()>
    where
        U: StoreSerialize + Send + Sync + 'static,
    {
        let store = self.store.clone();
        self.spawn_worker(move || {
            store.db.set(
                ColumnFamily::Values,
                key.as_bytes(),
                &value.serialize().ok_or_else(|| {
                    StoreError::SerializeError(format!("Failed to serialize value for key {}", key))
                })?,
            )
        })
        .await
    }

    pub fn queue_set_key<U>(&self, key: &'static str, value: U)
    where
        U: StoreSerialize + Send + Sync + 'static,
    {
        let store = self.store.clone();

        self.worker_pool.spawn(move || {
            let bytes = match value.serialize() {
                Some(bytes) => bytes,
                None => {
                    error!("Failed to serialize value for key {}", key);
                    return;
                }
            };

            if let Err(err) = store.db.set(ColumnFamily::Values, key.as_bytes(), &bytes) {
                error!("Failed to set key: {:?}", err);
            }
        });
    }

    pub async fn spawn_worker<U, V>(&self, f: U) -> store::Result<V>
    where
        U: FnOnce() -> store::Result<V> + Send + 'static,
        V: Sync + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        self.worker_pool.spawn(move || {
            tx.send(f()).ok();
        });

        rx.await
            .map_err(|e| StoreError::InternalError(format!("Await error: {}", e)))?
    }

    pub async fn spawn_jmap_request<U, V>(&self, f: U) -> jmap::Result<V>
    where
        U: FnOnce() -> jmap::Result<V> + Send + 'static,
        V: Sync + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        self.worker_pool.spawn(move || {
            tx.send(f()).ok();
        });

        rx.await
            .map_err(|e| StoreError::InternalError(format!("Await error: {}", e)))?
    }

    pub async fn shutdown(&self) {
        if let Some(cluster) = &self.cluster {
            if cluster.tx.send(cluster::Event::Shutdown).await.is_err() {
                error!("Failed to send shutdown event to cluster.");
            }
        }

        if self.lmtp.send(false).is_err() {
            error!("Failed to send shutdown event to LMTP service.");
        }

        if self
            .state_change
            .send(state_change::Event::Stop)
            .await
            .is_err()
        {
            error!("Failed to send shutdown event to state manager.");
        }

        if self
            .housekeeper
            .send(housekeeper::Event::Exit)
            .await
            .is_err()
        {
            error!("Failed to send shutdown event to housekeeper task.");
        }

        if self
            .email_delivery
            .send(email_delivery::Event::Stop)
            .await
            .is_err()
        {
            debug!("Failed to send shutdown event to e-mail delivery task.");
        }
    }

    #[cfg(test)]
    pub async fn set_offline(&self, is_offline: bool, notify_peers: bool) {
        self.is_offline
            .store(is_offline, std::sync::atomic::Ordering::Relaxed);
        self.set_follower(None).await;
        if self
            .cluster
            .as_ref()
            .unwrap()
            .tx
            .send(cluster::Event::SetOffline {
                is_offline,
                notify_peers,
            })
            .await
            .is_err()
        {
            error!("Failed to send offline event to cluster.");
        }
    }

    #[cfg(test)]
    pub fn is_offline(&self) -> bool {
        self.is_offline.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub trait UnwrapFailure<T> {
    fn failed_to(self, action: &str) -> T;
}

impl<T> UnwrapFailure<T> for Option<T> {
    fn failed_to(self, message: &str) -> T {
        match self {
            Some(result) => result,
            None => {
                println!("Failed to {}", message);
                std::process::exit(1);
            }
        }
    }
}

impl<T, E: std::fmt::Display> UnwrapFailure<T> for Result<T, E> {
    fn failed_to(self, message: &str) -> T {
        match self {
            Ok(result) => result,
            Err(err) => {
                println!("Failed to {}: {}", message, err);
                std::process::exit(1);
            }
        }
    }
}

pub fn failed_to(action: &str) -> ! {
    println!("Failed to {}", action);
    std::process::exit(1);
}
