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

pub mod blob;
pub mod config;
pub mod core;
pub mod log;
pub mod nlp;
pub mod read;
pub mod serialize;
pub mod write;

use crate::core::acl::ACL;
use crate::core::{acl::ACLToken, collection::Collection, error::StoreError};
use crate::nlp::Language;
use blob::local::LocalBlobStore;
use blob::BlobStore;
use config::{env_settings::EnvSettings, jmap::JMAPConfig};
use log::raft::{LogIndex, RaftId};
use moka::sync::Cache;
use parking_lot::{Mutex, MutexGuard};
use roaring::RoaringBitmap;
use serialize::StoreDeserialize;
use sieve::{Compiler, Runtime};
use std::sync::atomic::AtomicBool;
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};
use write::{
    id_assign::{IdAssigner, IdCacheKey},
    mutex_map::MutexMap,
    operation::WriteOperation,
};

pub use ahash;
pub use bincode;
pub use blake3;
pub use chrono;
pub use lz4_flex;
pub use moka;
pub use parking_lot;
pub use rand;
pub use roaring;
pub use sha2;
pub use sieve;
pub use tracing;

pub type Result<T> = std::result::Result<T, StoreError>;

pub type AccountId = u32;
pub type DocumentId = u32;
pub type ThreadId = u32;
pub type FieldId = u8;
pub type TagId = u8;
pub type Integer = u32;
pub type LongInteger = u64;
pub type Float = f64;
pub type JMAPId = u64;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub enum ColumnFamily {
    Bitmaps,
    Values,
    Indexes,
    Blobs,
    Logs,
}

pub enum Direction {
    Forward,
    Backward,
}

pub trait Store<'x>
where
    Self: Sized + Send + Sync,
{
    type Iterator: Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'x;

    fn open(settings: &EnvSettings) -> Result<Self>;
    fn delete(&self, cf: ColumnFamily, key: &[u8]) -> Result<()>;
    fn set(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<()>;
    fn get<U>(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<U>>
    where
        U: StoreDeserialize;
    fn exists(&self, cf: ColumnFamily, key: &[u8]) -> Result<bool>;

    fn merge(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<()>;
    fn write(&self, batch: Vec<WriteOperation>) -> Result<()>;
    fn multi_get<T, U>(&self, cf: ColumnFamily, keys: Vec<U>) -> Result<Vec<Option<T>>>
    where
        T: StoreDeserialize,
        U: AsRef<[u8]>;
    fn iterator<'y: 'x>(
        &'y self,
        cf: ColumnFamily,
        start: &[u8],
        direction: Direction,
    ) -> Result<Self::Iterator>;
    fn compact(&self, cf: ColumnFamily) -> Result<()>;
    fn close(&self) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SharedResource {
    pub owner_id: AccountId,
    pub shared_to: AccountId,
    pub collection: Collection,
    pub acl: ACL,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum RecipientType {
    Individual(AccountId),
    List(Vec<(AccountId, String)>),
    NotFound,
}

pub struct JMAPStore<T> {
    pub db: T,
    pub blob_store: LocalBlobStore,
    pub config: JMAPConfig,

    pub account_lock: MutexMap<()>,

    pub sieve_compiler: Compiler,
    pub sieve_runtime: Runtime,

    pub id_assigner: Cache<IdCacheKey, Arc<Mutex<IdAssigner>>>,
    pub shared_documents: Cache<SharedResource, Arc<Option<RoaringBitmap>>>,
    pub acl_tokens: Cache<AccountId, Arc<ACLToken>>,
    pub recipients: Cache<String, Arc<RecipientType>>,

    pub raft_term: AtomicU64,
    pub raft_index: AtomicU64,
    pub tombstone_deletions: AtomicBool,
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn new(db: T, config: JMAPConfig, settings: &EnvSettings) -> Self {
        let mut store = Self {
            config,
            blob_store: LocalBlobStore::new(settings).unwrap(),
            id_assigner: Cache::builder()
                .initial_capacity(128)
                .max_capacity(settings.parse("cache-size-ids").unwrap_or(32 * 1024 * 1024))
                .time_to_idle(Duration::from_secs(
                    settings.parse("cache-tti-ids").unwrap_or(3600),
                ))
                .build(),
            shared_documents: Cache::builder()
                .initial_capacity(128)
                .time_to_idle(Duration::from_secs(
                    settings.parse("cache-tti-sharings").unwrap_or(300),
                ))
                .build(),
            acl_tokens: Cache::builder()
                .initial_capacity(128)
                .time_to_idle(Duration::from_secs(
                    settings.parse("cache-tti-acl").unwrap_or(3600),
                ))
                .build(),
            recipients: Cache::builder()
                .initial_capacity(128)
                .time_to_idle(Duration::from_secs(
                    settings.parse("cache-tti-recipients").unwrap_or(86400),
                ))
                .build(),
            account_lock: MutexMap::with_capacity(1024),
            raft_index: 0.into(),
            raft_term: 0.into(),
            tombstone_deletions: false.into(),
            sieve_compiler: Compiler::new(),
            sieve_runtime: Runtime::new(),
            db,
        };

        // Obtain last Raft ID
        let raft_id = store
            .get_prev_raft_id(RaftId::new(LogIndex::MAX, LogIndex::MAX))
            .unwrap()
            .map(|mut id| {
                id.index += 1;
                id
            })
            .unwrap_or(RaftId {
                term: 0,
                index: LogIndex::MAX,
            });
        store.raft_term = raft_id.term.into();
        store.raft_index = raft_id.index.into();
        store
    }

    #[inline(always)]
    pub fn lock_collection(
        &self,
        account: AccountId,
        collection: Collection,
    ) -> MutexGuard<'_, ()> {
        self.account_lock.lock_hash((account, collection))
    }

    #[inline(always)]
    pub fn try_lock_collection(
        &self,
        account: AccountId,
        collection: Collection,
        timeout: Duration,
    ) -> Option<MutexGuard<'_, ()>> {
        self.account_lock
            .try_lock_hash((account, collection), timeout)
    }
}

impl SharedResource {
    pub fn new(
        owner_id: AccountId,
        shared_to: AccountId,
        collection: Collection,
        acl: ACL,
    ) -> Self {
        Self {
            owner_id,
            shared_to,
            collection,
            acl,
        }
    }
}

pub trait SharedBitmap {
    fn has_some_access(&self) -> bool;
    fn has_access(&self, document_id: DocumentId) -> bool;
}

impl SharedBitmap for Arc<Option<RoaringBitmap>> {
    fn has_some_access(&self) -> bool {
        self.as_ref().as_ref().map_or(false, |b| !b.is_empty())
    }

    fn has_access(&self, document_id: DocumentId) -> bool {
        self.as_ref()
            .as_ref()
            .map_or(false, |b| b.contains(document_id))
    }
}
