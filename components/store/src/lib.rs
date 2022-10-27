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
            sieve_compiler: Compiler::new()
                .with_max_script_size(
                    settings
                        .parse("sieve-max-script-size")
                        .unwrap_or(1024 * 1024),
                )
                .with_max_string_size(settings.parse("sieve-max-string-size").unwrap_or(4096))
                .with_max_variable_name_size(
                    settings.parse("sieve-max-variable-name-size").unwrap_or(32),
                )
                .with_max_nested_blocks(settings.parse("sieve-max-nested-blocks").unwrap_or(15))
                .with_max_nested_tests(settings.parse("sieve-max-nested-tests").unwrap_or(15))
                .with_max_nested_foreverypart(
                    settings.parse("sieve-max-nested-foreverypart").unwrap_or(3),
                )
                .with_max_match_variables(settings.parse("sieve-max-match-variables").unwrap_or(30))
                .with_max_local_variables(
                    settings.parse("sieve-max-local-variables").unwrap_or(128),
                )
                .with_max_header_size(settings.parse("sieve-max-header-size").unwrap_or(1024))
                .with_max_includes(settings.parse("sieve-max-includes").unwrap_or(3)),
            sieve_runtime: Runtime::new()
                .with_max_nested_includes(settings.parse("sieve-max-nested-includes").unwrap_or(3))
                .with_cpu_limit(settings.parse("sieve-cpu-limit").unwrap_or(5000))
                .with_max_variable_size(settings.parse("sieve-max-variable-size").unwrap_or(4096))
                .with_max_redirects(settings.parse("sieve-max-redirects").unwrap_or(1))
                .with_max_received_headers(
                    settings.parse("sieve-max-received-headers").unwrap_or(10),
                )
                .with_max_header_size(settings.parse("sieve-max-header-size").unwrap_or(1024))
                .with_max_out_messages(settings.parse("sieve-max-outgoing-messages").unwrap_or(3))
                .with_default_vacation_expiry(
                    settings
                        .parse("sieve-default-vacation-expiry")
                        .unwrap_or(30 * 86400),
                )
                .with_default_duplicate_expiry(
                    settings
                        .parse("sieve-default-duplicate-expiry")
                        .unwrap_or(7 * 86400),
                )
                .without_capabilities(
                    settings
                        .get("sieve-disable-capabilities")
                        .unwrap_or_default()
                        .split_ascii_whitespace()
                        .filter(|c| !c.is_empty()),
                )
                .with_valid_notification_uris(
                    settings
                        .get("sieve-notification-uris")
                        .unwrap_or_else(|| "mailto".to_string())
                        .split_ascii_whitespace()
                        .filter_map(|c| {
                            if !c.is_empty() {
                                c.to_string().into()
                            } else {
                                None
                            }
                        }),
                )
                .with_protected_headers(
                    settings
                        .get("sieve-protected-headers")
                        .unwrap_or_else(|| {
                            "Original-Subject Original-From Received Auto-Submitted".to_string()
                        })
                        .split_ascii_whitespace()
                        .filter_map(|c| {
                            if !c.is_empty() {
                                c.to_string().into()
                            } else {
                                None
                            }
                        }),
                )
                .with_vacation_default_subject(
                    settings
                        .get("sieve-vacation-default-subject")
                        .unwrap_or_else(|| "Automated reply".to_string()),
                )
                .with_vacation_subject_prefix(
                    settings
                        .get("sieve-vacation-subject-prefix")
                        .unwrap_or_else(|| "Auto: ".to_string()),
                )
                .with_env_variable("name", "Stalwart JMAP")
                .with_env_variable("version", env!("CARGO_PKG_VERSION"))
                .with_env_variable("location", "MS")
                .with_env_variable("phase", "during"),
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
