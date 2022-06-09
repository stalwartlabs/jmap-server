pub mod blob;
pub mod config;
pub mod core;
pub mod log;
pub mod nlp;
pub mod read;
pub mod serialize;
pub mod write;

use crate::core::{collection::Collection, error::StoreError};
use crate::nlp::Language;
use blob::BlobStoreWrapper;
use config::{env_settings::EnvSettings, jmap::JMAPConfig};
use log::raft::{LogIndex, RaftId};
use moka::sync::Cache;
use parking_lot::{Mutex, MutexGuard};
use serialize::StoreDeserialize;
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

pub use bincode;
pub use chrono;
pub use lz4_flex;
pub use moka;
pub use parking_lot;
pub use rand;
pub use roaring;
pub use sha2;
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
}

pub struct JMAPStore<T> {
    pub db: T,
    pub blob: BlobStoreWrapper,
    pub config: JMAPConfig,

    pub account_lock: MutexMap<()>,

    pub doc_id_cache: Cache<IdCacheKey, Arc<Mutex<IdAssigner>>>,

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
            blob: BlobStoreWrapper::new(settings).unwrap(),
            doc_id_cache: Cache::builder()
                .initial_capacity(128)
                .max_capacity(settings.parse("id-cache-size").unwrap_or(32 * 1024 * 1024))
                .time_to_idle(Duration::from_secs(60 * 60))
                .build(),
            account_lock: MutexMap::with_capacity(1024),
            raft_index: 0.into(),
            raft_term: 0.into(),
            tombstone_deletions: false.into(),
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

    pub fn lock_account(&self, account: AccountId, collection: Collection) -> MutexGuard<'_, ()> {
        self.account_lock.lock_hash((account, collection))
    }
}
