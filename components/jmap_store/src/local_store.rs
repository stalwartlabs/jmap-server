use std::sync::atomic::{AtomicU64, Ordering};

use nlp::Language;
use store::{
    changelog::RaftId, mutex_map::MutexMap, parking_lot::MutexGuard, AccountId, CollectionId, Store,
};

use crate::{JMAPMailConfig, JMAPStoreConfig};

pub struct JMAPLocalStore<T> {
    pub store: T,
    pub raft_log_term: AtomicU64,
    pub raft_log_index: AtomicU64,
    pub account_lock: MutexMap<()>,
    pub mail_config: JMAPMailConfig,
    pub default_language: Language,
}

impl<'x, T> JMAPLocalStore<T>
where
    T: Store<'x>,
{
    pub fn open(store: T, config: JMAPStoreConfig) -> store::Result<Self> {
        let raft_id = store
            .get_last_raft_id()?
            .map(|mut id| {
                id.index += 1;
                id
            })
            .unwrap_or(RaftId { term: 0, index: 0 });

        Ok(Self {
            store,
            account_lock: MutexMap::with_capacity(1024),
            mail_config: config.jmap_mail_options,
            default_language: config.default_language,
            raft_log_index: raft_id.index.into(),
            raft_log_term: raft_id.term.into(),
        })
    }

    pub fn lock_account(&self, account: AccountId, collection: CollectionId) -> MutexGuard<()> {
        self.account_lock.lock_hash((account, collection))
    }

    pub fn next_raft_id(&self) -> RaftId {
        RaftId {
            term: self.raft_log_term.load(Ordering::Relaxed),
            index: self.raft_log_index.fetch_add(1, Ordering::Relaxed),
        }
    }
}
