use nlp::Language;
use store::{mutex_map::MutexMap, parking_lot::MutexGuard, AccountId, CollectionId, Store};

use crate::{JMAPMailConfig, JMAPStoreConfig};

pub struct JMAPLocalStore<T> {
    pub store: T,
    pub account_lock: MutexMap<()>,
    pub mail_config: JMAPMailConfig,
    pub default_language: Language,
}

impl<'x, T> JMAPLocalStore<T>
where
    T: Store<'x>,
{
    pub fn open(store: T, config: JMAPStoreConfig) -> store::Result<Self> {
        Ok(Self {
            store,
            account_lock: MutexMap::with_capacity(1024),
            mail_config: config.jmap_mail_options,
            default_language: config.default_language,
        })
    }

    pub fn lock_account(&self, account: AccountId, collection: CollectionId) -> MutexGuard<()> {
        self.account_lock.lock_hash((account, collection))
    }
}
