use std::sync::MutexGuard;

use nlp::Language;
use store::{mutex_map::MutexMap, AccountId, CollectionId, Store, StoreError};

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

    pub fn lock_account(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> store::Result<MutexGuard<()>> {
        self.account_lock
            .lock(((collection as u64) << (8 * std::mem::size_of::<AccountId>())) | account as u64)
            .map_err(|_| StoreError::InternalError("Failed to obtain mutex".to_string()))
    }
}
