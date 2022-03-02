use std::{array::TryFromSliceError, convert::TryInto};

use rocksdb::{Direction, IteratorMode};
use store::{
    changelog::{ChangeLog, ChangeLogId, ChangeLogQuery},
    serialize::{serialize_changelog_key, COLLECTION_PREFIX_LEN},
    AccountId, CollectionId, StoreChangeLog, StoreError,
};

use crate::RocksDBStore;

impl StoreChangeLog for RocksDBStore {
    fn get_last_change_id(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> store::Result<Option<ChangeLogId>> {
        let key = serialize_changelog_key(account, collection, ChangeLogId::MAX);

        if let Some((key, _)) = self
            .db
            .iterator_cf(
                &self.get_handle("log")?,
                IteratorMode::From(&key, Direction::Reverse),
            )
            .into_iter()
            .next()
        {
            if key.starts_with(&key[0..COLLECTION_PREFIX_LEN]) {
                return Ok(Some(ChangeLogId::from_be_bytes(
                    key.get(COLLECTION_PREFIX_LEN..)
                        .ok_or_else(|| {
                            StoreError::InternalError(format!("Failed to changelog key: {:?}", key))
                        })?
                        .try_into()
                        .map_err(|e: TryFromSliceError| StoreError::InternalError(e.to_string()))?,
                )));
            }
        }
        Ok(None)
    }

    fn get_changes(
        &self,
        account: AccountId,
        collection: CollectionId,
        query: ChangeLogQuery,
    ) -> store::Result<Option<ChangeLog>> {
        let mut changelog = ChangeLog::default();
        /*let (is_inclusive, mut match_from_change_id, from_change_id, to_change_id) = match query {
            ChangeLogQuery::All => (true, false, 0, 0),
            ChangeLogQuery::Since(change_id) => (false, true, change_id, 0),
            ChangeLogQuery::SinceInclusive(change_id) => (true, true, change_id, 0),
            ChangeLogQuery::RangeInclusive(from_change_id, to_change_id) => {
                (true, true, from_change_id, to_change_id)
            }
        };*/
        let (is_inclusive, from_change_id, to_change_id) = match query {
            ChangeLogQuery::All => (true, 0, 0),
            ChangeLogQuery::Since(change_id) => (false, change_id, 0),
            ChangeLogQuery::SinceInclusive(change_id) => (true, change_id, 0),
            ChangeLogQuery::RangeInclusive(from_change_id, to_change_id) => {
                (true, from_change_id, to_change_id)
            }
        };
        let key = serialize_changelog_key(account, collection, from_change_id);
        let prefix = &key[0..COLLECTION_PREFIX_LEN];
        let mut is_first = true;

        for (key, value) in self.db.iterator_cf(
            &self.get_handle("log")?,
            IteratorMode::From(&key, Direction::Forward),
        ) {
            if !key.starts_with(prefix) {
                break;
            }
            let change_id = ChangeLogId::from_be_bytes(
                key.get(COLLECTION_PREFIX_LEN..)
                    .ok_or_else(|| {
                        StoreError::InternalError(format!("Failed to changelog key: {:?}", key))
                    })?
                    .try_into()
                    .map_err(|e: TryFromSliceError| StoreError::InternalError(e.to_string()))?,
            );

            /*if match_from_change_id {
                if change_id != from_change_id {
                    return Ok(None);
                } else {
                    match_from_change_id = false;
                }
            }*/

            if change_id > from_change_id || (is_inclusive && change_id == from_change_id) {
                if to_change_id > 0 && change_id > to_change_id {
                    break;
                }
                if is_first {
                    changelog.from_change_id = change_id;
                    is_first = false;
                }
                changelog.to_change_id = change_id;
                changelog.deserialize(value.as_ref())?;
            }
        }

        if is_first {
            changelog.from_change_id = from_change_id;
            changelog.to_change_id = if to_change_id > 0 {
                to_change_id
            } else {
                from_change_id
            };
        }

        Ok(Some(changelog))
    }
}
