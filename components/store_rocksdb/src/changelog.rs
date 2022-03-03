use rocksdb::{Direction, IteratorMode};
use store::{
    changelog::{ChangeLog, ChangeLogId, ChangeLogQuery, RaftId},
    serialize::{
        serialize_changelog_key, serialize_raftlog_key, DeserializeBigEndian,
        COLLECTION_PREFIX_LEN, INTERNAL_KEY_PREFIX,
    },
    AccountId, CollectionId, StoreChangeLog, StoreError,
};

use crate::RocksDBStore;

impl StoreChangeLog for RocksDBStore {
    fn get_last_raft_id(&self) -> store::Result<Option<RaftId>> {
        let key = serialize_raftlog_key(ChangeLogId::MAX, ChangeLogId::MAX);
        let key_len = key.len();

        if let Some((key, _)) = self
            .db
            .iterator_cf(
                &self.get_handle("log")?,
                IteratorMode::From(&key, Direction::Reverse),
            )
            .into_iter()
            .next()
        {
            if key.len() == key_len && key[0] == INTERNAL_KEY_PREFIX {
                let term = key.as_ref().deserialize_be_u64(1).ok_or_else(|| {
                    StoreError::InternalError(format!("Corrupted raft key for [{:?}]", key))
                })?;
                let index = key
                    .as_ref()
                    .deserialize_be_u64(1 + std::mem::size_of::<ChangeLogId>())
                    .ok_or_else(|| {
                        StoreError::InternalError(format!("Corrupted raft key for [{:?}]", key))
                    })?;

                return Ok(Some(RaftId { term, index }));
            }
        }
        Ok(None)
    }

    fn get_last_change_id(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> store::Result<Option<ChangeLogId>> {
        let key = serialize_changelog_key(account, collection, ChangeLogId::MAX);
        let key_len = key.len();

        if let Some((key, _)) = self
            .db
            .iterator_cf(
                &self.get_handle("log")?,
                IteratorMode::From(&key, Direction::Reverse),
            )
            .into_iter()
            .next()
        {
            if key.starts_with(&key[0..COLLECTION_PREFIX_LEN]) && key.len() == key_len {
                return Ok(Some(
                    key.as_ref()
                        .deserialize_be_u64(COLLECTION_PREFIX_LEN)
                        .ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Corrupted changelog key for [{}/{}]: [{:?}]",
                                account, collection, key
                            ))
                        })?,
                ));
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
        let key_len = key.len();
        let prefix = &key[0..COLLECTION_PREFIX_LEN];
        let mut is_first = true;

        for (key, value) in self.db.iterator_cf(
            &self.get_handle("log")?,
            IteratorMode::From(&key, Direction::Forward),
        ) {
            if !key.starts_with(prefix) {
                break;
            } else if key.len() != key_len {
                //TODO avoid collisions with Raft keys
                continue;
            }
            let change_id = key
                .as_ref()
                .deserialize_be_u64(COLLECTION_PREFIX_LEN)
                .ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Failed to deserialize changelog key for [{}/{}]: [{:?}]",
                        account, collection, key
                    ))
                })?;

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
                changelog.deserialize(&value).ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Failed to deserialize changelog for [{}/{}]: [{:?}]",
                        account, collection, query
                    ))
                })?;
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
