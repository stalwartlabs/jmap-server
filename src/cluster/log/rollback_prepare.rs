use super::changes_merge::MergedChanges;
use crate::JMAPServer;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::log::raft::LogIndex;
use store::serialize::key::LogKey;
use store::serialize::DeserializeBigEndian;
use store::write::operation::WriteOperation;
use store::{AccountId, ColumnFamily, Direction, JMAPStore, Store};

pub trait RaftStoreRollbackPrepare {
    fn prepare_rollback_changes(
        &self,
        after_index: LogIndex,
        restore_deletions: bool,
    ) -> store::Result<()>;
}

impl<T> RaftStoreRollbackPrepare for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn prepare_rollback_changes(
        &self,
        after_index: LogIndex,
        restore_deletions: bool,
    ) -> store::Result<()> {
        let mut current_account_id = AccountId::MAX;
        let mut current_collection = Collection::None;
        let mut changes = MergedChanges::new();
        let mut write_batch = Vec::new();

        for (key, value) in self.db.iterator(
            ColumnFamily::Logs,
            &[LogKey::CHANGE_KEY_PREFIX],
            Direction::Forward,
        )? {
            if !key.starts_with(&[LogKey::CHANGE_KEY_PREFIX]) {
                break;
            }

            if after_index != LogIndex::MAX {
                let change_id = LogKey::deserialize_change_id(&key).ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Failed to deserialize changelog key : [{:?}]",
                        key
                    ))
                })?;

                if change_id <= after_index {
                    continue;
                }
            }

            let account_id = (&key[..])
                .deserialize_be_u32(LogKey::ACCOUNT_POS)
                .ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Failed to deserialize account id from changelog key: [{:?}]",
                        key
                    ))
                })?;
            let collection: Collection = (*key.get(LogKey::COLLECTION_POS).ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to deserialize collection from changelog key: [{:?}]",
                    key
                ))
            })?)
            .into();

            if account_id != current_account_id || collection != current_collection {
                if !write_batch.is_empty() {
                    if !restore_deletions && !changes.deletes.is_empty() {
                        changes.deletes.clear();
                    }
                    if !changes.is_empty() {
                        write_batch.push(WriteOperation::set(
                            ColumnFamily::Logs,
                            LogKey::serialize_rollback(current_account_id, current_collection),
                            changes.serialize().ok_or_else(|| {
                                StoreError::InternalError(format!(
                                    "Failed to serialized merged changes for [{}/{:?}]",
                                    account_id, collection
                                ))
                            })?,
                        ));
                    }
                    self.db.write(write_batch)?;
                    write_batch = Vec::new();
                }
                changes = MergedChanges::new();
                current_account_id = account_id;
                current_collection = collection;
            }

            changes.deserialize_changes(&value).ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to deserialize raft changes for [{}/{:?}]",
                    account_id, collection
                ))
            })?;

            write_batch.push(WriteOperation::delete(ColumnFamily::Logs, key.to_vec()));
        }

        if !write_batch.is_empty() {
            if !restore_deletions && !changes.deletes.is_empty() {
                changes.deletes.clear();
            }
            if !changes.is_empty() {
                write_batch.push(WriteOperation::set(
                    ColumnFamily::Logs,
                    LogKey::serialize_rollback(current_account_id, current_collection),
                    changes.serialize().ok_or_else(|| {
                        StoreError::InternalError(format!(
                            "Failed to serialized merged changes for [{}/{:?}]",
                            current_account_id, current_collection
                        ))
                    })?,
                ));
            }
            self.db.write(write_batch)?;
            write_batch = Vec::new();
        }

        for (key, _) in self.db.iterator(
            ColumnFamily::Logs,
            &[LogKey::RAFT_KEY_PREFIX],
            Direction::Forward,
        )? {
            if key.starts_with(&[LogKey::RAFT_KEY_PREFIX]) {
                if after_index == LogIndex::MAX
                    || LogKey::deserialize_raft(&key)
                        .ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Corrupted raft entry for [{:?}]",
                                key
                            ))
                        })?
                        .index
                        > after_index
                {
                    write_batch.push(WriteOperation::delete(ColumnFamily::Logs, key.to_vec()));
                }
            } else {
                break;
            }
        }

        if !write_batch.is_empty() {
            self.db.write(write_batch)?;
        }

        Ok(())
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn prepare_rollback_changes(
        &self,
        after_index: LogIndex,
        restore_deletions: bool,
    ) -> store::Result<()> {
        let store = self.store.clone();
        self.spawn_worker(move || store.prepare_rollback_changes(after_index, restore_deletions))
            .await
    }
}
