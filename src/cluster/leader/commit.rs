use crate::cluster::log::rollback_prepare::RaftStoreRollbackPrepare;
use crate::JMAPServer;
use store::bincode;
use store::core::document::Document;
use store::core::error::StoreError;
use store::log::raft::LogIndex;
use store::serialize::key::{LogKey, LEADER_COMMIT_INDEX_KEY};
use store::serialize::{DeserializeBigEndian, StoreSerialize};
use store::write::batch::WriteBatch;
use store::write::operation::WriteOperation;
use store::{tracing::debug, ColumnFamily, Direction, Store};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn set_leader_commit_index(&self, commit_index: LogIndex) -> store::Result<()> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            store.db.set(
                ColumnFamily::Values,
                LEADER_COMMIT_INDEX_KEY,
                &commit_index.serialize().unwrap(),
            )
        })
        .await
    }

    pub async fn commit_leader(&self, apply_up_to: LogIndex, do_reset: bool) -> store::Result<()> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            let apply_up_to: LogIndex = if apply_up_to != LogIndex::MAX {
                store.db.set(
                    ColumnFamily::Values,
                    LEADER_COMMIT_INDEX_KEY,
                    &apply_up_to.serialize().unwrap(),
                )?;
                apply_up_to
            } else if let Some(apply_up_to) = store
                .db
                .get(ColumnFamily::Values, LEADER_COMMIT_INDEX_KEY)?
            {
                apply_up_to
            } else {
                return Ok(());
            };

            debug!(
                "Applying pending leader changes up to index {}.",
                apply_up_to
            );

            let mut log_batch = Vec::new();
            for (key, value) in store.db.iterator(
                ColumnFamily::Logs,
                &[LogKey::TOMBSTONE_KEY_PREFIX],
                Direction::Forward,
            )? {
                if !key.starts_with(&[LogKey::TOMBSTONE_KEY_PREFIX]) {
                    break;
                }
                let index = (&key[..])
                    .deserialize_be_u64(LogKey::TOMBSTONE_INDEX_POS)
                    .ok_or_else(|| {
                        StoreError::InternalError(format!(
                            "Failed to deserialize index from tombstone key: [{:?}]",
                            key
                        ))
                    })?;

                if apply_up_to != LogIndex::MAX && index <= apply_up_to {
                    let mut write_batch = WriteBatch::new(
                        (&key[..])
                            .deserialize_be_u32(LogKey::TOMBSTONE_ACCOUNT_POS)
                            .ok_or_else(|| {
                                StoreError::InternalError(format!(
                                    "Failed to deserialize account id from tombstone key: [{:?}]",
                                    key
                                ))
                            })?,
                    );

                    for document in bincode::deserialize::<Vec<Document>>(&value).map_err(|_| {
                        StoreError::SerializeError("Failed to deserialize tombstones".to_string())
                    })? {
                        /*println!(
                            "Committing delete document {} from account {}, {:?}",
                            document.document_id, write_batch.account_id, document.collection
                        );*/
                        write_batch.delete_document(document);
                    }

                    if !write_batch.is_empty() {
                        store.commit_write(write_batch)?;
                    }

                    log_batch.push(WriteOperation::Delete {
                        cf: ColumnFamily::Logs,
                        key: key.to_vec(),
                    });
                } else if do_reset {
                    //println!("Deleting uncommitted leader update: {}", index);
                    log_batch.push(WriteOperation::Delete {
                        cf: ColumnFamily::Logs,
                        key: key.to_vec(),
                    });
                } else {
                    break;
                }
            }

            if !log_batch.is_empty() {
                store.db.write(log_batch)?;
            }

            if do_reset {
                store.prepare_rollback_changes(apply_up_to, false)?;
                store
                    .db
                    .delete(ColumnFamily::Values, LEADER_COMMIT_INDEX_KEY)?;
            }

            Ok(())
        })
        .await
    }
}
