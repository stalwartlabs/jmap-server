use super::changes_merge::MergedChanges;
use crate::JMAPServer;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::log::raft::{LogIndex, RaftId, TermId};
use store::serialize::key::{LogKey, LEADER_COMMIT_INDEX_KEY};
use store::serialize::DeserializeBigEndian;
use store::{tracing::debug, AccountId, ColumnFamily, Direction, JMAPStore, Store};

pub trait RaftStoreRollbackGet {
    fn next_rollback_change(&self)
        -> store::Result<Option<(AccountId, Collection, MergedChanges)>>;
    fn has_pending_rollback(&self) -> store::Result<bool>;
}

impl<T> RaftStoreRollbackGet for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn next_rollback_change(
        &self,
    ) -> store::Result<Option<(AccountId, Collection, MergedChanges)>> {
        Ok(
            if let Some((key, value)) = self
                .db
                .iterator(
                    ColumnFamily::Logs,
                    &[LogKey::ROLLBACK_KEY_PREFIX],
                    Direction::Forward,
                )?
                .next()
            {
                if key.starts_with(&[LogKey::ROLLBACK_KEY_PREFIX]) {
                    Some((
                        (&key[..])
                            .deserialize_be_u32(LogKey::ACCOUNT_POS)
                            .ok_or_else(|| {
                                StoreError::InternalError(format!(
                                    "Failed to deserialize account id from changelog key: [{:?}]",
                                    key
                                ))
                            })?,
                        (*key.get(LogKey::COLLECTION_POS).ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Failed to deserialize collection from changelog key: [{:?}]",
                                key
                            ))
                        })?)
                        .into(),
                        MergedChanges::from_bytes(&value).ok_or_else(|| {
                            StoreError::InternalError(format!(
                                "Failed to deserialize rollback change: [{:?}]",
                                key
                            ))
                        })?,
                    ))
                } else {
                    None
                }
            } else {
                None
            },
        )
    }

    fn has_pending_rollback(&self) -> store::Result<bool> {
        if self
            .db
            .iterator(
                ColumnFamily::Logs,
                &[LogKey::ROLLBACK_KEY_PREFIX],
                Direction::Forward,
            )?
            .next()
            .is_some()
        {
            debug!("This node has pending a rollback and won't start a new election.");
            return Ok(true);
        } else if let Some(commit_index) = self
            .db
            .get::<LogIndex>(ColumnFamily::Values, LEADER_COMMIT_INDEX_KEY)?
        {
            let last_log = self
                .get_prev_raft_id(RaftId::new(TermId::MAX, LogIndex::MAX))?
                .unwrap_or_else(RaftId::none);
            if last_log.index != commit_index {
                debug!(
                    concat!(
                        "This node has uncommitted changes ({} != {}) ",
                        "requiring rollback and won't start a new election."
                    ),
                    last_log.index, commit_index
                );
                return Ok(true);
            }
        }

        Ok(false)
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn next_rollback_change(
        &self,
    ) -> store::Result<Option<(AccountId, Collection, MergedChanges)>> {
        let store = self.store.clone();
        self.spawn_worker(move || store.next_rollback_change())
            .await
    }

    pub async fn has_pending_rollback(&self) -> store::Result<bool> {
        let store = self.store.clone();
        self.spawn_worker(move || store.has_pending_rollback())
            .await
    }
}
