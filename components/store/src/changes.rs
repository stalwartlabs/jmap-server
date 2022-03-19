use crate::leb128::Leb128;

use crate::serialize::{LogKey, COLLECTION_PREFIX_LEN};
use crate::{AccountId, Collection, ColumnFamily, Direction, JMAPId, JMAPStore, Store, StoreError};

pub type ChangeId = u64;

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Change {
    Insert(JMAPId),
    Update(JMAPId),
    ChildUpdate(JMAPId),
    Delete(JMAPId),
}

pub struct Changes {
    pub changes: Vec<Change>,
    pub from_change_id: ChangeId,
    pub to_change_id: ChangeId,
}

#[derive(Debug)]
pub enum Query {
    All,
    Since(ChangeId),
    SinceInclusive(ChangeId),
    RangeInclusive(ChangeId, ChangeId),
}

impl Default for Changes {
    fn default() -> Self {
        Self {
            changes: Vec::with_capacity(10),
            from_change_id: 0,
            to_change_id: 0,
        }
    }
}

impl Changes {
    pub fn deserialize(&mut self, bytes: &[u8]) -> Option<()> {
        let mut bytes_it = bytes.iter();
        let total_inserts = usize::from_leb128_it(&mut bytes_it)?;
        let total_updates = usize::from_leb128_it(&mut bytes_it)?;
        let total_child_updates = usize::from_leb128_it(&mut bytes_it)?;
        let total_deletes = usize::from_leb128_it(&mut bytes_it)?;

        if total_inserts > 0 {
            for _ in 0..total_inserts {
                self.changes
                    .push(Change::Insert(JMAPId::from_leb128_it(&mut bytes_it)?));
            }
        }

        if total_updates > 0 || total_child_updates > 0 {
            'update_outer: for change_pos in 0..(total_updates + total_child_updates) {
                let id = JMAPId::from_leb128_it(&mut bytes_it)?;
                let mut is_child_update = change_pos >= total_updates;

                for (idx, change) in self.changes.iter().enumerate() {
                    match change {
                        Change::Insert(insert_id) if *insert_id == id => {
                            // Item updated after inserted, no need to count this change.
                            continue 'update_outer;
                        }
                        Change::Update(update_id) if *update_id == id => {
                            // Move update to the front
                            is_child_update = false;
                            self.changes.remove(idx);
                            break;
                        }
                        Change::ChildUpdate(update_id) if *update_id == id => {
                            // Move update to the front
                            self.changes.remove(idx);
                            break;
                        }
                        _ => (),
                    }
                }

                self.changes.push(if !is_child_update {
                    Change::Update(id)
                } else {
                    Change::ChildUpdate(id)
                });
            }
        }

        if total_deletes > 0 {
            'delete_outer: for _ in 0..total_deletes {
                let id = JMAPId::from_leb128_it(&mut bytes_it)?;

                'delete_inner: for (idx, change) in self.changes.iter().enumerate() {
                    match change {
                        Change::Insert(insert_id) if *insert_id == id => {
                            self.changes.remove(idx);
                            continue 'delete_outer;
                        }
                        Change::Update(update_id) | Change::ChildUpdate(update_id)
                            if *update_id == id =>
                        {
                            self.changes.remove(idx);
                            break 'delete_inner;
                        }
                        _ => (),
                    }
                }

                self.changes.push(Change::Delete(id));
            }
        }

        Some(())
    }
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn get_last_change_id(
        &self,
        account: AccountId,
        collection: Collection,
    ) -> crate::Result<Option<ChangeId>> {
        let key = LogKey::serialize_change(account, collection, ChangeId::MAX);
        let key_len = key.len();

        if let Some((key, _)) = self
            .db
            .iterator(ColumnFamily::Logs, &key, Direction::Backward)?
            .into_iter()
            .next()
        {
            if key.starts_with(&key[0..COLLECTION_PREFIX_LEN]) && key.len() == key_len {
                return Ok(Some(LogKey::deserialize_change_id(&key).ok_or_else(
                    || {
                        StoreError::InternalError(format!(
                            "Failed to deserialize changelog key for [{}/{:?}]: [{:?}]",
                            account, collection, key
                        ))
                    },
                )?));
            }
        }
        Ok(None)
    }

    pub fn get_changes(
        &self,
        account: AccountId,
        collection: Collection,
        query: Query,
    ) -> crate::Result<Option<Changes>> {
        let mut changelog = Changes::default();
        let (is_inclusive, from_change_id, to_change_id) = match query {
            Query::All => (true, 0, 0),
            Query::Since(change_id) => (false, change_id, 0),
            Query::SinceInclusive(change_id) => (true, change_id, 0),
            Query::RangeInclusive(from_change_id, to_change_id) => {
                (true, from_change_id, to_change_id)
            }
        };
        let key = LogKey::serialize_change(account, collection, from_change_id);
        let key_len = key.len();
        let prefix = &key[0..COLLECTION_PREFIX_LEN];
        let mut is_first = true;

        for (key, value) in self
            .db
            .iterator(ColumnFamily::Logs, &key, Direction::Forward)?
        {
            if !key.starts_with(prefix) {
                break;
            } else if key.len() != key_len {
                //TODO avoid collisions with Raft keys
                continue;
            }
            let change_id = LogKey::deserialize_change_id(&key).ok_or_else(|| {
                StoreError::InternalError(format!(
                    "Failed to deserialize changelog key for [{}/{:?}]: [{:?}]",
                    account, collection, key
                ))
            })?;

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
                        "Failed to deserialize changelog for [{}/{:?}]: [{:?}]",
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
