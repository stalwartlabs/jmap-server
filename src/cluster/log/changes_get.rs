use super::Update;
use store::core::bitmap::Bitmap;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::log::changes::ChangeId;
use store::serialize::key::LogKey;
use store::{AccountId, ColumnFamily, JMAPStore, Store};

pub trait RaftStoreGet {
    fn get_log_changes(
        &self,
        entries: &mut Vec<Update>,
        account_id: AccountId,
        changed_collections: Bitmap<Collection>,
        change_id: ChangeId,
    ) -> store::Result<usize>;
}

impl<T> RaftStoreGet for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_log_changes(
        &self,
        entries: &mut Vec<Update>,
        account_id: AccountId,
        changed_collections: Bitmap<Collection>,
        change_id: ChangeId,
    ) -> store::Result<usize> {
        let mut entries_size = 0;
        for changed_collection in changed_collections {
            let change = self
                .db
                .get::<Vec<u8>>(
                    ColumnFamily::Logs,
                    &LogKey::serialize_change(account_id, changed_collection, change_id),
                )?
                .ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Missing change for change {}/{:?}/{}",
                        account_id, changed_collection, change_id
                    ))
                })?;
            entries_size += change.len() + std::mem::size_of::<AccountId>() + 1;
            entries.push(Update::Begin {
                account_id,
                collection: changed_collection,
            });
            entries.push(Update::Change { change });
        }
        Ok(entries_size)
    }
}
