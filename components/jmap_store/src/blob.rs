use store::{AccountId, BlobEntry, Store};

use crate::{id::BlobId, local_store::JMAPLocalStore};

pub type InnerBlobFnc = fn(&[u8], usize) -> Option<Vec<u8>>;

pub trait JMAPLocalBlobStore<'x> {
    fn upload_blob(&self, account: AccountId, bytes: &[u8]) -> store::Result<BlobId>;
    fn download_blob(
        &self,
        account: AccountId,
        blob_id: &BlobId,
        blob_fnc: InnerBlobFnc,
    ) -> store::Result<Option<Vec<u8>>>;
}

impl<'x, T> JMAPLocalBlobStore<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn upload_blob(&self, account: AccountId, bytes: &[u8]) -> store::Result<BlobId> {
        // Insert temporary blob
        let (timestamp, hash) = self.store.store_temporary_blob(account, bytes)?;

        Ok(BlobId::new_temporary(account, timestamp, hash))
    }

    fn download_blob(
        &self,
        account: AccountId,
        blob_id: &BlobId,
        blob_fnc: InnerBlobFnc, //TODO use something nicer than a function pointer
    ) -> store::Result<Option<Vec<u8>>> {
        Ok(match blob_id {
            BlobId::Owned(blob_id) => {
                //TODO check ACL
                self.store
                    .get_blob(
                        blob_id.account,
                        blob_id.collection,
                        blob_id.document,
                        BlobEntry::new(blob_id.blob_index),
                    )?
                    .map(|entry| entry.value)
            }
            BlobId::Temporary(blob_id) => {
                self.store
                    .get_temporary_blob(account, blob_id.hash, blob_id.timestamp)?
            }
            BlobId::InnerOwned(blob_id) => self
                .store
                .get_blob(
                    blob_id.blob_id.account,
                    blob_id.blob_id.collection,
                    blob_id.blob_id.document,
                    BlobEntry::new(blob_id.blob_id.blob_index),
                )?
                .map(|entry| entry.value)
                .and_then(|v| blob_fnc(&v, blob_id.blob_index)),
            BlobId::InnerTemporary(blob_id) => self
                .store
                .get_temporary_blob(account, blob_id.blob_id.hash, blob_id.blob_id.timestamp)?
                .and_then(|v| blob_fnc(&v, blob_id.blob_index)),
        })
    }
}
