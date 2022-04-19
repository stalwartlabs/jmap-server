use store::{blob::BlobIndex, AccountId, JMAPStore, Store};

use crate::id::blob::BlobId;

pub type InnerBlobFnc = fn(&[u8], BlobIndex) -> Option<Vec<u8>>;

pub trait JMAPBlobStore {
    fn upload_blob(&self, account: AccountId, bytes: &[u8]) -> store::Result<BlobId>;
    fn download_blob(
        &self,
        account: AccountId,
        blob_id: &BlobId,
        blob_fnc: InnerBlobFnc,
    ) -> store::Result<Option<Vec<u8>>>;
}

impl<T> JMAPBlobStore for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn upload_blob(&self, account: AccountId, bytes: &[u8]) -> store::Result<BlobId> {
        // Insert temporary blob
        let (timestamp, hash) = self.store_temporary_blob(account, bytes)?;

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
                self.get_blob(
                    blob_id.account_id,
                    blob_id.collection,
                    blob_id.document,
                    blob_id.blob_index,
                )?
            }
            BlobId::Temporary(blob_id) => {
                self.get_temporary_blob(account, blob_id.hash, blob_id.timestamp)?
            }
            BlobId::InnerOwned(blob_id) => self
                .get_blob(
                    blob_id.blob_id.account_id,
                    blob_id.blob_id.collection,
                    blob_id.blob_id.document,
                    blob_id.blob_id.blob_index,
                )?
                .and_then(|v| blob_fnc(&v, blob_id.blob_index)),
            BlobId::InnerTemporary(blob_id) => self
                .get_temporary_blob(account, blob_id.blob_id.hash, blob_id.blob_id.timestamp)?
                .and_then(|v| blob_fnc(&v, blob_id.blob_index)),
        })
    }
}
