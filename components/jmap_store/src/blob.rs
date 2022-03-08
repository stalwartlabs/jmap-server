use async_trait::async_trait;
use store::{AccountId, JMAPStore, Store};

use crate::id::BlobId;

pub type InnerBlobFnc = fn(&[u8], usize) -> Option<Vec<u8>>;

#[async_trait]
pub trait JMAPBlobStore {
    async fn upload_blob(&self, account: AccountId, bytes: &[u8]) -> store::Result<BlobId>;
    async fn download_blob(
        &self,
        account: AccountId,
        blob_id: &BlobId,
        blob_fnc: InnerBlobFnc,
    ) -> store::Result<Option<Vec<u8>>>;
}

#[async_trait]
impl<T> JMAPBlobStore for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    async fn upload_blob(&self, account: AccountId, bytes: &[u8]) -> store::Result<BlobId> {
        // Insert temporary blob
        let (timestamp, hash) = self.store_temporary_blob(account, bytes).await?;

        Ok(BlobId::new_temporary(account, timestamp, hash))
    }

    async fn download_blob(
        &self,
        account: AccountId,
        blob_id: &BlobId,
        blob_fnc: InnerBlobFnc, //TODO use something nicer than a function pointer
    ) -> store::Result<Option<Vec<u8>>> {
        Ok(match blob_id {
            BlobId::Owned(blob_id) => {
                //TODO check ACL
                self.get_blob(
                    blob_id.account,
                    blob_id.collection,
                    blob_id.document,
                    blob_id.blob_index,
                    0..u32::MAX,
                )
                .await?
                .map(|entry| entry.1)
            }
            BlobId::Temporary(blob_id) => {
                self.get_temporary_blob(account, blob_id.hash, blob_id.timestamp)
                    .await?
            }
            BlobId::InnerOwned(blob_id) => self
                .get_blob(
                    blob_id.blob_id.account,
                    blob_id.blob_id.collection,
                    blob_id.blob_id.document,
                    blob_id.blob_id.blob_index,
                    0..u32::MAX,
                )
                .await?
                .map(|entry| entry.1)
                .and_then(|v| blob_fnc(&v, blob_id.blob_index)),
            BlobId::InnerTemporary(blob_id) => self
                .get_temporary_blob(account, blob_id.blob_id.hash, blob_id.blob_id.timestamp)
                .await?
                .and_then(|v| blob_fnc(&v, blob_id.blob_index)),
        })
    }
}
