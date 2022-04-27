use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    thread,
    time::SystemTime,
};

use store::{blob::BlobId, AccountId, JMAPStore, Store};

use crate::id::blob::JMAPBlob;

pub type InnerBlobFnc = fn(&[u8], u32) -> Option<Vec<u8>>;

pub trait JMAPBlobStore {
    fn upload_blob(&self, account: AccountId, bytes: &[u8]) -> store::Result<JMAPBlob>;
    fn download_blob(
        &self,
        account: AccountId,
        blob: &JMAPBlob,
        blob_fnc: InnerBlobFnc,
    ) -> store::Result<Option<Vec<u8>>>;
}

impl<T> JMAPBlobStore for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn upload_blob(&self, account_id: AccountId, bytes: &[u8]) -> store::Result<JMAPBlob> {
        let blob_id = self.blob_store(bytes)?;
        self.blob_link_ephimeral(&blob_id, account_id)?;
        Ok(JMAPBlob::new(blob_id))
    }

    fn download_blob(
        &self,
        account_id: AccountId,
        blob: &JMAPBlob,        //TODO check ACL
        blob_fnc: InnerBlobFnc, //TODO use something nicer than a function pointer
    ) -> store::Result<Option<Vec<u8>>> {
        if !self.blob_has_access(&blob.id, account_id)? {
            return Ok(None);
        }
        let bytes = self.blob_get(&blob.id)?;
        Ok(
            if let (Some(bytes), Some(inner_id)) = (&bytes, blob.inner_id) {
                blob_fnc(bytes, inner_id)
            } else {
                bytes
            },
        )
    }
}
