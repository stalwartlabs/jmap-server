use store::{AccountId, BlobEntry, Store};

use crate::{id::BlobId, JMAP_BLOB};

pub trait JMAPLocalBlobStore<'x>: Store<'x> {
    fn upload_blob(&self, account: AccountId, bytes: &[u8]) -> store::Result<BlobId> {
        // Insert temporary blob
        let (timestamp, id) = self.store_temporary_blob(account, bytes)?;

        Ok(BlobId::new(account, JMAP_BLOB, id, timestamp as usize))
    }

    fn download_blob(&self, account: AccountId, blob: BlobId) -> store::Result<Option<Vec<u8>>> {
        Ok(if blob.collection != JMAP_BLOB {
            //TODO check ACL
            self.get_blob(
                blob.account,
                blob.collection,
                blob.document,
                BlobEntry::new(blob.blob_index),
            )?
            .map(|entry| entry.value)
        } else {
            self.get_temporary_blob(account, blob.document, blob.blob_index as u64)?
        })
    }
}
