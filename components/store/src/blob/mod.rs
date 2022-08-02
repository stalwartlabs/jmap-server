use std::{convert::TryInto, fmt::Display, ops::Range};

use sha2::{Digest, Sha256};

use crate::{
    config::env_settings::EnvSettings,
    serialize::{StoreDeserialize, StoreSerialize},
    write::mutex_map::MutexMap,
};

use self::{local::LocalBlobStore, s3::S3BlobStore};

pub mod local;
pub mod purge;
pub mod s3;
pub mod store;

pub const BLOB_HASH_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum BlobId {
    Local { hash: [u8; BLOB_HASH_LEN] },
    External { hash: [u8; BLOB_HASH_LEN] },
}

impl BlobId {
    pub fn new_local(bytes: &[u8]) -> Self {
        // Create blob key
        let mut hasher = Sha256::new();
        hasher.update(bytes);

        BlobId::Local {
            hash: hasher.finalize().into(),
        }
    }

    pub fn new_external(bytes: &[u8]) -> Self {
        // Create blob key
        let mut hasher = Sha256::new();
        hasher.update(bytes);

        BlobId::External {
            hash: hasher.finalize().into(),
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, BlobId::Local { .. })
    }

    pub fn is_external(&self) -> bool {
        matches!(self, BlobId::External { .. })
    }

    pub fn hash(&self) -> &[u8] {
        match self {
            BlobId::Local { hash } => hash,
            BlobId::External { hash } => hash,
        }
    }
}

impl Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&base32::encode(
            base32::Alphabet::RFC4648 { padding: false },
            self.hash(),
        ))
    }
}

impl StoreSerialize for BlobId {
    fn serialize(&self) -> Option<Vec<u8>> {
        let mut bytes = Vec::with_capacity(BLOB_HASH_LEN + 1);
        bytes.push(if self.is_local() { 0 } else { 1 });
        bytes.extend_from_slice(self.hash());
        bytes.into()
    }
}

impl StoreDeserialize for BlobId {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        match bytes.get(0)? {
            0 => BlobId::Local {
                hash: bytes.get(1..BLOB_HASH_LEN + 1)?.try_into().ok()?,
            },
            _ => BlobId::External {
                hash: bytes.get(1..BLOB_HASH_LEN + 1)?.try_into().ok()?,
            },
        }
        .into()
    }
}

pub trait BlobStore: Sized {
    fn new(settings: &EnvSettings) -> crate::Result<Self>;
    fn get_range(&self, blob_id: &BlobId, range: Range<u32>) -> crate::Result<Option<Vec<u8>>>;
    fn get(&self, blob_id: &BlobId) -> crate::Result<Option<Vec<u8>>> {
        self.get_range(blob_id, 0..u32::MAX)
    }
    fn put(&self, blob_id: &BlobId, blob: &[u8]) -> crate::Result<bool>;
    fn delete(&self, blob_id: &BlobId) -> crate::Result<bool>;
}

pub struct BlobStoreWrapper {
    pub lock: MutexMap<()>,
    pub store: BlobStoreType,
}

pub enum BlobStoreType {
    Local(LocalBlobStore),
    S3(S3BlobStore),
}

impl BlobStoreWrapper {
    pub fn new(settings: &EnvSettings) -> crate::Result<Self> {
        Ok(BlobStoreWrapper {
            lock: MutexMap::with_capacity(1024),
            store: if !settings.contains_key("s3-config") {
                BlobStoreType::Local(LocalBlobStore::new(settings)?)
            } else {
                BlobStoreType::S3(S3BlobStore::new(settings)?)
            },
        })
    }
}

/*
pub struct UncommittedBlob<'x> {
    pub blob_store: &'x BlobStoreWrapper,
    pub blob_id: BlobId,
    pub did_commit: bool,
}

impl UncommittedBlob<'_> {
    pub fn new(blob_store: &BlobStoreWrapper, blob_id: BlobId) -> UncommittedBlob {
        UncommittedBlob {
            blob_store,
            blob_id,
            did_commit: false,
        }
    }

    pub fn commit(&mut self) {
        self.did_commit = true;
    }
}

impl Drop for UncommittedBlob<'_> {
    fn drop(&mut self) {
        if !self.did_commit {
            if let Err(err) = match self.blob_store {
                BlobStoreWrapper::Local(local) => local.delete(&self.blob_id),
                BlobStoreWrapper::S3(s3) => s3.delete(&self.blob_id),
            } {
                error!("Failed to delete blob {}: {:?}", self.blob_id, err);
            }
        }
    }
}
*/
