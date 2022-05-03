use std::{convert::TryInto, fmt::Display, ops::Range};

use sha2::{Digest, Sha256};

use crate::{
    config::env_settings::EnvSettings,
    serialize::{leb128::Leb128, StoreDeserialize, StoreSerialize},
    write::mutex_map::MutexMap,
};

use self::{local::LocalBlobStore, s3::S3BlobStore};

pub mod local;
pub mod purge;
pub mod s3;
pub mod store;

pub const BLOB_HASH_LEN: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct BlobId {
    pub hash: [u8; BLOB_HASH_LEN],
    pub size: u32,
}

impl From<&[u8]> for BlobId {
    fn from(bytes: &[u8]) -> Self {
        // Create blob key
        let mut hasher = Sha256::new();
        hasher.update(bytes);

        BlobId {
            hash: hasher.finalize().into(),
            size: bytes.len() as u32,
        }
    }
}

impl From<usize> for BlobId {
    fn from(size: usize) -> Self {
        BlobId {
            hash: [0; BLOB_HASH_LEN],
            size: size as u32,
        }
    }
}

impl Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<u32>());
        self.size.to_leb128_bytes(&mut bytes);
        write!(
            f,
            "{}{}",
            base32::encode(base32::Alphabet::RFC4648 { padding: false }, &self.hash),
            base32::encode(base32::Alphabet::RFC4648 { padding: false }, &bytes)
        )
    }
}

impl StoreSerialize for BlobId {
    fn serialize(&self) -> Option<Vec<u8>> {
        let mut bytes = Vec::with_capacity(BLOB_HASH_LEN + std::mem::size_of::<u32>());
        bytes.extend_from_slice(&self.hash);
        self.size.to_leb128_bytes(&mut bytes);
        bytes.into()
    }
}

impl StoreDeserialize for BlobId {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        BlobId {
            hash: bytes.get(0..BLOB_HASH_LEN)?.try_into().ok()?,
            size: u32::from_leb128_bytes(bytes.get(BLOB_HASH_LEN..)?)?.0,
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
