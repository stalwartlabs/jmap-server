use std::io::Write;

use store::{blob::BlobIndex, AccountId, Collection, DocumentId};

use crate::{error::method::MethodError, protocol::json::JSONValue};

use super::{hex_reader, HexWriter, JMAPIdSerialize};
use store::leb128::Leb128;

#[derive(Clone, Debug)]
pub struct OwnedBlob {
    pub account_id: AccountId,
    pub collection: Collection,
    pub document: DocumentId,
    pub blob_index: BlobIndex,
}

#[derive(Clone, Debug)]
pub struct TemporaryBlob {
    pub account_id: AccountId,
    pub timestamp: u64,
    pub hash: u64,
}

#[derive(Clone, Debug)]
pub struct InnerBlob<T> {
    pub blob_id: T,
    pub blob_index: BlobIndex,
}

#[derive(Clone, Debug)]
pub enum BlobId {
    Owned(OwnedBlob),
    Temporary(TemporaryBlob),
    InnerOwned(InnerBlob<OwnedBlob>),
    InnerTemporary(InnerBlob<TemporaryBlob>),
}

impl BlobId {
    pub fn new_owned(
        account_id: AccountId,
        collection: Collection,
        document: DocumentId,
        blob_index: BlobIndex,
    ) -> Self {
        BlobId::Owned(OwnedBlob {
            account_id,
            collection,
            document,
            blob_index,
        })
    }

    pub fn new_temporary(account_id: AccountId, timestamp: u64, hash: u64) -> Self {
        BlobId::Temporary(TemporaryBlob {
            account_id,
            timestamp,
            hash,
        })
    }

    pub fn new_inner(blob_id: BlobId, blob_index: BlobIndex) -> Option<Self> {
        match blob_id {
            BlobId::Owned(blob_id) => BlobId::InnerOwned(InnerBlob {
                blob_id,
                blob_index,
            })
            .into(),
            BlobId::Temporary(blob_id) => BlobId::InnerTemporary(InnerBlob {
                blob_id,
                blob_index,
            })
            .into(),
            BlobId::InnerOwned(_) | BlobId::InnerTemporary(_) => None,
        }
    }

    pub fn owner_id(&self) -> AccountId {
        match self {
            BlobId::Owned(blob_id) => blob_id.account_id,
            BlobId::Temporary(blob_id) => blob_id.account_id,
            BlobId::InnerOwned(blob_id) => blob_id.blob_id.account_id,
            BlobId::InnerTemporary(blob_id) => blob_id.blob_id.account_id,
        }
    }
}

impl JMAPIdSerialize for BlobId {
    fn from_jmap_string(id: &str) -> Option<Self>
    where
        Self: Sized,
    {
        match id.as_bytes().get(0)? {
            b'o' => {
                let mut it = hex_reader(id, 1);

                Some(BlobId::Owned(OwnedBlob {
                    account_id: AccountId::from_leb128_it(&mut it)?,
                    collection: it.next()?.into(),
                    document: DocumentId::from_leb128_it(&mut it)?,
                    blob_index: BlobIndex::from_leb128_it(&mut it)?,
                }))
            }
            b't' => {
                let mut it = hex_reader(id, 1);

                Some(BlobId::Temporary(TemporaryBlob {
                    account_id: AccountId::from_leb128_it(&mut it)?,
                    timestamp: u64::from_leb128_it(&mut it)?,
                    hash: u64::from_leb128_it(&mut it)?,
                }))
            }
            b'q' => {
                let mut it = hex_reader(id, 1);

                Some(BlobId::InnerTemporary(InnerBlob {
                    blob_id: TemporaryBlob {
                        account_id: AccountId::from_leb128_it(&mut it)?,
                        timestamp: u64::from_leb128_it(&mut it)?,
                        hash: u64::from_leb128_it(&mut it)?,
                    },
                    blob_index: BlobIndex::from_leb128_it(&mut it)?,
                }))
            }
            b'p' => {
                let mut it = hex_reader(id, 1);

                Some(BlobId::InnerOwned(InnerBlob {
                    blob_id: OwnedBlob {
                        account_id: AccountId::from_leb128_it(&mut it)?,
                        collection: it.next()?.into(),
                        document: DocumentId::from_leb128_it(&mut it)?,
                        blob_index: BlobIndex::from_leb128_it(&mut it)?,
                    },
                    blob_index: BlobIndex::from_leb128_it(&mut it)?,
                }))
            }
            _ => None,
        }
    }

    #[allow(clippy::unused_io_amount)]
    fn to_jmap_string(&self) -> String {
        let mut writer = HexWriter::with_capacity(10);
        match self {
            BlobId::Owned(blob_id) => {
                writer.result.push('o');
                blob_id.account_id.to_leb128_writer(&mut writer).unwrap();
                writer.write(&[blob_id.collection as u8]).unwrap();
                blob_id.document.to_leb128_writer(&mut writer).unwrap();
                blob_id.blob_index.to_leb128_writer(&mut writer).unwrap();
            }
            BlobId::Temporary(blob_id) => {
                writer.result.push('t');
                blob_id.account_id.to_leb128_writer(&mut writer).unwrap();
                blob_id.timestamp.to_leb128_writer(&mut writer).unwrap();
                blob_id.hash.to_leb128_writer(&mut writer).unwrap();
            }
            BlobId::InnerOwned(blob_id) => {
                writer.result.push('p');
                blob_id
                    .blob_id
                    .account_id
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                writer.write(&[blob_id.blob_id.collection as u8]).unwrap();
                blob_id
                    .blob_id
                    .document
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                blob_id
                    .blob_id
                    .blob_index
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                blob_id.blob_index.to_leb128_writer(&mut writer).unwrap();
            }
            BlobId::InnerTemporary(blob_id) => {
                writer.result.push('q');
                blob_id
                    .blob_id
                    .account_id
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                blob_id
                    .blob_id
                    .timestamp
                    .to_leb128_writer(&mut writer)
                    .unwrap();
                blob_id.blob_id.hash.to_leb128_writer(&mut writer).unwrap();
                blob_id.blob_index.to_leb128_writer(&mut writer).unwrap();
            }
        }
        writer.result
    }
}

impl JSONValue {
    pub fn to_blob_id(&self) -> Option<BlobId> {
        match self {
            JSONValue::String(string) => BlobId::from_jmap_string(string),
            _ => None,
        }
    }

    pub fn parse_blob_id(self, optional: bool) -> crate::Result<Option<BlobId>> {
        match self {
            JSONValue::String(string) => Ok(Some(BlobId::from_jmap_string(&string).ok_or_else(
                || MethodError::InvalidArguments("Failed to parse blobId.".to_string()),
            )?)),
            JSONValue::Null if optional => Ok(None),
            _ => Err(MethodError::InvalidArguments(
                "Expected string.".to_string(),
            )),
        }
    }
}
