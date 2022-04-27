use std::io::Write;

use store::{
    blob::{BlobId, BLOB_HASH_LEN},
    AccountId, Collection, DocumentId,
};

use crate::{error::method::MethodError, protocol::json::JSONValue};

use super::{hex_reader, HexWriter, JMAPIdSerialize};
use store::leb128::Leb128;

#[derive(Clone, Debug)]
pub struct OwnedBlob {
    pub account_id: AccountId,
    pub collection: Collection,
    pub document: DocumentId,
    pub blob_index: u32,
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
    pub blob_index: u32,
}

#[derive(Clone, Debug)]
pub struct JMAPBlob {
    pub id: BlobId,
    pub inner_id: Option<u32>,
}

impl JMAPBlob {
    pub fn new(id: BlobId) -> Self {
        JMAPBlob { id, inner_id: None }
    }

    pub fn new_inner(id: BlobId, inner_id: u32) -> Self {
        JMAPBlob {
            id,
            inner_id: inner_id.into(),
        }
    }
}

impl From<&BlobId> for JMAPBlob {
    fn from(id: &BlobId) -> Self {
        JMAPBlob::new(id.clone())
    }
}

impl From<BlobId> for JMAPBlob {
    fn from(id: BlobId) -> Self {
        JMAPBlob::new(id)
    }
}

impl JMAPIdSerialize for JMAPBlob {
    fn from_jmap_string(id: &str) -> Option<Self>
    where
        Self: Sized,
    {
        let is_inner = id.as_bytes().get(0)? == &b'i';
        let mut it = hex_reader(id, 1);
        let mut id = BlobId {
            hash: [0; BLOB_HASH_LEN],
            size: 0,
        };

        for pos in 0..BLOB_HASH_LEN {
            id.hash[pos] = it.next()?;
        }
        id.size = u32::from_leb128_it(&mut it)?;

        Some(JMAPBlob {
            id,
            inner_id: if is_inner {
                u32::from_leb128_it(&mut it)?.into()
            } else {
                None
            },
        })
    }

    #[allow(clippy::unused_io_amount)]
    fn to_jmap_string(&self) -> String {
        let mut writer = HexWriter::with_capacity(40);
        if let Some(inner_id) = self.inner_id {
            writer.result.push('i');
            writer.write(&self.id.hash).unwrap();
            self.id.size.to_leb128_writer(&mut writer).unwrap();
            inner_id.to_leb128_writer(&mut writer).unwrap();
        } else {
            writer.result.push('b');
            writer.write(&self.id.hash).unwrap();
            self.id.size.to_leb128_writer(&mut writer).unwrap();
        }
        writer.result
    }
}

impl JSONValue {
    pub fn to_blob(&self) -> Option<JMAPBlob> {
        match self {
            JSONValue::String(string) => JMAPBlob::from_jmap_string(string),
            _ => None,
        }
    }

    pub fn parse_blob(self, optional: bool) -> crate::Result<Option<JMAPBlob>> {
        match self {
            JSONValue::String(string) => Ok(Some(JMAPBlob::from_jmap_string(&string).ok_or_else(
                || MethodError::InvalidArguments("Failed to parse blobId.".to_string()),
            )?)),
            JSONValue::Null if optional => Ok(None),
            _ => Err(MethodError::InvalidArguments(
                "Expected string.".to_string(),
            )),
        }
    }
}
