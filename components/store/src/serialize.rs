use std::convert::TryInto;

use crate::{
    leb128::Leb128, AccountId, CollectionId, DocumentId, FieldId, FieldNumber, Float, Integer,
    LongInteger, StoreError, Tag, TermId,
};

pub const PREFIX_LEN: usize = std::mem::size_of::<AccountId>()
    + std::mem::size_of::<CollectionId>()
    + std::mem::size_of::<FieldId>();

pub const KEY_BASE_LEN: usize = PREFIX_LEN + std::mem::size_of::<DocumentId>();

pub const BM_TEXT: u8 = 0;
pub const BM_TERM_EXACT: u8 = 1;
pub const BM_TERM_STEMMED: u8 = 2;
pub const BM_TAG_ID: u8 = 3;
pub const BM_TAG_TEXT: u8 = 4;
pub const BM_TAG_STATIC: u8 = 5;
pub const BM_USED_IDS: u8 = 6;
pub const BM_TOMBSTONED_IDS: u8 = 7;
pub const BM_FREED_IDS: u8 = 8;

pub fn serialize_stored_key(
    account: AccountId,
    collection: CollectionId,
    document: DocumentId,
    field: FieldId,
    pos: FieldNumber,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN + std::mem::size_of::<FieldNumber>());
    account.to_leb128_bytes(&mut bytes);
    bytes.push(collection);
    document.to_leb128_bytes(&mut bytes);
    bytes.push(field);
    if pos > 0 {
        pos.to_leb128_bytes(&mut bytes);
    }
    bytes
}

pub fn serialize_stored_key_global(
    account: Option<AccountId>,
    collection: Option<CollectionId>,
    field: Option<FieldId>,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN);
    if let Some(account) = account {
        account.to_leb128_bytes(&mut bytes);
    }
    if let Some(collection) = collection {
        bytes.push(collection);
    }
    if let Some(field) = field {
        bytes.push(field);
    }
    bytes
}

pub fn serialize_bm_tag_key(
    account: AccountId,
    collection: CollectionId,
    field: FieldId,
    tag: &Tag,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN + tag.len() + 1);
    account.to_leb128_bytes(&mut bytes);
    let bm_type = match tag {
        Tag::Static(id) => {
            bytes.push(*id);
            BM_TAG_STATIC
        }
        Tag::Id(id) => {
            (*id).to_leb128_bytes(&mut bytes);
            BM_TAG_ID
        }
        Tag::Text(text) => {
            bytes.extend_from_slice(text.as_bytes());
            BM_TAG_TEXT
        }
    };
    bytes.push(collection);
    bytes.push(field);
    bytes.push(bm_type);
    bytes
}

pub fn serialize_bm_text_key(
    account: AccountId,
    collection: CollectionId,
    field: FieldId,
    text: &str,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN + text.len() + 1);
    account.to_leb128_bytes(&mut bytes);
    bytes.extend_from_slice(text.as_bytes());
    bytes.push(collection);
    bytes.push(field);
    bytes.push(BM_TEXT);
    bytes
}

pub fn serialize_bm_term_key(
    account: AccountId,
    collection: CollectionId,
    field: FieldId,
    term_id: TermId,
    is_exact: bool,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN + std::mem::size_of::<TermId>() + 2);
    account.to_leb128_bytes(&mut bytes);
    term_id.to_leb128_bytes(&mut bytes);
    bytes.push(collection);
    bytes.push(field);
    bytes.push(if is_exact {
        BM_TERM_EXACT
    } else {
        BM_TERM_STEMMED
    });
    bytes
}

pub fn serialize_bm_internal(account: AccountId, collection: CollectionId, id: u8) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN + 1);
    account.to_leb128_bytes(&mut bytes);
    bytes.push(collection);
    bytes.push(id);
    bytes
}

pub fn serialize_index_key(
    account: AccountId,
    collection: CollectionId,
    document: DocumentId,
    field: FieldId,
    key: &[u8],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN + key.len());
    bytes.extend_from_slice(&account.to_be_bytes());
    bytes.extend_from_slice(&collection.to_be_bytes());
    bytes.extend_from_slice(&field.to_be_bytes());
    bytes.extend_from_slice(key);
    bytes.extend_from_slice(&document.to_be_bytes());
    bytes
}

pub fn serialize_index_key_base(
    account: AccountId,
    collection: CollectionId,
    field: FieldId,
    key: &[u8],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN + key.len());
    bytes.extend_from_slice(&account.to_be_bytes());
    bytes.extend_from_slice(&collection.to_be_bytes());
    bytes.extend_from_slice(&field.to_be_bytes());
    bytes.extend_from_slice(key);
    bytes
}

pub fn serialize_index_key_prefix(
    account: AccountId,
    collection: CollectionId,
    field: FieldId,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN);
    bytes.extend_from_slice(&account.to_be_bytes());
    bytes.extend_from_slice(&collection.to_be_bytes());
    bytes.extend_from_slice(&field.to_be_bytes());
    bytes
}
pub fn serialize_ac_key_be(account: AccountId, collection: CollectionId) -> Vec<u8> {
    let mut bytes =
        Vec::with_capacity(std::mem::size_of::<AccountId>() + std::mem::size_of::<CollectionId>());
    bytes.extend_from_slice(&account.to_be_bytes());
    bytes.extend_from_slice(&collection.to_be_bytes());
    bytes
}

pub fn serialize_a_key_be(account: AccountId) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of::<AccountId>());
    bytes.extend_from_slice(&account.to_be_bytes());
    bytes
}

pub fn serialize_acd_key_leb128(
    account: AccountId,
    collection: CollectionId,
    document: DocumentId,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        std::mem::size_of::<AccountId>()
            + std::mem::size_of::<CollectionId>()
            + std::mem::size_of::<DocumentId>(),
    );
    account.to_leb128_bytes(&mut bytes);
    bytes.push(collection);
    document.to_leb128_bytes(&mut bytes);
    bytes
}

pub fn serialize_ac_key_leb128(account: AccountId, collection: CollectionId) -> Vec<u8> {
    let mut bytes =
        Vec::with_capacity(std::mem::size_of::<AccountId>() + std::mem::size_of::<CollectionId>());
    account.to_leb128_bytes(&mut bytes);
    bytes.push(collection);
    bytes
}

pub fn serialize_a_key_leb128(account: AccountId) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(std::mem::size_of::<AccountId>());
    account.to_leb128_bytes(&mut bytes);
    bytes
}

#[inline(always)]
pub fn deserialize_integer(bytes: Vec<u8>) -> Option<Integer> {
    Integer::from_le_bytes(bytes.try_into().ok()?).into()
}

#[inline(always)]
pub fn deserialize_long_integer(bytes: Vec<u8>) -> Option<LongInteger> {
    LongInteger::from_le_bytes(bytes.try_into().ok()?).into()
}

#[inline(always)]
pub fn deserialize_float(bytes: Vec<u8>) -> Option<Float> {
    Float::from_le_bytes(bytes.try_into().ok()?).into()
}

#[inline(always)]
pub fn deserialize_text(bytes: Vec<u8>) -> Option<String> {
    String::from_utf8(bytes).ok()
}

#[inline(always)]
pub fn deserialize_index_document_id(bytes: &[u8]) -> Option<DocumentId> {
    DocumentId::from_be_bytes(
        bytes
            .get(bytes.len() - std::mem::size_of::<DocumentId>()..)?
            .try_into()
            .ok()?,
    )
    .into()
}

#[inline(always)]
pub fn deserialize_document_id_from_leb128(bytes: &[u8]) -> Option<DocumentId> {
    DocumentId::from_leb128_bytes(bytes)?.0.into()
}

pub trait StoreDeserialize<T>: Sized {
    fn deserialize(self) -> crate::Result<T>;
}

impl StoreDeserialize<Vec<u8>> for Vec<u8> {
    fn deserialize(self) -> crate::Result<Vec<u8>> {
        Ok(self)
    }
}

impl StoreDeserialize<String> for Vec<u8> {
    fn deserialize(self) -> crate::Result<String> {
        deserialize_text(self)
            .ok_or_else(|| StoreError::InternalError("Failed to decode UTF-8 string".to_string()))
    }
}

impl StoreDeserialize<Float> for Vec<u8> {
    fn deserialize(self) -> crate::Result<Float> {
        deserialize_float(self)
            .ok_or_else(|| StoreError::InternalError("Failed to decode float".to_string()))
    }
}

impl StoreDeserialize<LongInteger> for Vec<u8> {
    fn deserialize(self) -> crate::Result<LongInteger> {
        deserialize_long_integer(self)
            .ok_or_else(|| StoreError::InternalError("Failed to decode long integer".to_string()))
    }
}

impl StoreDeserialize<Integer> for Vec<u8> {
    fn deserialize(self) -> crate::Result<Integer> {
        deserialize_integer(self)
            .ok_or_else(|| StoreError::InternalError("Failed to decode integer".to_string()))
    }
}
