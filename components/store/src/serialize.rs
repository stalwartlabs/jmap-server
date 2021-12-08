use std::convert::TryInto;

use crate::{
    document::IndexOptions, field::IndexField, AccountId, ArrayPos, CollectionId, DocumentId,
    FieldId, Float, Integer, LongInteger, Tag, TermId,
};

pub const PREFIX_LEN: usize = std::mem::size_of::<AccountId>()
    + std::mem::size_of::<CollectionId>()
    + std::mem::size_of::<FieldId>();

pub const KEY_BASE_LEN: usize = PREFIX_LEN + std::mem::size_of::<DocumentId>();

pub struct SerializedKeyValue<'x> {
    pub key: Vec<u8>,
    pub value: SerializedValue<'x>,
}

pub enum SerializedValue<'x> {
    Tag,
    Owned(Vec<u8>),
    Borrowed(&'x [u8]),
}

pub fn serialize_stored_key(
    account: AccountId,
    collection: CollectionId,
    document: DocumentId,
    field: FieldId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(KEY_BASE_LEN);
    key.extend_from_slice(&account.to_be_bytes());
    key.extend_from_slice(&collection.to_be_bytes());
    key.extend_from_slice(&document.to_be_bytes());
    key.extend_from_slice(&field.to_be_bytes());
    key
}

pub fn serialize_stored_key_pos(
    account: AccountId,
    collection: CollectionId,
    document: DocumentId,
    field: FieldId,
    pos: ArrayPos,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(KEY_BASE_LEN + std::mem::size_of::<ArrayPos>());
    key.extend_from_slice(&account.to_be_bytes());
    key.extend_from_slice(&collection.to_be_bytes());
    key.extend_from_slice(&document.to_be_bytes());
    key.extend_from_slice(&field.to_be_bytes());
    key.extend_from_slice(&pos.to_be_bytes());
    key
}

pub fn serialize_tag_key(
    account: AccountId,
    collection: CollectionId,
    field: FieldId,
    tag: &Tag,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(KEY_BASE_LEN + tag.len());
    key.extend_from_slice(&account.to_be_bytes());
    key.extend_from_slice(&collection.to_be_bytes());
    key.extend_from_slice(&field.to_be_bytes());
    match tag {
        Tag::Static(id) => key.extend_from_slice(&id.to_be_bytes()),
        Tag::Id(id) => key.extend_from_slice(&id.to_be_bytes()),
        Tag::Text(text) => key.extend_from_slice(text.as_bytes()),
    }
    key
}

pub fn serialize_text_key(
    account: AccountId,
    collection: CollectionId,
    field: FieldId,
    text: &str,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN + text.len());
    bytes.extend_from_slice(&account.to_be_bytes());
    bytes.extend_from_slice(text.as_bytes());
    bytes.extend_from_slice(&collection.to_be_bytes());
    bytes.extend_from_slice(&field.to_be_bytes());
    bytes
}

pub fn serialize_term_id_key(
    account: AccountId,
    collection: CollectionId,
    field: FieldId,
    term_id: TermId,
    is_exact: bool,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN + std::mem::size_of::<TermId>() + 1);
    bytes.extend_from_slice(&account.to_be_bytes());
    bytes.extend_from_slice(&term_id.to_be_bytes());
    bytes.extend_from_slice(&collection.to_be_bytes());
    bytes.extend_from_slice(&field.to_be_bytes());
    if !is_exact {
        bytes.push(1);
    }
    bytes
}

pub fn serialize_index_key(
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

pub fn serialize_term_index_key(
    account: AccountId,
    collection: CollectionId,
    document: DocumentId,
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(
        std::mem::size_of::<AccountId>()
            + std::mem::size_of::<CollectionId>()
            + std::mem::size_of::<DocumentId>(),
    );
    bytes.extend_from_slice(&account.to_be_bytes());
    bytes.extend_from_slice(&collection.to_be_bytes());
    bytes.extend_from_slice(&document.to_be_bytes());
    bytes
}

pub fn serialize_collection_key(account: AccountId, collection: CollectionId) -> Vec<u8> {
    let mut bytes =
        Vec::with_capacity(std::mem::size_of::<AccountId>() + std::mem::size_of::<CollectionId>());
    bytes.extend_from_slice(&account.to_be_bytes());
    bytes.extend_from_slice(&collection.to_be_bytes());
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
pub fn deserialize_document_id(bytes: &[u8]) -> Option<DocumentId> {
    DocumentId::from_be_bytes(
        bytes[bytes.len() - std::mem::size_of::<DocumentId>()..]
            .try_into()
            .ok()?,
    )
    .into()
}

impl<'x> IndexField<'x> {
    pub fn as_stored_value(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
    ) -> SerializedKeyValue {
        SerializedKeyValue {
            key: {
                let mut bytes =
                    Vec::with_capacity(KEY_BASE_LEN + self.len() + std::mem::size_of::<ArrayPos>());
                bytes.extend_from_slice(&account.to_be_bytes());
                bytes.extend_from_slice(&collection.to_be_bytes());
                if let IndexField::Tag(tag) = self {
                    bytes.extend_from_slice(&tag.get_field().to_be_bytes());
                    match &tag.value {
                        Tag::Static(id) => bytes.extend_from_slice(&id.to_be_bytes()),
                        Tag::Id(id) => bytes.extend_from_slice(&id.to_be_bytes()),
                        Tag::Text(text) => bytes.extend_from_slice(text.as_bytes()),
                    }
                } else {
                    let options = self.get_options();
                    bytes.extend_from_slice(&document.to_be_bytes());
                    bytes.extend_from_slice(&self.get_field().to_be_bytes());
                    if options.is_array() {
                        bytes.extend_from_slice(&options.get_pos().to_be_bytes());
                    }
                }

                bytes
            },
            value: match self {
                IndexField::FullText(t) => SerializedValue::Borrowed(t.value.text.as_bytes()),
                IndexField::Keyword(t) | IndexField::Text(t) => {
                    SerializedValue::Borrowed(t.value.as_bytes())
                }
                IndexField::Blob(b) => SerializedValue::Borrowed(b.value.as_ref()),
                IndexField::Integer(i) => SerializedValue::Owned(i.value.to_le_bytes().into()),
                IndexField::LongInteger(li) => {
                    SerializedValue::Owned(li.value.to_le_bytes().into())
                }
                IndexField::Tag(_) => SerializedValue::Tag,
                IndexField::Float(f) => SerializedValue::Owned(f.value.to_le_bytes().into()),
            },
        }
    }

    pub fn as_index_key(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
    ) -> Vec<u8> {
        let mut bytes = match self {
            IndexField::Keyword(text) | IndexField::Text(text) => {
                serialize_index_key(account, collection, text.field, text.value.as_bytes())
            }
            IndexField::FullText(text) => {
                serialize_index_key(account, collection, text.field, text.value.text.as_bytes())
            }
            IndexField::Integer(int) => {
                serialize_index_key(account, collection, int.field, &int.value.to_be_bytes())
            }
            IndexField::LongInteger(int) => {
                serialize_index_key(account, collection, int.field, &int.value.to_be_bytes())
            }
            IndexField::Float(float) => {
                serialize_index_key(account, collection, float.field, &float.value.to_be_bytes())
            }
            field => {
                panic!("{:?} cannot be serialized as sort keys.", field)
            }
        };
        bytes.extend_from_slice(&document.to_be_bytes());
        bytes
    }
}
