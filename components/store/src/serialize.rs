use nlp::tokenizers::Token;

use crate::{
    document::IndexOptions,
    field::{Field, IndexField, TextLang},
    AccountId, ArrayPos, CollectionId, DocumentId, FieldId, Tag, TermId,
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
    account: &AccountId,
    collection: &CollectionId,
    document: &DocumentId,
    field: &FieldId,
) -> Vec<u8> {
    let mut key = Vec::with_capacity(KEY_BASE_LEN);
    key.extend_from_slice(&account.to_be_bytes());
    key.extend_from_slice(&collection.to_be_bytes());
    key.extend_from_slice(&document.to_be_bytes());
    key.extend_from_slice(&field.to_be_bytes());
    key
}

pub fn serialize_stored_key_pos(
    account: &AccountId,
    collection: &CollectionId,
    document: &DocumentId,
    field: &FieldId,
    pos: &ArrayPos,
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
    account: &AccountId,
    collection: &CollectionId,
    field: &FieldId,
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
    account: &AccountId,
    collection: &CollectionId,
    field: &FieldId,
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
    account: &AccountId,
    collection: &CollectionId,
    field: &FieldId,
    term_id: &TermId,
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
    account: &AccountId,
    collection: &CollectionId,
    field: &FieldId,
    key: &[u8],
) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(KEY_BASE_LEN + key.len());
    bytes.extend_from_slice(&account.to_be_bytes());
    bytes.extend_from_slice(&collection.to_be_bytes());
    bytes.extend_from_slice(&field.to_be_bytes());
    bytes.extend_from_slice(key);
    bytes
}

pub fn serialize_term_index_key(
    account: &AccountId,
    collection: &CollectionId,
    document: &DocumentId,
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

pub fn serialize_collection_key(account: &AccountId, collection: &CollectionId) -> Vec<u8> {
    let mut bytes =
        Vec::with_capacity(std::mem::size_of::<AccountId>() + std::mem::size_of::<CollectionId>());
    bytes.extend_from_slice(&account.to_be_bytes());
    bytes.extend_from_slice(&collection.to_be_bytes());
    bytes
}

impl<'x> IndexField<'x> {
    pub fn as_stored_value(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
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
                IndexField::Text(t) => SerializedValue::Borrowed(t.value.text.as_bytes()),
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
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
    ) -> Vec<u8> {
        let mut bytes = match self {
            IndexField::Text(text) => {
                serialize_index_key(account, collection, &text.field, text.value.text.as_bytes())
            }
            IndexField::Integer(int) => {
                serialize_index_key(account, collection, &int.field, &int.value.to_be_bytes())
            }
            IndexField::LongInteger(int) => {
                serialize_index_key(account, collection, &int.field, &int.value.to_be_bytes())
            }
            IndexField::Float(float) => serialize_index_key(
                account,
                collection,
                &float.field,
                &float.value.to_be_bytes(),
            ),
            IndexField::Tag(_) | IndexField::Blob(_) => {
                panic!("Blobs and Tags cannot be serialized as sort keys.")
            }
        };
        bytes.extend_from_slice(&document.to_be_bytes());
        bytes
    }
}
