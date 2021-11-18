use nlp::tokenizers::Token;

use crate::{
    document::{DocumentBuilder, Field, IndexField, IndexOptions, TextField},
    AccountId, ArrayPos, CollectionId, DocumentId, FieldId, Float, Integer, LongInteger, Tag,
    TagId,
};

const KEY_BASE_LEN: usize = std::mem::size_of::<AccountId>()
    + std::mem::size_of::<CollectionId>()
    + std::mem::size_of::<FieldId>()
    + std::mem::size_of::<DocumentId>();

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

impl<'x> IndexField<'x> {
    pub fn as_stored_value(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
    ) -> SerializedKeyValue {
        SerializedKeyValue {
            key: {
                let field = self.unwrap();
                let mut bytes = Vec::with_capacity(
                    KEY_BASE_LEN + field.len() + std::mem::size_of::<ArrayPos>(),
                );
                bytes.extend_from_slice(&account.to_be_bytes());
                bytes.extend_from_slice(&collection.to_be_bytes());
                if let IndexField::Tag(tag) = self {
                    bytes.extend_from_slice(&field.get_field().to_be_bytes());
                    match &tag.value {
                        Tag::Static(id) => bytes.extend_from_slice(&id.to_be_bytes()),
                        Tag::Id(id) => bytes.extend_from_slice(&id.to_be_bytes()),
                        Tag::Text(text) => bytes.extend_from_slice(text.as_bytes()),
                    }
                } else {
                    bytes.extend_from_slice(&document.to_be_bytes());
                    bytes.extend_from_slice(&field.get_field().to_be_bytes());
                    if field.get_options().is_array() {
                        bytes.extend_from_slice(&field.get_options().get_pos().to_be_bytes());
                    }
                }

                bytes
            },
            value: match self {
                IndexField::Text(t) => SerializedValue::Borrowed(t.value.as_bytes()),
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

    pub fn as_sort_key(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
    ) -> Vec<u8> {
        let field = self.unwrap();
        let mut bytes = Vec::with_capacity(KEY_BASE_LEN + field.len());

        bytes.extend_from_slice(&account.to_be_bytes());
        bytes.extend_from_slice(&collection.to_be_bytes());
        bytes.extend_from_slice(&field.get_field().to_be_bytes());

        match self {
            IndexField::Text(text) => {
                bytes.extend_from_slice(text.value.as_bytes());
            }
            IndexField::Integer(int) => {
                bytes.extend_from_slice(&int.value.to_be_bytes());
            }
            IndexField::LongInteger(int) => {
                bytes.extend_from_slice(&int.value.to_be_bytes());
            }
            IndexField::Float(float) => {
                bytes.extend_from_slice(&float.value.to_be_bytes());
            }
            IndexField::Tag(_) | IndexField::Blob(_) => {
                panic!("Blobs and Tags cannot be serialized as sort keys.")
            }
        }

        bytes.extend_from_slice(&document.to_be_bytes());
        bytes
    }
}

pub trait TokenSerializer {
    fn as_index_key(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        field: &TextField,
    ) -> Vec<u8>;
}

impl<'x> TokenSerializer for Token<'x> {
    fn as_index_key(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        field: &TextField,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(KEY_BASE_LEN + self.word.len() + 1);
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes.extend_from_slice(self.word.as_bytes());
        bytes.extend_from_slice(&collection.to_be_bytes());
        bytes.extend_from_slice(&field.get_field().to_be_bytes());
        if field.options.is_full_text() {
            bytes.push(if self.is_exact { 0 } else { 1 });
        }
        bytes
    }
}
