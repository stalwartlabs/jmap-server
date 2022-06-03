use crate::{blob::BlobId, nlp::Language, write::field::Field, DocumentId, FieldId};

use super::{collection::Collection, number::Number, tag::Tag};

pub const MAX_TOKEN_LENGTH: usize = 40;
pub const MAX_ID_LENGTH: usize = 100;
pub const MAX_SORT_FIELD_LENGTH: usize = 255;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Text {
    pub text: String,
    pub language: Language,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Document {
    pub collection: Collection,
    pub document_id: DocumentId,
    pub term_index: Option<(BlobId, u64)>,

    pub text_fields: Vec<Field<Text>>,
    pub number_fields: Vec<Field<Number>>,
    pub binary_fields: Vec<Field<Vec<u8>>>,
    pub tag_fields: Vec<Field<Tag>>,
    pub blobs: Vec<(BlobId, u64)>,
}

impl Document {
    pub fn new(collection: Collection, document_id: DocumentId) -> Document {
        Document {
            collection,
            document_id,
            text_fields: Vec::new(),
            number_fields: Vec::new(),
            binary_fields: Vec::new(),
            tag_fields: Vec::new(),
            blobs: Vec::new(),
            term_index: None,
        }
    }

    pub fn text(
        &mut self,
        field: impl Into<FieldId>,
        value: String,
        language: Language,
        options: u64,
    ) {
        self.text_fields.push(Field::new(
            field.into(),
            Text {
                text: value,
                language,
            },
            options,
        ));
    }

    pub fn binary(&mut self, field: impl Into<FieldId>, value: Vec<u8>, options: u64) {
        self.binary_fields
            .push(Field::new(field.into(), value, options));
    }

    pub fn number(&mut self, field: impl Into<FieldId>, value: impl Into<Number>, options: u64) {
        self.number_fields
            .push(Field::new(field.into(), value.into(), options));
    }

    pub fn tag(&mut self, field: impl Into<FieldId>, value: Tag, options: u64) {
        self.tag_fields
            .push(Field::new(field.into(), value, options));
    }

    pub fn blob(&mut self, blob: BlobId, options: u64) {
        self.blobs.push((blob, options));
    }

    pub fn term_index(&mut self, blob: BlobId, options: u64) {
        self.term_index = Some((blob, options));
    }

    pub fn is_empty(&self) -> bool {
        self.text_fields.is_empty()
            && self.number_fields.is_empty()
            && self.binary_fields.is_empty()
            && self.tag_fields.is_empty()
    }
}
