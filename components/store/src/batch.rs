use std::borrow::Cow;

use nlp::Language;

use crate::{
    field::{Field, IndexField, Text},
    AccountId, CollectionId, DocumentId, FieldId, FieldNumber, Float, Integer, LongInteger, Tag,
};

pub const MAX_TOKEN_LENGTH: usize = 40;
pub const MAX_ID_LENGTH: usize = 80;
pub const MAX_SORT_FIELD_LENGTH: usize = 255;

#[derive(Debug)]
pub struct WriteOperation<'x> {
    account: AccountId,
    collection: CollectionId,
    action: WriteAction,
    language: Language,
    fields: Vec<IndexField<'x>>,
}

#[derive(Debug, Clone, Copy)]
pub enum WriteAction {
    Insert,
    Update(DocumentId),
    Delete(DocumentId),
    DeleteAll,
}

impl<'x> WriteOperation<'x> {
    pub fn insert(account: AccountId, collection: CollectionId) -> WriteOperation<'x> {
        WriteOperation {
            account,
            collection,
            action: WriteAction::Insert,
            language: Language::English,
            fields: Vec::new(),
        }
    }

    pub fn update(
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
    ) -> WriteOperation<'x> {
        WriteOperation {
            account,
            collection,
            action: WriteAction::Update(document),
            language: Language::English,
            fields: Vec::new(),
        }
    }

    pub fn delete(
        account: AccountId,
        collection: CollectionId,
        document: Option<DocumentId>,
    ) -> WriteOperation<'x> {
        WriteOperation {
            account,
            collection,
            action: if let Some(document) = document {
                WriteAction::Delete(document)
            } else {
                WriteAction::DeleteAll
            },
            language: Language::English,
            fields: Vec::new(),
        }
    }

    pub fn add_text(
        &mut self,
        field: FieldId,
        field_num: FieldNumber,
        value: Text<'x>,
        stored: bool,
        sorted: bool,
    ) {
        self.fields.push(IndexField::Text(Field::new(
            field, field_num, value, stored, sorted,
        )));
    }

    pub fn add_blob(&mut self, field: FieldId, field_num: FieldNumber, value: Cow<'x, [u8]>) {
        self.fields.push(IndexField::Blob(Field::new(
            field, field_num, value, true, false,
        )));
    }

    pub fn add_integer(
        &mut self,
        field: FieldId,
        field_num: FieldNumber,
        value: Integer,
        stored: bool,
        sorted: bool,
    ) {
        self.fields.push(IndexField::Integer(Field::new(
            field, field_num, value, stored, sorted,
        )));
    }

    pub fn add_long_int(
        &mut self,
        field: FieldId,
        field_num: FieldNumber,
        value: LongInteger,
        stored: bool,
        sorted: bool,
    ) {
        self.fields.push(IndexField::LongInteger(Field::new(
            field, field_num, value, stored, sorted,
        )));
    }

    pub fn add_tag(&mut self, field: FieldId, value: Tag<'x>) {
        self.fields
            .push(IndexField::Tag(Field::new(field, 0, value, false, false)));
    }

    pub fn add_float(
        &mut self,
        field: FieldId,
        field_num: FieldNumber,
        value: Float,
        stored: bool,
        sorted: bool,
    ) {
        self.fields.push(IndexField::Float(Field::new(
            field, field_num, value, stored, sorted,
        )));
    }

    pub fn get_account_id(&self) -> AccountId {
        self.account
    }

    pub fn get_collection_id(&self) -> CollectionId {
        self.collection
    }

    pub fn get_action(&self) -> WriteAction {
        self.action
    }

    pub fn set_default_language(&mut self, language: Language) {
        self.language = language;
    }

    pub fn get_default_language(&self) -> Language {
        self.language
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

impl<'x> IntoIterator for WriteOperation<'x> {
    type Item = IndexField<'x>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.into_iter()
    }
}
