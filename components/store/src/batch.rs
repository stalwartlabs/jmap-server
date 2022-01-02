use std::borrow::Cow;

use nlp::Language;

use crate::{
    field::{Field, Text, UpdateField},
    AccountId, ChangeLogId, CollectionId, DocumentId, FieldId, FieldNumber, Float, Integer,
    LongInteger, Tag, UncommittedDocumentId,
};

pub const MAX_TOKEN_LENGTH: usize = 40;
pub const MAX_ID_LENGTH: usize = 80;
pub const MAX_SORT_FIELD_LENGTH: usize = 255;

#[derive(Debug)]
pub struct DocumentWriter<'x, T: UncommittedDocumentId> {
    pub account: AccountId,
    pub collection: CollectionId,
    pub default_language: Language,
    pub log_action: LogAction,
    pub action: WriteAction<T>,
    pub fields: Vec<UpdateField<'x>>,
}

#[derive(Debug, Clone, Copy)]
pub enum LogAction {
    Insert(ChangeLogId),
    Update(ChangeLogId),
    Delete(ChangeLogId),
    Move(ChangeLogId, ChangeLogId),
    None,
}

#[derive(Debug, Clone, Copy)]
pub enum WriteAction<T: UncommittedDocumentId> {
    Insert(T),
    Update(DocumentId),
    Delete(DocumentId),
    UpdateMany,
    DeleteMany,
}

impl<'x, T: UncommittedDocumentId> DocumentWriter<'x, T> {
    pub fn insert(
        account: AccountId,
        collection: CollectionId,
        uncommited_id: T,
    ) -> DocumentWriter<'x, T> {
        DocumentWriter {
            account,
            collection,
            default_language: Language::English,
            log_action: LogAction::None,
            action: WriteAction::Insert(uncommited_id),
            fields: Vec::new(),
        }
    }

    pub fn update(
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
    ) -> DocumentWriter<'x, T> {
        DocumentWriter {
            account,
            collection,
            default_language: Language::English,
            log_action: LogAction::None,
            action: WriteAction::Update(document),
            fields: Vec::new(),
        }
    }

    pub fn delete(
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
    ) -> DocumentWriter<'x, T> {
        DocumentWriter {
            account,
            collection,
            default_language: Language::English,
            log_action: LogAction::None,
            action: WriteAction::Delete(document),
            fields: Vec::new(),
        }
    }

    pub fn update_many(account: AccountId, collection: CollectionId) -> DocumentWriter<'x, T> {
        DocumentWriter {
            account,
            collection,
            default_language: Language::English,
            log_action: LogAction::None,
            action: WriteAction::UpdateMany,
            fields: Vec::new(),
        }
    }

    pub fn delete_many(account: AccountId, collection: CollectionId) -> DocumentWriter<'x, T> {
        DocumentWriter {
            account,
            collection,
            default_language: Language::English,
            log_action: LogAction::None,
            action: WriteAction::DeleteMany,
            fields: Vec::new(),
        }
    }

    pub fn log_as_insert(&mut self, changelog_id: ChangeLogId) {
        self.log_action = LogAction::Insert(changelog_id);
    }

    pub fn log_as_update(&mut self, changelog_id: ChangeLogId) {
        self.log_action = LogAction::Update(changelog_id);
    }

    pub fn log_as_delete(&mut self, changelog_id: ChangeLogId) {
        self.log_action = LogAction::Delete(changelog_id);
    }

    pub fn log_as_move(&mut self, changelog_id: ChangeLogId, dest_changelog_id: ChangeLogId) {
        self.log_action = LogAction::Move(changelog_id, dest_changelog_id);
    }

    pub fn set_default_language(&mut self, language: Language) {
        self.default_language = language;
    }

    pub fn add_text(
        &mut self,
        field: FieldId,
        field_num: FieldNumber,
        value: Text<'x>,
        stored: bool,
        sorted: bool,
    ) {
        self.fields.push(UpdateField::Text(Field::new(
            field, field_num, value, stored, sorted,
        )));
    }

    pub fn add_blob(&mut self, field: FieldId, field_num: FieldNumber, value: Cow<'x, [u8]>) {
        self.fields.push(UpdateField::Blob(Field::new(
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
        self.fields.push(UpdateField::Integer(Field::new(
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
        self.fields.push(UpdateField::LongInteger(Field::new(
            field, field_num, value, stored, sorted,
        )));
    }

    pub fn add_tag(&mut self, field: FieldId, value: Tag<'x>) {
        self.fields
            .push(UpdateField::Tag(Field::new(field, 0, value, false, false)));
    }

    pub fn add_float(
        &mut self,
        field: FieldId,
        field_num: FieldNumber,
        value: Float,
        stored: bool,
        sorted: bool,
    ) {
        self.fields.push(UpdateField::Float(Field::new(
            field, field_num, value, stored, sorted,
        )));
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

impl<'x, T: UncommittedDocumentId> IntoIterator for DocumentWriter<'x, T> {
    type Item = UpdateField<'x>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.into_iter()
    }
}
