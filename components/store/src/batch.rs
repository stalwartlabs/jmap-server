use nlp::Language;

use crate::{
    field::{Field, FieldOptions, Text, UpdateField},
    ChangeLogId, CollectionId, DocumentId, FieldId, Float, Integer, LongInteger, Tag,
};

pub const MAX_TOKEN_LENGTH: usize = 40;
pub const MAX_ID_LENGTH: usize = 80;
pub const MAX_SORT_FIELD_LENGTH: usize = 255;

#[derive(Debug)]
pub struct WriteBatch {
    pub collection_id: CollectionId,
    pub default_language: Language,
    pub log_id: Option<ChangeLogId>,
    pub log_action: LogAction,
    pub action: WriteAction,
    pub fields: Vec<UpdateField>,
}

#[derive(Debug, Clone, Copy)]
pub enum LogAction {
    Insert(ChangeLogId),
    Update(ChangeLogId),
    Delete(ChangeLogId),
    Move(ChangeLogId, ChangeLogId),
}

#[derive(Debug, Clone, Copy)]
pub enum WriteAction {
    Insert(DocumentId),
    Update(DocumentId),
    Delete(DocumentId),
}

impl WriteBatch {
    pub fn insert(
        collection_id: CollectionId,
        document_id: DocumentId,
        full_id: impl Into<ChangeLogId>,
    ) -> WriteBatch {
        WriteBatch {
            collection_id,
            default_language: Language::English,
            log_action: LogAction::Insert(full_id.into()),
            action: WriteAction::Insert(document_id),
            fields: Vec::new(),
            log_id: None,
        }
    }

    pub fn update(
        collection_id: CollectionId,
        document_id: DocumentId,
        full_id: impl Into<ChangeLogId>,
    ) -> WriteBatch {
        WriteBatch {
            collection_id,
            default_language: Language::English,
            log_action: LogAction::Update(full_id.into()),
            action: WriteAction::Update(document_id),
            fields: Vec::new(),
            log_id: None,
        }
    }

    pub fn delete(
        collection_id: CollectionId,
        document_id: DocumentId,
        full_id: impl Into<ChangeLogId>,
    ) -> WriteBatch {
        WriteBatch {
            collection_id,
            default_language: Language::English,
            log_action: LogAction::Delete(full_id.into()),
            action: WriteAction::Delete(document_id),
            fields: Vec::new(),
            log_id: None,
        }
    }

    pub fn moved(
        collection_id: CollectionId,
        document_id: DocumentId,
        old_log_id: impl Into<ChangeLogId>,
        new_log_id: impl Into<ChangeLogId>,
    ) -> WriteBatch {
        WriteBatch {
            collection_id,
            default_language: Language::English,
            log_action: LogAction::Move(old_log_id.into(), new_log_id.into()),
            action: WriteAction::Update(document_id),
            fields: Vec::new(),
            log_id: None,
        }
    }

    pub fn update_full_id(&mut self, full_id: impl Into<ChangeLogId>) {
        match self.log_action {
            LogAction::Insert(ref mut id) => {
                *id = full_id.into();
            }
            LogAction::Update(ref mut id) => {
                *id = full_id.into();
            }
            LogAction::Delete(ref mut id) => {
                *id = full_id.into();
            }
            _ => (),
        }
    }

    pub fn log_with_id(mut self, change_id: ChangeLogId) -> Self {
        self.log_id = Some(change_id);
        self
    }

    pub fn set_default_language(&mut self, language: Language) {
        self.default_language = language;
    }

    pub fn text(&mut self, field: impl Into<FieldId>, value: Text, options: FieldOptions) {
        self.fields
            .push(UpdateField::Text(Field::new(field.into(), value, options)));
    }

    pub fn binary(&mut self, field: impl Into<FieldId>, value: Vec<u8>, options: FieldOptions) {
        self.fields.push(UpdateField::Binary(Field::new(
            field.into(),
            value,
            options,
        )));
    }

    pub fn integer(&mut self, field: impl Into<FieldId>, value: Integer, options: FieldOptions) {
        self.fields.push(UpdateField::Integer(Field::new(
            field.into(),
            value,
            options,
        )));
    }

    pub fn long_int(
        &mut self,
        field: impl Into<FieldId>,
        value: LongInteger,
        options: FieldOptions,
    ) {
        self.fields.push(UpdateField::LongInteger(Field::new(
            field.into(),
            value,
            options,
        )));
    }

    pub fn tag(&mut self, field: impl Into<FieldId>, value: Tag, options: FieldOptions) {
        self.fields
            .push(UpdateField::Tag(Field::new(field.into(), value, options)));
    }

    pub fn float(&mut self, field: impl Into<FieldId>, value: Float, options: FieldOptions) {
        self.fields
            .push(UpdateField::Float(Field::new(field.into(), value, options)));
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }
}

impl IntoIterator for WriteBatch {
    type Item = UpdateField;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.into_iter()
    }
}
