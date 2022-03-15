use nlp::Language;

use crate::{
    field::{Field, FieldOptions, Text, UpdateField},
    Collection, DocumentId, FieldId, Float, Integer, JMAPId, LongInteger, Tag,
};

pub const MAX_TOKEN_LENGTH: usize = 40;
pub const MAX_ID_LENGTH: usize = 80;
pub const MAX_SORT_FIELD_LENGTH: usize = 255;

#[derive(Debug)]
pub struct WriteBatch {
    pub collection: Collection,
    pub default_language: Language,
    pub log_action: LogAction,
    pub action: WriteAction,
    pub fields: Vec<UpdateField>,
}

#[derive(Debug, Clone, Copy)]
pub enum LogAction {
    Insert(JMAPId),
    Update(JMAPId),
    Delete(JMAPId),
    UpdateChild(JMAPId),
    Move(JMAPId, JMAPId),
}

#[derive(Debug, Clone, Copy)]
pub enum WriteAction {
    Insert(DocumentId),
    Update(DocumentId),
    Delete(DocumentId),
}

impl WriteBatch {
    pub fn insert(
        collection: Collection,
        document_id: DocumentId,
        jmap_id: impl Into<JMAPId>,
    ) -> WriteBatch {
        WriteBatch {
            collection,
            default_language: Language::English,
            log_action: LogAction::Insert(jmap_id.into()),
            action: WriteAction::Insert(document_id),
            fields: Vec::new(),
        }
    }

    pub fn update(
        collection: Collection,
        document_id: DocumentId,
        jmap_id: impl Into<JMAPId>,
    ) -> WriteBatch {
        WriteBatch {
            collection,
            default_language: Language::English,
            log_action: LogAction::Update(jmap_id.into()),
            action: WriteAction::Update(document_id),
            fields: Vec::new(),
        }
    }

    pub fn update_child(
        collection: Collection,
        document_id: DocumentId,
        jmap_id: impl Into<JMAPId>,
    ) -> WriteBatch {
        WriteBatch {
            collection,
            default_language: Language::English,
            log_action: LogAction::UpdateChild(jmap_id.into()),
            action: WriteAction::Update(document_id),
            fields: Vec::new(),
        }
    }

    pub fn delete(
        collection: Collection,
        document_id: DocumentId,
        jmap_id: impl Into<JMAPId>,
    ) -> WriteBatch {
        WriteBatch {
            collection,
            default_language: Language::English,
            log_action: LogAction::Delete(jmap_id.into()),
            action: WriteAction::Delete(document_id),
            fields: Vec::new(),
        }
    }

    pub fn moved(
        collection: Collection,
        document_id: DocumentId,
        old_jmap_id: impl Into<JMAPId>,
        new_jmap_id: impl Into<JMAPId>,
    ) -> WriteBatch {
        WriteBatch {
            collection,
            default_language: Language::English,
            log_action: LogAction::Move(old_jmap_id.into(), new_jmap_id.into()),
            action: WriteAction::Update(document_id),
            fields: Vec::new(),
        }
    }

    pub fn update_jmap_id(&mut self, jmap_id: impl Into<JMAPId>) {
        match self.log_action {
            LogAction::Insert(ref mut id) => {
                *id = jmap_id.into();
            }
            LogAction::Update(ref mut id) => {
                *id = jmap_id.into();
            }
            LogAction::Delete(ref mut id) => {
                *id = jmap_id.into();
            }
            _ => (),
        }
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
