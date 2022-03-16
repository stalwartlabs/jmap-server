use std::collections::HashMap;

use nlp::Language;

use crate::{
    changes::ChangeId,
    field::{Field, FieldOptions, Text, UpdateField},
    leb128::Leb128,
    serialize::{DeserializeBigEndian, COLLECTION_PREFIX_LEN, FIELD_PREFIX_LEN},
    AccountId, Collection, DocumentId, FieldId, Float, Integer, JMAPId, LongInteger, Tag,
};

pub const MAX_TOKEN_LENGTH: usize = 40;
pub const MAX_ID_LENGTH: usize = 80;
pub const MAX_SORT_FIELD_LENGTH: usize = 255;

#[derive(Debug)]
pub struct Document {
    pub collection: Collection,
    pub document_id: DocumentId,
    pub default_language: Language,
    pub fields: Vec<UpdateField>,
}
#[derive(Debug)]
pub enum WriteAction {
    Insert(Document),
    Update(Document),
    Delete {
        collection: Collection,
        document_id: DocumentId,
    },
}

pub struct WriteBatch {
    pub account_id: AccountId,
    pub changes: HashMap<Collection, Change>,
    pub documents: Vec<WriteAction>,
}

#[derive(Default)]
pub struct Change {
    pub inserts: Vec<JMAPId>,
    pub updates: Vec<JMAPId>,
    pub deletes: Vec<JMAPId>,
    pub child_updates: Vec<JMAPId>,
}

impl Document {
    pub fn new(collection: Collection, document_id: DocumentId) -> Document {
        Document {
            collection,
            document_id,
            default_language: Language::English,
            fields: Vec::new(),
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

impl IntoIterator for Document {
    type Item = UpdateField;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.into_iter()
    }
}

impl WriteBatch {
    pub fn new(account_id: AccountId) -> Self {
        WriteBatch {
            account_id,
            changes: HashMap::new(),
            documents: Vec::new(),
        }
    }

    pub fn insert(account_id: AccountId, document: Document) -> Self {
        WriteBatch {
            account_id,
            changes: HashMap::new(),
            documents: vec![WriteAction::Insert(document)],
        }
    }

    pub fn delete(account_id: AccountId, collection: Collection, document_id: DocumentId) -> Self {
        WriteBatch {
            account_id,
            changes: HashMap::new(),
            documents: vec![WriteAction::Delete {
                collection,
                document_id,
            }],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.documents.is_empty() && self.changes.is_empty()
    }

    pub fn insert_document(&mut self, document: Document) {
        self.documents.push(WriteAction::Insert(document));
    }

    pub fn update_document(&mut self, document: Document) {
        self.documents.push(WriteAction::Update(document));
    }

    pub fn delete_document(&mut self, collection: Collection, document_id: DocumentId) {
        self.documents.push(WriteAction::Delete {
            collection,
            document_id,
        });
    }

    pub fn log_insert(&mut self, collection: Collection, jmap_id: impl Into<JMAPId>) {
        self.changes
            .entry(collection)
            .or_insert_with(Change::new)
            .inserts
            .push(jmap_id.into());
    }

    pub fn log_update(&mut self, collection: Collection, jmap_id: impl Into<JMAPId>) {
        self.changes
            .entry(collection)
            .or_insert_with(Change::new)
            .updates
            .push(jmap_id.into());
    }

    pub fn log_child_update(&mut self, collection: Collection, jmap_id: impl Into<JMAPId>) {
        self.changes
            .entry(collection)
            .or_insert_with(Change::new)
            .child_updates
            .push(jmap_id.into());
    }

    pub fn log_delete(&mut self, collection: Collection, jmap_id: impl Into<JMAPId>) {
        self.changes
            .entry(collection)
            .or_insert_with(Change::new)
            .deletes
            .push(jmap_id.into());
    }

    pub fn log_move(
        &mut self,
        collection: Collection,
        old_jmap_id: impl Into<JMAPId>,
        new_jmap_id: impl Into<JMAPId>,
    ) {
        let change = self.changes.entry(collection).or_insert_with(Change::new);
        change.deletes.push(old_jmap_id.into());
        change.inserts.push(new_jmap_id.into());
    }
}

impl From<Change> for Vec<u8> {
    fn from(writer: Change) -> Self {
        writer.serialize()
    }
}

//TODO delete old changelog entries
impl Change {
    pub fn new() -> Self {
        Change::default()
    }

    pub fn serialize(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            (self.inserts.len()
                + self.updates.len()
                + self.child_updates.len()
                + self.deletes.len()
                + 4)
                * std::mem::size_of::<usize>(),
        );
        self.inserts.len().to_leb128_bytes(&mut buf);
        self.updates.len().to_leb128_bytes(&mut buf);
        self.child_updates.len().to_leb128_bytes(&mut buf);
        self.deletes.len().to_leb128_bytes(&mut buf);
        for list in [self.inserts, self.updates, self.child_updates, self.deletes] {
            for id in list {
                id.to_leb128_bytes(&mut buf);
            }
        }
        buf
    }

    pub fn serialize_key(
        account: AccountId,
        collection: Collection,
        change_id: ChangeId,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(FIELD_PREFIX_LEN + std::mem::size_of::<ChangeId>());
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes.push(collection.into());
        bytes.extend_from_slice(&change_id.to_be_bytes());
        bytes
    }

    pub fn deserialize_change_id(bytes: &[u8]) -> Option<ChangeId> {
        bytes.deserialize_be_u64(COLLECTION_PREFIX_LEN)
    }
}
