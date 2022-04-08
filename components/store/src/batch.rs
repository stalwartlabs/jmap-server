use std::collections::{HashMap, HashSet};

use nlp::Language;

use crate::{
    field::{Field, Number, Text, TextIndex, UpdateField},
    leb128::Leb128,
    AccountId, Collection, DocumentId, FieldId, JMAPId, Tag,
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
    pub has_keywords: bool,
}
#[derive(Debug)]
pub enum WriteAction {
    Insert(Document),
    Update(Document),
    Delete {
        collection: Collection,
        document_id: DocumentId,
    },
    Tombstone {
        collection: Collection,
        document_id: DocumentId,
    },
}

pub struct WriteBatch {
    pub account_id: AccountId,
    pub changes: HashMap<Collection, Change>,
    pub documents: Vec<WriteAction>,
    pub set_tombstones: bool,
}

#[derive(Default)]
pub struct Change {
    pub inserts: HashSet<JMAPId>,
    pub updates: HashSet<JMAPId>,
    pub deletes: HashSet<JMAPId>,
    pub child_updates: HashSet<JMAPId>,
}

impl Document {
    pub fn new(collection: Collection, document_id: DocumentId) -> Document {
        Document {
            collection,
            document_id,
            default_language: Language::English,
            fields: Vec::new(),
            has_keywords: false,
        }
    }

    pub fn set_default_language(&mut self, language: Language) {
        self.default_language = language;
    }

    pub fn text(&mut self, field: impl Into<FieldId>, value: Text, options: u64) {
        if !self.has_keywords && matches!(value.index, TextIndex::Keyword | TextIndex::Tokenized) {
            self.has_keywords = true;
        }
        self.fields
            .push(UpdateField::Text(Field::new(field.into(), value, options)));
    }

    pub fn binary(&mut self, field: impl Into<FieldId>, value: Vec<u8>, options: u64) {
        self.fields.push(UpdateField::Binary(Field::new(
            field.into(),
            value,
            options,
        )));
    }

    pub fn number(&mut self, field: impl Into<FieldId>, value: impl Into<Number>, options: u64) {
        self.fields.push(UpdateField::Number(Field::new(
            field.into(),
            value.into(),
            options,
        )));
    }

    pub fn tag(&mut self, field: impl Into<FieldId>, value: Tag, options: u64) {
        self.fields
            .push(UpdateField::Tag(Field::new(field.into(), value, options)));
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
    pub fn new(account_id: AccountId, set_tombstones: bool) -> Self {
        WriteBatch {
            account_id,
            changes: HashMap::new(),
            documents: Vec::new(),
            set_tombstones,
        }
    }

    pub fn insert(account_id: AccountId, document: Document) -> Self {
        WriteBatch {
            account_id,
            changes: HashMap::new(),
            documents: vec![WriteAction::Insert(document)],
            set_tombstones: false,
        }
    }

    pub fn delete(
        account_id: AccountId,
        collection: Collection,
        document_id: DocumentId,
        set_tombstones: bool,
    ) -> Self {
        WriteBatch {
            account_id,
            changes: HashMap::new(),
            documents: vec![WriteAction::Delete {
                collection,
                document_id,
            }],
            set_tombstones,
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
        self.documents.push(if self.set_tombstones {
            WriteAction::Tombstone {
                collection,
                document_id,
            }
        } else {
            WriteAction::Delete {
                collection,
                document_id,
            }
        });
    }

    pub fn log_insert(&mut self, collection: Collection, jmap_id: impl Into<JMAPId>) {
        self.changes
            .entry(collection)
            .or_insert_with(Change::new)
            .inserts
            .insert(jmap_id.into());
    }

    pub fn log_update(&mut self, collection: Collection, jmap_id: impl Into<JMAPId>) {
        self.changes
            .entry(collection)
            .or_insert_with(Change::new)
            .updates
            .insert(jmap_id.into());
    }

    pub fn log_child_update(&mut self, collection: Collection, jmap_id: impl Into<JMAPId>) {
        self.changes
            .entry(collection)
            .or_insert_with(Change::new)
            .child_updates
            .insert(jmap_id.into());
    }

    pub fn log_delete(&mut self, collection: Collection, jmap_id: impl Into<JMAPId>) {
        self.changes
            .entry(collection)
            .or_insert_with(Change::new)
            .deletes
            .insert(jmap_id.into());
    }

    pub fn log_move(
        &mut self,
        collection: Collection,
        old_jmap_id: impl Into<JMAPId>,
        new_jmap_id: impl Into<JMAPId>,
    ) {
        let change = self.changes.entry(collection).or_insert_with(Change::new);
        change.deletes.insert(old_jmap_id.into());
        change.inserts.insert(new_jmap_id.into());
    }
}

impl From<Change> for Vec<u8> {
    fn from(writer: Change) -> Self {
        writer.serialize()
    }
}

impl Change {
    pub const ENTRY: u8 = 0;
    pub const SNAPSHOT: u8 = 1;

    pub fn new() -> Self {
        Change::default()
    }

    pub fn serialize(self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(
            1 + (self.inserts.len()
                + self.updates.len()
                + self.child_updates.len()
                + self.deletes.len()
                + 4)
                * std::mem::size_of::<usize>(),
        );
        buf.push(Change::ENTRY);

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
}
