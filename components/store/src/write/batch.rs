use crate::core::document::Document;
use crate::serialize::leb128::Leb128;
use crate::{AccountId, Collection, DocumentId, JMAPId};
use std::collections::{HashMap, HashSet};

#[derive(Debug)]
pub enum WriteAction {
    Insert(Document),
    Update(Document),
    Delete(Document),
    Tombstone(Document),
}

pub struct WriteBatch {
    pub account_id: AccountId,
    pub changes: HashMap<Collection, Change>,
    pub documents: Vec<WriteAction>,
}

#[derive(Default)]
pub struct Change {
    pub inserts: HashSet<JMAPId>,
    pub updates: HashSet<JMAPId>,
    pub deletes: HashSet<JMAPId>,
    pub child_updates: HashSet<JMAPId>,
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
            documents: vec![WriteAction::Delete(Document::new(collection, document_id))],
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

    pub fn delete_document(&mut self, document: Document) {
        self.documents.push(WriteAction::Delete(document));
    }

    pub fn tombstone_document(&mut self, document: Document) {
        self.documents.push(WriteAction::Tombstone(document));
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

    pub fn take(&mut self) -> WriteBatch {
        WriteBatch {
            account_id: self.account_id,
            changes: std::mem::take(&mut self.changes),
            documents: std::mem::take(&mut self.documents),
        }
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
