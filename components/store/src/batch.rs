use std::collections::{BTreeMap, HashMap, HashSet};

use nlp::{
    lang::{LanguageDetector, MIN_LANGUAGE_SCORE},
    Language,
};

use crate::{
    blob::BlobId,
    field::{Field, Number, Options, Text, F_FULL_TEXT, F_KEYWORD, F_NONE, F_TOKENIZE},
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
    pub lang_detect: Option<LanguageDetector>,

    pub text_fields: Vec<Field<Text>>,
    pub number_fields: Vec<Field<Number>>,
    pub binary_fields: Vec<Field<Vec<u8>>>,
    pub tag_fields: Vec<Field<Tag>>,
    pub blobs: Vec<(BlobId, u64)>,
}

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
            lang_detect: None,
            text_fields: Vec::new(),
            number_fields: Vec::new(),
            binary_fields: Vec::new(),
            tag_fields: Vec::new(),
            blobs: Vec::new(),
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
            match options.get_text_options() {
                F_NONE => Text::None { value },
                F_KEYWORD => Text::Keyword { value },
                F_TOKENIZE => Text::Tokenized { value, language },
                part_id => Text::Full {
                    part_id: (part_id - F_FULL_TEXT) as u32,
                    language: if !matches!(language, Language::Unknown) {
                        language
                    } else {
                        self.lang_detect
                            .get_or_insert_with(LanguageDetector::new)
                            .detect(value.as_ref(), MIN_LANGUAGE_SCORE)
                    },
                    value,
                },
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

    pub fn finalize(&mut self) {
        if let Some(lang_detect) = &self.lang_detect {
            let default_language = lang_detect
                .most_frequent_language()
                .unwrap_or(Language::English);
            for text in self.text_fields.iter_mut() {
                if let Text::Full { language, .. } = &mut text.value {
                    if *language == Language::Unknown {
                        *language = default_language;
                    }
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.text_fields.is_empty()
            && self.number_fields.is_empty()
            && self.binary_fields.is_empty()
            && self.tag_fields.is_empty()
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
            documents: vec![WriteAction::Delete(Document::new(collection, document_id))],
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

    pub fn delete_document(&mut self, document: Document) {
        self.documents.push(if self.set_tombstones {
            WriteAction::Tombstone(document)
        } else {
            WriteAction::Delete(document)
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
