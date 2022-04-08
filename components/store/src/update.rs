use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    iter::FromIterator,
};

use nlp::Language;

use crate::{
    batch::{Change, WriteAction, WriteBatch},
    bitmap::{clear_bit, set_clear_bits},
    blob::BlobEntries,
    field::{Keywords, Tags, TextIndex, TokenIterator, UpdateField},
    leb128::Leb128,
    serialize::{BitmapKey, IndexKey, LogKey, StoreDeserialize, StoreSerialize, ValueKey},
    term_index::{TermIndex, TermIndexBuilder},
    AccountId, Collections, ColumnFamily, Direction, DocumentId, FieldId, JMAPStore, Store,
    StoreError, WriteOperation,
};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn write(&self, batch: WriteBatch) -> crate::Result<()> {
        let mut write_batch = Vec::with_capacity(batch.documents.len());
        let mut bitmap_list = HashMap::new();
        let mut tombstones = HashMap::new();

        for document in batch.documents {
            let (is_insert, document) = match document {
                WriteAction::Insert(document) => {
                    // Add document id to collection
                    bitmap_list
                        .entry(BitmapKey::serialize_document_ids(
                            batch.account_id,
                            document.collection,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document.document_id, true);

                    (true, document)
                }
                WriteAction::Update(document) => (false, document),
                WriteAction::Tombstone {
                    collection,
                    document_id,
                } => {
                    debug_assert!(!batch.changes.is_empty());
                    // Tombstone a document id
                    tombstones
                        .entry(collection)
                        .or_insert_with(HashSet::new)
                        .insert(document_id);
                    continue;
                }
                WriteAction::Delete {
                    collection,
                    document_id,
                } => {
                    // Remove document id from collection
                    bitmap_list
                        .entry(BitmapKey::serialize_document_ids(
                            batch.account_id,
                            collection,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document_id, false);

                    let term_index_key =
                        ValueKey::serialize_term_index(batch.account_id, collection, document_id);

                    // Delete values
                    let prefix = ValueKey::serialize_collection(batch.account_id, collection);
                    let merge_value = clear_bit(document_id);
                    for (key, value) in
                        self.db
                            .iterator(ColumnFamily::Values, &prefix, Direction::Forward)?
                    {
                        if !key.starts_with(&prefix) {
                            break;
                        } else if key.len() > prefix.len()
                            && DocumentId::from_leb128_bytes(&key[prefix.len()..])
                                .ok_or(StoreError::DataCorruption)?
                                .0
                                == document_id
                        {
                            if key[..] == term_index_key[..] {
                                // Delete terms
                                let term_index =
                                    TermIndex::deserialize(&value).ok_or_else(|| {
                                        StoreError::InternalError(
                                            "Failed to deserialize blob entries.".to_string(),
                                        )
                                    })?;

                                for term_group in
                                    term_index.uncompress_all_terms().map_err(|_| {
                                        StoreError::InternalError(
                                            "Failed to uncompress term index.".to_string(),
                                        )
                                    })?
                                {
                                    for exact_term_id in term_group.exact_terms {
                                        write_batch.push(WriteOperation::merge(
                                            ColumnFamily::Bitmaps,
                                            BitmapKey::serialize_term(
                                                batch.account_id,
                                                collection,
                                                term_group.field_id,
                                                exact_term_id,
                                                true,
                                            ),
                                            merge_value.clone(),
                                        ));
                                    }
                                    for stemmed_term_id in term_group.stemmed_terms {
                                        write_batch.push(WriteOperation::merge(
                                            ColumnFamily::Bitmaps,
                                            BitmapKey::serialize_term(
                                                batch.account_id,
                                                collection,
                                                term_group.field_id,
                                                stemmed_term_id,
                                                false,
                                            ),
                                            merge_value.clone(),
                                        ));
                                    }
                                }
                            } else if key.ends_with(&[ValueKey::BLOBS]) {
                                // Decrement blob count

                                BlobEntries::deserialize(&value)
                                    .ok_or_else(|| {
                                        StoreError::InternalError(
                                            "Failed to deserialize blob entries.".to_string(),
                                        )
                                    })?
                                    .items
                                    .into_iter()
                                    .for_each(|key| {
                                        write_batch.push(WriteOperation::merge(
                                            ColumnFamily::Values,
                                            key.as_key(),
                                            (-1i64).serialize().unwrap(),
                                        ));
                                    });
                            } else if key.ends_with(&[ValueKey::TAGS]) {
                                // Remove tags

                                let field_id = key[key.len() - 2];
                                for tag in Tags::deserialize(&value)
                                    .ok_or_else(|| {
                                        StoreError::InternalError(
                                            "Failed to deserialize tag list.".to_string(),
                                        )
                                    })?
                                    .items
                                {
                                    write_batch.push(WriteOperation::merge(
                                        ColumnFamily::Bitmaps,
                                        BitmapKey::serialize_tag(
                                            batch.account_id,
                                            collection,
                                            field_id,
                                            &tag,
                                        ),
                                        merge_value.clone(),
                                    ));
                                }
                            } else if key.ends_with(&[ValueKey::KEYWORDS]) {
                                // Remove keywords

                                for (keyword, fields) in Keywords::deserialize(&value)
                                    .ok_or_else(|| {
                                        StoreError::InternalError(
                                            "Failed to deserialize keywords list.".to_string(),
                                        )
                                    })?
                                    .items
                                {
                                    for field in fields {
                                        write_batch.push(WriteOperation::merge(
                                            ColumnFamily::Bitmaps,
                                            BitmapKey::serialize_keyword(
                                                batch.account_id,
                                                collection,
                                                field,
                                                &keyword,
                                            ),
                                            merge_value.clone(),
                                        ));
                                    }
                                }
                            }

                            write_batch
                                .push(WriteOperation::delete(ColumnFamily::Values, key.to_vec()));
                        }
                    }

                    // Delete indexes
                    let prefix = IndexKey::serialize_collection(batch.account_id, collection);
                    for (key, _) in
                        self.db
                            .iterator(ColumnFamily::Indexes, &prefix, Direction::Forward)?
                    {
                        if !key.starts_with(&prefix) {
                            break;
                        } else if key.len() > prefix.len()
                            && IndexKey::deserialize_document_id(&key)
                                .ok_or(StoreError::DataCorruption)?
                                == document_id
                        {
                            write_batch
                                .push(WriteOperation::delete(ColumnFamily::Indexes, key.to_vec()));
                        }
                    }

                    continue;
                }
            };

            // Tags and keywords
            let mut tagged_fields: HashMap<FieldId, Tags> = HashMap::new();
            let mut keywords = if !is_insert && document.has_keywords {
                self.db
                    .get(
                        ColumnFamily::Values,
                        &ValueKey::serialize_document_keywords_list(
                            batch.account_id,
                            document.collection,
                            document.document_id,
                        ),
                    )?
                    .unwrap_or_default()
            } else {
                Keywords::default()
            };

            // Full text term positions
            let mut term_index = TermIndexBuilder::new();
            let mut blob_fields = Vec::new();

            for field in document.fields {
                // TODO improve code below
                match field {
                    UpdateField::Text(t) => {
                        let is_stored = t.is_stored();
                        let is_clear = t.is_clear();
                        let is_sorted = t.is_sorted();
                        let blob_index = t.get_blob_index();

                        match t.value.index {
                            TextIndex::Keyword => {
                                merge_bitmap_clear(
                                    bitmap_list.entry(BitmapKey::serialize_keyword(
                                        batch.account_id,
                                        document.collection,
                                        t.field,
                                        &t.value.text,
                                    )),
                                    document.document_id,
                                    !is_clear,
                                );
                                if !is_clear {
                                    keywords.insert(t.value.text.clone(), t.field);
                                } else {
                                    keywords.remove(&t.value.text, &t.field);
                                }
                            }
                            TextIndex::Tokenized => {
                                for token in
                                    TokenIterator::new(&t.value.text, Language::English, false)
                                {
                                    merge_bitmap_clear(
                                        bitmap_list.entry(BitmapKey::serialize_keyword(
                                            batch.account_id,
                                            document.collection,
                                            t.field,
                                            &token.word,
                                        )),
                                        document.document_id,
                                        !is_clear,
                                    );
                                    if !is_clear {
                                        keywords.insert(token.word.into_owned(), t.field);
                                    } else {
                                        keywords.remove(token.word.as_ref(), &t.field);
                                    }
                                }
                            }
                            TextIndex::Full(language) => {
                                let terms = self.get_terms(TokenIterator::new(
                                    &t.value.text,
                                    if language == Language::Unknown {
                                        document.default_language
                                    } else {
                                        language
                                    },
                                    true,
                                ))?;

                                if !terms.is_empty() {
                                    for term in &terms {
                                        merge_bitmap_clear(
                                            bitmap_list.entry(BitmapKey::serialize_term(
                                                batch.account_id,
                                                document.collection,
                                                t.field,
                                                term.id,
                                                true,
                                            )),
                                            document.document_id,
                                            !is_clear,
                                        );

                                        if term.id_stemmed != term.id {
                                            merge_bitmap_clear(
                                                bitmap_list.entry(BitmapKey::serialize_term(
                                                    batch.account_id,
                                                    document.collection,
                                                    t.field,
                                                    term.id_stemmed,
                                                    false,
                                                )),
                                                document.document_id,
                                                !is_clear,
                                            );
                                        }
                                    }

                                    if !is_clear {
                                        term_index.add_item(
                                            t.field,
                                            blob_index.unwrap_or(0),
                                            terms,
                                        );
                                    }
                                }
                            }
                            TextIndex::None => {}
                        };

                        if let Some(blob_index) = blob_index {
                            blob_fields.push((blob_index, t.value.text.as_bytes().to_vec()));
                        } else if !is_clear {
                            if is_stored {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Values,
                                    ValueKey::serialize_value(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        t.field,
                                    ),
                                    t.value.text.as_bytes().to_vec(),
                                ));
                            }

                            if is_sorted {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Indexes,
                                    IndexKey::serialize(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        t.field,
                                        t.value.text.as_bytes(),
                                    ),
                                    vec![],
                                ));
                            }
                        } else {
                            if is_stored {
                                write_batch.push(WriteOperation::delete(
                                    ColumnFamily::Values,
                                    ValueKey::serialize_value(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        t.field,
                                    ),
                                ));
                            }

                            if is_sorted {
                                write_batch.push(WriteOperation::delete(
                                    ColumnFamily::Indexes,
                                    IndexKey::serialize(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        t.field,
                                        t.value.text.as_bytes(),
                                    ),
                                ));
                            }
                        }
                    }
                    UpdateField::Tag(tag) => {
                        let field = tag.get_field();
                        let set = !tag.is_clear();

                        bitmap_list
                            .entry(BitmapKey::serialize_tag(
                                batch.account_id,
                                document.collection,
                                field,
                                &tag.value,
                            ))
                            .or_insert_with(HashMap::new)
                            .insert(document.document_id, set);

                        match tagged_fields.entry(field) {
                            Entry::Occupied(mut entry) => {
                                if set {
                                    entry.get_mut().insert(tag.value);
                                } else {
                                    entry.get_mut().remove(&tag.value);
                                }
                            }
                            Entry::Vacant(entry) => {
                                let mut tag_list = if !is_insert {
                                    self.get_document_tags(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        field,
                                    )?
                                    .unwrap_or_default()
                                } else {
                                    Tags::default()
                                };
                                if set {
                                    tag_list.insert(tag.value);
                                } else {
                                    tag_list.remove(&tag.value);
                                }
                                entry.insert(tag_list);
                            }
                        }
                    }
                    UpdateField::Binary(b) => {
                        if let Some(blob_index) = b.get_blob_index() {
                            blob_fields.push((blob_index, b.value));
                        } else {
                            write_batch.push(WriteOperation::set(
                                ColumnFamily::Values,
                                ValueKey::serialize_value(
                                    batch.account_id,
                                    document.collection,
                                    document.document_id,
                                    b.get_field(),
                                ),
                                b.value,
                            ));
                        }
                    }
                    UpdateField::Number(number) => {
                        if !number.is_clear() {
                            if number.is_stored() {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Values,
                                    ValueKey::serialize_value(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        number.get_field(),
                                    ),
                                    number.value.serialize().unwrap(),
                                ));
                            }

                            if number.is_sorted() {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Indexes,
                                    IndexKey::serialize(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        number.get_field(),
                                        &number.value.to_be_bytes(),
                                    ),
                                    vec![],
                                ));
                            }
                        } else {
                            if number.is_stored() {
                                write_batch.push(WriteOperation::delete(
                                    ColumnFamily::Values,
                                    ValueKey::serialize_value(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        number.get_field(),
                                    ),
                                ));
                            }

                            if number.is_sorted() {
                                write_batch.push(WriteOperation::delete(
                                    ColumnFamily::Indexes,
                                    IndexKey::serialize(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        number.get_field(),
                                        &number.value.to_be_bytes(),
                                    ),
                                ));
                            }
                        }
                    }
                };
            }

            // Compress and store TermIndex
            if !term_index.is_empty() {
                write_batch.push(WriteOperation::set(
                    ColumnFamily::Values,
                    ValueKey::serialize_term_index(
                        batch.account_id,
                        document.collection,
                        document.document_id,
                    ),
                    term_index.compress(),
                ));
            }

            // Store external blobs
            if !blob_fields.is_empty() {
                let mut blob_entries = BlobEntries::new();

                blob_fields.sort_unstable_by_key(|(blob_index, _)| *blob_index);

                for (_, blob) in blob_fields {
                    let blob_entry = self.store_blob(&blob)?;

                    // Increment blob count
                    write_batch.push(WriteOperation::merge(
                        ColumnFamily::Values,
                        blob_entry.as_key(),
                        (1i64).serialize().unwrap(),
                    ));

                    blob_entries.add(blob_entry);
                }

                write_batch.push(WriteOperation::set(
                    ColumnFamily::Values,
                    ValueKey::serialize_document_blob(
                        batch.account_id,
                        document.collection,
                        document.document_id,
                    ),
                    blob_entries.serialize().unwrap(),
                ));
            }

            // Store tag lists
            for (field_id, tag_list) in tagged_fields {
                if tag_list.has_changed() {
                    write_batch.push(WriteOperation::set(
                        ColumnFamily::Values,
                        ValueKey::serialize_document_tag_list(
                            batch.account_id,
                            document.collection,
                            document.document_id,
                            field_id,
                        ),
                        tag_list.serialize().unwrap(),
                    ));
                }
            }

            // Store keyword lists
            if keywords.has_changed() {
                write_batch.push(WriteOperation::set(
                    ColumnFamily::Values,
                    ValueKey::serialize_document_keywords_list(
                        batch.account_id,
                        document.collection,
                        document.document_id,
                    ),
                    keywords.serialize().unwrap(),
                ));
            }
        }

        // Serialize Raft and change log
        if !batch.changes.is_empty() {
            let raft_id = self.assign_raft_id();
            let mut collections = Collections::default();

            for (collection, log_entry) in batch.changes {
                collections.insert(collection);

                write_batch.push(WriteOperation::set(
                    ColumnFamily::Logs,
                    LogKey::serialize_change(batch.account_id, collection, raft_id.index),
                    log_entry.serialize(),
                ));
            }

            // Serialize raft entry
            let mut bytes = Vec::with_capacity(
                std::mem::size_of::<AccountId>() + std::mem::size_of::<u64>() + 1,
            );
            bytes.push(Change::ENTRY);
            bytes.extend_from_slice(&batch.account_id.to_le_bytes());
            bytes.extend_from_slice(&collections.to_le_bytes());
            write_batch.push(WriteOperation::set(
                ColumnFamily::Logs,
                LogKey::serialize_raft(&raft_id),
                bytes,
            ));

            // Serialize raft tombstones
            if !tombstones.is_empty() {
                let tombstones_len = tombstones.len();
                let mut bytes = Vec::with_capacity(
                    ((std::mem::size_of::<DocumentId>() + std::mem::size_of::<usize>() + 1)
                        * tombstones_len)
                        + std::mem::size_of::<usize>(),
                );
                tombstones_len.to_leb128_bytes(&mut bytes);
                for (collection, document_ids) in tombstones {
                    bytes.push(collection as u8);
                    document_ids.len().to_leb128_bytes(&mut bytes);
                    for document_id in document_ids {
                        document_id.to_leb128_bytes(&mut bytes);
                    }
                }
                write_batch.push(WriteOperation::set(
                    ColumnFamily::Logs,
                    LogKey::serialize_tombstone(raft_id.index, batch.account_id),
                    bytes,
                ));
            }
        }

        // Update bitmaps
        for (key, doc_id_list) in bitmap_list {
            write_batch.push(WriteOperation::merge(
                ColumnFamily::Bitmaps,
                key,
                set_clear_bits(doc_id_list.into_iter()),
            ));
        }

        // Submit write batch
        self.db.write(write_batch)
    }
}

fn merge_bitmap_clear(
    entry: Entry<Vec<u8>, HashMap<DocumentId, bool>>,
    document_id: DocumentId,
    is_set: bool,
) {
    match entry {
        Entry::Occupied(mut bitmap_entry) => match bitmap_entry.get_mut().entry(document_id) {
            Entry::Occupied(mut document_entry) => {
                let is_set_current = *document_entry.get();
                if (is_set && !is_set_current) || (!is_set && is_set_current) {
                    *document_entry.get_mut() = true;
                }
            }
            Entry::Vacant(document_entry) => {
                document_entry.insert(is_set);
            }
        },
        Entry::Vacant(bitmap_entry) => {
            bitmap_entry.insert(HashMap::from_iter([(document_id, is_set)]));
        }
    }
}
