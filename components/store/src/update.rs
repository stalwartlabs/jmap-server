use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    iter::FromIterator,
};

use nlp::{
    stemmer::Stemmer,
    tokenizers::{Token, Tokenizer},
    Language,
};

use crate::{
    batch::{Change, WriteAction, WriteBatch, MAX_TOKEN_LENGTH},
    bitmap::{clear_bit, set_clear_bits},
    field::{Options, Tags, Text, UpdateField},
    leb128::Leb128,
    log::ChangeId,
    serialize::{BitmapKey, IndexKey, LogKey, StoreDeserialize, StoreSerialize, ValueKey},
    term_index::{Term, TermIndex, TermIndexBuilder},
    AccountId, Collections, ColumnFamily, Direction, DocumentId, FieldId, JMAPStore, Store,
    StoreError, WriteOperation,
};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn write(&self, batch: WriteBatch) -> crate::Result<Option<ChangeId>> {
        let mut write_batch = Vec::with_capacity(batch.documents.len());
        let mut bitmap_list = HashMap::new();
        let mut tombstones = HashMap::new();
        let mut change_id = None;

        for document in batch.documents {
            let (is_insert, mut document) = match document {
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
                    { /*
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
                                             BitmapKey::serialize_term(
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
                         }*/
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

            // Full text term positions
            let mut term_index = TermIndexBuilder::new();

            // Process text fields
            document.finalize();
            for field in document.text_fields {
                let is_stored = field.is_stored();
                let is_clear = field.is_clear();
                let is_sorted = field.is_sorted();
                let build_term_index = field.build_term_index();

                let text = match field.value {
                    Text::None { value } => value,
                    Text::Keyword { value } => {
                        merge_bitmap_clear(
                            bitmap_list.entry(BitmapKey::serialize_term(
                                batch.account_id,
                                document.collection,
                                field.field,
                                &value,
                                true,
                            )),
                            document.document_id,
                            !is_clear,
                        );
                        if build_term_index {
                            let term = term_index.add_token(Token {
                                word: value.clone().into(),
                                offset: 0,
                                len: value.len() as u8,
                            });
                            term_index.add_terms(field.field, 0, vec![term]);
                        }
                        value
                    }
                    Text::Tokenized { value } => {
                        let mut terms = Vec::new();

                        for token in Tokenizer::new(&value, Language::English, MAX_TOKEN_LENGTH) {
                            merge_bitmap_clear(
                                bitmap_list.entry(BitmapKey::serialize_term(
                                    batch.account_id,
                                    document.collection,
                                    field.field,
                                    &token.word,
                                    true,
                                )),
                                document.document_id,
                                !is_clear,
                            );
                            if build_term_index {
                                terms.push(term_index.add_token(token));
                            }
                        }

                        if !terms.is_empty() {
                            term_index.add_terms(field.field, 0, terms);
                        }

                        value
                    }
                    Text::Full {
                        value,
                        part_id,
                        language,
                    } => {
                        let mut terms = Vec::new();

                        for token in Stemmer::new(&value, language, MAX_TOKEN_LENGTH) {
                            merge_bitmap_clear(
                                bitmap_list.entry(BitmapKey::serialize_term(
                                    batch.account_id,
                                    document.collection,
                                    field.field,
                                    &token.word,
                                    true,
                                )),
                                document.document_id,
                                !is_clear,
                            );

                            if let Some(stemmed_word) = token.stemmed_word.as_ref() {
                                merge_bitmap_clear(
                                    bitmap_list.entry(BitmapKey::serialize_term(
                                        batch.account_id,
                                        document.collection,
                                        field.field,
                                        stemmed_word,
                                        false,
                                    )),
                                    document.document_id,
                                    !is_clear,
                                );
                            }

                            if build_term_index {
                                terms.push(term_index.add_stemmed_token(token));
                            }
                        }

                        if !terms.is_empty() {
                            term_index.add_terms(field.field, part_id, terms);
                        }

                        value
                    }
                };

                if is_stored {
                    let key = ValueKey::serialize_value(
                        batch.account_id,
                        document.collection,
                        document.document_id,
                        field.field,
                    );
                    if !is_clear {
                        write_batch.push(WriteOperation::set(
                            ColumnFamily::Values,
                            key,
                            text.as_bytes().to_vec(),
                        ));
                    } else {
                        write_batch.push(WriteOperation::delete(ColumnFamily::Values, key));
                    }
                }

                if is_sorted {
                    let key = IndexKey::serialize(
                        batch.account_id,
                        document.collection,
                        document.document_id,
                        field.field,
                        text.as_bytes(),
                    );
                    if !is_clear {
                        write_batch.push(WriteOperation::set(ColumnFamily::Indexes, key, vec![]));
                    } else {
                        write_batch.push(WriteOperation::delete(ColumnFamily::Indexes, key));
                    }
                }
            }

            // Process numeric values
            for field in document.number_fields {
                if field.is_stored() {
                    let key = ValueKey::serialize_value(
                        batch.account_id,
                        document.collection,
                        document.document_id,
                        field.field,
                    );
                    if !field.is_clear() {
                        write_batch.push(WriteOperation::set(
                            ColumnFamily::Values,
                            key,
                            field.value.serialize().unwrap(),
                        ));
                    } else {
                        write_batch.push(WriteOperation::delete(ColumnFamily::Values, key));
                    }
                }

                if field.is_sorted() {
                    let key = IndexKey::serialize(
                        batch.account_id,
                        document.collection,
                        document.document_id,
                        field.field,
                        &field.value.to_be_bytes(),
                    );
                    if !field.is_clear() {
                        write_batch.push(WriteOperation::set(ColumnFamily::Indexes, key, vec![]));
                    } else {
                        write_batch.push(WriteOperation::delete(ColumnFamily::Indexes, key));
                    }
                }
            }

            // Process tags
            for field in document.tag_fields {
                bitmap_list
                    .entry(BitmapKey::serialize_tag(
                        batch.account_id,
                        document.collection,
                        field.field,
                        &field.value,
                    ))
                    .or_insert_with(HashMap::new)
                    .insert(document.document_id, !field.is_clear());
            }

            // Process binary fields
            for field in document.binary_fields {
                write_batch.push(WriteOperation::set(
                    ColumnFamily::Values,
                    ValueKey::serialize_value(
                        batch.account_id,
                        document.collection,
                        document.document_id,
                        field.get_field(),
                    ),
                    field.value,
                ));
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
                    term_index.serialize().ok_or_else(|| {
                        StoreError::InternalError("Failed to serialize Term Index.".to_string())
                    })?,
                ));
            }

            // Store external blobs references
            for (id, index, options) in document.blobs {
                let is_set = !options.is_clear();
                // Increment blob count
                write_batch.push(WriteOperation::merge(
                    ColumnFamily::Values,
                    ValueKey::serialize_blob(&id),
                    if is_set { 1i64 } else { -1i64 }.serialize().unwrap(),
                ));

                // Store reference to blob
                let key = ValueKey::serialize_document_blob(
                    batch.account_id,
                    document.collection,
                    document.document_id,
                    index,
                );
                write_batch.push(if is_set {
                    WriteOperation::set(ColumnFamily::Values, key, id.serialize().unwrap())
                } else {
                    WriteOperation::delete(ColumnFamily::Values, key)
                });
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
            change_id = raft_id.index.into();

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
        self.db.write(write_batch)?;

        Ok(change_id)
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
