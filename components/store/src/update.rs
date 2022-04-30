use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    iter::FromIterator,
};

use nlp::{
    stemmer::Stemmer,
    tokenizers::{Token, Tokenizer},
};

use crate::{
    batch::{Change, WriteAction, WriteBatch, MAX_TOKEN_LENGTH},
    bitmap::set_clear_bits,
    field::{Options, Text},
    leb128::Leb128,
    log::ChangeId,
    serialize::{BitmapKey, BlobKey, IndexKey, LogKey, StoreSerialize, ValueKey},
    term_index::TermIndexBuilder,
    AccountId, Collections, ColumnFamily, DocumentId, JMAPStore, Store, StoreError, WriteOperation,
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
            let mut document = match document {
                WriteAction::Insert(document) => {
                    // Add document id to collection
                    bitmap_list
                        .entry(BitmapKey::serialize_document_ids(
                            batch.account_id,
                            document.collection,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document.document_id, true);

                    document
                }
                WriteAction::Update(document) => document,
                WriteAction::Tombstone(document) => {
                    debug_assert!(!batch.changes.is_empty());
                    // Tombstone a document id
                    tombstones
                        .entry(document.collection)
                        .or_insert_with(HashSet::new)
                        .insert(document.document_id);
                    continue;
                }
                WriteAction::Delete(document) => {
                    // Remove document id from collection
                    bitmap_list
                        .entry(BitmapKey::serialize_document_ids(
                            batch.account_id,
                            document.collection,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document.document_id, false);

                    document
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
                        value
                    }
                    Text::Tokenized { value, language } => {
                        for token in Tokenizer::new(&value, language, MAX_TOKEN_LENGTH) {
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

                            terms.push(term_index.add_stemmed_token(token));
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
                let key = ValueKey::serialize_value(
                    batch.account_id,
                    document.collection,
                    document.document_id,
                    field.get_field(),
                );
                write_batch.push(if !field.is_clear() {
                    WriteOperation::set(ColumnFamily::Values, key, field.value)
                } else {
                    WriteOperation::delete(ColumnFamily::Values, key)
                });
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
            for (id, options) in document.blobs {
                let is_set = !options.is_clear();
                // Store reference to blob
                let key = BlobKey::serialize_link(
                    &id,
                    batch.account_id,
                    document.collection,
                    document.document_id,
                );
                write_batch.push(if is_set {
                    WriteOperation::set(ColumnFamily::Blobs, key, vec![])
                } else {
                    WriteOperation::delete(ColumnFamily::Blobs, key)
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
