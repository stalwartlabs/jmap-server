use ahash::AHashMap;

use crate::{
    blob::BlobId,
    core::{bitmap::Bitmap, collection::Collection, document::MAX_TOKEN_LENGTH, error::StoreError},
    log::changes::ChangeId,
    nlp::{
        lang::{LanguageDetector, MIN_LANGUAGE_SCORE},
        stemmer::Stemmer,
        term_index::{TermIndexBuilder, TokenIndex},
        tokenizers::Tokenizer,
        Language,
    },
    serialize::{
        bitmap::set_clear_bits,
        key::{BitmapKey, BlobKey, IndexKey, LogKey, ValueKey},
        StoreDeserialize, StoreSerialize,
    },
    AccountId, ColumnFamily, JMAPStore, Store,
};

use super::{
    batch::{Change, WriteAction, WriteBatch},
    operation::WriteOperation,
    options::{IndexOptions, Options},
};

#[derive(Debug)]
pub struct Changes {
    pub collections: Bitmap<Collection>,
    pub change_id: ChangeId,
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn write(&self, mut batch: WriteBatch) -> crate::Result<Option<Changes>> {
        let mut ops = Vec::with_capacity(batch.documents.len());

        // Prepare linked batch
        if let Some(linked_batch) = batch.linked_batch.take() {
            self.prepare_batch(&mut ops, *linked_batch)?;
        }

        // Prepare main batch
        let changes = self.prepare_batch(&mut ops, batch)?;

        // Submit write batch
        self.db.write(ops)?;

        Ok(changes)
    }

    fn prepare_batch(
        &self,
        ops: &mut Vec<WriteOperation>,
        batch: WriteBatch,
    ) -> crate::Result<Option<Changes>> {
        let mut bitmap_list = AHashMap::default();

        let tombstone_deletions = self
            .tombstone_deletions
            .load(std::sync::atomic::Ordering::Relaxed);
        let mut tombstones = Vec::new();

        for document in batch.documents {
            let mut document = match document {
                WriteAction::Insert(document) => {
                    // Add document id to collection
                    bitmap_list
                        .entry(BitmapKey::serialize_document_ids(
                            batch.account_id,
                            document.collection,
                        ))
                        .or_insert_with(AHashMap::default)
                        .insert(document.document_id, true);

                    document
                }
                WriteAction::Update(document) => document,
                WriteAction::Delete(mut document) => {
                    if !tombstone_deletions {
                        // Remove document id from collection
                        bitmap_list
                            .entry(BitmapKey::serialize_document_ids(
                                batch.account_id,
                                document.collection,
                            ))
                            .or_insert_with(AHashMap::default)
                            .insert(document.document_id, false);

                        // Delete term index
                        let term_index_key = ValueKey::serialize_term_index(
                            batch.account_id,
                            document.collection,
                            document.document_id,
                        );
                        if let Some(blob_id) = self
                            .db
                            .get::<BlobId>(ColumnFamily::Values, &term_index_key)?
                        {
                            document.term_index = Some((blob_id, IndexOptions::new().clear()));
                        }

                        document
                    } else {
                        debug_assert!(!batch.changes.is_empty());
                        // Add to tombstones
                        tombstones.push(document);
                        continue;
                    }
                }
            };

            // Process text fields
            if !document.text_fields.is_empty() {
                // Detect language for unknown fields
                let mut lang_detector = LanguageDetector::new();
                document
                    .text_fields
                    .iter()
                    .filter(|field| {
                        field.options.is_full_text() && field.value.language == Language::Unknown
                    })
                    .for_each(|field| {
                        lang_detector.detect(&field.value.text, MIN_LANGUAGE_SCORE);
                    });
                let default_language = lang_detector
                    .most_frequent_language()
                    .unwrap_or(self.config.default_language);
                let mut term_index = TermIndexBuilder::new();

                for field in document.text_fields {
                    let is_clear = field.is_clear();

                    match field.options.get_text_options() {
                        <u64 as Options>::F_KEYWORD => {
                            bitmap_list
                                .entry(BitmapKey::serialize_term(
                                    batch.account_id,
                                    document.collection,
                                    field.field,
                                    &field.value.text,
                                    true,
                                ))
                                .or_insert_with(AHashMap::default)
                                .insert(document.document_id, !is_clear);
                        }
                        <u64 as Options>::F_TOKENIZE => {
                            for token in Tokenizer::new(
                                &field.value.text,
                                field.value.language,
                                MAX_TOKEN_LENGTH,
                            ) {
                                bitmap_list
                                    .entry(BitmapKey::serialize_term(
                                        batch.account_id,
                                        document.collection,
                                        field.field,
                                        token.word.as_ref(),
                                        true,
                                    ))
                                    .or_insert_with(AHashMap::default)
                                    .insert(document.document_id, !is_clear);
                            }
                        }
                        <u64 as Options>::F_NONE => (),
                        part_id => {
                            let language = if field.value.language != Language::Unknown {
                                field.value.language
                            } else {
                                default_language
                            };
                            let mut terms = Vec::new();

                            for token in Stemmer::new(&field.value.text, language, MAX_TOKEN_LENGTH)
                            {
                                bitmap_list
                                    .entry(BitmapKey::serialize_term(
                                        batch.account_id,
                                        document.collection,
                                        field.field,
                                        &token.word,
                                        true,
                                    ))
                                    .or_insert_with(AHashMap::default)
                                    .insert(document.document_id, !is_clear);

                                if let Some(stemmed_word) = token.stemmed_word.as_ref() {
                                    bitmap_list
                                        .entry(BitmapKey::serialize_term(
                                            batch.account_id,
                                            document.collection,
                                            field.field,
                                            stemmed_word,
                                            false,
                                        ))
                                        .or_insert_with(AHashMap::default)
                                        .insert(document.document_id, !is_clear);
                                }

                                terms.push(term_index.add_stemmed_token(token));
                            }

                            if !terms.is_empty() {
                                term_index.add_terms(
                                    field.field,
                                    (part_id - <u64 as Options>::F_FULL_TEXT) as u32,
                                    terms,
                                );
                            }
                        }
                    }

                    if field.is_stored() {
                        let key = ValueKey::serialize_value(
                            batch.account_id,
                            document.collection,
                            document.document_id,
                            field.field,
                        );
                        if !is_clear {
                            ops.push(WriteOperation::set(
                                ColumnFamily::Values,
                                key,
                                field.value.text.as_bytes().to_vec(),
                            ));
                        } else {
                            ops.push(WriteOperation::delete(ColumnFamily::Values, key));
                        }
                    }

                    if field.is_indexed() {
                        let key = IndexKey::serialize(
                            batch.account_id,
                            document.collection,
                            document.document_id,
                            field.field,
                            field.value.text.as_bytes(),
                        );
                        if !is_clear {
                            ops.push(WriteOperation::set(ColumnFamily::Indexes, key, vec![]));
                        } else {
                            ops.push(WriteOperation::delete(ColumnFamily::Indexes, key));
                        }
                    }
                }

                // Serialize term index as a linked blob.
                if !term_index.is_empty() {
                    let term_index_blob_id =
                        self.blob_store(&term_index.serialize().ok_or_else(|| {
                            StoreError::InternalError("Failed to serialize Term Index.".to_string())
                        })?)?;

                    ops.push(WriteOperation::set(
                        ColumnFamily::Values,
                        ValueKey::serialize_term_index(
                            batch.account_id,
                            document.collection,
                            document.document_id,
                        ),
                        term_index_blob_id.serialize().ok_or_else(|| {
                            StoreError::InternalError(
                                "Failed to serialize Term Index blobId.".to_string(),
                            )
                        })?,
                    ));

                    // Link blob
                    document
                        .blobs
                        .push((term_index_blob_id, IndexOptions::new()));
                }
            }

            // Add/remove terms from existing term index
            if let Some((term_index_id, options)) = document.term_index {
                let token_index =
                    TokenIndex::deserialize(&self.blob_get(&term_index_id)?.ok_or_else(|| {
                        StoreError::InternalError("Term Index blob not found.".to_string())
                    })?)
                    .ok_or_else(|| {
                        StoreError::InternalError("Failed to deserialize Term Index.".to_string())
                    })?;
                let is_clear = options.is_clear();
                for term in token_index.terms {
                    for (term_ids, is_exact) in
                        [(term.exact_terms, true), (term.stemmed_terms, false)]
                    {
                        for term_id in term_ids {
                            /*println!(
                                "Delete '{}' ({})",
                                token_index.tokens.get(term_id as usize).unwrap(),
                                is_exact
                            );*/
                            bitmap_list
                                .entry(BitmapKey::serialize_term(
                                    batch.account_id,
                                    document.collection,
                                    term.field_id,
                                    token_index.tokens.get(term_id as usize).ok_or_else(|| {
                                        StoreError::InternalError(
                                            "Corrupted term index.".to_string(),
                                        )
                                    })?,
                                    is_exact,
                                ))
                                .or_insert_with(AHashMap::default)
                                .insert(document.document_id, !is_clear);
                        }
                    }
                }

                let term_index_key = ValueKey::serialize_term_index(
                    batch.account_id,
                    document.collection,
                    document.document_id,
                );
                ops.push(if !is_clear {
                    WriteOperation::set(
                        ColumnFamily::Values,
                        term_index_key,
                        term_index_id.serialize().ok_or_else(|| {
                            StoreError::InternalError(
                                "Failed to serialize Term Index blobId.".to_string(),
                            )
                        })?,
                    )
                } else {
                    WriteOperation::Delete {
                        cf: ColumnFamily::Values,
                        key: term_index_key,
                    }
                });

                document.blobs.push((term_index_id, options));
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
                        ops.push(WriteOperation::set(
                            ColumnFamily::Values,
                            key,
                            field.value.serialize().unwrap(),
                        ));
                    } else {
                        ops.push(WriteOperation::delete(ColumnFamily::Values, key));
                    }
                }

                if field.is_indexed() {
                    let key = IndexKey::serialize(
                        batch.account_id,
                        document.collection,
                        document.document_id,
                        field.field,
                        &field.value.to_be_bytes(),
                    );
                    if !field.is_clear() {
                        ops.push(WriteOperation::set(ColumnFamily::Indexes, key, vec![]));
                    } else {
                        ops.push(WriteOperation::delete(ColumnFamily::Indexes, key));
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
                    .or_insert_with(AHashMap::default)
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
                ops.push(if !field.is_clear() {
                    WriteOperation::set(ColumnFamily::Values, key, field.value)
                } else {
                    WriteOperation::delete(ColumnFamily::Values, key)
                });
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
                ops.push(if is_set {
                    WriteOperation::set(ColumnFamily::Blobs, key, vec![])
                } else {
                    WriteOperation::delete(ColumnFamily::Blobs, key)
                });
            }

            // Process ACLs
            for (acl, options) in document.acls {
                let key = ValueKey::serialize_acl(
                    acl.id,
                    batch.account_id,
                    document.collection,
                    document.document_id,
                );
                if !options.is_clear() {
                    ops.push(WriteOperation::Set {
                        cf: ColumnFamily::Values,
                        key,
                        value: acl.acl.serialize().unwrap(),
                    });
                } else {
                    ops.push(WriteOperation::Delete {
                        cf: ColumnFamily::Values,
                        key,
                    });
                }
            }
        }

        // Update bitmaps
        for (key, doc_id_list) in bitmap_list {
            ops.push(WriteOperation::merge(
                ColumnFamily::Bitmaps,
                key,
                set_clear_bits(doc_id_list.into_iter()),
            ));
        }

        // Serialize Raft and change log
        if !batch.changes.is_empty() {
            let raft_id = self.assign_raft_id();
            let mut collections = Bitmap::default();

            for (collection, log_entry) in batch.changes {
                collections.insert(collection);

                ops.push(WriteOperation::set(
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
            ops.push(WriteOperation::set(
                ColumnFamily::Logs,
                LogKey::serialize_raft(&raft_id),
                bytes,
            ));

            // Serialize raft tombstones
            if !tombstones.is_empty() {
                ops.push(WriteOperation::set(
                    ColumnFamily::Logs,
                    LogKey::serialize_tombstone(raft_id.index, batch.account_id),
                    bincode::serialize(&tombstones).map_err(|_| {
                        StoreError::SerializeError("Failed to serialize tombstones".to_string())
                    })?,
                ));
            }

            Ok(Changes {
                collections,
                change_id: raft_id.index,
            }
            .into())
        } else {
            Ok(None)
        }
    }
}
