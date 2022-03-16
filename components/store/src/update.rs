use std::collections::HashMap;

use nlp::Language;

use crate::{
    batch::{self, WriteAction, WriteBatch},
    bitmap::set_clear_bits,
    blob::BlobEntries,
    field::{FieldOptions, Text, TokenIterator, UpdateField},
    leb128::Leb128,
    serialize::{
        serialize_acd_key_leb128, serialize_blob_key, serialize_bm_internal, serialize_bm_tag_key,
        serialize_bm_term_key, serialize_bm_text_key, serialize_index_key, serialize_stored_key,
        StoreSerialize, BM_TOMBSTONED_IDS, BM_USED_IDS,
    },
    term_index::TermIndexBuilder,
    AccountId, Collection, ColumnFamily, JMAPId, JMAPStore, Store, WriteOperation,
};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn write(&self, batch: WriteBatch) -> crate::Result<()> {
        let mut write_batch = Vec::with_capacity(batch.documents.len());
        let mut bitmap_list = HashMap::new();

        for document in batch.documents {
            let document = match document {
                WriteAction::Insert(document) => {
                    // Add document id to collection
                    bitmap_list
                        .entry(serialize_bm_internal(
                            batch.account_id,
                            document.collection,
                            BM_USED_IDS,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document.document_id, true);

                    document
                }
                WriteAction::Update(document) => document,
                WriteAction::Delete {
                    collection,
                    document_id,
                } => {
                    // Remove any external blobs
                    if let Some(blob) = self.db.get::<BlobEntries>(
                        ColumnFamily::Values,
                        &serialize_blob_key(batch.account_id, collection, document_id),
                    )? {
                        // Decrement blob count
                        blob.items.into_iter().for_each(|key| {
                            write_batch.push(WriteOperation::merge(
                                ColumnFamily::Values,
                                key.as_key(),
                                (-1i64).serialize().unwrap(),
                            ));
                        });
                    }

                    // Add document id to tombstoned ids
                    bitmap_list
                        .entry(serialize_bm_internal(
                            batch.account_id,
                            collection,
                            BM_TOMBSTONED_IDS,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document_id, true);
                    continue;
                }
            };

            // Full text term positions
            let mut term_index = TermIndexBuilder::new();
            let mut blob_fields = Vec::new();

            for field in document.fields {
                // TODO improve code below
                match field {
                    UpdateField::Text(t) => {
                        let (is_stored, is_sorted, is_clear, blob_index) = match t.get_options() {
                            FieldOptions::None => (false, false, false, None),
                            FieldOptions::Store => (true, false, false, None),
                            FieldOptions::Sort => (false, true, false, None),
                            FieldOptions::StoreAndSort => (true, true, false, None),
                            FieldOptions::StoreAsBlob(blob_index) => {
                                (false, false, false, Some(blob_index))
                            }
                            FieldOptions::Clear => (false, false, true, None),
                        };

                        let text = match t.value {
                            Text::Default(text) => text,
                            Text::Keyword(text) => {
                                bitmap_list
                                    .entry(serialize_bm_text_key(
                                        batch.account_id,
                                        document.collection,
                                        t.field,
                                        &text,
                                    ))
                                    .or_insert_with(HashMap::new)
                                    .insert(document.document_id, !is_clear);
                                text
                            }
                            Text::Tokenized(text) => {
                                for token in TokenIterator::new(&text, Language::English, false) {
                                    bitmap_list
                                        .entry(serialize_bm_text_key(
                                            batch.account_id,
                                            document.collection,
                                            t.field,
                                            &token.word,
                                        ))
                                        .or_insert_with(HashMap::new)
                                        .insert(document.document_id, !is_clear);
                                }
                                text
                            }
                            Text::Full(ft) => {
                                let terms = self.get_terms(TokenIterator::new(
                                    &ft.text,
                                    if ft.language == Language::Unknown {
                                        document.default_language
                                    } else {
                                        ft.language
                                    },
                                    true,
                                ))?;

                                if !terms.is_empty() {
                                    for term in &terms {
                                        bitmap_list
                                            .entry(serialize_bm_term_key(
                                                batch.account_id,
                                                document.collection,
                                                t.field,
                                                term.id,
                                                true,
                                            ))
                                            .or_insert_with(HashMap::new)
                                            .insert(document.document_id, !is_clear);

                                        if term.id_stemmed != term.id {
                                            bitmap_list
                                                .entry(serialize_bm_term_key(
                                                    batch.account_id,
                                                    document.collection,
                                                    t.field,
                                                    term.id_stemmed,
                                                    false,
                                                ))
                                                .or_insert_with(HashMap::new)
                                                .insert(document.document_id, !is_clear);
                                        }
                                    }

                                    term_index.add_item(t.field, blob_index.unwrap_or(0), terms);
                                }
                                ft.text
                            }
                        };

                        if let Some(blob_index) = blob_index {
                            blob_fields.push((blob_index, text.as_bytes().to_vec()));
                        } else if !is_clear {
                            if is_stored {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Values,
                                    serialize_stored_key(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        t.field,
                                    ),
                                    text.as_bytes().to_vec(),
                                ));
                            }

                            if is_sorted {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Indexes,
                                    serialize_index_key(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        t.field,
                                        text.as_bytes(),
                                    ),
                                    vec![],
                                ));
                            }
                        } else {
                            write_batch.push(WriteOperation::delete(
                                ColumnFamily::Values,
                                serialize_stored_key(
                                    batch.account_id,
                                    document.collection,
                                    document.document_id,
                                    t.field,
                                ),
                            ));

                            write_batch.push(WriteOperation::delete(
                                ColumnFamily::Indexes,
                                serialize_index_key(
                                    batch.account_id,
                                    document.collection,
                                    document.document_id,
                                    t.field,
                                    text.as_bytes(),
                                ),
                            ));
                        }
                    }
                    UpdateField::Tag(t) => {
                        bitmap_list
                            .entry(serialize_bm_tag_key(
                                batch.account_id,
                                document.collection,
                                t.get_field(),
                                &t.value,
                            ))
                            .or_insert_with(HashMap::new)
                            .insert(document.document_id, !t.is_clear());
                    }
                    UpdateField::Binary(b) => {
                        if let FieldOptions::StoreAsBlob(blob_index) = b.get_options() {
                            blob_fields.push((blob_index, b.value));
                        } else {
                            write_batch.push(WriteOperation::set(
                                ColumnFamily::Values,
                                serialize_stored_key(
                                    batch.account_id,
                                    document.collection,
                                    document.document_id,
                                    b.get_field(),
                                ),
                                b.value,
                            ));
                        }
                    }
                    UpdateField::Integer(i) => {
                        if !i.is_clear() {
                            if i.is_stored() {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Values,
                                    serialize_stored_key(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        i.get_field(),
                                    ),
                                    i.value.serialize().unwrap(),
                                ));
                            }

                            if i.is_sorted() {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Indexes,
                                    serialize_index_key(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        i.get_field(),
                                        &i.value.to_be_bytes(),
                                    ),
                                    vec![],
                                ));
                            }
                        } else {
                            write_batch.push(WriteOperation::delete(
                                ColumnFamily::Values,
                                serialize_stored_key(
                                    batch.account_id,
                                    document.collection,
                                    document.document_id,
                                    i.get_field(),
                                ),
                            ));

                            write_batch.push(WriteOperation::delete(
                                ColumnFamily::Indexes,
                                serialize_index_key(
                                    batch.account_id,
                                    document.collection,
                                    document.document_id,
                                    i.get_field(),
                                    &i.value.to_be_bytes(),
                                ),
                            ));
                        }
                    }
                    UpdateField::LongInteger(i) => {
                        if !i.is_clear() {
                            if i.is_stored() {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Values,
                                    serialize_stored_key(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        i.get_field(),
                                    ),
                                    i.value.serialize().unwrap(),
                                ));
                            }

                            if i.is_sorted() {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Indexes,
                                    serialize_index_key(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        i.get_field(),
                                        &i.value.to_be_bytes(),
                                    ),
                                    vec![],
                                ));
                            }
                        } else {
                            write_batch.push(WriteOperation::delete(
                                ColumnFamily::Values,
                                serialize_stored_key(
                                    batch.account_id,
                                    document.collection,
                                    document.document_id,
                                    i.get_field(),
                                ),
                            ));

                            write_batch.push(WriteOperation::delete(
                                ColumnFamily::Indexes,
                                serialize_index_key(
                                    batch.account_id,
                                    document.collection,
                                    document.document_id,
                                    i.get_field(),
                                    &i.value.to_be_bytes(),
                                ),
                            ));
                        }
                    }
                    UpdateField::Float(f) => {
                        if !f.is_clear() {
                            if f.is_stored() {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Values,
                                    serialize_stored_key(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        f.get_field(),
                                    ),
                                    f.value.serialize().unwrap(),
                                ));
                            }

                            if f.is_sorted() {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Indexes,
                                    serialize_index_key(
                                        batch.account_id,
                                        document.collection,
                                        document.document_id,
                                        f.get_field(),
                                        &f.value.to_be_bytes(),
                                    ),
                                    vec![],
                                ));
                            }
                        } else {
                            write_batch.push(WriteOperation::delete(
                                ColumnFamily::Values,
                                serialize_stored_key(
                                    batch.account_id,
                                    document.collection,
                                    document.document_id,
                                    f.get_field(),
                                ),
                            ));

                            write_batch.push(WriteOperation::delete(
                                ColumnFamily::Indexes,
                                serialize_index_key(
                                    batch.account_id,
                                    document.collection,
                                    document.document_id,
                                    f.get_field(),
                                    &f.value.to_be_bytes(),
                                ),
                            ));
                        }
                    }
                };
            }

            // Compress and store TermIndex
            if !term_index.is_empty() {
                write_batch.push(WriteOperation::set(
                    ColumnFamily::Values,
                    serialize_acd_key_leb128(
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
                    serialize_blob_key(batch.account_id, document.collection, document.document_id),
                    blob_entries.serialize().unwrap(),
                ));
            }
        }

        // Serialize Raft and change log
        if !batch.changes.is_empty() {
            let mut raft_bytes = Vec::with_capacity(
                std::mem::size_of::<AccountId>()
                    + std::mem::size_of::<usize>()
                    + (batch.changes.len()
                        * (std::mem::size_of::<JMAPId>() + std::mem::size_of::<Collection>())),
            );

            batch.account_id.to_leb128_bytes(&mut raft_bytes);
            batch.changes.len().to_leb128_bytes(&mut raft_bytes);

            for (collection, log_entry) in batch.changes {
                let change_id = self.assign_change_id(batch.account_id, collection)?;
                raft_bytes.push(collection.into());
                change_id.to_leb128_bytes(&mut raft_bytes);

                write_batch.push(WriteOperation::set(
                    ColumnFamily::Logs,
                    batch::Change::serialize_key(batch.account_id, collection, change_id),
                    log_entry.serialize(),
                ));
            }

            write_batch.push(WriteOperation::set(
                ColumnFamily::Logs,
                self.assign_raft_id().serialize_key(),
                raft_bytes,
            ));
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
