use std::collections::HashMap;

use nlp::Language;

use crate::{
    batch::{WriteAction, WriteBatch},
    bitmap::set_clear_bits,
    blob::{BlobEntries, BlobEntry},
    changelog::{LogWriter, RaftId},
    field::{FieldOptions, Text, TokenIterator, UpdateField},
    serialize::{
        serialize_acd_key_leb128, serialize_blob_key, serialize_bm_internal, serialize_bm_tag_key,
        serialize_bm_term_key, serialize_bm_text_key, serialize_index_key, serialize_stored_key,
        StoreSerialize, BLOB_KEY, BM_TOMBSTONED_IDS, BM_USED_IDS,
    },
    term_index::TermIndexBuilder,
    AccountId, ColumnFamily, JMAPStore, Store, StoreError, WriteOperation,
};

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn set(&self, cf: ColumnFamily, key: Vec<u8>, value: Vec<u8>) -> crate::Result<()> {
        let db = self.db.clone();
        self.spawn_blocking(move || db.set(cf, key, value)).await
    }

    pub async fn merge(&self, cf: ColumnFamily, key: Vec<u8>, value: Vec<u8>) -> crate::Result<()> {
        let db = self.db.clone();
        self.spawn_blocking(move || db.merge(cf, key, value)).await
    }

    pub async fn write(&self, batch: Vec<WriteOperation>) -> crate::Result<()> {
        let db = self.db.clone();
        self.spawn_blocking(move || db.write(batch)).await
    }

    pub async fn update_documents(
        &self,
        account_id: AccountId,
        raft_id: RaftId,
        batches: Vec<WriteBatch>,
    ) -> crate::Result<()> {
        let mut write_batch = Vec::with_capacity(batches.len());
        let mut change_log = LogWriter::new(account_id, raft_id);
        let mut bitmap_list = HashMap::new();

        for batch in batches {
            let update_id = match batch.action {
                WriteAction::Insert(document_id) => {
                    // Add document id to collection
                    bitmap_list
                        .entry(serialize_bm_internal(
                            account_id,
                            batch.collection_id,
                            BM_USED_IDS,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document_id, true);

                    Some(document_id)
                }
                WriteAction::Update(document_id) => Some(document_id),
                WriteAction::Delete(document_id) => {
                    // Remove any external blobs
                    if let Some(blob) = self
                        .get::<BlobEntries>(
                            ColumnFamily::Values,
                            serialize_blob_key(account_id, batch.collection_id, document_id),
                        )
                        .await?
                    {
                        // Decrement blob count
                        blob.items.into_iter().for_each(|key| {
                            write_batch.push(WriteOperation::merge(
                                ColumnFamily::Values,
                                key.as_key(),
                                (-1i64).to_le_bytes().into(),
                            ));
                        });
                    }

                    // Add document id to tombstoned ids
                    bitmap_list
                        .entry(serialize_bm_internal(
                            account_id,
                            batch.collection_id,
                            BM_TOMBSTONED_IDS,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document_id, true);
                    None
                }
            };

            if let Some(document_id) = update_id {
                // Full text term positions
                let mut term_index = TermIndexBuilder::new();
                let mut blob_fields = Vec::new();

                for field in batch.fields {
                    match field {
                        UpdateField::Text(t) => {
                            let (is_stored, is_sorted, is_clear, blob_index) = match t.get_options()
                            {
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
                                            account_id,
                                            batch.collection_id,
                                            t.field,
                                            &text,
                                        ))
                                        .or_insert_with(HashMap::new)
                                        .insert(document_id, !is_clear);
                                    text
                                }
                                Text::Tokenized(text) => {
                                    for token in TokenIterator::new(&text, Language::English, false)
                                    {
                                        bitmap_list
                                            .entry(serialize_bm_text_key(
                                                account_id,
                                                batch.collection_id,
                                                t.field,
                                                &token.word,
                                            ))
                                            .or_insert_with(HashMap::new)
                                            .insert(document_id, !is_clear);
                                    }
                                    text
                                }
                                Text::Full(ft) => {
                                    let terms = self
                                        .get_terms(TokenIterator::new(
                                            &ft.text,
                                            if ft.language == Language::Unknown {
                                                batch.default_language
                                            } else {
                                                ft.language
                                            },
                                            true,
                                        ))
                                        .await?;
                                    if !terms.is_empty() {
                                        for term in &terms {
                                            bitmap_list
                                                .entry(serialize_bm_term_key(
                                                    account_id,
                                                    batch.collection_id,
                                                    t.field,
                                                    term.id,
                                                    true,
                                                ))
                                                .or_insert_with(HashMap::new)
                                                .insert(document_id, !is_clear);

                                            if term.id_stemmed != term.id {
                                                bitmap_list
                                                    .entry(serialize_bm_term_key(
                                                        account_id,
                                                        batch.collection_id,
                                                        t.field,
                                                        term.id_stemmed,
                                                        false,
                                                    ))
                                                    .or_insert_with(HashMap::new)
                                                    .insert(document_id, !is_clear);
                                            }
                                        }

                                        term_index.add_item(
                                            t.field,
                                            blob_index.unwrap_or(0),
                                            terms,
                                        );
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
                                            account_id,
                                            batch.collection_id,
                                            document_id,
                                            t.field,
                                        ),
                                        text.as_bytes().to_vec(),
                                    ));
                                }

                                if is_sorted {
                                    write_batch.push(WriteOperation::set(
                                        ColumnFamily::Indexes,
                                        serialize_index_key(
                                            account_id,
                                            batch.collection_id,
                                            document_id,
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
                                        account_id,
                                        batch.collection_id,
                                        document_id,
                                        t.field,
                                    ),
                                ));

                                write_batch.push(WriteOperation::delete(
                                    ColumnFamily::Indexes,
                                    serialize_index_key(
                                        account_id,
                                        batch.collection_id,
                                        document_id,
                                        t.field,
                                        text.as_bytes(),
                                    ),
                                ));
                            }
                        }
                        UpdateField::Tag(t) => {
                            bitmap_list
                                .entry(serialize_bm_tag_key(
                                    account_id,
                                    batch.collection_id,
                                    t.get_field(),
                                    &t.value,
                                ))
                                .or_insert_with(HashMap::new)
                                .insert(document_id, !t.is_clear());
                        }
                        UpdateField::Binary(b) => {
                            if let FieldOptions::StoreAsBlob(blob_index) = b.get_options() {
                                blob_fields.push((blob_index, b.value));
                            } else {
                                write_batch.push(WriteOperation::set(
                                    ColumnFamily::Values,
                                    serialize_stored_key(
                                        account_id,
                                        batch.collection_id,
                                        document_id,
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
                                            account_id,
                                            batch.collection_id,
                                            document_id,
                                            i.get_field(),
                                        ),
                                        i.value.to_le_bytes().into(),
                                    ));
                                }

                                if i.is_sorted() {
                                    write_batch.push(WriteOperation::set(
                                        ColumnFamily::Indexes,
                                        serialize_index_key(
                                            account_id,
                                            batch.collection_id,
                                            document_id,
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
                                        account_id,
                                        batch.collection_id,
                                        document_id,
                                        i.get_field(),
                                    ),
                                ));

                                write_batch.push(WriteOperation::delete(
                                    ColumnFamily::Indexes,
                                    serialize_index_key(
                                        account_id,
                                        batch.collection_id,
                                        document_id,
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
                                            account_id,
                                            batch.collection_id,
                                            document_id,
                                            i.get_field(),
                                        ),
                                        i.value.to_le_bytes().into(),
                                    ));
                                }

                                if i.is_sorted() {
                                    write_batch.push(WriteOperation::set(
                                        ColumnFamily::Indexes,
                                        serialize_index_key(
                                            account_id,
                                            batch.collection_id,
                                            document_id,
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
                                        account_id,
                                        batch.collection_id,
                                        document_id,
                                        i.get_field(),
                                    ),
                                ));

                                write_batch.push(WriteOperation::delete(
                                    ColumnFamily::Indexes,
                                    serialize_index_key(
                                        account_id,
                                        batch.collection_id,
                                        document_id,
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
                                            account_id,
                                            batch.collection_id,
                                            document_id,
                                            f.get_field(),
                                        ),
                                        f.value.to_le_bytes().into(),
                                    ));
                                }

                                if f.is_sorted() {
                                    write_batch.push(WriteOperation::set(
                                        ColumnFamily::Indexes,
                                        serialize_index_key(
                                            account_id,
                                            batch.collection_id,
                                            document_id,
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
                                        account_id,
                                        batch.collection_id,
                                        document_id,
                                        f.get_field(),
                                    ),
                                ));

                                write_batch.push(WriteOperation::delete(
                                    ColumnFamily::Indexes,
                                    serialize_index_key(
                                        account_id,
                                        batch.collection_id,
                                        document_id,
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
                        serialize_acd_key_leb128(account_id, batch.collection_id, document_id),
                        term_index.compress(),
                    ));
                }

                // Store external blobs
                if !blob_fields.is_empty() {
                    let mut blob_entries = BlobEntries::new();

                    blob_fields.sort_unstable_by_key(|(blob_index, _)| *blob_index);

                    for (_, blob) in blob_fields {
                        let blob_entry = self.store_blob(&blob).await?;

                        // Increment blob count
                        write_batch.push(WriteOperation::merge(
                            ColumnFamily::Values,
                            blob_entry.as_key(),
                            (1i64).to_le_bytes().into(),
                        ));

                        blob_entries.add(blob_entry);
                    }

                    write_batch.push(WriteOperation::set(
                        ColumnFamily::Values,
                        serialize_blob_key(account_id, batch.collection_id, document_id),
                        blob_entries.serialize().unwrap(),
                    ));
                }
            }

            change_log.add_change(
                batch.collection_id,
                if let Some(change_id) = batch.log_id {
                    change_id
                } else {
                    self.assign_change_id(account_id, batch.collection_id)
                        .await?
                },
                batch.log_action,
            );
        }

        // Write Raft and change log
        for (key, value) in change_log.serialize() {
            write_batch.push(WriteOperation::set(ColumnFamily::Logs, key, value));
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
        self.write(write_batch).await
    }
}
