use std::{
    collections::HashMap,
    sync::{Arc, MutexGuard},
};

use nlp::Language;
use rocksdb::BoundColumnFamily;
use store::{
    batch::{DocumentWriter, LogAction, WriteAction},
    field::{FieldOptions, Text, TokenIterator, UpdateField},
    serialize::{
        serialize_acd_key_leb128, serialize_blob_key, serialize_bm_internal, serialize_bm_tag_key,
        serialize_bm_term_key, serialize_bm_text_key, serialize_changelog_key, serialize_index_key,
        serialize_stored_key, serialize_stored_key_global, BLOB_KEY, BM_FREED_IDS,
        BM_TOMBSTONED_IDS, BM_USED_IDS,
    },
    term_index::TermIndexBuilder,
    AccountId, CollectionId, DocumentId, StoreBlob, StoreChangeLog, StoreError, StoreUpdate,
};

use crate::{
    bitmaps::set_clear_bits, blob::serialize_blob_keys_from_value, changelog::ChangeLogWriter,
    document_id::AssignedDocumentId, RocksDBStore,
};

impl StoreUpdate for RocksDBStore {
    type UncommittedId = AssignedDocumentId;

    fn update_documents(
        &self,
        account: AccountId,
        batches: Vec<DocumentWriter<AssignedDocumentId>>,
        lock_collection: Option<CollectionId>,
    ) -> store::Result<()> {
        let cf_values = self.get_handle("values")?;
        let cf_indexes = self.get_handle("indexes")?;
        let cf_bitmaps = self.get_handle("bitmaps")?;
        let mut write_batch = rocksdb::WriteBatch::default();

        let mut change_log_list = HashMap::new();
        let mut bitmap_list = HashMap::new();

        let _collection_lock = if let Some(lock_collection) = lock_collection {
            self.lock_collection(account, lock_collection)?.into()
        } else {
            None
        };

        for batch in batches {
            match batch.action {
                WriteAction::Insert(document_id) => {
                    let document_id = match document_id {
                        AssignedDocumentId::Freed(document_id) => {
                            // Remove document id from freed ids
                            bitmap_list
                                .entry(serialize_bm_internal(
                                    account,
                                    batch.collection,
                                    BM_FREED_IDS,
                                ))
                                .or_insert_with(HashMap::new)
                                .insert(document_id, false);
                            document_id
                        }
                        AssignedDocumentId::New(document_id) => document_id,
                    };

                    // Add document id to collection
                    bitmap_list
                        .entry(serialize_bm_internal(
                            account,
                            batch.collection,
                            BM_USED_IDS,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document_id, true);

                    self._update_document(
                        &mut write_batch,
                        &cf_values,
                        &cf_indexes,
                        &mut bitmap_list,
                        account,
                        batch.collection,
                        document_id,
                        batch.default_language,
                        batch.fields,
                    )?;
                }
                WriteAction::Update(document_id) => {
                    self._update_document(
                        &mut write_batch,
                        &cf_values,
                        &cf_indexes,
                        &mut bitmap_list,
                        account,
                        batch.collection,
                        document_id,
                        batch.default_language,
                        batch.fields,
                    )?;
                }
                WriteAction::Delete(document_id) => {
                    // Remove any external blobs
                    if let Some(blob) = self
                        .db
                        .get_cf(
                            &cf_values,
                            &serialize_blob_key(account, batch.collection, document_id),
                        )
                        .map_err(|e| StoreError::InternalError(e.into_string()))?
                    {
                        // Decrement blob count
                        serialize_blob_keys_from_value(&blob)
                            .ok_or(StoreError::DataCorruption)?
                            .into_iter()
                            .for_each(|key| {
                                write_batch.merge_cf(&cf_values, &key, (-1i64).to_le_bytes());
                            });
                    }

                    // Add document id to tombstoned ids
                    bitmap_list
                        .entry(serialize_bm_internal(
                            account,
                            batch.collection,
                            BM_TOMBSTONED_IDS,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document_id, true);
                }
                WriteAction::UpdateMany => {
                    self._update_global(
                        &mut write_batch,
                        &cf_values,
                        &cf_indexes,
                        &cf_bitmaps,
                        account.into(),
                        batch.collection.into(),
                        batch.fields,
                    )?;
                }
                WriteAction::DeleteMany => {
                    self._delete_global(
                        &mut write_batch,
                        &cf_values,
                        &cf_indexes,
                        &cf_bitmaps,
                        account.into(),
                        batch.collection.into(),
                        batch.fields,
                    )?;
                }
            }

            match batch.log_action {
                LogAction::Insert(id) => change_log_list
                    .entry((account, batch.collection))
                    .or_insert_with(ChangeLogWriter::default)
                    .inserts
                    .push(id),
                LogAction::Update(id) => change_log_list
                    .entry((account, batch.collection))
                    .or_insert_with(ChangeLogWriter::default)
                    .updates
                    .push(id),
                LogAction::Delete(id) => change_log_list
                    .entry((account, batch.collection))
                    .or_insert_with(ChangeLogWriter::default)
                    .deletes
                    .push(id),
                LogAction::Move(old_id, id) => {
                    let change_log_list = change_log_list
                        .entry((account, batch.collection))
                        .or_insert_with(ChangeLogWriter::default);
                    change_log_list.inserts.push(id);
                    change_log_list.deletes.push(old_id);
                }
                LogAction::None => (),
            }
        }

        if !change_log_list.is_empty() {
            let cf_log = self.get_handle("log")?;
            for ((account, collection), change_log) in change_log_list {
                let change_id = self
                    .get_last_change_id(account, collection)?
                    .map(|id| id + 1)
                    .unwrap_or(0);
                // TODO find better key name for change id
                write_batch.put_cf(
                    &cf_values,
                    serialize_stored_key_global(account.into(), collection.into(), None),
                    &change_id.to_le_bytes(),
                );
                write_batch.put_cf(
                    &cf_log,
                    serialize_changelog_key(account, collection, change_id),
                    change_log.serialize(),
                );
            }
        }

        for (key, doc_id_list) in bitmap_list {
            write_batch.merge_cf(&cf_bitmaps, key, set_clear_bits(doc_id_list.into_iter()))
        }

        self.db
            .write(write_batch)
            .map_err(|e| StoreError::InternalError(e.to_string()))?;

        Ok(())
    }

    fn assign_document_id(
        &self,
        account: AccountId,
        collection: CollectionId,
        last_assigned_id: Option<Self::UncommittedId>,
    ) -> store::Result<AssignedDocumentId> {
        if let Some(last_assigned_id) = last_assigned_id {
            match last_assigned_id {
                AssignedDocumentId::Freed(last_assigned_id) => {
                    if let Some(mut freed_ids) = self.get_document_ids_freed(account, collection)? {
                        freed_ids.remove_range(0..=last_assigned_id);
                        if !freed_ids.is_empty() {
                            return Ok(AssignedDocumentId::Freed(freed_ids.min().unwrap()));
                        }
                    }
                }
                AssignedDocumentId::New(last_assigned_id) => {
                    return Ok(AssignedDocumentId::New(last_assigned_id + 1));
                }
            }
        } else if let Some(freed_ids) = self.get_document_ids_freed(account, collection)? {
            return Ok(AssignedDocumentId::Freed(freed_ids.min().unwrap()));
        };

        Ok(
            if let Some(used_ids) = self.get_document_ids_used(account, collection)? {
                AssignedDocumentId::New(used_ids.max().unwrap() + 1)
            } else {
                AssignedDocumentId::New(0)
            },
        )
    }

    fn lock_collection(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> store::Result<MutexGuard<usize>> {
        self.account_lock
            .lock(
                ((account as u64) << (8 * std::mem::size_of::<CollectionId>())) | collection as u64,
            )
            .map_err(|_| StoreError::InternalError("Failed to obtain mutex".to_string()))
    }
}

impl RocksDBStore {
    #[allow(clippy::too_many_arguments)]
    fn _update_global(
        &self,
        write_batch: &mut rocksdb::WriteBatch,
        cf_values: &Arc<BoundColumnFamily>,
        _cf_indexes: &Arc<BoundColumnFamily>,
        _cf_bitmaps: &Arc<BoundColumnFamily>,
        account: Option<AccountId>,
        collection: Option<CollectionId>,
        fields: Vec<UpdateField>,
    ) -> crate::Result<()> {
        for field in fields {
            match field {
                UpdateField::LongInteger(ref i) => {
                    write_batch.put_cf(
                        cf_values,
                        serialize_stored_key_global(account, collection, i.get_field().into()),
                        &i.value.to_le_bytes(),
                    );
                }
                UpdateField::Integer(ref i) => {
                    write_batch.put_cf(
                        cf_values,
                        serialize_stored_key_global(account, collection, i.get_field().into()),
                        &i.value.to_le_bytes(),
                    );
                }
                UpdateField::Float(ref f) => {
                    write_batch.put_cf(
                        cf_values,
                        serialize_stored_key_global(account, collection, f.get_field().into()),
                        &f.value.to_le_bytes(),
                    );
                }
                _ => unimplemented!(),
            };
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn _delete_global(
        &self,
        write_batch: &mut rocksdb::WriteBatch,
        _cf_values: &Arc<BoundColumnFamily>,
        _cf_indexes: &Arc<BoundColumnFamily>,
        cf_bitmaps: &Arc<BoundColumnFamily>,
        account: Option<AccountId>,
        collection: Option<CollectionId>,
        fields: Vec<UpdateField>,
    ) -> crate::Result<()> {
        for field in fields {
            match field {
                UpdateField::TagSet(ref tag) | UpdateField::TagRemove(ref tag) => {
                    write_batch.delete_cf(
                        cf_bitmaps,
                        &serialize_bm_tag_key(
                            account.unwrap(),
                            collection.unwrap(),
                            *field.get_field(),
                            &tag.value,
                        ),
                    );
                }
                _ => unimplemented!(),
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn _update_document(
        &self,
        batch: &mut rocksdb::WriteBatch,
        cf_values: &Arc<BoundColumnFamily>,
        cf_indexes: &Arc<BoundColumnFamily>,
        bitmap_list: &mut HashMap<Vec<u8>, HashMap<DocumentId, bool>>,
        account: AccountId,
        collection: CollectionId,
        document_id: DocumentId,
        default_language: Language,
        fields: Vec<UpdateField>,
    ) -> crate::Result<()> {
        // Full text term positions
        let mut term_index = TermIndexBuilder::new();
        let mut blob_fields = Vec::new();

        for field in fields.iter() {
            match field {
                UpdateField::Text(t) => {
                    let (is_stored, is_sorted, blob_index) = match t.get_options() {
                        FieldOptions::None => (false, false, None),
                        FieldOptions::Store => (true, false, None),
                        FieldOptions::Sort => (false, true, None),
                        FieldOptions::StoreAndSort => (true, true, None),
                        FieldOptions::StoreAsBlob(blob_index) => (false, false, Some(blob_index)),
                    };

                    let text = match &t.value {
                        Text::Default(text) => text,
                        Text::Keyword(text) => {
                            bitmap_list
                                .entry(serialize_bm_text_key(
                                    account,
                                    collection,
                                    t.get_field(),
                                    text,
                                ))
                                .or_insert_with(HashMap::new)
                                .insert(document_id, true);
                            text
                        }
                        Text::Tokenized(text) => {
                            for token in TokenIterator::new(text, Language::English, false) {
                                bitmap_list
                                    .entry(serialize_bm_text_key(
                                        account,
                                        collection,
                                        t.get_field(),
                                        &token.word,
                                    ))
                                    .or_insert_with(HashMap::new)
                                    .insert(document_id, true);
                            }
                            text
                        }
                        Text::Full(ft) => {
                            let terms = self.get_terms(TokenIterator::new(
                                ft.text.as_ref(),
                                if ft.language == Language::Unknown {
                                    default_language
                                } else {
                                    ft.language
                                },
                                true,
                            ))?;
                            if !terms.is_empty() {
                                for term in &terms {
                                    bitmap_list
                                        .entry(serialize_bm_term_key(
                                            account,
                                            collection,
                                            t.get_field(),
                                            term.id,
                                            true,
                                        ))
                                        .or_insert_with(HashMap::new)
                                        .insert(document_id, true);

                                    if term.id_stemmed > 0 {
                                        bitmap_list
                                            .entry(serialize_bm_term_key(
                                                account,
                                                collection,
                                                t.get_field(),
                                                term.id_stemmed,
                                                false,
                                            ))
                                            .or_insert_with(HashMap::new)
                                            .insert(document_id, true);
                                    }
                                }

                                term_index.add_item(t.get_field(), blob_index.unwrap_or(0), terms);
                            }
                            &ft.text
                        }
                    };

                    if let Some(blob_index) = blob_index {
                        blob_fields.push((blob_index, text.as_bytes()));
                    } else {
                        if is_stored {
                            batch.put_cf(
                                cf_values,
                                serialize_stored_key(
                                    account,
                                    collection,
                                    document_id,
                                    t.get_field(),
                                ),
                                text.as_bytes(),
                            );
                        }

                        if is_sorted {
                            batch.put_cf(
                                cf_indexes,
                                &serialize_index_key(
                                    account,
                                    collection,
                                    document_id,
                                    t.get_field(),
                                    text.as_bytes(),
                                ),
                                &[],
                            );
                        }
                    }
                }
                UpdateField::TagSet(t) => {
                    bitmap_list
                        .entry(serialize_bm_tag_key(
                            account,
                            collection,
                            t.get_field(),
                            &t.value,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document_id, true);
                }
                UpdateField::TagRemove(t) => {
                    bitmap_list
                        .entry(serialize_bm_tag_key(
                            account,
                            collection,
                            t.get_field(),
                            &t.value,
                        ))
                        .or_insert_with(HashMap::new)
                        .insert(document_id, false);
                }
                UpdateField::Binary(b) => {
                    if let FieldOptions::StoreAsBlob(blob_index) = b.get_options() {
                        blob_fields.push((blob_index, b.value.as_ref()));
                    } else {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(account, collection, document_id, b.get_field()),
                            &b.value,
                        );
                    }
                }
                UpdateField::Integer(i) => {
                    if i.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(account, collection, document_id, i.get_field()),
                            &i.value.to_le_bytes(),
                        );
                    }

                    if i.is_sorted() {
                        batch.put_cf(
                            cf_indexes,
                            &serialize_index_key(
                                account,
                                collection,
                                document_id,
                                i.get_field(),
                                &i.value.to_be_bytes(),
                            ),
                            &[],
                        );
                    }
                }
                UpdateField::LongInteger(i) => {
                    if i.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(account, collection, document_id, i.get_field()),
                            &i.value.to_le_bytes(),
                        );
                    }

                    if i.is_sorted() {
                        batch.put_cf(
                            cf_indexes,
                            &serialize_index_key(
                                account,
                                collection,
                                document_id,
                                i.get_field(),
                                &i.value.to_be_bytes(),
                            ),
                            &[],
                        );
                    }
                }
                UpdateField::Float(f) => {
                    if f.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(account, collection, document_id, f.get_field()),
                            &f.value.to_le_bytes(),
                        );
                    }

                    if f.is_sorted() {
                        batch.put_cf(
                            cf_indexes,
                            &serialize_index_key(
                                account,
                                collection,
                                document_id,
                                f.get_field(),
                                &f.value.to_be_bytes(),
                            ),
                            &[],
                        );
                    }
                }
            };
        }

        // Compress and store TermIndex
        if !term_index.is_empty() {
            batch.put_cf(
                cf_values,
                &serialize_acd_key_leb128(account, collection, document_id),
                &term_index.compress(),
            );
        }

        // Store external blobs
        if !blob_fields.is_empty() {
            let mut blob_index_last = None;
            let mut blob_entries = Vec::with_capacity(
                std::mem::size_of::<usize>()
                    + (blob_fields.len() * (32 + std::mem::size_of::<u32>())),
            );

            blob_fields.sort_unstable_by_key(|(blob_index, _)| *blob_index);

            for (blob_index, blob) in &blob_fields {
                if let Some(blob_index_last) = blob_index_last {
                    if blob_index_last + 1 != *blob_index {
                        return Err(StoreError::InternalError(
                            "Blob indexes are not sequential".into(),
                        ));
                    }
                } else if *blob_index != 0 {
                    return Err(StoreError::InternalError(
                        "First blob index is not 0".into(),
                    ));
                }
                blob_index_last = Some(blob_index);
                let blob_key = self.store_blob(blob)?;

                // Increment blob count
                batch.merge_cf(cf_values, &blob_key, (1i64).to_le_bytes());

                blob_entries.extend_from_slice(&blob_key[BLOB_KEY.len()..]);
            }

            batch.put_cf(
                cf_values,
                &serialize_blob_key(account, collection, document_id),
                &blob_entries,
            );
        }
        Ok(())
    }
}
