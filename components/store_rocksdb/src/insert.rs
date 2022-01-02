use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    sync::Arc,
};

use nlp::Language;
use rocksdb::BoundColumnFamily;
use store::{
    batch::{DocumentWriter, LogAction, WriteAction},
    field::{Text, TokenIterator, UpdateField},
    serialize::{
        serialize_acd_key_leb128, serialize_bm_internal, serialize_bm_tag_key,
        serialize_bm_term_key, serialize_bm_text_key, serialize_changelog_key, serialize_index_key,
        serialize_stored_key, serialize_stored_key_global, BM_FREED_IDS, BM_TOMBSTONED_IDS,
        BM_USED_IDS,
    },
    term_index::TermIndexBuilder,
    AccountId, CollectionId, DocumentId, StoreChangeLog, StoreError, StoreUpdate,
};

use crate::{
    bitmaps::{clear_bits, set_bits},
    changelog::ChangeLogWriter,
    document_id::AssignedDocumentId,
    RocksDBStore,
};

impl StoreUpdate for RocksDBStore {
    type UncommittedId = AssignedDocumentId;

    fn update_documents(
        &self,
        batches: Vec<DocumentWriter<AssignedDocumentId>>,
    ) -> store::Result<()> {
        let cf_values = self.get_handle("values")?;
        let cf_indexes = self.get_handle("indexes")?;
        let cf_bitmaps = self.get_handle("bitmaps")?;
        let mut write_batch = rocksdb::WriteBatch::default();

        let mut change_log_list = HashMap::new();
        let mut set_bitmap_list = HashMap::new();
        let mut clear_bitmap_list = HashMap::new();

        let mut collection_locks = HashMap::new();

        for batch in batches {
            match batch.action {
                WriteAction::Insert(document_id) => {
                    if let Entry::Vacant(e) =
                        collection_locks.entry((batch.account, batch.collection))
                    {
                        e.insert(self.lock_collection(batch.account, batch.collection)?);
                    }
                    let document_id = match document_id {
                        AssignedDocumentId::Freed(document_id) => {
                            // Remove document id from freed ids
                            clear_bitmap_list
                                .entry(serialize_bm_internal(
                                    batch.account,
                                    batch.collection,
                                    BM_FREED_IDS,
                                ))
                                .or_insert_with(HashSet::new)
                                .insert(document_id);
                            document_id
                        }
                        AssignedDocumentId::New(document_id) => document_id,
                    };

                    // Add document id to collection
                    set_bitmap_list
                        .entry(serialize_bm_internal(
                            batch.account,
                            batch.collection,
                            BM_USED_IDS,
                        ))
                        .or_insert_with(HashSet::new)
                        .insert(document_id);

                    self._update_document(
                        &mut write_batch,
                        &cf_values,
                        &cf_indexes,
                        &mut set_bitmap_list,
                        batch.account,
                        batch.collection,
                        document_id,
                        batch.default_language,
                        batch.fields,
                    )?;
                }
                WriteAction::Update(document_id) => {
                    if let Entry::Vacant(e) =
                        collection_locks.entry((batch.account, batch.collection))
                    {
                        e.insert(self.lock_collection(batch.account, batch.collection)?);
                    }
                    self._update_document(
                        &mut write_batch,
                        &cf_values,
                        &cf_indexes,
                        &mut set_bitmap_list,
                        batch.account,
                        batch.collection,
                        document_id,
                        batch.default_language,
                        batch.fields,
                    )?
                }
                WriteAction::Delete(document_id) => {
                    // Add document id to tombstoned ids
                    set_bitmap_list
                        .entry(serialize_bm_internal(
                            batch.account,
                            batch.collection,
                            BM_TOMBSTONED_IDS,
                        ))
                        .or_insert_with(HashSet::new)
                        .insert(document_id);
                }
                WriteAction::UpdateMany => {
                    self._update_global(
                        &mut write_batch,
                        &cf_values,
                        &cf_indexes,
                        &cf_bitmaps,
                        batch.account.into(),
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
                        batch.account.into(),
                        batch.collection.into(),
                        batch.fields,
                    )?;
                }
            }

            match batch.log_action {
                LogAction::Insert(id) => change_log_list
                    .entry((batch.account, batch.collection))
                    .or_insert_with(ChangeLogWriter::default)
                    .inserts
                    .push(id),
                LogAction::Update(id) => change_log_list
                    .entry((batch.account, batch.collection))
                    .or_insert_with(ChangeLogWriter::default)
                    .updates
                    .push(id),
                LogAction::Delete(id) => change_log_list
                    .entry((batch.account, batch.collection))
                    .or_insert_with(ChangeLogWriter::default)
                    .deletes
                    .push(id),
                LogAction::Move(old_id, id) => {
                    let change_log_list = change_log_list
                        .entry((batch.account, batch.collection))
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

        for (key, doc_id_list) in set_bitmap_list {
            write_batch.merge_cf(&cf_bitmaps, key, set_bits(doc_id_list.into_iter()))
        }

        for (key, doc_id_list) in clear_bitmap_list {
            write_batch.merge_cf(&cf_bitmaps, key, clear_bits(doc_id_list.into_iter()))
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
                UpdateField::Tag(ref tag) => {
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
        bitmap_list: &mut HashMap<Vec<u8>, HashSet<DocumentId>>,
        account: AccountId,
        collection: CollectionId,
        document_id: DocumentId,
        default_language: Language,
        fields: Vec<UpdateField>,
    ) -> crate::Result<()> {
        // Full text term positions
        let mut term_index = TermIndexBuilder::new();

        for field in fields {
            match &field {
                UpdateField::Text(t) => {
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
                                .or_insert_with(HashSet::new)
                                .insert(document_id);
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
                                    .or_insert_with(HashSet::new)
                                    .insert(document_id);
                            }
                            text
                        }
                        Text::Full((text, language)) => {
                            let terms = self.get_terms(TokenIterator::new(
                                text,
                                if *language == Language::Unknown {
                                    default_language
                                } else {
                                    *language
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
                                        .or_insert_with(HashSet::new)
                                        .insert(document_id);

                                    if term.id_stemmed > 0 {
                                        bitmap_list
                                            .entry(serialize_bm_term_key(
                                                account,
                                                collection,
                                                t.get_field(),
                                                term.id_stemmed,
                                                false,
                                            ))
                                            .or_insert_with(HashSet::new)
                                            .insert(document_id);
                                    }
                                }

                                term_index.add_item(t.get_field(), t.get_field_num(), terms);
                            }
                            text
                        }
                    };

                    if t.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(
                                account,
                                collection,
                                document_id,
                                t.get_field(),
                                t.get_field_num(),
                            ),
                            text.as_bytes(),
                        );
                    }

                    if t.is_sorted() {
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

                UpdateField::Tag(t) => {
                    bitmap_list
                        .entry(serialize_bm_tag_key(
                            account,
                            collection,
                            t.get_field(),
                            &t.value,
                        ))
                        .or_insert_with(HashSet::new)
                        .insert(document_id);
                }
                UpdateField::Blob(b) => {
                    batch.put_cf(
                        cf_values,
                        serialize_stored_key(
                            account,
                            collection,
                            document_id,
                            b.get_field(),
                            b.get_field_num(),
                        ),
                        &b.value,
                    );
                }
                UpdateField::Integer(i) => {
                    if i.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(
                                account,
                                collection,
                                document_id,
                                i.get_field(),
                                i.get_field_num(),
                            ),
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
                            serialize_stored_key(
                                account,
                                collection,
                                document_id,
                                i.get_field(),
                                i.get_field_num(),
                            ),
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
                            serialize_stored_key(
                                account,
                                collection,
                                document_id,
                                f.get_field(),
                                f.get_field_num(),
                            ),
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

        Ok(())
    }
}
