use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use nlp::Language;
use rocksdb::BoundColumnFamily;
use store::{
    batch::{WriteAction, WriteOperation},
    field::{IndexField, Text, TokenIterator},
    serialize::{
        serialize_acd_key_leb128, serialize_bm_internal, serialize_bm_tag_key,
        serialize_bm_term_key, serialize_bm_text_key, serialize_index_key, serialize_stored_key,
        BM_USED_IDS,
    },
    term_index::TermIndexBuilder,
    DocumentId, StoreError, StoreUpdate,
};

use crate::{bitmaps::set_bits, RocksDBStore};

impl StoreUpdate for RocksDBStore {
    fn update_bulk(&self, batches: Vec<WriteOperation>) -> store::Result<Vec<DocumentId>> {
        let cf_values = self.get_handle("values")?;
        let cf_indexes = self.get_handle("indexes")?;
        let cf_bitmaps = self.get_handle("bitmaps")?;
        let mut write_batch = rocksdb::WriteBatch::default();
        let mut uncommitted_ids = Vec::with_capacity(batches.len());
        let mut bitmap_list = HashMap::new();

        for batch in batches {
            match batch.get_action() {
                WriteAction::Insert => {
                    let uncommitted_id = self
                        .reserve_document_id(batch.get_account_id(), batch.get_collection_id())?;
                    self._update_document(
                        &mut write_batch,
                        &cf_values,
                        &cf_indexes,
                        &mut bitmap_list,
                        batch,
                        uncommitted_id.get_id(),
                    )?;
                    uncommitted_ids.push(uncommitted_id);
                }
                WriteAction::Update(document_id) => self._update_document(
                    &mut write_batch,
                    &cf_values,
                    &cf_indexes,
                    &mut bitmap_list,
                    batch,
                    document_id,
                )?,
                WriteAction::DeleteAll => {
                    self._delete(
                        &mut write_batch,
                        &cf_values,
                        &cf_indexes,
                        &cf_bitmaps,
                        batch,
                    )?;
                }
                WriteAction::Delete(_) => unimplemented!(),
            }
        }

        for (key, doc_id_list) in bitmap_list {
            write_batch.merge_cf(&cf_bitmaps, key, set_bits(doc_id_list.into_iter()))
        }

        self.db
            .write(write_batch)
            .map_err(|e| StoreError::InternalError(e.to_string()))?;

        Ok(uncommitted_ids
            .into_iter()
            .map(|mut id| id.commit())
            .collect())
    }
}

impl RocksDBStore {
    #[allow(clippy::too_many_arguments)]
    fn _delete(
        &self,
        write_batch: &mut rocksdb::WriteBatch,
        _cf_values: &Arc<BoundColumnFamily>,
        _cf_indexes: &Arc<BoundColumnFamily>,
        cf_bitmaps: &Arc<BoundColumnFamily>,
        batch: WriteOperation,
    ) -> crate::Result<()> {
        let account_id = batch.get_account_id();
        let collection_id = batch.get_collection_id();

        for field in batch {
            match field {
                IndexField::Tag(ref tag) => {
                    write_batch.delete_cf(
                        cf_bitmaps,
                        &serialize_bm_tag_key(
                            account_id,
                            collection_id,
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
        document: WriteOperation,
        document_id: DocumentId,
    ) -> crate::Result<()> {
        let account = document.get_account_id();
        let collection = document.get_collection_id();
        let default_language = document.get_default_language();

        // Add document id to collection
        bitmap_list
            .entry(serialize_bm_internal(account, collection, BM_USED_IDS))
            .or_insert_with(HashSet::new)
            .insert(document_id);

        // Full text term positions
        let mut term_index = TermIndexBuilder::new();

        for field in document {
            match &field {
                IndexField::Text(t) => {
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

                IndexField::Tag(t) => {
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
                IndexField::Blob(b) => {
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
                IndexField::Integer(i) => {
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
                IndexField::LongInteger(i) => {
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
                IndexField::Float(f) => {
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
