use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use nlp::Language;
use rocksdb::{BoundColumnFamily, WriteBatch};
use store::{
    document::{DocumentBuilder, IndexOptions},
    field::{IndexField, TokenIterator},
    serialize::{
        serialize_acd_key_leb128, serialize_bm_internal, serialize_bm_tag_key,
        serialize_bm_term_key, serialize_bm_text_key, serialize_index_key, serialize_stored_key,
        BM_USED_IDS,
    },
    term_index::TermIndexBuilder,
    AccountId, CollectionId, DocumentId, StoreError, StoreInsert,
};

use crate::{bitmaps::set_bits, document_id::UncommittedDocumentId, RocksDBStore};

impl StoreInsert for RocksDBStore {
    fn insert_bulk(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: Vec<DocumentBuilder>,
    ) -> store::Result<Vec<DocumentId>> {
        let cf_values = self.get_handle("values")?;
        let cf_indexes = self.get_handle("indexes")?;
        let cf_bitmaps = self.get_handle("bitmaps")?;
        let mut batch = WriteBatch::default();
        let mut document_ids = Vec::with_capacity(documents.len());
        let mut bitmap_list = HashMap::new();

        for document in documents {
            document_ids.push(self.insert_document(
                &mut batch,
                &cf_values,
                &cf_indexes,
                &mut bitmap_list,
                account,
                collection,
                document,
            )?);
        }

        for (key, doc_id_list) in bitmap_list {
            batch.merge_cf(&cf_bitmaps, key, set_bits(doc_id_list.into_iter()))
        }

        self.db
            .write(batch)
            .map_err(|e| StoreError::InternalError(e.to_string()))?;

        Ok(document_ids.into_iter().map(|mut id| id.commit()).collect())
    }
}

impl RocksDBStore {
    #[allow(clippy::too_many_arguments)]
    fn insert_document(
        &self,
        batch: &mut WriteBatch,
        cf_values: &Arc<BoundColumnFamily>,
        cf_indexes: &Arc<BoundColumnFamily>,
        bitmap_list: &mut HashMap<Vec<u8>, HashSet<DocumentId>>,
        account: AccountId,
        collection: CollectionId,
        document: DocumentBuilder,
    ) -> crate::Result<UncommittedDocumentId> {
        // Reserve a document id
        let document_id = self.reserve_document_id(account, collection)?;

        // Add document id to collection
        bitmap_list
            .entry(serialize_bm_internal(account, collection, BM_USED_IDS))
            .or_insert_with(HashSet::new)
            .insert(document_id.get_id());

        // Full text term positions
        let mut term_index = TermIndexBuilder::new();

        for field in document {
            match &field {
                IndexField::FullText(t) => {
                    let opt = t.get_options();
                    let terms =
                        self.get_terms(TokenIterator::new(&t.value.text, t.value.language, true))?;
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
                                .insert(document_id.get_id());

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
                                    .insert(document_id.get_id());
                            }
                        }

                        term_index.add_item(
                            t.get_field(),
                            if opt.is_array() { opt.get_pos() + 1 } else { 0 },
                            terms,
                        );
                    }

                    let opt = t.get_options();
                    if opt.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(
                                account,
                                collection,
                                document_id.get_id(),
                                t.get_field(),
                                opt.get_pos(),
                            ),
                            t.value.text.as_bytes(),
                        );
                    }

                    if opt.is_sortable() {
                        batch.put_cf(
                            cf_indexes,
                            &serialize_index_key(
                                account,
                                collection,
                                document_id.get_id(),
                                t.get_field(),
                                t.value.text.as_bytes(),
                            ),
                            &[],
                        );
                    }
                }
                IndexField::Text(t) => {
                    for token in TokenIterator::new(&t.value, Language::English, false) {
                        bitmap_list
                            .entry(serialize_bm_text_key(
                                account,
                                collection,
                                t.get_field(),
                                &token.word,
                            ))
                            .or_insert_with(HashSet::new)
                            .insert(document_id.get_id());
                    }

                    let opt = t.get_options();
                    if opt.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(
                                account,
                                collection,
                                document_id.get_id(),
                                t.get_field(),
                                opt.get_pos(),
                            ),
                            t.value.as_bytes(),
                        );
                    }
                    if opt.is_sortable() {
                        batch.put_cf(
                            cf_indexes,
                            &serialize_index_key(
                                account,
                                collection,
                                document_id.get_id(),
                                t.get_field(),
                                t.value.as_bytes(),
                            ),
                            &[],
                        );
                    }
                }
                IndexField::Keyword(k) => {
                    bitmap_list
                        .entry(serialize_bm_text_key(
                            account,
                            collection,
                            k.get_field(),
                            &k.value,
                        ))
                        .or_insert_with(HashSet::new)
                        .insert(document_id.get_id());

                    let opt = k.get_options();
                    if opt.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(
                                account,
                                collection,
                                document_id.get_id(),
                                k.get_field(),
                                opt.get_pos(),
                            ),
                            k.value.as_bytes(),
                        );
                    }

                    if opt.is_sortable() {
                        batch.put_cf(
                            cf_indexes,
                            &serialize_index_key(
                                account,
                                collection,
                                document_id.get_id(),
                                k.get_field(),
                                k.value.as_bytes(),
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
                        .insert(document_id.get_id());
                }
                IndexField::Blob(b) => {
                    let opt = b.get_options();
                    if opt.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(
                                account,
                                collection,
                                document_id.get_id(),
                                b.get_field(),
                                opt.get_pos(),
                            ),
                            &b.value,
                        );
                    }
                }
                IndexField::Integer(i) => {
                    let opt = i.get_options();
                    if opt.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(
                                account,
                                collection,
                                document_id.get_id(),
                                i.get_field(),
                                opt.get_pos(),
                            ),
                            &i.value.to_le_bytes(),
                        );
                    }

                    if opt.is_sortable() {
                        batch.put_cf(
                            cf_indexes,
                            &serialize_index_key(
                                account,
                                collection,
                                document_id.get_id(),
                                i.get_field(),
                                &i.value.to_be_bytes(),
                            ),
                            &[],
                        );
                    }
                }
                IndexField::LongInteger(i) => {
                    let opt = i.get_options();
                    if opt.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(
                                account,
                                collection,
                                document_id.get_id(),
                                i.get_field(),
                                opt.get_pos(),
                            ),
                            &i.value.to_le_bytes(),
                        );
                    }

                    if opt.is_sortable() {
                        batch.put_cf(
                            cf_indexes,
                            &serialize_index_key(
                                account,
                                collection,
                                document_id.get_id(),
                                i.get_field(),
                                &i.value.to_be_bytes(),
                            ),
                            &[],
                        );
                    }
                }
                IndexField::Float(f) => {
                    let opt = f.get_options();
                    if opt.is_stored() {
                        batch.put_cf(
                            cf_values,
                            serialize_stored_key(
                                account,
                                collection,
                                document_id.get_id(),
                                f.get_field(),
                                opt.get_pos(),
                            ),
                            &f.value.to_le_bytes(),
                        );
                    }

                    if opt.is_sortable() {
                        batch.put_cf(
                            cf_indexes,
                            &serialize_index_key(
                                account,
                                collection,
                                document_id.get_id(),
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
                &serialize_acd_key_leb128(account, collection, document_id.get_id()),
                &term_index.compress(),
            );
        }

        Ok(document_id)
    }
}
