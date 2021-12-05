use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
    vec::IntoIter,
};

use nlp::Language;
use rocksdb::{BoundColumnFamily, WriteBatch};
use store::{
    document::{DocumentBuilder, IndexOptions},
    field::{IndexField, TokenIterator},
    serialize::{
        serialize_collection_key, serialize_term_id_key, serialize_term_index_key,
        serialize_text_key, SerializedKeyValue, SerializedValue,
    },
    term_index::TermIndexBuilder,
    AccountId, CollectionId, DocumentId, StoreError, StoreInsert,
};

use crate::{
    bitmaps::{set_bit, set_bit_list},
    document_id::UncommittedDocumentId,
    RocksDBStore,
};

impl StoreInsert for RocksDBStore {
    fn insert(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentBuilder,
    ) -> crate::Result<DocumentId> {
        self.insert_bulk(account, collection, vec![document])?
            .pop()
            .ok_or_else(|| StoreError::InternalError("No document id returned".to_string()))
    }

    fn insert_bulk(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: Vec<DocumentBuilder>,
    ) -> store::Result<Vec<DocumentId>> {
        let cf_values = self
            .db
            .cf_handle("values")
            .ok_or_else(|| StoreError::InternalError("No values column family found.".into()))?;
        let cf_indexes = self
            .db
            .cf_handle("indexes")
            .ok_or_else(|| StoreError::InternalError("No indexes column family found.".into()))?;
        let cf_bitmaps = self
            .db
            .cf_handle("bitmaps")
            .ok_or_else(|| StoreError::InternalError("No bitmaps column family found.".into()))?;
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
            batch.merge_cf(
                &cf_bitmaps,
                key,
                if doc_id_list.len() > 1 {
                    set_bit_list(doc_id_list)
                } else {
                    set_bit(*doc_id_list.iter().next().unwrap())
                },
            )
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
            .entry(serialize_collection_key(account, collection))
            .or_insert_with(HashSet::new)
            .insert(document_id.get_id());

        // Full text term positions
        let mut term_index = TermIndexBuilder::new();

        for field in document {
            let field_opt = match &field {
                IndexField::FullText(t) => {
                    let field_opt = t.get_options();
                    let terms =
                        self.get_terms(TokenIterator::new(&t.value.text, t.value.language, true))?;
                    if !terms.is_empty() {
                        for term in &terms {
                            bitmap_list
                                .entry(serialize_term_id_key(
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
                                    .entry(serialize_term_id_key(
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
                            if field_opt.is_array() {
                                field_opt.get_pos() + 1
                            } else {
                                0
                            },
                            terms,
                        );
                    }

                    field_opt
                }
                IndexField::Text(t) => {
                    for token in TokenIterator::new(&t.value, Language::English, false) {
                        bitmap_list
                            .entry(serialize_text_key(
                                account,
                                collection,
                                t.get_field(),
                                &token.word,
                            ))
                            .or_insert_with(HashSet::new)
                            .insert(document_id.get_id());
                    }
                    t.get_options()
                }
                IndexField::Keyword(k) => {
                    bitmap_list
                        .entry(serialize_text_key(
                            account,
                            collection,
                            k.get_field(),
                            &k.value,
                        ))
                        .or_insert_with(HashSet::new)
                        .insert(document_id.get_id());
                    k.get_options()
                }
                IndexField::Blob(b) => b.get_options(),
                IndexField::Integer(i) => i.get_options(),
                IndexField::LongInteger(i) => i.get_options(),
                IndexField::Tag(t) => t.get_options(),
                IndexField::Float(f) => f.get_options(),
            };

            if field_opt.is_sortable() {
                batch.put_cf(
                    cf_indexes,
                    &field.as_index_key(account, collection, document_id.get_id()),
                    &[],
                );
            }

            if field_opt.is_stored() {
                match field.as_stored_value(account, collection, document_id.get_id()) {
                    SerializedKeyValue {
                        key,
                        value: SerializedValue::Tag,
                    } => {
                        bitmap_list
                            .entry(key)
                            .or_insert_with(HashSet::new)
                            .insert(document_id.get_id());
                    }
                    SerializedKeyValue {
                        key,
                        value: SerializedValue::Owned(value),
                    } => {
                        batch.put_cf(cf_values, &key, &value);
                    }
                    SerializedKeyValue {
                        key,
                        value: SerializedValue::Borrowed(value),
                    } => {
                        batch.put_cf(cf_values, &key, value);
                    }
                }
            }
        }

        // Compress and store TermIndex
        if !term_index.is_empty() {
            batch.put_cf(
                cf_values,
                &serialize_term_index_key(account, collection, document_id.get_id()),
                &term_index.compress(),
            );
        }

        Ok(document_id)
    }
}
