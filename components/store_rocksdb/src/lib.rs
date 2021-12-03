pub mod bitmaps;
pub mod document_id;
pub mod iterator;
pub mod term;

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    convert::{TryFrom, TryInto},
    sync::Mutex,
};

use bitmaps::{clear_bit, has_bit, set_bit};
use dashmap::DashMap;
use iterator::RocksDBIterator;

use nlp::{lang::detect_language, Language};
use roaring::RoaringBitmap;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded, Options, WriteBatch};
use store::{
    document::{DocumentBuilder, IndexOptions},
    field::TokenIterator,
    serialize::{
        serialize_collection_key, serialize_index_key, serialize_stored_key,
        serialize_stored_key_pos, serialize_tag_key, serialize_term_id_key,
        serialize_term_index_key, serialize_text_key, SerializedKeyValue, SerializedValue,
    },
    term_index::{TermIndex, TermIndexBuilder},
    AccountId, ArrayPos, CollectionId, Condition, DocumentId, FieldId, FieldValue, FilterOperator,
    LogicalOperator, OrderBy, Result, Store, StoreError, Tag, TermId,
};

use crate::{
    bitmaps::{bitmap_full_merge, bitmap_op, bitmap_partial_merge},
    term::get_last_term_id,
};

pub struct RocksDBStore {
    db: DBWithThreadMode<MultiThreaded>,
    reserved_ids: DashMap<(AccountId, CollectionId), HashSet<DocumentId>>,
    term_id_lock: DashMap<String, (TermId, u32)>,
    term_id_last: Mutex<u64>,
}

impl RocksDBStore {
    pub fn open(path: &str) -> Result<Self> {
        // Bitmaps
        let cf_bitmaps = {
            let mut cf_opts = Options::default();
            //cf_opts.set_max_write_buffer_number(16);
            cf_opts.set_merge_operator("bitmap merge", bitmap_full_merge, bitmap_partial_merge);
            ColumnFamilyDescriptor::new("bitmaps", cf_opts)
        };

        // Stored values
        let cf_values = {
            let cf_opts = Options::default();
            ColumnFamilyDescriptor::new("values", cf_opts)
        };

        // Secondary indexes
        let cf_indexes = {
            let cf_opts = Options::default();
            ColumnFamilyDescriptor::new("indexes", cf_opts)
        };

        // Term index
        let cf_terms = {
            let cf_opts = Options::default();
            ColumnFamilyDescriptor::new("terms", cf_opts)
        };

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        let db: DBWithThreadMode<MultiThreaded> = DBWithThreadMode::open_cf_descriptors(
            &db_opts,
            path,
            vec![cf_bitmaps, cf_values, cf_indexes, cf_terms],
        )
        .map_err(|e| StoreError::InternalError(e.into_string()))?;

        Ok(Self {
            reserved_ids: DashMap::new(),
            term_id_lock: DashMap::new(),
            term_id_last: Mutex::new(get_last_term_id(&db)?),
            db,
        })
    }
}

impl Store<RocksDBIterator> for RocksDBStore {
    fn insert(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: DocumentBuilder,
    ) -> Result<DocumentId> {
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

        // Reserve a document id
        let document_id = self.reserve_document_id(account, collection, true)?;
        let mut batch = WriteBatch::default();

        // Add document id to collection
        batch.merge_cf(
            &cf_bitmaps,
            &serialize_collection_key(account, collection),
            &set_bit(&document_id),
        );

        // Full text term positions
        let mut term_index = TermIndexBuilder::new();

        for field in document {
            let field_opt = field.get_options();
            if field_opt.is_sortable() {
                batch.put_cf(
                    &cf_indexes,
                    &field.as_index_key(account, collection, &document_id),
                    &[],
                );
            }
            if field_opt.is_stored() {
                match field.as_stored_value(account, collection, &document_id) {
                    SerializedKeyValue {
                        key,
                        value: SerializedValue::Tag,
                    } => {
                        batch.merge_cf(&cf_bitmaps, &key, &set_bit(&document_id));
                    }
                    SerializedKeyValue {
                        key,
                        value: SerializedValue::Owned(value),
                    } => {
                        batch.put_cf(&cf_values, &key, &value);
                    }
                    SerializedKeyValue {
                        key,
                        value: SerializedValue::Borrowed(value),
                    } => {
                        batch.put_cf(&cf_values, &key, value);
                    }
                }
            }

            if field_opt.is_full_text() {
                let field = field.unwrap_text();
                let terms = self.get_terms(field.tokenize())?;
                if !terms.is_empty() {
                    for term in &terms {
                        batch.merge_cf(
                            &cf_bitmaps,
                            &serialize_term_id_key(
                                account,
                                collection,
                                field.get_field(),
                                &term.id,
                                true,
                            ),
                            &set_bit(&document_id),
                        );
                        if term.id_stemmed > 0 {
                            batch.merge_cf(
                                &cf_bitmaps,
                                &serialize_term_id_key(
                                    account,
                                    collection,
                                    field.get_field(),
                                    &term.id_stemmed,
                                    false,
                                ),
                                &set_bit(&document_id),
                            );
                        }
                    }
                    let opt = field.get_options();
                    term_index.add_item(
                        *field.get_field(),
                        if opt.is_array() { opt.get_pos() + 1 } else { 0 },
                        terms,
                    );
                }
            } else if field_opt.is_text() {
                let field = field.unwrap_text();
                for token in field.tokenize() {
                    batch.merge_cf(
                        &cf_bitmaps,
                        &serialize_text_key(account, collection, field.get_field(), &token.word),
                        &set_bit(&document_id),
                    );
                }
            } else if field_opt.is_keyword() {
                batch.merge_cf(
                    &cf_bitmaps,
                    &serialize_text_key(
                        account,
                        collection,
                        field.get_field(),
                        &field.unwrap_text().value.text,
                    ),
                    &set_bit(&document_id),
                );
            }
        }

        // Compress and store TermIndex
        if !term_index.is_empty() {
            batch.put_cf(
                &cf_values,
                &serialize_term_index_key(account, collection, &document_id),
                &term_index.compress(),
            );
        }

        let result = self.db.write(batch);
        self.release_document_id(account, collection, &document_id);

        match result {
            Ok(_) => Ok(document_id),
            Err(e) => Err(StoreError::InternalError(e.into_string())),
        }
    }

    fn get_value(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
    ) -> Result<Option<Vec<u8>>> {
        self.db
            .get_cf(
                &self.db.cf_handle("values").ok_or_else(|| {
                    StoreError::InternalError("No values column family found.".into())
                })?,
                &serialize_stored_key(account, collection, document, field),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))
    }

    fn get_value_by_pos(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
        pos: &ArrayPos,
    ) -> Result<Option<Vec<u8>>> {
        self.db
            .get_cf(
                &self.db.cf_handle("values").ok_or_else(|| {
                    StoreError::InternalError("No values column family found.".into())
                })?,
                &serialize_stored_key_pos(account, collection, document, field, pos),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))
    }

    fn set_tag(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
        tag: &Tag,
    ) -> Result<()> {
        self.db
            .merge_cf(
                &self.db.cf_handle("bitmaps").ok_or_else(|| {
                    StoreError::InternalError("No bitmaps column family found.".into())
                })?,
                &serialize_tag_key(account, collection, field, tag),
                &set_bit(document),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))
    }

    fn clear_tag(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
        tag: &Tag,
    ) -> Result<()> {
        self.db
            .merge_cf(
                &self.db.cf_handle("bitmaps").ok_or_else(|| {
                    StoreError::InternalError("No bitmaps column family found.".into())
                })?,
                &serialize_tag_key(account, collection, field, tag),
                &clear_bit(document),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))
    }

    fn has_tag(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
        tag: &Tag,
    ) -> Result<bool> {
        self.db
            .get_cf(
                &self.db.cf_handle("bitmaps").ok_or_else(|| {
                    StoreError::InternalError("No bitmaps column family found.".into())
                })?,
                &serialize_tag_key(account, collection, field, tag),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))?
            .map_or(Ok(false), |b| has_bit(&b, document))
    }

    #[allow(clippy::blocks_in_if_conditions)]
    fn search(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        filter: &FilterOperator,
        order_by: &[OrderBy],
    ) -> Result<RocksDBIterator> {
        struct State<'x> {
            op: &'x LogicalOperator,
            it: std::slice::Iter<'x, Condition<'x>>,
            rb: Option<RoaringBitmap>,
        }

        let mut stack = Vec::new();
        let mut state = State {
            op: &filter.operator,
            it: filter.conditions.iter(),
            rb: None,
        };
        let not_mask = self.get_document_ids(account, collection)?;

        let cf_indexes = self
            .db
            .cf_handle("indexes")
            .ok_or_else(|| StoreError::InternalError("No indexes column family found.".into()))?;
        let cf_bitmaps = self
            .db
            .cf_handle("bitmaps")
            .ok_or_else(|| StoreError::InternalError("No bitmaps column family found.".into()))?;
        let cf_values = self
            .db
            .cf_handle("values")
            .ok_or_else(|| StoreError::InternalError("No values column family found.".into()))?;

        'outer: loop {
            while let Some(cond) = state.it.next() {
                match cond {
                    Condition::FilterCondition(filter_cond) => {
                        match &filter_cond.value {
                            FieldValue::Keyword(keyword) => {
                                bitmap_op(
                                    state.op,
                                    &mut state.rb,
                                    self.get_bitmap(
                                        &cf_bitmaps,
                                        &serialize_text_key(
                                            account,
                                            collection,
                                            &filter_cond.field,
                                            keyword,
                                        ),
                                    )?,
                                    &not_mask,
                                );
                            }
                            FieldValue::Text(text) => {
                                let mut keys = Vec::new();
                                for token in TokenIterator::new(text, Language::English, false) {
                                    keys.push((
                                        &cf_bitmaps,
                                        serialize_text_key(
                                            account,
                                            collection,
                                            &filter_cond.field,
                                            &token.word,
                                        ),
                                    ));
                                }
                                bitmap_op(
                                    state.op,
                                    &mut state.rb,
                                    self.get_bitmaps_intersection(keys)?,
                                    &not_mask,
                                );
                            }
                            FieldValue::FullText(text) => {
                                let match_phrase = (text.starts_with('"') && text.ends_with('"'))
                                    || (text.starts_with('\'') && text.ends_with('\''));
                                let language = detect_language(text);

                                if let Some(match_terms) = self.get_match_terms(
                                    TokenIterator::new(text, language.0, !match_phrase),
                                )? {
                                    if match_phrase {
                                        let mut requested_ids = HashSet::new();
                                        let mut keys = Vec::new();
                                        for match_term in &match_terms {
                                            if !requested_ids.contains(&match_term.id) {
                                                requested_ids.insert(match_term.id);
                                                keys.push((
                                                    &cf_bitmaps,
                                                    serialize_term_id_key(
                                                        account,
                                                        collection,
                                                        &filter_cond.field,
                                                        &match_term.id,
                                                        true,
                                                    ),
                                                ));
                                            }
                                        }

                                        // Retrieve the Term Index for each candidate and match the exact phrase
                                        let mut candidates = self.get_bitmaps_intersection(keys)?;
                                        if let Some(candidates) = &mut candidates {
                                            if match_terms.len() > 1 {
                                                let mut results = RoaringBitmap::new();
                                                for document_id in candidates.iter() {
                                                    if let Some(compressed_term_index) = self
                                                        .db
                                                        .get_cf(
                                                            &cf_values,
                                                            &serialize_term_index_key(
                                                                account,
                                                                collection,
                                                                &document_id,
                                                            ),
                                                        )
                                                        .map_err(|e| {
                                                            StoreError::InternalError(
                                                                e.into_string(),
                                                            )
                                                        })?
                                                    {
                                                        if TermIndex::try_from(
                                                            &compressed_term_index[..],
                                                        )
                                                        .map_err(|e| {
                                                            StoreError::InternalError(format!(
                                                                "Corrupted TermIndex for {}: {:?}",
                                                                document_id, e
                                                            ))
                                                        })?
                                                        .match_terms(
                                                            &match_terms,
                                                            None,
                                                            true,
                                                            false,
                                                            false,
                                                        )
                                                        .map_err(|e| {
                                                            StoreError::InternalError(format!(
                                                                "Corrupted TermIndex for {}: {:?}",
                                                                document_id, e
                                                            ))
                                                        })?
                                                        .is_some()
                                                        {
                                                            results.insert(document_id);
                                                        }
                                                    }
                                                }
                                                *candidates = results;
                                            }
                                        }

                                        bitmap_op(state.op, &mut state.rb, candidates, &not_mask);
                                    } else {
                                        let mut requested_ids = HashSet::new();
                                        let mut text_bitmap = None;

                                        for match_term in &match_terms {
                                            let mut keys =
                                                Vec::with_capacity(match_terms.len() * 2);
                                            
                                            for term_op in [
                                                (match_term.id, true),
                                                (match_term.id, false),
                                                (match_term.id_stemmed, true),
                                                (match_term.id_stemmed, false),
                                            ] {
                                                if term_op.0 > 0 && !requested_ids.contains(&term_op)
                                                {
                                                    requested_ids.insert(term_op);
                                                    keys.push((
                                                        &cf_bitmaps,
                                                        serialize_term_id_key(
                                                            account,
                                                            collection,
                                                            &filter_cond.field,
                                                            &term_op.0,
                                                            term_op.1,
                                                        ),
                                                    ));
                                                }
                                            }

                                            // Term already matched on a previous iteration
                                            if keys.is_empty() {
                                                continue;
                                            }

                                            bitmap_op(
                                                &LogicalOperator::And,
                                                &mut text_bitmap,
                                                self.get_bitmaps_union(keys)?,
                                                &not_mask,
                                            );

                                            if text_bitmap.as_ref().unwrap().is_empty() {
                                                break;
                                            }
                                        }
                                        bitmap_op(state.op, &mut state.rb, text_bitmap, &not_mask);
                                    }
                                } else {
                                    bitmap_op(state.op, &mut state.rb, None, &not_mask);
                                }
                            }
                            FieldValue::Integer(i) => {
                                bitmap_op(
                                    state.op,
                                    &mut state.rb,
                                    self.range_to_bitmap(
                                        &cf_indexes,
                                        &serialize_index_key(
                                            account,
                                            collection,
                                            &filter_cond.field,
                                            &i.to_be_bytes(),
                                        ),
                                        &filter_cond.op,
                                    )?,
                                    &not_mask,
                                );
                            }
                            FieldValue::LongInteger(i) => {
                                bitmap_op(
                                    state.op,
                                    &mut state.rb,
                                    self.range_to_bitmap(
                                        &cf_indexes,
                                        &serialize_index_key(
                                            account,
                                            collection,
                                            &filter_cond.field,
                                            &i.to_be_bytes(),
                                        ),
                                        &filter_cond.op,
                                    )?,
                                    &not_mask,
                                );
                            }
                            FieldValue::Float(f) => {
                                bitmap_op(
                                    state.op,
                                    &mut state.rb,
                                    self.range_to_bitmap(
                                        &cf_indexes,
                                        &serialize_index_key(
                                            account,
                                            collection,
                                            &filter_cond.field,
                                            &f.to_be_bytes(),
                                        ),
                                        &filter_cond.op,
                                    )?,
                                    &not_mask,
                                );
                            }
                            FieldValue::Tag(tag) => {
                                bitmap_op(
                                    state.op,
                                    &mut state.rb,
                                    self.get_bitmap(
                                        &cf_bitmaps,
                                        &serialize_tag_key(
                                            account,
                                            collection,
                                            &filter_cond.field,
                                            tag,
                                        ),
                                    )?,
                                    &not_mask,
                                );
                            }
                        }
                    }
                    Condition::FilterOperator(filter_op) => {
                        stack.push(state);
                        state = State {
                            op: &filter_op.operator,
                            it: filter_op.conditions.iter(),
                            rb: None,
                        };
                        continue 'outer;
                    }
                }

                if state.op == &LogicalOperator::And && state.rb.as_ref().unwrap().is_empty() {
                    break;
                }
            }
            if let Some(mut prev_state) = stack.pop() {
                bitmap_op(state.op, &mut prev_state.rb, state.rb, &not_mask);
                state = prev_state;
            } else {
                break;
            }
        }

        println!("{:?}", state.rb.as_ref().unwrap());

        Ok(RocksDBIterator::new(
            state.rb.unwrap_or_else(RoaringBitmap::new),
        ))
    }
}
