use std::{collections::HashSet, convert::TryFrom};

use nlp::{lang::detect_language, Language};
use roaring::RoaringBitmap;
use store::{
    field::TokenIterator,
    serialize::{
        serialize_index_key, serialize_tag_key, serialize_term_id_key, serialize_term_index_key,
        serialize_text_key,
    },
    term_index::TermIndex,
    AccountId, CollectionId, Condition, FieldValue, FilterOperator, LogicalOperator, OrderBy,
    StoreError, StoreQuery,
};

use crate::{bitmaps::bitmap_op, iterator::RocksDBIterator, RocksDBStore};

impl StoreQuery<RocksDBIterator> for RocksDBStore {
    #[allow(clippy::blocks_in_if_conditions)]
    fn query(
        &self,
        account: AccountId,
        collection: CollectionId,
        filter: &FilterOperator,
        _order_by: &[OrderBy],
    ) -> crate::Result<RocksDBIterator> {
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
        let not_mask = self
            .get_document_ids(account, collection)?
            .unwrap_or_else(RoaringBitmap::new);

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
                                            filter_cond.field,
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
                                            filter_cond.field,
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
                                                        filter_cond.field,
                                                        match_term.id,
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
                                                                document_id,
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
                                                if term_op.0 > 0
                                                    && !requested_ids.contains(&term_op)
                                                {
                                                    requested_ids.insert(term_op);
                                                    keys.push((
                                                        &cf_bitmaps,
                                                        serialize_term_id_key(
                                                            account,
                                                            collection,
                                                            filter_cond.field,
                                                            term_op.0,
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
                                            filter_cond.field,
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
                                            filter_cond.field,
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
                                            filter_cond.field,
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
                                            filter_cond.field,
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
