/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use crate::{
    core::{collection::Collection, document::MAX_TOKEN_LENGTH, error::StoreError},
    nlp::{stemmer::Stemmer, tokenizers::Tokenizer, Language},
    serialize::key::{BitmapKey, IndexKey},
    AccountId, DocumentId, JMAPId, JMAPStore, Store,
};

use ahash::AHashSet;
use roaring::RoaringBitmap;
use std::vec::IntoIter;

use super::{
    comparator::Comparator,
    filter::{Filter, FilterOperator, LogicalOperator, Query},
    iterator::StoreIterator,
};

struct State {
    op: LogicalOperator,
    it: IntoIter<Filter>,
    bm: Option<RoaringBitmap>,
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    #[allow(clippy::blocks_in_if_conditions)]
    pub fn query_store<'y: 'x, 'x, U>(
        &'y self,
        account_id: AccountId,
        collection: Collection,
        filter: Filter,
        sort: Comparator,
    ) -> crate::Result<StoreIterator<'x, T, U>>
    where
        U: FnMut(DocumentId) -> crate::Result<Option<JMAPId>>,
    {
        let document_ids = self
            .get_document_ids(account_id, collection)?
            .unwrap_or_else(RoaringBitmap::new);

        let filter = match filter {
            Filter::Operator(filter) => filter,
            Filter::None => {
                return Ok(StoreIterator::new(
                    self,
                    document_ids.clone(),
                    document_ids,
                    account_id,
                    collection,
                    sort,
                ));
            }
            Filter::DocumentSet(set) => {
                return Ok(StoreIterator::new(
                    self,
                    set,
                    document_ids,
                    account_id,
                    collection,
                    sort,
                ));
            }
            _ => FilterOperator {
                operator: LogicalOperator::And,
                conditions: vec![filter],
            },
        };

        let mut state = State {
            op: filter.operator,
            it: filter.conditions.into_iter(),
            bm: None,
        };

        let mut stack = Vec::new();

        'outer: loop {
            while let Some(cond) = state.it.next() {
                match cond {
                    Filter::Condition(filter_cond) => {
                        match filter_cond.value {
                            Query::Keyword(keyword) => {
                                state.op.apply(
                                    &mut state.bm,
                                    self.get_bitmap(&BitmapKey::serialize_term(
                                        account_id,
                                        collection,
                                        filter_cond.field,
                                        &keyword,
                                        true,
                                    ))?,
                                    &document_ids,
                                );
                            }
                            Query::Tokenize(text) => {
                                let field_cond_field = filter_cond.field;
                                state.op.apply(
                                    &mut state.bm,
                                    self.get_bitmaps_intersection(
                                        Tokenizer::new(&text, Language::English, MAX_TOKEN_LENGTH)
                                            .map(|token| {
                                                BitmapKey::serialize_term(
                                                    account_id,
                                                    collection,
                                                    field_cond_field,
                                                    &token.word,
                                                    true,
                                                )
                                            })
                                            .collect(),
                                    )?,
                                    &document_ids,
                                );
                            }
                            Query::Match(text) => {
                                if text.match_phrase {
                                    let mut phrase: Vec<String> = Vec::new();
                                    let field = filter_cond.field;

                                    // Retrieve the Term Index for each candidate and match the exact phrase
                                    if let Some(candidates) = self.get_bitmaps_intersection(
                                        Tokenizer::new(&text.text, text.language, MAX_TOKEN_LENGTH)
                                            .into_iter()
                                            .filter_map(|token| {
                                                let word = token.word.into_owned();
                                                let r = if !phrase.contains(&word) {
                                                    BitmapKey::serialize_term(
                                                        account_id, collection, field, &word, true,
                                                    )
                                                    .into()
                                                } else {
                                                    None
                                                };
                                                phrase.push(word);
                                                r
                                            })
                                            .collect(),
                                    )? {
                                        let mut results = RoaringBitmap::new();
                                        for document_id in candidates.iter() {
                                            if let Some(term_index) = self.get_term_index(
                                                account_id,
                                                collection,
                                                document_id,
                                            )? {
                                                if term_index
                                                    .match_terms(
                                                        &phrase
                                                            .iter()
                                                            .map(|w| {
                                                                term_index.get_match_term(w, None)
                                                            })
                                                            .collect::<Vec<_>>(),
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
                                        state.op.apply(
                                            &mut state.bm,
                                            results.into(),
                                            &document_ids,
                                        );
                                    } else {
                                        state.op.apply(&mut state.bm, None, &document_ids);
                                    }
                                } else {
                                    let mut requested_keys = AHashSet::default();
                                    let mut text_bitmap = None;

                                    // Default language for stemming
                                    let language = if text.language != Language::Unknown {
                                        text.language
                                    } else {
                                        self.config.default_language
                                    };

                                    for token in
                                        Stemmer::new(&text.text, language, MAX_TOKEN_LENGTH)
                                    {
                                        let mut keys = Vec::new();

                                        for (word, is_exact) in [
                                            (token.word.as_ref().into(), true),
                                            (token.word.as_ref().into(), false),
                                            (token.stemmed_word.as_ref().map(|w| w.as_ref()), true),
                                            (
                                                token.stemmed_word.as_ref().map(|w| w.as_ref()),
                                                false,
                                            ),
                                        ] {
                                            if let Some(word) = word {
                                                let key = BitmapKey::serialize_term(
                                                    account_id,
                                                    collection,
                                                    filter_cond.field,
                                                    word,
                                                    is_exact,
                                                );
                                                if !requested_keys.contains(&key) {
                                                    requested_keys.insert(key.clone());
                                                    keys.push(key);
                                                }
                                            }
                                        }

                                        // Term already matched on a previous iteration
                                        if keys.is_empty() {
                                            continue;
                                        }

                                        LogicalOperator::And.apply(
                                            &mut text_bitmap,
                                            self.get_bitmaps_union(keys)?,
                                            &document_ids,
                                        );

                                        if text_bitmap.as_ref().unwrap().is_empty() {
                                            break;
                                        }
                                    }
                                    state.op.apply(&mut state.bm, text_bitmap, &document_ids);
                                }
                            }
                            Query::Integer(i) => {
                                state.op.apply(
                                    &mut state.bm,
                                    self.range_to_bitmap(
                                        &IndexKey::serialize_key(
                                            account_id,
                                            collection,
                                            filter_cond.field,
                                            &i.to_be_bytes(),
                                        ),
                                        filter_cond.op,
                                    )?,
                                    &document_ids,
                                );
                            }
                            Query::LongInteger(i) => {
                                state.op.apply(
                                    &mut state.bm,
                                    self.range_to_bitmap(
                                        &IndexKey::serialize_key(
                                            account_id,
                                            collection,
                                            filter_cond.field,
                                            &i.to_be_bytes(),
                                        ),
                                        filter_cond.op,
                                    )?,
                                    &document_ids,
                                );
                            }
                            Query::Float(f) => {
                                state.op.apply(
                                    &mut state.bm,
                                    self.range_to_bitmap(
                                        &IndexKey::serialize_key(
                                            account_id,
                                            collection,
                                            filter_cond.field,
                                            &f.to_be_bytes(),
                                        ),
                                        filter_cond.op,
                                    )?,
                                    &document_ids,
                                );
                            }
                            Query::Index(text) => {
                                state.op.apply(
                                    &mut state.bm,
                                    self.range_to_bitmap(
                                        &IndexKey::serialize_key(
                                            account_id,
                                            collection,
                                            filter_cond.field,
                                            text.as_bytes(),
                                        ),
                                        filter_cond.op,
                                    )?,
                                    &document_ids,
                                );
                            }
                            Query::Tag(tag) => {
                                state.op.apply(
                                    &mut state.bm,
                                    self.get_bitmap(&BitmapKey::serialize_tag(
                                        account_id,
                                        collection,
                                        filter_cond.field,
                                        &tag,
                                    ))?,
                                    &document_ids,
                                );
                            }
                        }
                    }
                    Filter::DocumentSet(set) => {
                        state.op.apply(&mut state.bm, Some(set), &document_ids);
                    }
                    Filter::Operator(filter_op) => {
                        stack.push(state);
                        state = State {
                            op: filter_op.operator,
                            it: filter_op.conditions.into_iter(),
                            bm: None,
                        };
                        continue 'outer;
                    }
                    Filter::None => (),
                }

                if state.op == LogicalOperator::And && state.bm.as_ref().unwrap().is_empty() {
                    break;
                }
            }
            if let Some(mut prev_state) = stack.pop() {
                prev_state
                    .op
                    .apply(&mut prev_state.bm, state.bm, &document_ids);
                state = prev_state;
            } else {
                break;
            }
        }

        Ok(StoreIterator::new(
            self,
            state.bm.unwrap_or_else(RoaringBitmap::new),
            document_ids,
            account_id,
            collection,
            sort,
        ))
    }
}
