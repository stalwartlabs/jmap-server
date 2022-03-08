use crate::{
    bitmap::bitmap_op,
    field::TokenIterator,
    serialize::{
        deserialize_index_document_id, serialize_acd_key_leb128, serialize_bm_tag_key,
        serialize_bm_term_key, serialize_bm_text_key, serialize_index_key_base,
        serialize_index_key_prefix, serialize_stored_key,
    },
    term_index::TermIndex,
    AccountId, CollectionId, ColumnFamily, Comparator, Direction, DocumentId, FieldId, FieldValue,
    Filter, FilterOperator, JMAPId, JMAPStore, LogicalOperator, Store, StoreError,
};
use nlp::Language;
use roaring::RoaringBitmap;
use std::{
    collections::HashSet,
    ops::{BitAndAssign, BitXorAssign},
    vec::IntoIter,
};

struct State {
    op: LogicalOperator,
    it: IntoIter<Filter>,
    bm: Option<RoaringBitmap>,
}

pub struct JMAPPrefix {
    pub collection_id: CollectionId,
    pub field_id: FieldId,
    pub unique: bool,
}

pub struct JMAPStoreQuery {
    pub account_id: AccountId,
    pub collection_id: CollectionId,
    pub jmap_prefix: Option<JMAPPrefix>,
    pub limit: usize,
    pub position: i32,
    pub anchor: Option<JMAPId>,
    pub anchor_offset: i32,
    pub filter: Filter,
    pub sort: Comparator,
}

impl JMAPStoreQuery {
    pub fn new(
        account_id: AccountId,
        collection_id: CollectionId,
        filter: Filter,
        sort: Comparator,
        limit: usize,
    ) -> Self {
        Self {
            account_id,
            collection_id,
            jmap_prefix: None,
            limit,
            position: 0,
            anchor: None,
            anchor_offset: 0,
            filter,
            sort,
        }
    }
}

#[derive(Default)]
pub struct JMAPStoreResult {
    pub start_position: usize,
    pub total_results: usize,
    pub results: Vec<JMAPId>,
}

struct DocumentSetIndex {
    set: RoaringBitmap,
    it: Option<roaring::bitmap::IntoIter>,
}

struct DBIndex<'x, T>
where
    T: Store<'x>,
{
    it: Option<T::Iterator>,
    prefix: Vec<u8>,
    start_key: Vec<u8>,
    ascending: bool,
    prev_item: Option<DocumentId>,
    prev_key: Option<Box<[u8]>>,
}

enum IndexType<'x, T>
where
    T: Store<'x>,
{
    DocumentSet(DocumentSetIndex),
    DB(DBIndex<'x, T>),
    None,
}

struct IndexIterator<'x, T>
where
    T: Store<'x>,
{
    index: IndexType<'x, T>,
    remaining: RoaringBitmap,
    eof: bool,
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    #[allow(clippy::blocks_in_if_conditions)]
    pub async fn query(&self, mut request: JMAPStoreQuery) -> crate::Result<JMAPStoreResult> {
        let mut document_ids = self
            .get_document_ids_used(request.account_id, request.collection_id)
            .await?
            .unwrap_or_else(RoaringBitmap::new);
        let tombstoned_ids = self
            .get_tombstoned_ids(request.account_id, request.collection_id)
            .await?;

        let filter = match request.filter {
            Filter::Operator(filter) => filter,
            Filter::None => {
                if let Some(tombstoned_ids) = tombstoned_ids {
                    document_ids.bitxor_assign(tombstoned_ids)
                }
                return self
                    .process_results(document_ids.clone(), document_ids, request)
                    .await;
            }
            Filter::DocumentSet(set) => {
                if let Some(tombstoned_ids) = tombstoned_ids {
                    document_ids.bitxor_assign(tombstoned_ids)
                }
                request.filter = Filter::None;
                return self.process_results(set, document_ids, request).await;
            }
            _ => FilterOperator {
                operator: LogicalOperator::And,
                conditions: vec![request.filter],
            },
        };
        request.filter = Filter::None;

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
                            FieldValue::Keyword(keyword) => {
                                bitmap_op(
                                    state.op,
                                    &mut state.bm,
                                    self.get_bitmap(serialize_bm_text_key(
                                        request.account_id,
                                        request.collection_id,
                                        filter_cond.field,
                                        &keyword,
                                    ))
                                    .await?,
                                    &document_ids,
                                );
                            }
                            FieldValue::Text(text) => {
                                let field_cond_field = filter_cond.field;
                                bitmap_op(
                                    state.op,
                                    &mut state.bm,
                                    self.get_bitmaps_intersection(
                                        TokenIterator::new(&text, Language::English, false)
                                            .map(|token| {
                                                serialize_bm_text_key(
                                                    request.account_id,
                                                    request.collection_id,
                                                    field_cond_field,
                                                    &token.word,
                                                )
                                            })
                                            .collect(),
                                    )
                                    .await?,
                                    &document_ids,
                                );
                            }
                            FieldValue::FullText(query) => {
                                if let Some(match_terms) = self
                                    .get_match_terms(TokenIterator::new(
                                        query.text.as_ref(),
                                        query.language,
                                        !query.match_phrase,
                                    ))
                                    .await?
                                {
                                    if query.match_phrase {
                                        let mut requested_ids = HashSet::new();
                                        let mut keys = Vec::new();
                                        for match_term in &match_terms {
                                            if !requested_ids.contains(&match_term.id) {
                                                requested_ids.insert(match_term.id);
                                                keys.push(serialize_bm_term_key(
                                                    request.account_id,
                                                    request.collection_id,
                                                    filter_cond.field,
                                                    match_term.id,
                                                    true,
                                                ));
                                            }
                                        }

                                        // Retrieve the Term Index for each candidate and match the exact phrase
                                        let mut candidates =
                                            self.get_bitmaps_intersection(keys).await?;
                                        if let Some(candidates) = &mut candidates {
                                            if match_terms.len() > 1 {
                                                let mut results = RoaringBitmap::new();
                                                for document_id in candidates.iter() {
                                                    if let Some(term_index) = self
                                                        .get::<TermIndex>(
                                                            ColumnFamily::Values,
                                                            serialize_acd_key_leb128(
                                                                request.account_id,
                                                                request.collection_id,
                                                                document_id,
                                                            ),
                                                        )
                                                        .await?
                                                    {
                                                        if term_index
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

                                        bitmap_op(
                                            state.op,
                                            &mut state.bm,
                                            candidates,
                                            &document_ids,
                                        );
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
                                                if !requested_ids.contains(&term_op) {
                                                    requested_ids.insert(term_op);
                                                    keys.push(serialize_bm_term_key(
                                                        request.account_id,
                                                        request.collection_id,
                                                        filter_cond.field,
                                                        term_op.0,
                                                        term_op.1,
                                                    ));
                                                }
                                            }

                                            // Term already matched on a previous iteration
                                            if keys.is_empty() {
                                                continue;
                                            }

                                            bitmap_op(
                                                LogicalOperator::And,
                                                &mut text_bitmap,
                                                self.get_bitmaps_union(keys).await?,
                                                &document_ids,
                                            );

                                            if text_bitmap.as_ref().unwrap().is_empty() {
                                                break;
                                            }
                                        }
                                        bitmap_op(
                                            state.op,
                                            &mut state.bm,
                                            text_bitmap,
                                            &document_ids,
                                        );
                                    }
                                } else {
                                    bitmap_op(state.op, &mut state.bm, None, &document_ids);
                                }
                            }
                            FieldValue::Integer(i) => {
                                bitmap_op(
                                    state.op,
                                    &mut state.bm,
                                    self.range_to_bitmap(
                                        serialize_index_key_base(
                                            request.account_id,
                                            request.collection_id,
                                            filter_cond.field,
                                            &i.to_be_bytes(),
                                        ),
                                        filter_cond.op,
                                    )
                                    .await?,
                                    &document_ids,
                                );
                            }
                            FieldValue::LongInteger(i) => {
                                bitmap_op(
                                    state.op,
                                    &mut state.bm,
                                    self.range_to_bitmap(
                                        serialize_index_key_base(
                                            request.account_id,
                                            request.collection_id,
                                            filter_cond.field,
                                            &i.to_be_bytes(),
                                        ),
                                        filter_cond.op,
                                    )
                                    .await?,
                                    &document_ids,
                                );
                            }
                            FieldValue::Float(f) => {
                                bitmap_op(
                                    state.op,
                                    &mut state.bm,
                                    self.range_to_bitmap(
                                        serialize_index_key_base(
                                            request.account_id,
                                            request.collection_id,
                                            filter_cond.field,
                                            &f.to_be_bytes(),
                                        ),
                                        filter_cond.op,
                                    )
                                    .await?,
                                    &document_ids,
                                );
                            }
                            FieldValue::Tag(tag) => {
                                bitmap_op(
                                    state.op,
                                    &mut state.bm,
                                    self.get_bitmap(serialize_bm_tag_key(
                                        request.account_id,
                                        request.collection_id,
                                        filter_cond.field,
                                        &tag,
                                    ))
                                    .await?,
                                    &document_ids,
                                );
                            }
                        }
                    }
                    Filter::DocumentSet(set) => {
                        bitmap_op(state.op, &mut state.bm, Some(set), &document_ids);
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
                bitmap_op(prev_state.op, &mut prev_state.bm, state.bm, &document_ids);
                state = prev_state;
            } else {
                break;
            }
        }

        let mut results = state.bm.unwrap_or_else(RoaringBitmap::new);
        if let Some(tombstoned_ids) = tombstoned_ids {
            document_ids.bitxor_assign(tombstoned_ids);
            if !results.is_empty() {
                results.bitand_assign(&document_ids);
            }
        }

        self.process_results(results, document_ids, request).await
    }

    #[allow(clippy::while_let_on_iterator)]
    pub async fn process_results(
        &self,
        mut results: RoaringBitmap,
        document_ids: RoaringBitmap,
        mut request: JMAPStoreQuery,
    ) -> crate::Result<JMAPStoreResult> {
        let total_results = results.len() as usize;
        if total_results == 0 {
            return Ok(JMAPStoreResult::default());
        }
        let db = self.db.clone();

        // Sort results on a worker thread
        self.spawn_worker(move || {
            let mut iterators: Vec<IndexIterator<T>> = Vec::new();
            for comp in (if let Comparator::List(list) = request.sort {
                list
            } else {
                vec![request.sort]
            })
            .into_iter()
            {
                iterators.push(IndexIterator {
                    index: match comp {
                        Comparator::Field(comp) => {
                            let prefix = serialize_index_key_prefix(
                                request.account_id,
                                request.collection_id,
                                comp.field,
                            );
                            IndexType::DB(DBIndex {
                                it: None,
                                start_key: if !comp.ascending {
                                    let (key_account_id, key_collection_id, key_field) = if comp
                                        .field
                                        < FieldId::MAX
                                    {
                                        (request.account_id, request.collection_id, comp.field + 1)
                                    } else if request.collection_id < CollectionId::MAX {
                                        (request.account_id, request.collection_id + 1, comp.field)
                                    } else {
                                        (request.account_id + 1, request.collection_id, comp.field)
                                    };
                                    serialize_index_key_prefix(
                                        key_account_id,
                                        key_collection_id,
                                        key_field,
                                    )
                                } else {
                                    prefix.clone()
                                },
                                prefix,
                                ascending: comp.ascending,
                                prev_item: None,
                                prev_key: None,
                            })
                        }
                        Comparator::DocumentSet(mut comp) => {
                            IndexType::DocumentSet(DocumentSetIndex {
                                set: if !comp.ascending {
                                    if !comp.set.is_empty() {
                                        comp.set.bitxor_assign(&document_ids);
                                        comp.set
                                    } else {
                                        document_ids.clone()
                                    }
                                } else {
                                    comp.set
                                },
                                it: None,
                            })
                        }
                        _ => IndexType::None,
                    },
                    eof: false,
                    remaining: results,
                });

                results = RoaringBitmap::new();
            }

            let start_position;
            let mut current = 0;
            let mut seen_prefixes = HashSet::new();

            let mut results = Vec::with_capacity(if request.limit > 0 {
                request.limit
            } else {
                total_results as usize
            });
            let has_anchor = request.anchor.is_some();
            let mut anchor_found = false;

            'outer: loop {
                let mut doc_id;

                'inner: loop {
                    let (it_opts, mut next_it_opts) = if current < iterators.len() - 1 {
                        let (iterators_first, iterators_last) = iterators.split_at_mut(current + 1);
                        (
                            iterators_first.last_mut().unwrap(),
                            iterators_last.first_mut(),
                        )
                    } else {
                        (&mut iterators[current], None)
                    };

                    if it_opts.remaining.is_empty() {
                        if current > 0 {
                            current -= 1;
                            continue 'inner;
                        } else {
                            break 'outer;
                        }
                    } else if it_opts.remaining.len() == 1 || it_opts.eof {
                        doc_id = it_opts.remaining.min().unwrap();
                        it_opts.remaining.remove(doc_id);
                        break 'inner;
                    }

                    match &mut it_opts.index {
                        IndexType::DB(index) => {
                            let it = if let Some(it) = &mut index.it {
                                it
                            } else {
                                index.it = Some(db.iterator(
                                    ColumnFamily::Indexes,
                                    index.start_key.clone(),
                                    if index.ascending {
                                        Direction::Forward
                                    } else {
                                        Direction::Backward
                                    },
                                )?);
                                index.it.as_mut().unwrap()
                            };

                            let mut prev_key_prefix = if let Some(prev_key) = &index.prev_key {
                                prev_key
                                    .get(..prev_key.len() - std::mem::size_of::<DocumentId>())
                                    .ok_or_else(|| {
                                        StoreError::InternalError(format!(
                                            "prev_key {:?} is too short",
                                            prev_key
                                        ))
                                    })?
                            } else {
                                &[][..]
                            };

                            if let Some(prev_item) = index.prev_item {
                                index.prev_item = None;
                                if let Some(next_it_opts) = &mut next_it_opts {
                                    next_it_opts.remaining.insert(prev_item);
                                } else {
                                    doc_id = prev_item;
                                    break 'inner;
                                }
                            }

                            while let Some((key, _)) = it.next() {
                                if !key.starts_with(&index.prefix) {
                                    index.prev_key = None;
                                    break;
                                }

                                doc_id = deserialize_index_document_id(&key).ok_or_else(|| {
                                    StoreError::InternalError(format!(
                                        "invalid index key {:?}",
                                        key
                                    ))
                                })?;
                                if it_opts.remaining.contains(doc_id) {
                                    it_opts.remaining.remove(doc_id);

                                    if let Some(next_it_opts) = &mut next_it_opts {
                                        if let Some(prev_key) = &index.prev_key {
                                            if key.len() != prev_key.len()
                                                || !key.starts_with(prev_key_prefix)
                                            {
                                                index.prev_item = Some(doc_id);
                                                index.prev_key = Some(key);
                                                break;
                                            }
                                        } else {
                                            index.prev_key = Some(key);
                                            prev_key_prefix = index
                                                .prev_key
                                                .as_ref()
                                                .and_then(|key| {
                                                    key.get(
                                                        ..key.len()
                                                            - std::mem::size_of::<DocumentId>(),
                                                    )
                                                })
                                                .ok_or_else(|| {
                                                    StoreError::InternalError(
                                                        "prev_key is too short".to_string(),
                                                    )
                                                })?;
                                        }

                                        next_it_opts.remaining.insert(doc_id);
                                    } else {
                                        // doc id found
                                        break 'inner;
                                    }
                                }
                            }
                        }
                        IndexType::DocumentSet(index) => {
                            if let Some(it) = &mut index.it {
                                if let Some(_doc_id) = it.next() {
                                    doc_id = _doc_id;
                                    break 'inner;
                                }
                            } else {
                                let mut set = index.set.clone();
                                set.bitand_assign(&it_opts.remaining);
                                let set_len = set.len();
                                if set_len > 0 {
                                    it_opts.remaining.bitxor_assign(&set);

                                    match &mut next_it_opts {
                                        Some(next_it_opts) if set_len > 1 => {
                                            next_it_opts.remaining = set;
                                        }
                                        _ if set_len == 1 => {
                                            doc_id = set.min().unwrap();
                                            break 'inner;
                                        }
                                        _ => {
                                            let mut it = set.into_iter();
                                            let result = it.next();
                                            index.it = Some(it);
                                            if let Some(result) = result {
                                                doc_id = result;
                                                break 'inner;
                                            } else {
                                                break 'outer;
                                            }
                                        }
                                    }
                                } else if !it_opts.remaining.is_empty() {
                                    if let Some(ref mut next_it_opts) = next_it_opts {
                                        next_it_opts.remaining =
                                            std::mem::take(&mut it_opts.remaining);
                                    }
                                }
                            };
                        }
                        IndexType::None => (),
                    };

                    if let Some(next_it_opts) = next_it_opts {
                        if !next_it_opts.remaining.is_empty() {
                            if next_it_opts.remaining.len() == 1 {
                                doc_id = next_it_opts.remaining.min().unwrap();
                                next_it_opts.remaining.remove(doc_id);
                                break 'inner;
                            } else {
                                match &mut next_it_opts.index {
                                    IndexType::DB(index) => {
                                        if let Some(it) = &mut index.it {
                                            *it = db.iterator(
                                                ColumnFamily::Indexes,
                                                index.start_key.clone(),
                                                if index.ascending {
                                                    Direction::Forward
                                                } else {
                                                    Direction::Backward
                                                },
                                            )?;
                                        }
                                        index.prev_item = None;
                                        index.prev_key = None;
                                    }
                                    IndexType::DocumentSet(index) => {
                                        index.it = None;
                                    }
                                    IndexType::None => (),
                                }

                                current += 1;
                                next_it_opts.eof = false;
                                continue 'inner;
                            }
                        }
                    }

                    it_opts.eof = true;

                    if it_opts.remaining.is_empty() {
                        if current > 0 {
                            current -= 1;
                        } else {
                            break 'outer;
                        }
                    }
                }

                let result = if let Some(jmap_prefix) = &request.jmap_prefix {
                    if let Some(prefix_id) = db.get::<DocumentId>(
                        ColumnFamily::Values,
                        serialize_stored_key(
                            request.account_id,
                            jmap_prefix.collection_id,
                            doc_id,
                            jmap_prefix.field_id,
                        ),
                    )? {
                        if jmap_prefix.unique && !seen_prefixes.insert(prefix_id) {
                            continue;
                        }
                        (prefix_id as JMAPId) << 32 | doc_id as JMAPId
                    } else {
                        continue;
                    }
                } else {
                    doc_id as JMAPId
                };

                if !has_anchor {
                    if request.position >= 0 {
                        if request.position > 0 {
                            request.position -= 1;
                        } else {
                            results.push(result);
                            if request.limit > 0 && results.len() == request.limit {
                                break;
                            }
                        }
                    } else {
                        results.push(result);
                    }
                } else if request.anchor_offset >= 0 {
                    if !anchor_found {
                        if &result != request.anchor.as_ref().unwrap() {
                            continue;
                        }
                        anchor_found = true;
                    }

                    if request.anchor_offset > 0 {
                        request.anchor_offset -= 1;
                    } else {
                        results.push(result);
                        if request.limit > 0 && results.len() == request.limit {
                            break;
                        }
                    }
                } else {
                    anchor_found = &result == request.anchor.as_ref().unwrap();
                    results.push(result);

                    if !anchor_found {
                        continue;
                    }

                    request.position = request.anchor_offset;

                    break;
                }
            }

            let results = if !has_anchor || anchor_found {
                if request.position >= 0 {
                    start_position = request.position as usize;
                    results
                } else {
                    let position = request.position.abs() as usize;
                    let start_offset = if position < results.len() {
                        results.len() - position
                    } else {
                        0
                    };
                    start_position = start_offset;
                    let end_offset = if request.limit > 0 {
                        std::cmp::min(start_offset + request.limit, results.len())
                    } else {
                        results.len()
                    };

                    results[start_offset..end_offset].to_vec()
                }
            } else {
                return Err(StoreError::AnchorNotFound);
            };

            Ok(JMAPStoreResult {
                start_position,
                results,
                total_results,
            })
        })
        .await
    }
}
