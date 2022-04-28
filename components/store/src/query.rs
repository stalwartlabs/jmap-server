use crate::{
    batch::MAX_TOKEN_LENGTH,
    bitmap::bitmap_op,
    serialize::{BitmapKey, IndexKey, ValueKey},
    term_index::TermIndex,
    AccountId, Collection, ColumnFamily, Comparator, Direction, DocumentId, FieldId, FieldValue,
    Filter, FilterOperator, JMAPId, JMAPStore, LogicalOperator, Store, StoreError,
};
use nlp::{stemmer::Stemmer, tokenizers::Tokenizer, Language};
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

pub trait QueryFilterMap {
    fn filter_map_id(&mut self, document_id: DocumentId) -> crate::Result<Option<JMAPId>>;
}

pub struct DefaultIdMapper {}

pub struct StoreIterator<'x, T, U>
where
    T: Store<'x>,
    U: QueryFilterMap,
{
    store: &'x JMAPStore<T>,
    iterators: Vec<IndexIterator<'x, T>>,
    filter_map: Option<&'x mut U>,
    current: usize,
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
    pub fn query_store<'y: 'x, 'x, U>(
        &'y self,
        account_id: AccountId,
        collection: Collection,
        filter: Filter,
        sort: Comparator,
    ) -> crate::Result<StoreIterator<'x, T, U>>
    where
        U: QueryFilterMap,
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
                            FieldValue::Keyword(keyword) => {
                                bitmap_op(
                                    state.op,
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
                            FieldValue::Text(text) => {
                                let field_cond_field = filter_cond.field;
                                bitmap_op(
                                    state.op,
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
                            FieldValue::FullText(query) => {
                                if query.match_phrase {
                                    let mut phrase: Vec<String> = Vec::new();
                                    let field = filter_cond.field;

                                    // Retrieve the Term Index for each candidate and match the exact phrase

                                    if let Some(candidates) = self.get_bitmaps_intersection(
                                        Tokenizer::new(
                                            &query.text,
                                            query.language,
                                            MAX_TOKEN_LENGTH,
                                        )
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
                                            if let Some(term_index) = self.db.get::<TermIndex>(
                                                ColumnFamily::Values,
                                                &ValueKey::serialize_term_index(
                                                    account_id,
                                                    collection,
                                                    document_id,
                                                ),
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
                                        bitmap_op(
                                            state.op,
                                            &mut state.bm,
                                            results.into(),
                                            &document_ids,
                                        );
                                    } else {
                                        bitmap_op(state.op, &mut state.bm, None, &document_ids);
                                    }
                                } else {
                                    let mut requested_keys = HashSet::new();
                                    let mut text_bitmap = None;

                                    for token in
                                        Stemmer::new(&query.text, query.language, MAX_TOKEN_LENGTH)
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

                                        bitmap_op(
                                            LogicalOperator::And,
                                            &mut text_bitmap,
                                            self.get_bitmaps_union(keys)?,
                                            &document_ids,
                                        );

                                        if text_bitmap.as_ref().unwrap().is_empty() {
                                            break;
                                        }
                                    }
                                    bitmap_op(state.op, &mut state.bm, text_bitmap, &document_ids);
                                }
                            }
                            FieldValue::Integer(i) => {
                                bitmap_op(
                                    state.op,
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
                            FieldValue::LongInteger(i) => {
                                bitmap_op(
                                    state.op,
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
                            FieldValue::Float(f) => {
                                bitmap_op(
                                    state.op,
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
                            FieldValue::Tag(tag) => {
                                bitmap_op(
                                    state.op,
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

impl<'x, T, U> StoreIterator<'x, T, U>
where
    T: Store<'x>,
    U: QueryFilterMap,
{
    pub fn new(
        store: &'x JMAPStore<T>,
        mut results: RoaringBitmap,
        document_ids: RoaringBitmap,
        account_id: AccountId,
        collection: Collection,
        sort: Comparator,
    ) -> Self {
        let mut iterators: Vec<IndexIterator<T>> = Vec::new();
        for comp in (if let Comparator::List(list) = sort {
            list
        } else {
            vec![sort]
        })
        .into_iter()
        {
            iterators.push(IndexIterator {
                index: match comp {
                    Comparator::Field(comp) => {
                        let prefix =
                            IndexKey::serialize_field(account_id, collection as u8, comp.field);
                        IndexType::DB(DBIndex {
                            it: None,
                            start_key: if !comp.ascending {
                                let (key_account_id, key_collection, key_field) =
                                    if comp.field < FieldId::MAX {
                                        (account_id, collection as u8, comp.field + 1)
                                    } else if (collection as u8) < u8::MAX {
                                        (account_id, (collection as u8) + 1, comp.field)
                                    } else {
                                        (account_id + 1, collection as u8, comp.field)
                                    };
                                IndexKey::serialize_field(key_account_id, key_collection, key_field)
                            } else {
                                prefix.clone()
                            },
                            prefix,
                            ascending: comp.ascending,
                            prev_item: None,
                            prev_key: None,
                        })
                    }
                    Comparator::DocumentSet(mut comp) => IndexType::DocumentSet(DocumentSetIndex {
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
                    }),
                    _ => IndexType::None,
                },
                eof: false,
                remaining: results,
            });

            results = RoaringBitmap::new();
        }

        StoreIterator {
            store,
            iterators,
            filter_map: None,
            current: 0,
        }
    }

    pub fn set_filter_map(mut self, filter_map: &'x mut U) -> Self {
        self.filter_map = Some(filter_map);
        self
    }

    pub fn len(&self) -> usize {
        self.iterators[0].remaining.len() as usize
    }

    pub fn is_empty(&self) -> bool {
        self.iterators[0].remaining.is_empty()
    }
}

impl<'x, T, U> Iterator for StoreIterator<'x, T, U>
where
    T: Store<'x>,
    U: QueryFilterMap,
{
    type Item = JMAPId;

    #[allow(clippy::while_let_on_iterator)]
    fn next(&mut self) -> Option<Self::Item> {
        'outer: loop {
            let mut doc_id;

            'inner: loop {
                let (it_opts, mut next_it_opts) = if self.current < self.iterators.len() - 1 {
                    let (iterators_first, iterators_last) =
                        self.iterators.split_at_mut(self.current + 1);
                    (
                        iterators_first.last_mut().unwrap(),
                        iterators_last.first_mut(),
                    )
                } else {
                    (&mut self.iterators[self.current], None)
                };

                if it_opts.remaining.is_empty() {
                    if self.current > 0 {
                        self.current -= 1;
                        continue 'inner;
                    } else {
                        return None;
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
                            index.it = Some(
                                self.store
                                    .db
                                    .iterator(
                                        ColumnFamily::Indexes,
                                        &index.start_key,
                                        if index.ascending {
                                            Direction::Forward
                                        } else {
                                            Direction::Backward
                                        },
                                    )
                                    .ok()?,
                            );
                            index.it.as_mut().unwrap()
                        };

                        let mut prev_key_prefix = if let Some(prev_key) = &index.prev_key {
                            prev_key.get(..prev_key.len() - std::mem::size_of::<DocumentId>())?
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

                            doc_id = IndexKey::deserialize_document_id(&key)?;
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
                                        prev_key_prefix =
                                            index.prev_key.as_ref().and_then(|key| {
                                                key.get(
                                                    ..key.len() - std::mem::size_of::<DocumentId>(),
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
                                            return None;
                                        }
                                    }
                                }
                            } else if !it_opts.remaining.is_empty() {
                                if let Some(ref mut next_it_opts) = next_it_opts {
                                    next_it_opts.remaining = std::mem::take(&mut it_opts.remaining);
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
                                        *it = self
                                            .store
                                            .db
                                            .iterator(
                                                ColumnFamily::Indexes,
                                                &index.start_key,
                                                if index.ascending {
                                                    Direction::Forward
                                                } else {
                                                    Direction::Backward
                                                },
                                            )
                                            .ok()?;
                                    }
                                    index.prev_item = None;
                                    index.prev_key = None;
                                }
                                IndexType::DocumentSet(index) => {
                                    index.it = None;
                                }
                                IndexType::None => (),
                            }

                            self.current += 1;
                            next_it_opts.eof = false;
                            continue 'inner;
                        }
                    }
                }

                it_opts.eof = true;

                if it_opts.remaining.is_empty() {
                    if self.current > 0 {
                        self.current -= 1;
                    } else {
                        return None;
                    }
                }
            }

            if let Some(filter_map) = &mut self.filter_map {
                if let Some(jmap_id) = filter_map.filter_map_id(doc_id).ok()? {
                    return Some(jmap_id);
                } else {
                    continue 'outer;
                }
            } else {
                return Some(doc_id as JMAPId);
            };
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let it = &self.iterators[0];

        (
            it.remaining.len() as usize,
            Some(it.remaining.len() as usize),
        )
    }
}

impl QueryFilterMap for DefaultIdMapper {
    fn filter_map_id(&mut self, document_id: DocumentId) -> crate::Result<Option<JMAPId>> {
        Ok(Some(document_id as JMAPId))
    }
}
