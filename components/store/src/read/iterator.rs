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

use std::ops::{BitAndAssign, BitXorAssign};

use roaring::RoaringBitmap;

use crate::{
    core::collection::Collection, serialize::key::IndexKey, AccountId, ColumnFamily, Direction,
    DocumentId, FieldId, JMAPId, JMAPStore, Store,
};

use super::comparator::Comparator;

pub struct StoreIterator<'x, T, U>
where
    T: Store<'x>,
    U: FnMut(DocumentId) -> crate::Result<Option<JMAPId>>,
{
    store: &'x JMAPStore<T>,
    iterators: Vec<IndexIterator<'x, T>>,
    filter_map: Option<U>,
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

impl<'x, T> IndexType<'x, T>
where
    T: Store<'x>,
{
    pub fn has_prev_item(&self) -> bool {
        match self {
            IndexType::DB(index) => index.prev_item.is_some(),
            _ => false,
        }
    }
}

struct IndexIterator<'x, T>
where
    T: Store<'x>,
{
    index: IndexType<'x, T>,
    remaining: RoaringBitmap,
    eof: bool,
}

impl<'x, T, U> StoreIterator<'x, T, U>
where
    T: Store<'x>,
    U: FnMut(DocumentId) -> crate::Result<Option<JMAPId>>,
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

    pub fn set_filter_map(mut self, filter_map: U) -> Self {
        self.filter_map = Some(filter_map);
        self
    }

    pub fn len(&self) -> usize {
        self.iterators[0].remaining.len() as usize
    }

    pub fn into_bitmap(mut self) -> RoaringBitmap {
        self.iterators.swap_remove(0).remaining
    }

    pub fn get_min(&self) -> Option<DocumentId> {
        self.iterators.get(0).and_then(|i| i.remaining.min())
    }

    pub fn get_max(&self) -> Option<DocumentId> {
        self.iterators.get(0).and_then(|i| i.remaining.max())
    }

    pub fn is_empty(&self) -> bool {
        self.iterators[0].remaining.is_empty()
    }
}

impl<'x, T, U> Iterator for StoreIterator<'x, T, U>
where
    T: Store<'x>,
    U: FnMut(DocumentId) -> crate::Result<Option<JMAPId>>,
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

                if !it_opts.index.has_prev_item() {
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

                        let mut is_eof = false;
                        loop {
                            if let Some((key, _)) = it.next() {
                                if !key.starts_with(&index.prefix) {
                                    index.prev_key = None;
                                    is_eof = true;
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
                                                        ..key.len()
                                                            - std::mem::size_of::<DocumentId>(),
                                                    )
                                                })?;
                                        }

                                        next_it_opts.remaining.insert(doc_id);
                                    } else {
                                        // doc id found
                                        break 'inner;
                                    }
                                }
                            } else {
                                is_eof = true;
                                break;
                            }
                        }

                        if is_eof {
                            if let Some(next_it_opts) = &mut next_it_opts {
                                if !it_opts.remaining.is_empty() {
                                    next_it_opts.remaining |= &it_opts.remaining;
                                    it_opts.remaining.clear();
                                }
                                index.prev_key = None;
                                it_opts.eof = true;
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
                if let Some(jmap_id) = filter_map(doc_id).ok()? {
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
