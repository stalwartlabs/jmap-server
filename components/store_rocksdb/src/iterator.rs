use std::{
    ops::{BitAndAssign, BitXorAssign},
    sync::Arc,
};

use roaring::RoaringBitmap;
use rocksdb::{
    BoundColumnFamily, DBIteratorWithThreadMode, DBWithThreadMode, Direction, IteratorMode,
    MultiThreaded,
};
use store::{
    serialize::{deserialize_index_document_id, serialize_index_key_prefix},
    AccountId, CollectionId, Comparator, DocumentId, DocumentSet, FieldId,
};

use crate::{bitmaps::RocksDBDocumentSet, RocksDBStore};

pub struct RocksDBIterator<'x> {
    db: &'x DBWithThreadMode<MultiThreaded>,
    cf_indexes: Arc<BoundColumnFamily<'x>>,
    iterators: Vec<IndexIterator<'x>>,
    current: usize,
}

struct DocumentSetIndex {
    set: RoaringBitmap,
    it: Option<roaring::bitmap::IntoIter>,
}

struct DBIndex<'x> {
    it: Option<DBIteratorWithThreadMode<'x, DBWithThreadMode<MultiThreaded>>>,
    prefix: Vec<u8>,
    start_key: Vec<u8>,
    ascending: bool,
    prev_item: Option<DocumentId>,
    prev_key: Option<Box<[u8]>>,
}

enum IndexType<'x> {
    DocumentSet(DocumentSetIndex),
    DB(DBIndex<'x>),
    None,
}

struct IndexIterator<'x> {
    index: IndexType<'x>,
    remaining: RoaringBitmap,
    eof: bool,
}

impl<'x> RocksDBIterator<'x> {
    pub fn new(
        account: AccountId,
        collection: CollectionId,
        mut results: RoaringBitmap,
        db: &'x RocksDBStore,
        sort: Comparator<RocksDBDocumentSet>,
    ) -> store::Result<RocksDBIterator<'x>> {
        let mut iterators = Vec::new();
        let mut all_doc_ids = None;

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
                        let prefix = serialize_index_key_prefix(account, collection, comp.field);
                        IndexType::DB(DBIndex {
                            it: None,
                            start_key: if !comp.ascending {
                                let (account, collection, field) = if comp.field < FieldId::MAX {
                                    (account, collection, comp.field + 1)
                                } else if collection < CollectionId::MAX {
                                    (account, collection + 1, comp.field)
                                } else {
                                    (account + 1, collection, comp.field)
                                };
                                serialize_index_key_prefix(account, collection, field)
                            } else {
                                prefix.clone()
                            },
                            prefix,
                            ascending: comp.ascending,
                            prev_item: None,
                            prev_key: None,
                        })
                    }
                    Comparator::DocumentSet(comp) => IndexType::DocumentSet(DocumentSetIndex {
                        set: if !comp.ascending {
                            let all_doc_ids = if let Some(all_doc_ids) = &all_doc_ids {
                                all_doc_ids
                            } else {
                                all_doc_ids = db
                                    .get_document_ids(account, collection)?
                                    .unwrap_or_else(RoaringBitmap::new)
                                    .into();
                                &all_doc_ids.as_ref().unwrap()
                            };
                            if !comp.set.is_empty() {
                                let mut set = comp.set.unwrap();
                                set.bitxor_assign(all_doc_ids);
                                set
                            } else {
                                all_doc_ids.clone()
                            }
                        } else {
                            comp.set.unwrap()
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

        Ok(RocksDBIterator {
            cf_indexes: db.get_handle("indexes")?,
            db: db.get_db(),
            iterators,
            current: 0,
        })
    }
}

impl<'x> Iterator for RocksDBIterator<'x> {
    type Item = DocumentId;

    #[allow(clippy::while_let_on_iterator)]
    fn next(&mut self) -> Option<DocumentId> {
        loop {
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
                    continue;
                } else {
                    return None;
                }
            } else if it_opts.remaining.len() == 1 || it_opts.eof {
                let next = it_opts.remaining.min().unwrap();
                it_opts.remaining.remove(next);
                return Some(next);
            }

            match &mut it_opts.index {
                IndexType::DB(index) => {
                    let it = if let Some(it) = &mut index.it {
                        it
                    } else {
                        index.it = Some(self.db.iterator_cf(
                            &self.cf_indexes,
                            IteratorMode::From(
                                &index.start_key,
                                if index.ascending {
                                    Direction::Forward
                                } else {
                                    Direction::Reverse
                                },
                            ),
                        ));
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
                            return Some(prev_item);
                        }
                    }

                    while let Some((key, _)) = it.next() {
                        if !key.starts_with(&index.prefix) {
                            index.prev_key = None;
                            break;
                        }

                        let doc_id = deserialize_index_document_id(&key)?;
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
                                    prev_key_prefix = index.prev_key.as_ref().and_then(|key| {
                                        key.get(..key.len() - std::mem::size_of::<DocumentId>())
                                    })?;
                                }

                                next_it_opts.remaining.insert(doc_id);
                            } else {
                                return Some(doc_id);
                            }
                        }
                    }
                }
                IndexType::DocumentSet(index) => {
                    if let Some(it) = &mut index.it {
                        if let Some(doc_id) = it.next() {
                            return Some(doc_id);
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
                                    return set.min();
                                }
                                _ => {
                                    let mut it = set.into_iter();
                                    let doc_id = it.next();
                                    index.it = Some(it);
                                    return doc_id;
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
                        let next = next_it_opts.remaining.min().unwrap();
                        next_it_opts.remaining.remove(next);
                        return Some(next);
                    } else {
                        match &mut next_it_opts.index {
                            IndexType::DB(index) => {
                                if let Some(it) = &mut index.it {
                                    it.set_mode(IteratorMode::From(
                                        &index.start_key,
                                        if index.ascending {
                                            Direction::Forward
                                        } else {
                                            Direction::Reverse
                                        },
                                    ));
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
                        continue;
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
            } /*else {
                  debug_assert!(
                      false,
                      "Index has missing documents: {:?}",
                      it_opts.remaining
                  );
              }*/
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
