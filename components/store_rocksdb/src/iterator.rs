use std::sync::Arc;

use roaring::RoaringBitmap;
use rocksdb::{
    BoundColumnFamily, DBIteratorWithThreadMode, DBWithThreadMode, Direction, IteratorMode,
    MultiThreaded,
};
use store::{
    serialize::{deserialize_document_id, serialize_index_key_prefix},
    AccountId, CollectionId, Comparator, DocumentId, FieldId,
};

pub struct RocksDBIterator<'x> {
    db: &'x DBWithThreadMode<MultiThreaded>,
    cf_indexes: Arc<BoundColumnFamily<'x>>,
    iterators: Vec<IndexIterator<'x>>,
    current: usize,
}

struct IndexIterator<'x> {
    it: Option<DBIteratorWithThreadMode<'x, DBWithThreadMode<MultiThreaded>>>,
    prefix: Vec<u8>,
    start_key: Vec<u8>,
    ascending: bool,
    remaining: RoaringBitmap,
    next_item: Option<DocumentId>,
    eof: bool,
}

impl<'x> IndexIterator<'x> {
    pub fn new(
        account: AccountId,
        collection: CollectionId,
        comp: &Comparator,
        results: RoaringBitmap,
    ) -> IndexIterator<'x> {
        let prefix = serialize_index_key_prefix(account, collection, comp.field);
        let start_key = if !comp.ascending {
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
        };
        IndexIterator {
            it: None,
            start_key,
            prefix,
            ascending: comp.ascending,
            eof: false,
            remaining: results,
            next_item: None,
        }
    }
}

impl<'x> RocksDBIterator<'x> {
    pub fn new(
        account: AccountId,
        collection: CollectionId,
        results: RoaringBitmap,
        db: &'x DBWithThreadMode<MultiThreaded>,
        cf_indexes: Arc<BoundColumnFamily<'x>>,
        sort: Option<Vec<Comparator>>,
    ) -> RocksDBIterator<'x> {
        let iterators = if let Some(sort) = sort {
            let mut iterators = Vec::with_capacity(sort.len());
            let mut sort_it = sort.iter();
            if let Some(comp) = sort_it.next() {
                iterators.push(IndexIterator::new(account, collection, comp, results));
            }
            for comp in sort_it {
                iterators.push(IndexIterator::new(
                    account,
                    collection,
                    comp,
                    RoaringBitmap::new(),
                ));
            }
            iterators
        } else {
            vec![IndexIterator {
                it: None,
                prefix: vec![],
                start_key: vec![],
                ascending: false,
                eof: true,
                remaining: results,
                next_item: None,
            }]
        };
        RocksDBIterator {
            cf_indexes,
            db,
            iterators,
            current: 0,
        }
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

            if let Some(next_item) = it_opts.next_item {
                it_opts.next_item = None;
                return Some(next_item);
            } else if it_opts.remaining.is_empty() {
                if self.current > 0 {
                    self.current -= 1;
                    continue;
                } else {
                    return None;
                }
            } else if it_opts.remaining.len() == 1 || it_opts.eof {
                let next = it_opts.remaining.min().unwrap();
                //println!("Got {} from remaining {:?}", next, it_opts.eof);
                it_opts.remaining.remove(next);
                return Some(next);
            }

            let it = if let Some(it) = &mut it_opts.it {
                it
            } else {
                it_opts.it = Some(self.db.iterator_cf(
                    &self.cf_indexes,
                    IteratorMode::From(
                        &it_opts.start_key,
                        if it_opts.ascending {
                            Direction::Forward
                        } else {
                            Direction::Reverse
                        },
                    ),
                ));
                it_opts.it.as_mut().unwrap()
            };

            let mut last_key: Option<Box<[u8]>> = None;
            let mut last_key_prefix = &[][..];

            while let Some((key, _)) = it.next() {
                if !key.starts_with(&it_opts.prefix) {
                    break;
                }

                let doc_id = deserialize_document_id(&key)?;
                if it_opts.remaining.contains(doc_id) {
                    it_opts.remaining.remove(doc_id);

                    if let Some(next_it_opts) = &mut next_it_opts {
                        if let Some(last_key) = &last_key {
                            if key.len() != last_key.len() || !key.starts_with(last_key_prefix) {
                                //println!("Saved next item {:?}", doc_id);
                                it_opts.next_item = Some(doc_id);
                                break;
                            }
                        } else {
                            let last_key_len = key.len() - std::mem::size_of::<DocumentId>();
                            last_key = Some(key);
                            last_key_prefix = last_key.as_ref().unwrap().get(0..last_key_len)?;
                        }
                        //println!("Added to next iterator {:?}", doc_id);
                        next_it_opts.remaining.insert(doc_id);
                    } else {
                        return Some(doc_id);
                    }
                }
            }

            if let Some(next_it_opts) = next_it_opts {
                if !next_it_opts.remaining.is_empty() {
                    if next_it_opts.remaining.len() == 1 {
                        let next = next_it_opts.remaining.min().unwrap();
                        next_it_opts.remaining.remove(next);
                        //println!("Returning single item from next_it {:?}", next);
                        return Some(next);
                    } else {
                        if let Some(it) = &mut next_it_opts.it {
                            it.set_mode(IteratorMode::From(
                                &it_opts.start_key,
                                if it_opts.ascending {
                                    Direction::Forward
                                } else {
                                    Direction::Reverse
                                },
                            ));
                        }
                        self.current += 1;
                        //println!("Moving to iterator {}", self.current + 1);
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
            } /* else {

                  debug_assert!(false,
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
