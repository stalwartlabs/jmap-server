use std::ops::BitAndAssign;

use rocksdb::{Direction, IteratorMode, WriteBatch};
use store::{
    serialize::{
        deserialize_document_id_from_leb128, deserialize_index_document_id, serialize_a_key_be,
        serialize_a_key_leb128, serialize_ac_key_be, serialize_ac_key_leb128,
        serialize_bm_internal, BM_TOMBSTONED_IDS, BM_USED_IDS,
    },
    AccountId, CollectionId, DocumentId, StoreDelete, StoreError,
};

use crate::{
    bitmaps::{clear_bits, into_bitmap, set_bits},
    RocksDBStore,
};

const DELETE_BATCH_SIZE: usize = 1000;

impl StoreDelete for RocksDBStore {
    fn delete_account(&self, account: AccountId) -> store::Result<()> {
        let mut batch = WriteBatch::default();
        let mut batch_size = 0;

        for (cf, prefix) in [
            (self.get_handle("values")?, serialize_a_key_leb128(account)),
            (self.get_handle("indexes")?, serialize_a_key_be(account)),
            (self.get_handle("bitmaps")?, serialize_a_key_be(account)),
        ] {
            for (key, _) in self
                .db
                .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward))
            {
                if key.starts_with(&prefix) {
                    batch.delete_cf(&cf, key);
                    batch_size += 1;

                    if batch_size == DELETE_BATCH_SIZE {
                        self.db
                            .write(batch)
                            .map_err(|e| StoreError::InternalError(e.to_string()))?;
                        batch = WriteBatch::default();
                        batch_size = 0;
                    }
                } else {
                    break;
                }
            }
        }

        if batch_size > 0 {
            self.db
                .write(batch)
                .map_err(|e| StoreError::InternalError(e.to_string()))?;
        }

        Ok(())
    }

    fn delete_collection(&self, account: AccountId, collection: CollectionId) -> store::Result<()> {
        let mut batch = WriteBatch::default();
        let mut batch_size = 0;

        for (cf, prefix) in [
            (
                self.get_handle("values")?,
                serialize_ac_key_leb128(account, collection),
            ),
            (
                self.get_handle("indexes")?,
                serialize_ac_key_be(account, collection),
            ),
        ] {
            for (key, _) in self
                .db
                .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward))
            {
                if key.starts_with(&prefix) {
                    batch.delete_cf(&cf, key);
                    batch_size += 1;

                    if batch_size == DELETE_BATCH_SIZE {
                        self.db
                            .write(batch)
                            .map_err(|e| StoreError::InternalError(e.to_string()))?;
                        batch = WriteBatch::default();
                        batch_size = 0;
                    }
                } else {
                    break;
                }
            }
        }

        let cf_bitmaps = self.get_handle("bitmaps")?;
        let doc_list_key =
            serialize_bm_internal(account, collection, BM_USED_IDS).into_boxed_slice();
        let tombstone_key =
            serialize_bm_internal(account, collection, BM_TOMBSTONED_IDS).into_boxed_slice();
        let prefix = serialize_a_key_leb128(account);

        for (key, _) in self
            .db
            .iterator_cf(&cf_bitmaps, IteratorMode::From(&prefix, Direction::Forward))
        {
            if !key.starts_with(&prefix) {
                break;
            } else if (key.len() > 3 && key[key.len() - 3] == collection)
                || key == doc_list_key
                || key == tombstone_key
            {
                batch.delete_cf(&cf_bitmaps, key);
                batch_size += 1;

                if batch_size == DELETE_BATCH_SIZE {
                    self.db
                        .write(batch)
                        .map_err(|e| StoreError::InternalError(e.to_string()))?;
                    batch = WriteBatch::default();
                    batch_size = 0;
                }
            }
        }

        if batch_size > 0 {
            self.db
                .write(batch)
                .map_err(|e| StoreError::InternalError(e.to_string()))?;
        }

        Ok(())
    }

    fn delete_document_bulk(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: &[DocumentId],
    ) -> store::Result<()> {
        self.db
            .merge_cf(
                &self.get_handle("bitmaps")?,
                &serialize_bm_internal(account, collection, BM_TOMBSTONED_IDS),
                &set_bits(documents.iter().copied()),
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))
    }
}

impl RocksDBStore {
    pub fn purge_tombstoned(
        &self,
        account: AccountId,
        collection: CollectionId,
    ) -> store::Result<()> {
        let documents = if let Some(documents) = self.get_tombstoned_ids(account, collection)? {
            documents
        } else {
            return Ok(());
        };
        let mut batch = WriteBatch::default();
        let mut batch_size = 0;

        for (cf, prefix, deserialize_fnc) in [
            (
                self.get_handle("values")?,
                serialize_ac_key_leb128(account, collection),
                deserialize_document_id_from_leb128 as fn(&[u8]) -> Option<DocumentId>,
            ),
            (
                self.get_handle("indexes")?,
                serialize_ac_key_be(account, collection),
                deserialize_index_document_id as fn(&[u8]) -> Option<DocumentId>,
            ),
        ] {
            for (key, _) in self
                .db
                .iterator_cf(&cf, IteratorMode::From(&prefix, Direction::Forward))
            {
                if !key.starts_with(&prefix) {
                    break;
                }
                if key.len() > prefix.len() {
                    if let Some(doc_id) = deserialize_fnc(&key[prefix.len()..]) {
                        if documents.contains(doc_id) {
                            batch.delete_cf(&cf, key);
                            batch_size += 1;

                            if batch_size == DELETE_BATCH_SIZE {
                                self.db
                                    .write(batch)
                                    .map_err(|e| StoreError::InternalError(e.to_string()))?;
                                batch = WriteBatch::default();
                                batch_size = 0;
                            }
                        }
                    } else {
                        return Err(StoreError::InternalError(
                            "Failed to deserialize document id".into(),
                        ));
                    }
                }
            }
        }

        let cf_bitmaps = self.get_handle("bitmaps")?;
        let prefix = serialize_a_key_leb128(account);

        for (key, value) in self
            .db
            .iterator_cf(&cf_bitmaps, IteratorMode::From(&prefix, Direction::Forward))
        {
            if !key.starts_with(&prefix) {
                break;
            } else if key.len() > 3 && key[key.len() - 3] == collection {
                let mut bm = into_bitmap(&value)?;
                bm.bitand_assign(&documents);

                if !bm.is_empty() {
                    batch.merge_cf(&cf_bitmaps, key, clear_bits(bm.iter()));
                    batch_size += 1;

                    if batch_size == DELETE_BATCH_SIZE {
                        self.db
                            .write(batch)
                            .map_err(|e| StoreError::InternalError(e.to_string()))?;
                        batch = WriteBatch::default();
                        batch_size = 0;
                    }
                }
            }
        }

        batch.merge_cf(
            &cf_bitmaps,
            serialize_bm_internal(account, collection, BM_USED_IDS),
            clear_bits(documents.iter()),
        );

        batch.merge_cf(
            &cf_bitmaps,
            serialize_bm_internal(account, collection, BM_TOMBSTONED_IDS),
            clear_bits(documents.iter()),
        );

        self.db
            .write(batch)
            .map_err(|e| StoreError::InternalError(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::iter::FromIterator;

    use nlp::Language;
    use store::batch::WriteOperation;
    use store::field::Text;
    use store::{
        Comparator, DocumentId, FieldId, FieldValue, Filter, Float, Integer, LongInteger,
        StoreDelete, StoreGet, StoreQuery, StoreTag, StoreUpdate, Tag, TextQuery,
    };

    use crate::RocksDBStore;

    #[test]
    fn delete() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_delete_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }
        {
            let db = RocksDBStore::open(temp_dir.to_str().unwrap()).unwrap();

            for raw_doc_num in 0..10 {
                let mut builder = WriteOperation::insert_document(0, 0);
                builder.add_text(
                    0,
                    0,
                    Text::Keyword(format!("keyword_{}", raw_doc_num).into()),
                    true,
                    true,
                );
                builder.add_text(
                    1,
                    0,
                    Text::Tokenized(format!("this is the text number {}", raw_doc_num).into()),
                    true,
                    true,
                );
                builder.add_text(
                    2,
                    0,
                    Text::Full((
                        format!("and here goes the full text number {}", raw_doc_num).into(),
                        Language::English,
                    )),
                    true,
                    true,
                );
                builder.add_float(3, 0, raw_doc_num as Float, true, true);
                builder.add_integer(4, 0, raw_doc_num as Integer, true, true);
                builder.add_long_int(5, 0, raw_doc_num as LongInteger, true, true);
                builder.add_tag(6, Tag::Id(0));
                builder.add_tag(7, Tag::Static(0));
                builder.add_tag(8, Tag::Text("my custom tag".into()));

                db.update(builder).unwrap();
            }

            db.delete_document(0, 0, 9).unwrap();
            db.delete_document(0, 0, 0).unwrap();

            for do_purge in [true, false] {
                for field in 0..6 {
                    assert_eq!(
                        db.query(0, 0, Filter::None, Comparator::ascending(field))
                            .unwrap()
                            .collect::<Vec<DocumentId>>(),
                        Vec::from_iter(1..9),
                        "Field {}",
                        field
                    );

                    for field in 0..6 {
                        assert!(db
                            .get_document_value::<Vec<u8>>(0, 0, 0, field, 0)
                            .unwrap()
                            .is_none());
                        assert!(db
                            .get_document_value::<Vec<u8>>(0, 0, 9, field, 0)
                            .unwrap()
                            .is_none());
                        for doc_id in 1..9 {
                            assert!(db
                                .get_document_value::<Vec<u8>>(0, 0, doc_id, field, 0)
                                .unwrap()
                                .is_some());
                        }
                    }
                }

                assert_eq!(
                    db.query(
                        0,
                        0,
                        Filter::eq(1, FieldValue::Text("text".into())),
                        Comparator::None
                    )
                    .unwrap()
                    .collect::<Vec<DocumentId>>(),
                    Vec::from_iter(1..9)
                );

                assert_eq!(
                    db.query(
                        0,
                        0,
                        Filter::eq(
                            2,
                            FieldValue::FullText(TextQuery::query_english("text".into()))
                        ),
                        Comparator::None
                    )
                    .unwrap()
                    .collect::<Vec<DocumentId>>(),
                    Vec::from_iter(1..9)
                );

                for (pos, tag) in [
                    Tag::Id(0),
                    Tag::Static(0),
                    Tag::Text("my custom tag".into()),
                ]
                .iter()
                .enumerate()
                {
                    assert!(!db
                        .has_tag(0, 0, 0, 6 + pos as FieldId, tag.clone())
                        .unwrap());
                    assert!(!db
                        .has_tag(0, 0, 9, 6 + pos as FieldId, tag.clone())
                        .unwrap());
                    for doc_id in 1..9 {
                        assert!(db
                            .has_tag(0, 0, doc_id, 6 + pos as FieldId, tag.clone())
                            .unwrap());
                    }
                }

                if do_purge {
                    assert_eq!(
                        db.get_tombstoned_ids(0, 0).unwrap().unwrap(),
                        [0, 9].iter().copied().collect()
                    );
                    db.purge_tombstoned(0, 0).unwrap();
                    assert!(db.get_tombstoned_ids(0, 0).unwrap().is_none());
                }
            }
        }
        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
}
