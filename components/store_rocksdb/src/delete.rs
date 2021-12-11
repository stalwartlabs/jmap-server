use std::collections::HashSet;

use rocksdb::{Direction, IteratorMode, WriteBatch};
use store::{
    leb128::Leb128,
    serialize::{
        deserialize_document_id_from_leb128, deserialize_index_document_id, serialize_a_key_be,
        serialize_a_key_leb128, serialize_ac_key_be, serialize_ac_key_leb128,
    },
    AccountId, CollectionId, DocumentId, StoreDelete, StoreError,
};

use crate::{
    bitmaps::{clear_bits, into_bitmap},
    RocksDBStore,
};

const DELETE_BATCH_SIZE: usize = 1000;

impl StoreDelete for RocksDBStore {
    fn delete_document_bulk(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: HashSet<DocumentId>,
    ) -> store::Result<()> {
        let mut batch = WriteBatch::default();
        let mut batch_size = 0;

        for (cf, prefix, deserialize_fnc) in [
            (
                self.db.cf_handle("values").ok_or_else(|| {
                    StoreError::InternalError("No values column family found.".into())
                })?,
                serialize_ac_key_leb128(account, collection),
                deserialize_document_id_from_leb128 as fn(&[u8]) -> Option<DocumentId>,
            ),
            (
                self.db.cf_handle("indexes").ok_or_else(|| {
                    StoreError::InternalError("No indexes column family found.".into())
                })?,
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
                        if documents.contains(&doc_id) {
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

        let cf_bitmaps = self
            .db
            .cf_handle("bitmaps")
            .ok_or_else(|| StoreError::InternalError("No bitmaps column family found.".into()))?;
        let mut deletion_list = Vec::with_capacity(documents.len());
        let doc_list_key = serialize_ac_key_leb128(account, collection).into_boxed_slice();
        let prefix = serialize_a_key_leb128(account);

        for (key, value) in self
            .db
            .iterator_cf(&cf_bitmaps, IteratorMode::From(&prefix, Direction::Forward))
        {
            if !key.starts_with(&prefix) {
                break;
            } else if (key.len() > 3 && key[key.len() - 3] == collection) || key == doc_list_key {
                let bm = into_bitmap(&value)?;

                for &doc_id in &documents {
                    if bm.contains(doc_id) {
                        deletion_list.push(doc_id);
                    }
                }

                if !deletion_list.is_empty() {
                    batch.merge_cf(&cf_bitmaps, key, clear_bits(deletion_list.drain(..)));
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

        if batch_size > 0 {
            self.db
                .write(batch)
                .map_err(|e| StoreError::InternalError(e.to_string()))?;
        }

        Ok(())
    }

    fn delete_account(&self, account: AccountId) -> store::Result<()> {
        let mut batch = WriteBatch::default();
        let mut batch_size = 0;

        for (cf, prefix) in [
            (
                self.db.cf_handle("values").ok_or_else(|| {
                    StoreError::InternalError("No values column family found.".into())
                })?,
                serialize_a_key_leb128(account),
            ),
            (
                self.db.cf_handle("indexes").ok_or_else(|| {
                    StoreError::InternalError("No indexes column family found.".into())
                })?,
                serialize_a_key_be(account),
            ),
            (
                self.db.cf_handle("bitmaps").ok_or_else(|| {
                    StoreError::InternalError("No bitmaps column family found.".into())
                })?,
                serialize_a_key_be(account),
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
                self.db.cf_handle("values").ok_or_else(|| {
                    StoreError::InternalError("No values column family found.".into())
                })?,
                serialize_ac_key_leb128(account, collection),
            ),
            (
                self.db.cf_handle("indexes").ok_or_else(|| {
                    StoreError::InternalError("No indexes column family found.".into())
                })?,
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

        let cf_bitmaps = self
            .db
            .cf_handle("bitmaps")
            .ok_or_else(|| StoreError::InternalError("No bitmaps column family found.".into()))?;
        let doc_list_key = serialize_ac_key_leb128(account, collection).into_boxed_slice();
        let prefix = serialize_a_key_leb128(account);

        for (key, _) in self
            .db
            .iterator_cf(&cf_bitmaps, IteratorMode::From(&prefix, Direction::Forward))
        {
            if !key.starts_with(&prefix) {
                break;
            } else if (key.len() > 3 && key[key.len() - 3] == collection) || key == doc_list_key {
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
}

#[cfg(test)]
mod tests {

    use std::iter::FromIterator;

    use nlp::Language;
    use store::document::IndexOptions;
    use store::document::{DocumentBuilder, OptionValue};
    use store::{
        Comparator, DocumentId, FieldId, FieldValue, Filter, Float, Integer, LongInteger,
        StoreDelete, StoreGet, StoreInsert, StoreQuery, StoreTag, Tag, TextQuery,
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
                let mut builder = DocumentBuilder::new();
                builder.add_keyword(
                    0,
                    format!("keyword_{}", raw_doc_num).into(),
                    <OptionValue>::Sortable | <OptionValue>::Stored,
                );
                builder.add_text(
                    1,
                    format!("this is the text number {}", raw_doc_num).into(),
                    <OptionValue>::Sortable | <OptionValue>::Stored,
                );
                builder.add_full_text(
                    2,
                    format!("and here goes the full text number {}", raw_doc_num).into(),
                    Some(Language::English),
                    <OptionValue>::Sortable | <OptionValue>::Stored,
                );
                builder.add_float(
                    3,
                    raw_doc_num as Float,
                    <OptionValue>::Sortable | <OptionValue>::Stored,
                );
                builder.add_integer(
                    4,
                    raw_doc_num as Integer,
                    <OptionValue>::Sortable | <OptionValue>::Stored,
                );
                builder.add_long_int(
                    5,
                    raw_doc_num as LongInteger,
                    <OptionValue>::Sortable | <OptionValue>::Stored,
                );
                builder.add_tag(6, Tag::Id(0), 0);
                builder.add_tag(7, Tag::Static(0), 0);
                builder.add_tag(8, Tag::Text("my custom tag"), 0);

                db.insert(0, 0, builder).unwrap();
            }

            db.delete_document(0, 0, 9).unwrap();
            db.delete_document(0, 0, 0).unwrap();

            for field in 0..6 {
                assert_eq!(
                    db.query(0, 0, None, Some(vec![Comparator::ascending(field)]))
                        .unwrap()
                        .collect::<Vec<DocumentId>>(),
                    Vec::from_iter(1..9),
                    "Field {}",
                    field
                );
                assert!(db.get_stored_value(0, 0, 0, field, 0).unwrap().is_none());
                assert!(db.get_stored_value(0, 0, 9, field, 0).unwrap().is_none());
                for doc_id in 1..9 {
                    assert!(db
                        .get_stored_value(0, 0, doc_id, field, 0)
                        .unwrap()
                        .is_some());
                }
            }

            assert_eq!(
                db.query(0, 0, Some(Filter::eq(1, FieldValue::Text("text"))), None)
                    .unwrap()
                    .collect::<Vec<DocumentId>>(),
                Vec::from_iter(1..9)
            );

            assert_eq!(
                db.query(
                    0,
                    0,
                    Some(Filter::eq(
                        2,
                        FieldValue::FullText(TextQuery::query_english("text"))
                    )),
                    None
                )
                .unwrap()
                .collect::<Vec<DocumentId>>(),
                Vec::from_iter(1..9)
            );

            for (pos, tag) in [Tag::Id(0), Tag::Static(0), Tag::Text("my custom tag")]
                .iter()
                .enumerate()
            {
                assert!(!db.has_tag(0, 0, 0, 6 + pos as FieldId, tag).unwrap());
                assert!(!db.has_tag(0, 0, 9, 6 + pos as FieldId, tag).unwrap());
                for doc_id in 1..9 {
                    assert!(db.has_tag(0, 0, doc_id, 6 + pos as FieldId, tag).unwrap());
                }
            }
        }
        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
}
