use std::convert::TryInto;

use roaring::RoaringBitmap;
use rocksdb::{ColumnFamilyDescriptor, DBWithThreadMode, MergeOperands, MultiThreaded, Options};
use store::{
    document::{DocumentBuilder, IndexOptions},
    serialize::{
        serialize_stored_key, serialize_stored_key_pos, SerializedKeyValue, SerializedValue,
        TokenSerializer,
    },
    AccountId, ArrayPos, CollectionId, DocumentId, FieldId, Result, Store, StoreError, Tag,
};

pub struct RocksDBStore {
    db: DBWithThreadMode<MultiThreaded>,
}

fn bitmap_merge(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let mut rb = if let Some(existing_val) = existing_val {
        RoaringBitmap::deserialize_from(existing_val).ok()?
    } else {
        RoaringBitmap::new()
    };

    for op in operands {
        let id = DocumentId::from_ne_bytes(
            op.get(1..1 + std::mem::size_of::<DocumentId>())?
                .try_into()
                .ok()?,
        );

        if op.get(0)? == &1 {
            rb.insert(id);
        } else {
            rb.remove(id);
        }
    }

    let mut bytes = Vec::with_capacity(rb.serialized_size());
    rb.serialize_into(&mut bytes).ok()?;
    Some(bytes)
}

impl RocksDBStore {
    pub fn open(path: &str) -> Result<Self> {
        // Bitmaps
        let cf_bitmaps = {
            let mut cf_opts = Options::default();
            //cf_opts.set_max_write_buffer_number(16);
            cf_opts.set_merge_operator_associative("bitmap merge", bitmap_merge);
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

        let mut db_opts = Options::default();
        db_opts.create_missing_column_families(true);
        db_opts.create_if_missing(true);

        Ok(Self {
            db: DBWithThreadMode::open_cf_descriptors(
                &db_opts,
                path,
                vec![cf_bitmaps, cf_values, cf_indexes],
            )
            .map_err(|e| StoreError::InternalError(e.into_string()))?,
        })
    }
}

impl Store for RocksDBStore {
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

        let document_id: DocumentId = 0;

        for field in document {
            let field_opt = field.unwrap().get_options();
            if field_opt.is_sortable() {
                self.db
                    .put_cf(
                        &cf_indexes,
                        &field.as_sort_key(account, collection, &document_id),
                        &[],
                    )
                    .map_err(|e| StoreError::InternalError(e.into_string()))?;
            }
            if field_opt.is_stored() {
                match field.as_stored_value(account, collection, &document_id) {
                    SerializedKeyValue {
                        key,
                        value: SerializedValue::Tag,
                    } => {
                        self.db
                            .put_cf(&cf_bitmaps, &key, &set_tag(&document_id))
                            .map_err(|e| StoreError::InternalError(e.into_string()))?;
                    }
                    SerializedKeyValue {
                        key,
                        value: SerializedValue::Owned(value),
                    } => {
                        self.db
                            .put_cf(&cf_values, &key, &value)
                            .map_err(|e| StoreError::InternalError(e.into_string()))?;
                    }
                    SerializedKeyValue {
                        key,
                        value: SerializedValue::Borrowed(value),
                    } => {
                        self.db
                            .put_cf(&cf_values, &key, value)
                            .map_err(|e| StoreError::InternalError(e.into_string()))?;
                    }
                }
            }

            if field_opt.is_tokenized() {
                let field = field.unwrap_text();
                for token in field.tokenize() {
                    self.db
                        .put_cf(
                            &cf_bitmaps,
                            &token.as_index_key(account, collection, field),
                            &set_tag(&document_id),
                        )
                        .map_err(|e| StoreError::InternalError(e.into_string()))?;
                }
            }
        }
        Ok(document_id)
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
        &mut self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
        tag: &Tag,
    ) -> Result<()> {
        todo!()
    }

    fn clear_tag(
        &mut self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
        tag: &Tag,
    ) -> Result<()> {
        todo!()
    }

    fn has_tag(
        &mut self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
        tag: &Tag,
    ) -> Result<bool> {
        todo!()
    }

    fn search(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        filter: &store::Filter,
        order_by: &[store::OrderBy],
    ) -> Result<Vec<DocumentId>> {
        todo!()
    }
}

fn set_tag(document: &DocumentId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(std::mem::size_of::<DocumentId>() + 1);
    buf.push(1);
    buf.extend_from_slice(&document.to_ne_bytes());
    buf
}

fn clear_tag(document: &DocumentId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(std::mem::size_of::<DocumentId>() + 1);
    buf.push(0);
    buf.extend_from_slice(&document.to_ne_bytes());
    buf
}

/*

        Ok(self
        .db
        .get_pinned_cf(
            &self.db.cf_handle("values").ok_or_else(|| {
                StoreError::InternalError("No values column family found.".into())
            })?,
            &serialize_stored_key(account, collection, document, field),
        )
        .map_err(|e| StoreError::InternalError(e.into_string()))?.as_deref())


*/
