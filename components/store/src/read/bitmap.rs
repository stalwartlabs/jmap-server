use std::ops::{BitAndAssign, BitOrAssign};

use roaring::RoaringBitmap;

use crate::{
    core::error::StoreError,
    serialize::{key::FIELD_PREFIX_LEN, DeserializeBigEndian},
    ColumnFamily, Direction, DocumentId, JMAPStore, Store,
};

use super::filter::ComparisonOperator;

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn get_bitmap(&self, key: &[u8]) -> crate::Result<Option<RoaringBitmap>> {
        Ok(self
            .db
            .get::<RoaringBitmap>(ColumnFamily::Bitmaps, key)?
            .and_then(|bm| if !bm.is_empty() { Some(bm) } else { None }))
    }

    pub fn get_bitmaps_intersection(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> crate::Result<Option<RoaringBitmap>> {
        let mut result: Option<RoaringBitmap> = None;
        for bitmap in self
            .db
            .multi_get::<RoaringBitmap, _>(ColumnFamily::Bitmaps, keys)?
        {
            if let Some(bitmap) = bitmap {
                if let Some(result) = &mut result {
                    result.bitand_assign(&bitmap);
                    if result.is_empty() {
                        break;
                    }
                } else {
                    result = Some(bitmap);
                }
            } else {
                return Ok(None);
            }
        }
        Ok(result)
    }

    pub fn get_bitmaps_union(&self, keys: Vec<Vec<u8>>) -> crate::Result<Option<RoaringBitmap>> {
        let mut result: Option<RoaringBitmap> = None;
        for bitmap in (self
            .db
            .multi_get::<RoaringBitmap, _>(ColumnFamily::Bitmaps, keys)?)
        .into_iter()
        .flatten()
        {
            if let Some(result) = &mut result {
                result.bitor_assign(&bitmap);
            } else {
                result = Some(bitmap);
            }
        }
        Ok(result)
    }

    pub fn range_to_bitmap(
        &self,
        match_key: &[u8],
        op: ComparisonOperator,
    ) -> crate::Result<Option<RoaringBitmap>> {
        let mut bm = RoaringBitmap::new();
        let match_prefix = &match_key[0..FIELD_PREFIX_LEN];
        let match_value = &match_key[FIELD_PREFIX_LEN..];
        for (key, _) in self.db.iterator(
            ColumnFamily::Indexes,
            match_key,
            match op {
                ComparisonOperator::GreaterThan => Direction::Forward,
                ComparisonOperator::GreaterEqualThan => Direction::Forward,
                ComparisonOperator::Equal => Direction::Forward,
                _ => Direction::Backward,
            },
        )? {
            if !key.starts_with(match_prefix) {
                break;
            }
            let doc_id_pos = key.len() - std::mem::size_of::<DocumentId>();
            let value = key.get(FIELD_PREFIX_LEN..doc_id_pos).ok_or_else(|| {
                StoreError::InternalError(
                    "Invalid key found in 'indexes' column family.".to_string(),
                )
            })?;

            match op {
                ComparisonOperator::LowerThan if value >= match_value => {
                    if value == match_value {
                        continue;
                    } else {
                        break;
                    }
                }
                ComparisonOperator::LowerEqualThan if value > match_value => break,
                ComparisonOperator::GreaterThan if value <= match_value => {
                    if value == match_value {
                        continue;
                    } else {
                        break;
                    }
                }
                ComparisonOperator::GreaterEqualThan if value < match_value => break,
                ComparisonOperator::Equal if value != match_value => break,
                _ => {
                    bm.insert(key.as_ref().deserialize_be_u32(doc_id_pos).ok_or_else(|| {
                        StoreError::InternalError(
                            "Invalid key found in 'indexes' column family.".to_string(),
                        )
                    })?);
                }
            }
        }

        Ok(Some(bm))
    }
}
