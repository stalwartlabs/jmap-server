use roaring::RoaringBitmap;
use rocksdb::{
    compaction_filter::Decision, BoundColumnFamily, Direction, IteratorMode, MergeOperands,
};
use std::{
    array::TryFromSliceError,
    convert::TryInto,
    ops::{BitAndAssign, BitOrAssign, BitXorAssign},
    sync::Arc,
};
use store::{
    leb128::Leb128, serialize::PREFIX_LEN, ComparisonOperator, DocumentId, LogicalOperator,
    StoreError,
};

use crate::RocksDBStore;

pub const BIT_SET: u8 = 0x80;
pub const BIT_CLEAR: u8 = 0;

pub const IS_BITLIST: u8 = 0;
pub const IS_BITMAP: u8 = 1;

pub fn bitmap_merge(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    /*print!(
        "Merge operands {:?}, has val {} -> ",
        operands.size_hint().0,
        existing_val.is_some(),
    );*/

    let mut bm = match existing_val {
        Some(existing_val) => into_bitmap(existing_val).ok()?,
        None if operands.size_hint().0 == 1 => {
            //println!("return unserialized");
            return Some(Vec::from(operands.into_iter().next().unwrap()));
        }
        _ => RoaringBitmap::new(),
    };

    for op in operands {
        match *op.get(0)? {
            IS_BITMAP => {
                if let Ok(union_bm) = deserialize_bitmap(op) {
                    //print!("Bitmap union");
                    if !bm.is_empty() {
                        bm.bitor_assign(&union_bm);
                    } else {
                        bm = union_bm;
                    }
                } else {
                    return None;
                }
            }
            IS_BITLIST => {
                deserialize_bitlist(&mut bm, op).ok()?;
            }
            _ => {
                return None;
            }
        }
    }

    //println!(" -> {}", bm.len());

    let mut bytes = Vec::with_capacity(bm.serialized_size() + 1);
    bytes.push(IS_BITMAP);
    bm.serialize_into(&mut bytes).ok()?;
    Some(bytes)
}

pub fn bitmap_compact(_level: u32, _key: &[u8], value: &[u8]) -> Decision {
    //println!("Compact entry with {:?} bytes.", value.len());
    match into_bitmap(value) {
        Ok(bm) if bm.is_empty() => Decision::Remove,
        _ => Decision::Keep,
    }
}

#[inline(always)]
pub fn deserialize_bitlist(bm: &mut RoaringBitmap, bytes: &[u8]) -> crate::Result<()> {
    let mut it = bytes[1..].iter();
    'inner: while let Some(header) = it.next() {
        let mut items = (header & 0x7F) + 1;
        let is_set = (header & BIT_SET) != 0;
        //print!("[{} {}] ", if is_set { "set" } else { "clear" }, items);

        while items > 0 {
            if let Some(doc_id) = DocumentId::from_leb128_it(&mut it) {
                if is_set {
                    bm.insert(doc_id);
                } else {
                    bm.remove(doc_id);
                }
                items -= 1;
            } else {
                debug_assert!(items == 0, "{:?}", bytes);
                break 'inner;
            }
        }
    }
    Ok(())
}

#[inline(always)]
pub fn deserialize_bitmap(bytes: &[u8]) -> crate::Result<RoaringBitmap> {
    RoaringBitmap::deserialize_from(&bytes[1..])
        .map_err(|e| StoreError::InternalError(e.to_string()))
}

#[inline(always)]
pub fn into_bitmap(bytes: &[u8]) -> crate::Result<RoaringBitmap> {
    match *bytes.get(0).ok_or(StoreError::DataCorruption)? {
        IS_BITMAP => deserialize_bitmap(bytes),
        IS_BITLIST => {
            let mut bm = RoaringBitmap::new();
            deserialize_bitlist(&mut bm, bytes)?;
            Ok(bm)
        }
        _ => Err(StoreError::DataCorruption),
    }
}

impl RocksDBStore {
    #[inline(always)]
    pub fn get_bitmap(
        &self,
        cf_bitmaps: &Arc<BoundColumnFamily>,
        key: &[u8],
    ) -> crate::Result<Option<RoaringBitmap>> {
        if let Some(bytes) = self
            .db
            .get_pinned_cf(cf_bitmaps, key)
            .map_err(|e| StoreError::InternalError(e.into_string()))?
        {
            Ok(Some(into_bitmap(&bytes)?))
        } else {
            Ok(None)
        }
    }

    #[inline(always)]
    pub fn get_bitmaps_union(
        &self,
        keys: Vec<(&Arc<BoundColumnFamily>, Vec<u8>)>,
    ) -> crate::Result<Option<RoaringBitmap>> {
        let mut result: Option<RoaringBitmap> = None;
        for bitmap in self.db.multi_get_cf(keys) {
            if let Some(bytes) = bitmap.map_err(|e| StoreError::InternalError(e.into_string()))? {
                let bm = into_bitmap(&bytes)?;

                if let Some(result) = &mut result {
                    result.bitor_assign(&bm);
                } else {
                    result = Some(bm);
                }
            }
        }
        Ok(result)
    }

    #[inline(always)]
    pub fn get_bitmaps_intersection(
        &self,
        keys: Vec<(&Arc<BoundColumnFamily>, Vec<u8>)>,
    ) -> crate::Result<Option<RoaringBitmap>> {
        let mut result: Option<RoaringBitmap> = None;
        for bitmap in self.db.multi_get_cf(keys) {
            if let Some(bytes) = bitmap.map_err(|e| StoreError::InternalError(e.into_string()))? {
                let bm = into_bitmap(&bytes)?;

                if let Some(result) = &mut result {
                    result.bitand_assign(&bm);
                    if result.is_empty() {
                        break;
                    }
                } else {
                    result = Some(bm);
                }
            } else {
                return Ok(None);
            }
        }
        Ok(result)
    }

    pub fn range_to_bitmap(
        &self,
        cf_indexes: &Arc<BoundColumnFamily>,
        match_key: &[u8],
        op: &ComparisonOperator,
    ) -> crate::Result<Option<RoaringBitmap>> {
        let mut bm = RoaringBitmap::new();
        let match_prefix = &match_key[0..PREFIX_LEN];
        let match_value = &match_key[PREFIX_LEN..];

        for (key, _) in self.db.iterator_cf(
            cf_indexes,
            IteratorMode::From(
                match_key,
                match op {
                    ComparisonOperator::GreaterThan => Direction::Forward,
                    ComparisonOperator::GreaterEqualThan => Direction::Forward,
                    ComparisonOperator::Equal => Direction::Forward,
                    _ => Direction::Reverse,
                },
            ),
        ) {
            if !key.starts_with(match_prefix) {
                break;
            }
            let doc_id_pos = key.len() - std::mem::size_of::<DocumentId>();
            let value = key.get(PREFIX_LEN..doc_id_pos).ok_or_else(|| {
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
                    bm.insert(DocumentId::from_be_bytes(
                        key.get(doc_id_pos..)
                            .ok_or_else(|| {
                                StoreError::InternalError(
                                    "Invalid key found in 'indexes' column family.".to_string(),
                                )
                            })?
                            .try_into()
                            .map_err(|e: TryFromSliceError| {
                                StoreError::InternalError(e.to_string())
                            })?,
                    ));
                }
            }
        }

        Ok(Some(bm))
    }
}

#[inline(always)]
pub fn has_bit(bytes: &[u8], document: DocumentId) -> crate::Result<bool> {
    Ok(into_bitmap(bytes)?.contains(document))
}

#[inline(always)]
pub fn bitmap_op<'x>(
    op: LogicalOperator,
    dest: &'x mut Option<RoaringBitmap>,
    mut src: Option<RoaringBitmap>,
    not_mask: &'x RoaringBitmap,
) {
    if let Some(dest) = dest {
        match op {
            LogicalOperator::And => {
                if let Some(src) = src {
                    dest.bitand_assign(src);
                } else {
                    dest.clear();
                }
            }
            LogicalOperator::Or => {
                if let Some(src) = src {
                    dest.bitor_assign(src);
                }
            }
            LogicalOperator::Not => {
                if let Some(mut src) = src {
                    src.bitxor_assign(not_mask);
                    dest.bitand_assign(src);
                }
            }
        }
    } else if let Some(ref mut src_) = src {
        if let LogicalOperator::Not = op {
            src_.bitxor_assign(not_mask);
        }
        *dest = src;
    } else {
        *dest = Some(RoaringBitmap::new());
    }
}

macro_rules! impl_bit {
    ($single:ident, $many:ident, $flag:ident) => {
        #[inline(always)]
        pub fn $single(document: DocumentId) -> Vec<u8> {
            let mut buf = Vec::with_capacity(std::mem::size_of::<DocumentId>() + 2);
            buf.push(IS_BITLIST);
            buf.push($flag);
            document.to_leb128_bytes(&mut buf);
            buf
        }

        #[inline(always)]
        pub fn $many<T>(documents: T) -> Vec<u8>
        where
            T: Iterator<Item = DocumentId>,
        {
            debug_assert!(documents.size_hint().0 > 0);

            let mut buf = Vec::with_capacity(
                ((std::mem::size_of::<DocumentId>() + 1)
                    * documents
                        .size_hint()
                        .1
                        .unwrap_or_else(|| documents.size_hint().0))
                    + 2,
            );

            buf.push(IS_BITLIST);

            let mut header_pos = 0;
            let mut total_docs = 0;

            for (pos, document) in documents.enumerate() {
                if pos & 0x7F == 0 {
                    header_pos = buf.len();
                    buf.push($flag | 0x7F);
                }
                document.to_leb128_bytes(&mut buf);
                total_docs = pos;
            }

            buf[header_pos] = $flag | ((total_docs & 0x7F) as u8);

            buf
        }
    };
}

impl_bit!(set_bit, set_bits, BIT_SET);
impl_bit!(clear_bit, clear_bits, BIT_CLEAR);

#[cfg(test)]
mod tests {

    use roaring::RoaringBitmap;
    use rocksdb::{ColumnFamilyDescriptor, Options, DB};

    use crate::bitmaps::into_bitmap;

    use super::{bitmap_compact, bitmap_merge, clear_bit, clear_bits, set_bits};

    #[test]
    fn bitmap_merge_compact() {
        let mut temp_dir = std::env::temp_dir();
        temp_dir.push("strdb_bitmap_test");
        if temp_dir.exists() {
            std::fs::remove_dir_all(&temp_dir).unwrap();
        }

        {
            let cf_bitmaps = {
                let mut cf_opts = Options::default();
                cf_opts.set_merge_operator_associative("merge", bitmap_merge);
                cf_opts.set_compaction_filter("compact", bitmap_compact);
                ColumnFamilyDescriptor::new("bitmaps", cf_opts)
            };
            let mut db_opts = Options::default();
            db_opts.create_missing_column_families(true);
            db_opts.create_if_missing(true);

            let db = DB::open_cf_descriptors(&db_opts, &temp_dir, vec![cf_bitmaps]).unwrap();
            let cf_bitmaps = db.cf_handle("bitmaps").unwrap();
            let key = "key1".as_bytes();

            db.merge_cf(&cf_bitmaps, key, set_bits(0..1)).unwrap();
            assert_eq!(
                into_bitmap(&db.get_cf(&cf_bitmaps, key).unwrap().unwrap()).unwrap(),
                (0..1).collect()
            );
            db.merge_cf(&cf_bitmaps, key, set_bits(1..500)).unwrap();
            db.merge_cf(&cf_bitmaps, key, set_bits(500..1000)).unwrap();
            assert_eq!(
                into_bitmap(&db.get_cf(&cf_bitmaps, key).unwrap().unwrap()).unwrap(),
                (0..1000).collect()
            );
            db.merge_cf(&cf_bitmaps, key, clear_bits(0..128)).unwrap();
            db.merge_cf(&cf_bitmaps, key, clear_bits(128..384)).unwrap();
            db.merge_cf(&cf_bitmaps, key, clear_bit(384)).unwrap();
            db.merge_cf(&cf_bitmaps, key, clear_bit(385)).unwrap();
            db.merge_cf(&cf_bitmaps, key, clear_bit(386)).unwrap();
            assert_eq!(
                into_bitmap(&db.get_cf(&cf_bitmaps, key).unwrap().unwrap()).unwrap(),
                (387..1000).collect()
            );
            db.merge_cf(&cf_bitmaps, key, clear_bits(387..1000))
                .unwrap();
            db.compact_range_cf(&cf_bitmaps, None::<&[u8]>, None::<&[u8]>);
            assert_eq!(
                into_bitmap(&db.get_cf(&cf_bitmaps, key).unwrap().unwrap()).unwrap(),
                RoaringBitmap::new()
            );
            // Commented out as compaction filters are not executed when there is a merge taking place.
            //assert!(db.get_cf(&cf_bitmaps, key).unwrap().is_none());
        }

        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
}
