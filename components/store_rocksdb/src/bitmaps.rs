use roaring::RoaringBitmap;
use rocksdb::{BoundColumnFamily, Direction, IteratorMode, MergeOperands};
use std::{
    array::TryFromSliceError,
    convert::TryInto,
    ops::{BitAndAssign, BitOrAssign, BitXorAssign},
    sync::Arc, collections::HashSet,
};
use store::{serialize::PREFIX_LEN, ComparisonOperator, DocumentId, LogicalOperator, StoreError};

use crate::RocksDBStore;

const BIT_SET: u8 = 1;
const BIT_CLEAR: u8 = 0;
const BIT_LIST: u8 = 2;

pub fn bitmap_full_merge(
    _new_key: &[u8],
    existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    let mut rb = if let Some(existing_val) = existing_val {
        RoaringBitmap::deserialize_from(existing_val).ok()?
    } else {
        RoaringBitmap::new()
    };

    //println!("Full merge {:?} {}", operands.size_hint().0, rb.len());

    for op in operands {
        match *(op.get(0)?) {
            BIT_SET => {
                rb.insert(DocumentId::from_ne_bytes(
                    op.get(1..1 + std::mem::size_of::<DocumentId>())?
                        .try_into()
                        .ok()?,
                ));
            }
            BIT_CLEAR => {
                rb.remove(DocumentId::from_ne_bytes(
                    op.get(1..1 + std::mem::size_of::<DocumentId>())?
                        .try_into()
                        .ok()?,
                ));
            }
            BIT_LIST => {
                for op in op.get(1..)?.chunks(std::mem::size_of::<DocumentId>() + 1) {
                    let id = DocumentId::from_ne_bytes(
                        op.get(1..1 + std::mem::size_of::<DocumentId>())?
                            .try_into()
                            .ok()?,
                    );
                    match *(op.get(0)?) {
                        BIT_SET => {
                            rb.insert(id);
                        }
                        BIT_CLEAR => {
                            rb.remove(id);
                        }
                        _ => return None,
                    }
                }
            }
            _ => {
                return None;
            }
        }
    }

    let mut bytes = Vec::with_capacity(rb.serialized_size());
    rb.serialize_into(&mut bytes).ok()?;
    Some(bytes)
}

pub fn bitmap_partial_merge(
    _new_key: &[u8],
    _existing_val: Option<&[u8]>,
    operands: &mut MergeOperands,
) -> Option<Vec<u8>> {
    debug_assert!(_existing_val.is_none());

    let mut bytes =
        Vec::with_capacity((operands.size_hint().0 * (std::mem::size_of::<DocumentId>() + 1)) + 1);

    bytes.push(BIT_LIST);

    //println!("Partial merge {:?}", operands.size_hint().0);

    for op in operands {
        match *(op.get(0)?) {
            BIT_SET | BIT_CLEAR => {
                bytes.extend_from_slice(op);
            }
            BIT_LIST => {
                bytes.extend_from_slice(&op[1..]);
            }
            _ => return None,
        }
    }

    Some(bytes)
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
            Ok(Some(
                RoaringBitmap::deserialize_from(&bytes[..])
                    .map_err(|e| StoreError::InternalError(e.to_string()))?,
            ))
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
                let rb = RoaringBitmap::deserialize_from(&bytes[..])
                    .map_err(|e| StoreError::InternalError(e.to_string()))?;

                if let Some(result) = &mut result {
                    result.bitor_assign(&rb);
                } else {
                    result = Some(rb);
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
                let rb = RoaringBitmap::deserialize_from(&bytes[..])
                    .map_err(|e| StoreError::InternalError(e.to_string()))?;

                if let Some(result) = &mut result {
                    result.bitand_assign(&rb);
                    if result.is_empty() {
                        break;
                    }
                } else {
                    result = Some(rb);
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
        let mut rb = RoaringBitmap::new();
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
            //print!("{} -> {:?} {:?}", key.starts_with(match_prefix), key, match_prefix);
            if !key.starts_with(match_prefix) {
                break;
            }
            let doc_id_pos = key.len() - std::mem::size_of::<DocumentId>();
            let value = key.get(PREFIX_LEN..doc_id_pos).ok_or_else(|| {
                StoreError::InternalError(
                    "Invalid key found in 'indexes' column family.".to_string(),
                )
            })?;
            /*println!(
                " {} {}",
                u32::from_be_bytes(value.try_into().map_err(|e: TryFromSliceError| {
                    StoreError::InternalError(e.to_string())
                })?,),
                u32::from_be_bytes(match_value.try_into().map_err(|e: TryFromSliceError| {
                    StoreError::InternalError(e.to_string())
                })?,)
            );*/

            match op {
                ComparisonOperator::LowerThan if value >= match_value => break,
                ComparisonOperator::LowerEqualThan if value > match_value => break,
                ComparisonOperator::GreaterThan if value <= match_value => break,
                ComparisonOperator::GreaterEqualThan if value < match_value => break,
                ComparisonOperator::Equal if value != match_value => break,
                _ => {
                    rb.insert(DocumentId::from_be_bytes(
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

        Ok(Some(rb))
    }
}

#[inline(always)]
pub fn set_bit(document: DocumentId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(std::mem::size_of::<DocumentId>() + 1);
    buf.push(BIT_SET);
    buf.extend_from_slice(&document.to_ne_bytes());
    buf
}

#[inline(always)]
pub fn set_bit_list(documents: HashSet<DocumentId>) -> Vec<u8> {
    let mut buf = Vec::with_capacity(((std::mem::size_of::<DocumentId>() + 1) * documents.len()) + 1);
    buf.push(BIT_LIST);
    
    for document in documents {
        buf.push(BIT_SET);
        buf.extend_from_slice(&document.to_ne_bytes());
    }

    buf
}

#[inline(always)]
pub fn clear_bit(document: DocumentId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(std::mem::size_of::<DocumentId>() + 1);
    buf.push(BIT_CLEAR);
    buf.extend_from_slice(&document.to_ne_bytes());
    buf
}

#[inline(always)]
pub fn has_bit(bytes: &[u8], document: DocumentId) -> crate::Result<bool> {
    Ok(RoaringBitmap::deserialize_from(bytes)
        .map_err(|e| StoreError::InternalError(e.to_string()))?
        .contains(document))
}

#[inline(always)]
pub fn bitmap_op<'x>(
    op: &LogicalOperator,
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
