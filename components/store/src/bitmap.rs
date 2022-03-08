use crate::{
    leb128::Leb128,
    serialize::{DeserializeBigEndian, StoreDeserialize, FIELD_PREFIX_LEN},
    ColumnFamily, ComparisonOperator, Direction, JMAPStore, Store,
};
use roaring::RoaringBitmap;
use std::ops::{BitAndAssign, BitOrAssign, BitXorAssign};

use crate::{DocumentId, LogicalOperator, StoreError};

pub const BIT_SET: u8 = 0x80;
pub const BIT_CLEAR: u8 = 0;

pub const IS_BITLIST: u8 = 0;
pub const IS_BITMAP: u8 = 1;

#[inline(always)]
pub fn deserialize_bitlist(bm: &mut RoaringBitmap, bytes: &[u8]) {
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
}

#[inline(always)]
pub fn deserialize_bitmap(bytes: &[u8]) -> Option<RoaringBitmap> {
    RoaringBitmap::deserialize_unchecked_from(&bytes[1..]).ok()
}

impl StoreDeserialize for RoaringBitmap {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        match *bytes.get(0)? {
            IS_BITMAP => deserialize_bitmap(bytes),
            IS_BITLIST => {
                let mut bm = RoaringBitmap::new();
                deserialize_bitlist(&mut bm, bytes);
                Some(bm)
            }
            _ => None,
        }
    }
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

#[inline(always)]
pub fn set_clear_bits<T>(documents: T) -> Vec<u8>
where
    T: Iterator<Item = (DocumentId, bool)>,
{
    debug_assert!(documents.size_hint().0 > 0);

    let total_docs = documents
        .size_hint()
        .1
        .unwrap_or_else(|| documents.size_hint().0);
    let buf_len = (std::mem::size_of::<DocumentId>() * total_docs) + (total_docs / 0x7F) + 2;
    let mut set_buf = Vec::with_capacity(buf_len);
    let mut clear_buf = Vec::with_capacity(buf_len);

    let mut set_header_pos = 0;
    let mut set_total_docs = 0;

    let mut clear_header_pos = 0;
    let mut clear_total_docs = 0;

    set_buf.push(IS_BITLIST);
    clear_buf.push(IS_BITLIST);

    for (document, is_set) in documents {
        if is_set {
            if set_total_docs & 0x7F == 0 {
                set_header_pos = set_buf.len();
                set_buf.push(BIT_SET | 0x7F);
            }
            document.to_leb128_bytes(&mut set_buf);
            set_total_docs += 1;
        } else {
            if clear_total_docs & 0x7F == 0 {
                clear_header_pos = clear_buf.len();
                clear_buf.push(BIT_CLEAR | 0x7F);
            }
            document.to_leb128_bytes(&mut clear_buf);
            clear_total_docs += 1;
        }
    }

    if set_total_docs > 0 {
        set_buf[set_header_pos] = BIT_SET | (((set_total_docs - 1) & 0x7F) as u8);
    }

    if clear_total_docs > 0 {
        clear_buf[clear_header_pos] = BIT_CLEAR | (((clear_total_docs - 1) & 0x7F) as u8);
    }

    if set_total_docs > 0 && clear_total_docs > 0 {
        set_buf.extend_from_slice(&clear_buf[1..]);
        set_buf
    } else if set_total_docs > 0 {
        set_buf
    } else {
        clear_buf
    }
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn get_bitmap(&self, key: Vec<u8>) -> crate::Result<Option<RoaringBitmap>> {
        let db = self.db.clone();
        self.spawn_worker(move || {
            Ok(db
                .get::<RoaringBitmap>(ColumnFamily::Bitmaps, key)?
                .and_then(|bm| if !bm.is_empty() { Some(bm) } else { None }))
        })
        .await
    }
    pub async fn get_bitmaps_intersection(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> crate::Result<Option<RoaringBitmap>> {
        let db = self.db.clone();
        self.spawn_worker(move || {
            let mut result: Option<RoaringBitmap> = None;
            for bitmap in db.multi_get::<RoaringBitmap>(ColumnFamily::Bitmaps, keys)? {
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
        })
        .await
    }
    pub async fn get_bitmaps_union(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> crate::Result<Option<RoaringBitmap>> {
        let db = self.db.clone();
        self.spawn_worker(move || {
            let mut result: Option<RoaringBitmap> = None;
            for bitmap in (db.multi_get::<RoaringBitmap>(ColumnFamily::Bitmaps, keys)?)
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
        })
        .await
    }

    pub async fn range_to_bitmap(
        &self,
        match_key: Vec<u8>,
        op: ComparisonOperator,
    ) -> crate::Result<Option<RoaringBitmap>> {
        let db = self.db.clone();
        self.spawn_worker(move || {
            let mut bm = RoaringBitmap::new();
            let _match = match_key.clone();
            let match_prefix = &_match[0..FIELD_PREFIX_LEN];
            let match_value = &_match[FIELD_PREFIX_LEN..];
            for (key, _) in db.iterator(
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
                        bm.insert(key.as_ref().deserialize_be_u32(doc_id_pos).ok_or_else(
                            || {
                                StoreError::InternalError(
                                    "Invalid key found in 'indexes' column family.".to_string(),
                                )
                            },
                        )?);
                    }
                }
            }

            Ok(Some(bm))
        })
        .await
    }
}
