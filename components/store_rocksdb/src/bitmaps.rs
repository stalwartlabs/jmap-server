use roaring::RoaringBitmap;
use rocksdb::MergeOperands;
use std::convert::TryInto;
use store::DocumentId;

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
            _ => return None,
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

pub fn set_bit(document: &DocumentId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(std::mem::size_of::<DocumentId>() + 1);
    buf.push(BIT_SET);
    buf.extend_from_slice(&document.to_ne_bytes());
    buf
}

pub fn clear_bit(document: &DocumentId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(std::mem::size_of::<DocumentId>() + 1);
    buf.push(BIT_CLEAR);
    buf.extend_from_slice(&document.to_ne_bytes());
    buf
}

