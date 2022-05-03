pub mod bitmap;
pub mod key;
pub mod leb128;

use crate::{Float, Integer, LongInteger};
use std::convert::TryInto;

pub trait DeserializeBigEndian {
    fn deserialize_be_u32(&self, index: usize) -> Option<Integer>;
    fn deserialize_be_u64(&self, index: usize) -> Option<LongInteger>;
}

impl DeserializeBigEndian for &[u8] {
    fn deserialize_be_u32(&self, index: usize) -> Option<Integer> {
        Integer::from_be_bytes(
            self.get(index..index + std::mem::size_of::<Integer>())?
                .try_into()
                .ok()?,
        )
        .into()
    }

    fn deserialize_be_u64(&self, index: usize) -> Option<LongInteger> {
        LongInteger::from_be_bytes(
            self.get(index..index + std::mem::size_of::<LongInteger>())?
                .try_into()
                .ok()?,
        )
        .into()
    }
}

pub trait StoreDeserialize: Sized + Sync + Send {
    fn deserialize(bytes: &[u8]) -> Option<Self>;
}

pub trait StoreSerialize: Sized {
    fn serialize(&self) -> Option<Vec<u8>>;
}

impl StoreDeserialize for Vec<u8> {
    fn deserialize(bytes: &[u8]) -> Option<Vec<u8>> {
        bytes.to_vec().into()
    }
}

impl StoreDeserialize for String {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        String::from_utf8(bytes.to_vec()).ok()
    }
}

impl StoreDeserialize for Float {
    fn deserialize(bytes: &[u8]) -> Option<Float> {
        Float::from_le_bytes(bytes.try_into().ok()?).into()
    }
}

impl StoreDeserialize for LongInteger {
    fn deserialize(bytes: &[u8]) -> Option<LongInteger> {
        LongInteger::from_le_bytes(bytes.try_into().ok()?).into()
    }
}

impl StoreDeserialize for Integer {
    fn deserialize(bytes: &[u8]) -> Option<Integer> {
        Integer::from_le_bytes(bytes.try_into().ok()?).into()
    }
}

impl StoreDeserialize for i64 {
    fn deserialize(bytes: &[u8]) -> Option<i64> {
        i64::from_le_bytes(bytes.try_into().ok()?).into()
    }
}

impl StoreSerialize for LongInteger {
    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_le_bytes().to_vec())
    }
}

impl StoreSerialize for Integer {
    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_le_bytes().to_vec())
    }
}

impl StoreSerialize for i64 {
    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_le_bytes().to_vec())
    }
}

impl StoreSerialize for f64 {
    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_le_bytes().to_vec())
    }
}
