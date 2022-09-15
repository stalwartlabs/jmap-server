/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

pub mod base32;
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
