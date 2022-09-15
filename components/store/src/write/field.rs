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

use crate::FieldId;

use super::options::Options;

#[allow(clippy::len_without_is_empty)]
pub trait FieldLen {
    fn len(&self) -> usize;
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Field<T> {
    pub field: FieldId,
    pub options: u64,
    pub value: T,
}

impl<T> Field<T> {
    pub fn new(field: FieldId, value: T, options: u64) -> Self {
        Self {
            field,
            value,
            options,
        }
    }

    #[inline(always)]
    pub fn get_field(&self) -> FieldId {
        self.field
    }

    #[inline(always)]
    pub fn get_options(&self) -> u64 {
        self.options
    }

    #[inline(always)]
    pub fn is_indexed(&self) -> bool {
        self.options.is_index()
    }

    #[inline(always)]
    pub fn is_stored(&self) -> bool {
        self.options.is_store()
    }

    #[inline(always)]
    pub fn is_clear(&self) -> bool {
        self.options.is_clear()
    }

    pub fn size_of(&self) -> usize {
        std::mem::size_of::<T>()
    }
}
