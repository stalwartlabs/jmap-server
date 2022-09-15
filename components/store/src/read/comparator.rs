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

use roaring::RoaringBitmap;

use crate::FieldId;

#[derive(Debug)]
pub struct FieldComparator {
    pub field: FieldId,
    pub ascending: bool,
}

#[derive(Debug)]
pub struct DocumentSetComparator {
    pub set: RoaringBitmap,
    pub ascending: bool,
}

#[derive(Debug)]
pub enum Comparator {
    List(Vec<Comparator>),
    Field(FieldComparator),
    DocumentSet(DocumentSetComparator),
    None,
}

impl Default for Comparator {
    fn default() -> Self {
        Comparator::None
    }
}

impl Comparator {
    pub fn ascending(field: FieldId) -> Self {
        Comparator::Field(FieldComparator {
            field,
            ascending: true,
        })
    }

    pub fn descending(field: FieldId) -> Self {
        Comparator::Field(FieldComparator {
            field,
            ascending: false,
        })
    }
}
