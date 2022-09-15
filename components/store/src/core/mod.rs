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

use ahash::AHashMap;

use crate::{DocumentId, JMAPId};

pub mod acl;
pub mod bitmap;
pub mod collection;
pub mod document;
pub mod error;
pub mod number;
pub mod tag;
pub mod vec_map;

pub trait JMAPIdPrefix {
    fn from_parts(prefix_id: DocumentId, doc_id: DocumentId) -> JMAPId;
    fn get_document_id(&self) -> DocumentId;
    fn get_prefix_id(&self) -> DocumentId;
}

impl JMAPIdPrefix for JMAPId {
    fn from_parts(prefix_id: DocumentId, doc_id: DocumentId) -> JMAPId {
        (prefix_id as JMAPId) << 32 | doc_id as JMAPId
    }

    fn get_document_id(&self) -> DocumentId {
        (self & 0xFFFFFFFF) as DocumentId
    }

    fn get_prefix_id(&self) -> DocumentId {
        (self >> 32) as DocumentId
    }
}

#[inline(always)]
pub fn ahash_is_empty<K, V>(map: &AHashMap<K, V>) -> bool {
    map.is_empty()
}
