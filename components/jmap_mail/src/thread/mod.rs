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

use jmap::{jmap_store::Object, types::jmap::JMAPId};
use store::core::collection::Collection;

use self::schema::{Property, Thread};

pub mod changes;
pub mod get;
pub mod schema;

impl Object for Thread {
    type Property = Property;

    type Value = ();

    fn new(id: JMAPId) -> Self {
        Thread {
            id,
            email_ids: Vec::new(),
        }
    }

    fn id(&self) -> Option<&JMAPId> {
        Some(&self.id)
    }

    fn required() -> &'static [Self::Property] {
        &[]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[]
    }

    fn max_len() -> &'static [(Self::Property, usize)] {
        &[]
    }

    fn collection() -> store::core::collection::Collection {
        Collection::Thread
    }
}
