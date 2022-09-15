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

pub mod changes;
pub mod get;
pub mod query;
pub mod raft;
pub mod schema;
pub mod serialize;
pub mod set;

use jmap::{jmap_store::Object, types::jmap::JMAPId};
use store::{core::collection::Collection, write::options::Options};

use self::schema::{EmailSubmission, Property, Value};

impl Object for EmailSubmission {
    type Property = Property;

    type Value = Value;

    fn new(id: JMAPId) -> Self {
        let mut item = EmailSubmission::default();
        item.properties
            .append(Property::Id, Value::Id { value: id });
        item
    }

    fn id(&self) -> Option<&JMAPId> {
        self.properties.get(&Property::Id).and_then(|id| match id {
            Value::Id { value } => Some(value),
            _ => None,
        })
    }

    fn required() -> &'static [Self::Property] {
        &[Property::IdentityId, Property::EmailId]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[
            (Property::UndoStatus, <u64 as Options>::F_KEYWORD),
            (Property::EmailId, <u64 as Options>::F_INDEX),
            (Property::IdentityId, <u64 as Options>::F_INDEX),
            (Property::ThreadId, <u64 as Options>::F_INDEX),
            (Property::SendAt, <u64 as Options>::F_INDEX),
        ]
    }

    fn max_len() -> &'static [(Self::Property, usize)] {
        &[(Property::Envelope, 2048)]
    }

    fn collection() -> Collection {
        Collection::EmailSubmission
    }
}
