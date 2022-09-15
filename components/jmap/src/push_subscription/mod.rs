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

pub mod get;
pub mod raft;
pub mod schema;
pub mod serialize;
pub mod set;

use crate::{jmap_store::Object, types::jmap::JMAPId};
use store::core::collection::Collection;

use self::schema::{Property, PushSubscription, Value};

impl Object for PushSubscription {
    type Property = Property;

    type Value = Value;

    fn new(id: JMAPId) -> Self {
        let mut item = PushSubscription::default();
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
        &[Property::DeviceClientId, Property::Url]
    }

    fn indexed() -> &'static [(Self::Property, u64)] {
        &[]
    }

    fn max_len() -> &'static [(Self::Property, usize)] {
        &[
            (Property::DeviceClientId, 255),
            (Property::Url, 512),
            (Property::Keys, 2045),
        ]
    }

    fn collection() -> Collection {
        Collection::PushSubscription
    }
}
