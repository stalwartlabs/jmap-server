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

use super::bitmap::BitmapItem;

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum Collection {
    Principal = 0,
    PushSubscription = 1,
    Mail = 2,
    Mailbox = 3,
    Thread = 4,
    Identity = 5,
    EmailSubmission = 6,
    SieveScript = 7,
    None = 8,
}

impl Default for Collection {
    fn default() -> Self {
        Collection::None
    }
}

impl From<u8> for Collection {
    fn from(value: u8) -> Self {
        match value {
            0 => Collection::Principal,
            1 => Collection::PushSubscription,
            2 => Collection::Mail,
            3 => Collection::Mailbox,
            4 => Collection::Thread,
            5 => Collection::Identity,
            6 => Collection::EmailSubmission,
            7 => Collection::SieveScript,
            _ => {
                debug_assert!(false, "Invalid collection value: {}", value);
                Collection::None
            }
        }
    }
}

impl From<Collection> for u8 {
    fn from(collection: Collection) -> u8 {
        collection as u8
    }
}

impl From<Collection> for u64 {
    fn from(collection: Collection) -> u64 {
        collection as u64
    }
}

impl From<u64> for Collection {
    fn from(value: u64) -> Self {
        match value {
            0 => Collection::Principal,
            1 => Collection::PushSubscription,
            2 => Collection::Mail,
            3 => Collection::Mailbox,
            4 => Collection::Thread,
            5 => Collection::Identity,
            6 => Collection::EmailSubmission,
            7 => Collection::SieveScript,
            _ => {
                debug_assert!(false, "Invalid collection value: {}", value);
                Collection::None
            }
        }
    }
}

impl BitmapItem for Collection {
    fn max() -> u64 {
        Collection::None as u64
    }

    fn is_valid(&self) -> bool {
        !matches!(self, Collection::None)
    }
}
