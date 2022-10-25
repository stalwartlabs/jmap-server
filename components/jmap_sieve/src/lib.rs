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

use std::hash::Hash;

use store::{
    ahash::AHashSet,
    sha2::{Digest, Sha256},
};

pub mod sieve_script;

#[derive(Debug, Clone)]
pub struct SeenIds {
    pub ids: AHashSet<SeenIdHash>,
    pub has_changes: bool,
}

#[derive(Debug, Clone)]
pub struct SeenIdHash {
    hash: [u8; 32],
    expiry: u64,
}

impl PartialEq for SeenIds {
    fn eq(&self, other: &Self) -> bool {
        self.ids.len() == other.ids.len() && self.has_changes == other.has_changes
    }
}

impl Eq for SeenIds {}

impl SeenIdHash {
    pub fn new(id: &str, expiry: u64) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(id.as_bytes());
        SeenIdHash {
            hash: hasher.finalize().into(),
            expiry,
        }
    }
}

impl PartialOrd for SeenIdHash {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.expiry.partial_cmp(&other.expiry)
    }
}

impl Ord for SeenIdHash {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.expiry.cmp(&other.expiry)
    }
}

impl Hash for SeenIdHash {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.hash.hash(state);
    }
}

impl PartialEq for SeenIdHash {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl Eq for SeenIdHash {}
