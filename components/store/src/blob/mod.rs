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

use std::{convert::TryInto, fmt::Display, io::Write, ops::Range};

use sha2::{Digest, Sha256};

use crate::{
    config::env_settings::EnvSettings,
    serialize::{base32::Base32Writer, StoreDeserialize, StoreSerialize},
};

pub mod local;
pub mod purge;
pub mod store;

pub const BLOB_HASH_LEN: usize = 32;
pub const BLOB_LOCAL: u8 = 0;
pub const BLOB_EXTERNAL: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum BlobId {
    Local { hash: [u8; BLOB_HASH_LEN] },
    External { hash: [u8; BLOB_HASH_LEN] },
}

impl BlobId {
    pub fn new_local(bytes: &[u8]) -> Self {
        // Create blob key
        let mut hasher = Sha256::new();
        hasher.update(bytes);

        BlobId::Local {
            hash: hasher.finalize().into(),
        }
    }

    pub fn new_external(bytes: &[u8]) -> Self {
        // Create blob key
        let mut hasher = Sha256::new();
        hasher.update(bytes);

        BlobId::External {
            hash: hasher.finalize().into(),
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, BlobId::Local { .. })
    }

    pub fn is_external(&self) -> bool {
        matches!(self, BlobId::External { .. })
    }

    pub fn hash(&self) -> &[u8] {
        match self {
            BlobId::Local { hash } => hash,
            BlobId::External { hash } => hash,
        }
    }
}

impl Display for BlobId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let bytes = self.hash();
        let mut writer = Base32Writer::with_capacity(bytes.len());
        writer.write_all(bytes).unwrap();
        f.write_str(&writer.finalize())
    }
}

impl StoreSerialize for BlobId {
    fn serialize(&self) -> Option<Vec<u8>> {
        let mut bytes = Vec::with_capacity(BLOB_HASH_LEN + 1);
        bytes.push(if self.is_local() {
            BLOB_LOCAL
        } else {
            BLOB_EXTERNAL
        });
        bytes.extend_from_slice(self.hash());
        bytes.into()
    }
}

impl StoreDeserialize for BlobId {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        match *bytes.first()? {
            BLOB_LOCAL => BlobId::Local {
                hash: bytes.get(1..BLOB_HASH_LEN + 1)?.try_into().ok()?,
            },
            _ => BlobId::External {
                hash: bytes.get(1..BLOB_HASH_LEN + 1)?.try_into().ok()?,
            },
        }
        .into()
    }
}

pub trait BlobStore: Sized {
    fn new(settings: &EnvSettings) -> crate::Result<Self>;
    fn get_range(&self, blob_id: &BlobId, range: Range<u32>) -> crate::Result<Option<Vec<u8>>>;
    fn get(&self, blob_id: &BlobId) -> crate::Result<Option<Vec<u8>>> {
        self.get_range(blob_id, 0..u32::MAX)
    }
    fn put(&self, blob_id: &BlobId, blob: &[u8]) -> crate::Result<bool>;
    fn delete(&self, blob_id: &BlobId) -> crate::Result<bool>;
}
