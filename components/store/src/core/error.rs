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

use std::fmt::Display;

#[derive(Debug, Clone)]
pub enum StoreError {
    InternalError(String),
    SerializeError(String),
    DeserializeError(String),
    InvalidArguments(String),
    AnchorNotFound,
    DataCorruption(String),
    NotFound(String),
}

impl StoreError {
    pub fn into_owned(&self) -> StoreError {
        self.clone()
    }
}

impl Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::InternalError(s) => write!(f, "Internal error: {}", s),
            StoreError::SerializeError(s) => write!(f, "Serialization error: {}", s),
            StoreError::DeserializeError(s) => write!(f, "Deserialization error: {}", s),
            StoreError::InvalidArguments(s) => write!(f, "Invalid arguments: {}", s),
            StoreError::AnchorNotFound => write!(f, "Anchor not found."),
            StoreError::DataCorruption(s) => write!(f, "Data corruption: {}", s),
            StoreError::NotFound(s) => write!(f, "Not found: {}", s),
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(err: std::io::Error) -> Self {
        StoreError::InternalError(format!("I/O failure: {}", err))
    }
}
