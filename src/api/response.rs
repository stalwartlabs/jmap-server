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

use serde::Serialize;

use jmap::{error::method::MethodError, types::jmap::JMAPId};
use store::ahash::AHashMap;
use store::core::ahash_is_empty;

use super::method;

#[derive(Debug, serde::Serialize)]

pub struct Response {
    #[serde(rename = "methodResponses")]
    pub method_responses: Vec<method::Call<method::Response>>,

    #[serde(rename = "sessionState")]
    #[serde(serialize_with = "serialize_hex")]
    pub session_state: u32,

    #[serde(rename(deserialize = "createdIds"))]
    #[serde(skip_serializing_if = "ahash_is_empty")]
    pub created_ids: AHashMap<String, JMAPId>,
}

impl Response {
    pub fn new(session_state: u32, created_ids: AHashMap<String, JMAPId>, capacity: usize) -> Self {
        Response {
            session_state,
            created_ids,
            method_responses: Vec::with_capacity(capacity),
        }
    }

    pub fn push_response(&mut self, id: String, method: method::Response) {
        self.method_responses.push(method::Call { id, method });
    }

    pub fn push_created_id(&mut self, create_id: String, id: JMAPId) {
        self.created_ids.insert(create_id, id);
    }

    pub fn push_error(&mut self, id: String, error: MethodError) {
        self.method_responses.push(method::Call {
            id,
            method: method::Response::Error(error),
        });
    }
}

pub fn serialize_hex<S>(value: &u32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    format!("{:x}", value).serialize(serializer)
}
