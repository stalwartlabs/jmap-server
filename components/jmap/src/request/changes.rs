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

use std::sync::Arc;

use store::core::acl::ACLToken;

use crate::{
    jmap_store::changes::ChangesObject,
    types::json_pointer::{JSONPointer, JSONPointerEval},
    types::{jmap::JMAPId, state::JMAPState},
};

#[derive(Debug, Clone, serde::Deserialize)]
pub struct ChangesRequest {
    #[serde(skip)]
    pub acl: Option<Arc<ACLToken>>,

    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "sinceState")]
    pub since_state: JMAPState,

    #[serde(rename = "maxChanges")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_changes: Option<usize>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ChangesResponse<O: ChangesObject> {
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "oldState")]
    pub old_state: JMAPState,

    #[serde(rename = "newState")]
    pub new_state: JMAPState,

    #[serde(rename = "hasMoreChanges")]
    pub has_more_changes: bool,

    pub created: Vec<JMAPId>,

    pub updated: Vec<JMAPId>,

    pub destroyed: Vec<JMAPId>,

    #[serde(flatten)]
    pub arguments: O::ChangesResponse,

    #[serde(skip)]
    pub total_changes: usize,
    #[serde(skip)]
    pub has_children_changes: bool,
}

impl<O: ChangesObject> ChangesResponse<O> {
    pub fn empty(account_id: JMAPId) -> Self {
        Self {
            account_id,
            old_state: JMAPState::default(),
            new_state: JMAPState::default(),
            has_more_changes: false,
            created: Vec::with_capacity(0),
            updated: Vec::with_capacity(0),
            destroyed: Vec::with_capacity(0),
            arguments: O::ChangesResponse::default(),
            total_changes: 0,
            has_children_changes: false,
        }
    }
}

impl<O: ChangesObject> JSONPointerEval for ChangesResponse<O> {
    fn eval_json_pointer(&self, ptr: &JSONPointer) -> Option<Vec<u64>> {
        let property = match ptr {
            JSONPointer::String(property) => property,
            JSONPointer::Path(path) if path.len() == 2 => {
                if let (Some(JSONPointer::String(property)), Some(JSONPointer::Wildcard)) =
                    (path.get(0), path.get(1))
                {
                    property
                } else {
                    return None;
                }
            }
            _ => {
                return None;
            }
        };

        match property.as_str() {
            "created" => Some(self.created.iter().map(Into::into).collect()),
            "updated" => Some(self.updated.iter().map(Into::into).collect()),
            _ => self.arguments.eval_json_pointer(ptr),
        }
    }
}
