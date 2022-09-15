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
    jmap_store::query::QueryObject,
    types::json_pointer::{JSONPointer, JSONPointerEval},
    types::{jmap::JMAPId, state::JMAPState},
};

use super::query::{Comparator, Filter};
#[derive(Debug, Clone, serde::Deserialize)]
pub struct QueryChangesRequest<O: QueryObject> {
    #[serde(skip)]
    pub acl: Option<Arc<ACLToken>>,

    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "filter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter<O::Filter>>,

    #[serde(rename = "sort")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<Vec<Comparator<O::Comparator>>>,

    #[serde(rename = "sinceQueryState")]
    pub since_query_state: JMAPState,

    #[serde(rename = "maxChanges")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_changes: Option<usize>,

    #[serde(rename = "upToId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub up_to_id: Option<JMAPId>,

    #[serde(rename = "calculateTotal")]
    pub calculate_total: Option<bool>,

    #[serde(flatten)]
    pub arguments: O::QueryArguments,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct QueryChangesResponse {
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "oldQueryState")]
    pub old_query_state: JMAPState,

    #[serde(rename = "newQueryState")]
    pub new_query_state: JMAPState,

    #[serde(rename = "total")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub total: Option<usize>,

    #[serde(rename = "removed")]
    pub removed: Vec<JMAPId>,

    #[serde(rename = "added")]
    pub added: Vec<AddedItem>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AddedItem {
    id: JMAPId,
    index: usize,
}

impl AddedItem {
    pub fn new(id: JMAPId, index: usize) -> Self {
        Self { id, index }
    }
}

impl JSONPointerEval for QueryChangesResponse {
    fn eval_json_pointer(&self, ptr: &JSONPointer) -> Option<Vec<u64>> {
        match ptr {
            JSONPointer::Path(path) if path.len() == 3 => {
                match (path.get(0)?, path.get(1)?, path.get(2)?) {
                    (
                        JSONPointer::String(root),
                        JSONPointer::Wildcard,
                        JSONPointer::String(property),
                    ) if root == "added" && property == "id" => {
                        Some(self.added.iter().map(|item| item.id.into()).collect())
                    }
                    _ => None,
                }
            }
            _ => None,
        }
    }
}
