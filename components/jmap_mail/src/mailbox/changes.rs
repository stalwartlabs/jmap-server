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

use jmap::{
    jmap_store::{
        changes::{ChangesObject, JMAPChanges},
        query_changes::QueryChangesHelper,
    },
    request::{
        changes::{ChangesRequest, ChangesResponse},
        query_changes::{QueryChangesRequest, QueryChangesResponse},
    },
    types::json_pointer::{JSONPointer, JSONPointerEval},
};
use store::{JMAPStore, Store};

use super::{
    query::JMAPMailboxQuery,
    schema::{Mailbox, Property},
};

#[derive(Debug, serde::Serialize, Default)]
pub struct ChangesResponseArguments {
    #[serde(rename = "updatedProperties")]
    updated_properties: Option<Vec<Property>>,
}

impl ChangesObject for Mailbox {
    type ChangesResponse = ChangesResponseArguments;
}

pub trait JMAPMailboxChanges {
    fn mailbox_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResponse<Mailbox>>;
    fn mailbox_query_changes(
        &self,
        request: QueryChangesRequest<Mailbox>,
    ) -> jmap::Result<QueryChangesResponse>;
}

impl<T> JMAPMailboxChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mailbox_changes(&self, request: ChangesRequest) -> jmap::Result<ChangesResponse<Mailbox>> {
        self.changes(request)
            .map(|mut r: ChangesResponse<Mailbox>| {
                if r.has_children_changes {
                    r.arguments.updated_properties = vec![
                        Property::TotalEmails,
                        Property::UnreadEmails,
                        Property::TotalThreads,
                        Property::UnreadThreads,
                    ]
                    .into();
                }
                r
            })
    }

    fn mailbox_query_changes(
        &self,
        request: QueryChangesRequest<Mailbox>,
    ) -> jmap::Result<QueryChangesResponse> {
        let mut helper = QueryChangesHelper::new(self, request)?;
        let has_changes = helper.has_changes();

        helper.query_changes(if let Some(has_changes) = has_changes {
            self.mailbox_query(has_changes)?.into()
        } else {
            None
        })
    }
}

impl JSONPointerEval for ChangesResponseArguments {
    fn eval_json_pointer(&self, ptr: &JSONPointer) -> Option<Vec<u64>> {
        if ptr.is_item_query("updatedProperties") {
            Some(if let Some(updated_properties) = &self.updated_properties {
                updated_properties.iter().map(|p| *p as u64).collect()
            } else {
                Vec::with_capacity(0)
            })
        } else {
            None
        }
    }
}
