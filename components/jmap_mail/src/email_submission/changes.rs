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
};
use store::{JMAPStore, Store};

use super::{query::JMAPEmailSubmissionQuery, schema::EmailSubmission};

impl ChangesObject for EmailSubmission {
    type ChangesResponse = ();
}

pub trait JMAPEmailSubmissionChanges {
    fn email_submission_changes(
        &self,
        request: ChangesRequest,
    ) -> jmap::Result<ChangesResponse<EmailSubmission>>;
    fn email_submission_query_changes(
        &self,
        request: QueryChangesRequest<EmailSubmission>,
    ) -> jmap::Result<QueryChangesResponse>;
}

impl<T> JMAPEmailSubmissionChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn email_submission_changes(
        &self,
        request: ChangesRequest,
    ) -> jmap::Result<ChangesResponse<EmailSubmission>> {
        self.changes(request)
    }

    fn email_submission_query_changes(
        &self,
        request: QueryChangesRequest<EmailSubmission>,
    ) -> jmap::Result<QueryChangesResponse> {
        let mut helper = QueryChangesHelper::new(self, request)?;
        let has_changes = helper.has_changes();

        helper.query_changes(if let Some(has_changes) = has_changes {
            self.email_submission_query(has_changes)?.into()
        } else {
            None
        })
    }
}
