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

use jmap::error::method::MethodError;
use jmap::jmap_store::get::SharedDocsFnc;
use jmap::jmap_store::query::{ExtraFilterFnc, QueryHelper, QueryObject};
use jmap::request::query::{QueryRequest, QueryResponse};

use store::read::comparator::{self, FieldComparator};
use store::read::default_filter_mapper;
use store::read::filter::{self, Query};
use store::JMAPStore;
use store::Store;

use super::schema::{Comparator, EmailSubmission, Filter, Property, UndoStatus};

impl QueryObject for EmailSubmission {
    type QueryArguments = ();

    type Filter = Filter;

    type Comparator = Comparator;
}

pub trait JMAPEmailSubmissionQuery<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn email_submission_query(
        &self,
        request: QueryRequest<EmailSubmission>,
    ) -> jmap::Result<QueryResponse>;
}

impl<T> JMAPEmailSubmissionQuery<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn email_submission_query(
        &self,
        request: QueryRequest<EmailSubmission>,
    ) -> jmap::Result<QueryResponse> {
        let mut helper = QueryHelper::new(self, request, None::<SharedDocsFnc>)?;

        helper.parse_filter(|filter| {
            Ok(match filter {
                Filter::IdentityIds { value } => filter::Filter::or(
                    value
                        .into_iter()
                        .map(|id| {
                            filter::Filter::eq(
                                Property::IdentityId.into(),
                                Query::LongInteger(id.into()),
                            )
                        })
                        .collect(),
                ),
                Filter::EmailIds { value } => filter::Filter::or(
                    value
                        .into_iter()
                        .map(|id| {
                            filter::Filter::eq(
                                Property::EmailId.into(),
                                Query::LongInteger(id.into()),
                            )
                        })
                        .collect(),
                ),
                Filter::ThreadIds { value } => filter::Filter::or(
                    value
                        .into_iter()
                        .map(|id| {
                            filter::Filter::eq(
                                Property::ThreadId.into(),
                                Query::LongInteger(id.into()),
                            )
                        })
                        .collect(),
                ),
                Filter::UndoStatus { value } => filter::Filter::eq(
                    Property::UndoStatus.into(),
                    Query::Keyword(match value {
                        UndoStatus::Pending => "p".to_string(),
                        UndoStatus::Final => "f".to_string(),
                        UndoStatus::Canceled => "c".to_string(),
                    }),
                ),
                Filter::Before { value } => filter::Filter::lt(
                    Property::SendAt.into(),
                    Query::LongInteger(value.timestamp() as u64),
                ),
                Filter::After { value } => filter::Filter::gt(
                    Property::SendAt.into(),
                    Query::LongInteger(value.timestamp() as u64),
                ),
                Filter::Unsupported { value } => {
                    return Err(MethodError::UnsupportedFilter(value));
                }
            })
        })?;

        helper.parse_comparator(|comparator| {
            Ok(comparator::Comparator::Field(FieldComparator {
                field: {
                    match comparator.property {
                        Comparator::EmailId => Property::EmailId,
                        Comparator::ThreadId => Property::ThreadId,
                        Comparator::SentAt => Property::SendAt,
                    }
                }
                .into(),
                ascending: comparator.is_ascending,
            }))
        })?;

        helper.query(default_filter_mapper, None::<ExtraFilterFnc>)
    }
}
