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

use super::schema::{Comparator, Filter, Property, SieveScript};

impl QueryObject for SieveScript {
    type QueryArguments = ();

    type Filter = Filter;

    type Comparator = Comparator;
}

pub trait JMAPSieveScriptQuery<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn sieve_script_query(&self, request: QueryRequest<SieveScript>)
        -> jmap::Result<QueryResponse>;
}

impl<T> JMAPSieveScriptQuery<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn sieve_script_query(
        &self,
        request: QueryRequest<SieveScript>,
    ) -> jmap::Result<QueryResponse> {
        let mut helper = QueryHelper::new(self, request, None::<SharedDocsFnc>)?;

        helper.parse_filter(|filter| {
            Ok(match filter {
                Filter::Name { value } => {
                    filter::Filter::eq(Property::Name.into(), Query::Tokenize(value.to_lowercase()))
                }
                Filter::IsActive { value } => filter::Filter::eq(
                    Property::IsActive.into(),
                    Query::Index((if value { "1" } else { "0" }).to_string()),
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
                        Comparator::Name => Property::Name,
                        Comparator::IsActive => Property::IsActive,
                    }
                }
                .into(),
                ascending: comparator.is_ascending,
            }))
        })?;

        helper.query(default_filter_mapper, None::<ExtraFilterFnc>)
    }
}
