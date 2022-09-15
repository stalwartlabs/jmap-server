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
use jmap::jmap_store::query::{ExtraFilterFnc, QueryHelper};
use jmap::request::query::{QueryRequest, QueryResponse};

use jmap::principal::schema::{Comparator, Filter, Principal, Property, Type};
use store::read::comparator::{self, FieldComparator};
use store::read::default_filter_mapper;
use store::read::filter::{self, Query};
use store::JMAPStore;
use store::Store;

pub trait JMAPPrincipalQuery<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_query(&self, request: QueryRequest<Principal>) -> jmap::Result<QueryResponse>;
}

impl<T> JMAPPrincipalQuery<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn principal_query(&self, request: QueryRequest<Principal>) -> jmap::Result<QueryResponse> {
        let mut helper = QueryHelper::new(self, request, None::<SharedDocsFnc>)?;

        helper.parse_filter(|filter| {
            Ok(match filter {
                Filter::Email { value } => {
                    filter::Filter::eq(Property::Email.into(), Query::Index(value))
                }
                Filter::Name { value } => {
                    filter::Filter::eq(Property::Name.into(), Query::Tokenize(value))
                }
                Filter::DomainName { value } => {
                    filter::Filter::eq(Property::Name.into(), Query::Index(value))
                }
                Filter::Timezone { value } => {
                    filter::Filter::eq(Property::Timezone.into(), Query::Tokenize(value))
                }
                Filter::Text { value } => filter::Filter::or(vec![
                    filter::Filter::eq(Property::Name.into(), Query::Tokenize(value.clone())),
                    filter::Filter::eq(Property::Email.into(), Query::Tokenize(value.clone())),
                    filter::Filter::eq(Property::Aliases.into(), Query::Tokenize(value.clone())),
                    filter::Filter::eq(Property::Description.into(), Query::Tokenize(value)),
                ]),
                Filter::Type { value } => filter::Filter::eq(
                    Property::Type.into(),
                    Query::Keyword(match value {
                        Type::Individual => "i".to_string(),
                        Type::Group => "g".to_string(),
                        Type::Resource => "r".to_string(),
                        Type::Location => "l".to_string(),
                        Type::Domain => "d".to_string(),
                        Type::List => "t".to_string(),
                        Type::Other => "o".to_string(),
                    }),
                ),
                Filter::Members { value } => filter::Filter::eq(
                    Property::Members.into(),
                    Query::Integer(value.get_document_id()),
                ),
                Filter::QuotaLt { value } => {
                    filter::Filter::lt(Property::Quota.into(), Query::LongInteger(value))
                }
                Filter::QuotaGt { value } => {
                    filter::Filter::gt(Property::Quota.into(), Query::LongInteger(value))
                }
                Filter::Unsupported { value } => {
                    return Err(MethodError::UnsupportedFilter(value));
                }
            })
        })?;

        helper.parse_comparator(|comparator| {
            Ok(comparator::Comparator::Field(FieldComparator {
                field: {
                    match comparator.property {
                        Comparator::Type => Property::Type,
                        Comparator::Name => Property::Name,
                        Comparator::Email => Property::Email,
                    }
                }
                .into(),
                ascending: comparator.is_ascending,
            }))
        })?;

        helper.query(default_filter_mapper, None::<ExtraFilterFnc>)
    }
}
