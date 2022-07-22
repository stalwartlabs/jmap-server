use jmap::error::method::MethodError;
use jmap::jmap_store::get::SharedDocsFnc;
use jmap::jmap_store::query::{ExtraFilterFnc, QueryHelper};
use jmap::request::query::{QueryRequest, QueryResponse};

use jmap::types::principal::{Comparator, Filter, Principal, Property, Type};
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
                    filter::Filter::eq(Property::Email.into(), Query::Tokenize(value))
                }
                Filter::Name { value } => {
                    filter::Filter::eq(Property::Name.into(), Query::Tokenize(value))
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
