use jmap::error::method::MethodError;
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
        let mut helper = QueryHelper::new(self, request)?;

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
