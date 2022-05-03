use std::collections::HashMap;

use jmap::error::method::MethodError;

use jmap::jmap_store::query::QueryObject;
use jmap::protocol::json::JSONValue;
use jmap::request::query::QueryRequest;

use store::core::collection::Collection;
use store::read::comparator::{Comparator, FieldComparator};
use store::read::filter::{FieldValue, Filter};
use store::read::QueryFilterMap;
use store::{DocumentId, Store};
use store::{JMAPId, JMAPStore};

use super::EmailSubmissionProperty;

pub struct QueryEmailSubmission {}

impl QueryFilterMap for QueryEmailSubmission {
    fn filter_map_id(&mut self, document_id: DocumentId) -> store::Result<Option<JMAPId>> {
        Ok(Some(document_id as JMAPId))
    }
}

impl<'y, T> QueryObject<'y, T> for QueryEmailSubmission
where
    T: for<'x> Store<'x> + 'static,
{
    fn new(_store: &'y JMAPStore<T>, _request: &QueryRequest) -> jmap::Result<Self> {
        Ok(QueryEmailSubmission {})
    }

    fn parse_filter(&mut self, cond: HashMap<String, JSONValue>) -> jmap::Result<Filter> {
        if let Some((cond_name, cond_value)) = cond.into_iter().next() {
            Ok(match cond_name.as_str() {
                "identityIds" => Filter::or(
                    cond_value
                        .parse_array_items(false)?
                        .unwrap()
                        .into_iter()
                        .map(|id| {
                            Filter::eq(
                                EmailSubmissionProperty::IdentityId.into(),
                                FieldValue::LongInteger(id),
                            )
                        })
                        .collect(),
                ),
                "emailIds" => Filter::or(
                    cond_value
                        .parse_array_items(false)?
                        .unwrap()
                        .into_iter()
                        .map(|id| {
                            Filter::eq(
                                EmailSubmissionProperty::EmailId.into(),
                                FieldValue::LongInteger(id),
                            )
                        })
                        .collect(),
                ),
                "threadIds" => Filter::or(
                    cond_value
                        .parse_array_items(false)?
                        .unwrap()
                        .into_iter()
                        .map(|id| {
                            Filter::eq(
                                EmailSubmissionProperty::ThreadId.into(),
                                FieldValue::LongInteger(id),
                            )
                        })
                        .collect(),
                ),
                "undoStatus" => Filter::eq(
                    EmailSubmissionProperty::UndoStatus.into(),
                    FieldValue::Text(cond_value.parse_string()?),
                ),
                "before" => Filter::lt(
                    EmailSubmissionProperty::SendAt.into(),
                    FieldValue::LongInteger(cond_value.parse_utc_date(false)?.unwrap() as u64),
                ),
                "after" => Filter::gt(
                    EmailSubmissionProperty::SendAt.into(),
                    FieldValue::LongInteger(cond_value.parse_utc_date(false)?.unwrap() as u64),
                ),

                _ => {
                    return Err(MethodError::UnsupportedFilter(format!(
                        "Unsupported filter '{}'.",
                        cond_name
                    )))
                }
            })
        } else {
            Ok(Filter::None)
        }
    }

    fn parse_comparator(
        &mut self,
        property: String,
        is_ascending: bool,
        _collation: Option<String>,
        _arguments: HashMap<String, JSONValue>,
    ) -> jmap::Result<Comparator> {
        Ok(Comparator::Field(FieldComparator {
            field: match property.as_ref() {
                "emailId" => EmailSubmissionProperty::EmailId,
                "threadId" => EmailSubmissionProperty::ThreadId,
                "sentAt" => EmailSubmissionProperty::SendAt,
                _ => {
                    return Err(MethodError::UnsupportedSort(format!(
                        "Unsupported sort property '{}'.",
                        property
                    )))
                }
            }
            .into(),
            ascending: is_ascending,
        }))
    }

    fn has_more_filters(&self) -> bool {
        false
    }

    fn apply_filters(&mut self, _results: Vec<JMAPId>) -> jmap::Result<Vec<JMAPId>> {
        Ok(vec![])
    }

    fn is_immutable(&self) -> bool {
        false
    }

    fn collection() -> Collection {
        Collection::EmailSubmission
    }
}
