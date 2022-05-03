use std::collections::{HashMap, HashSet};

use crate::mail::{Keyword, MessageField};
use jmap::error::method::MethodError;
use jmap::jmap_store::query::{JMAPQuery, QueryObject, QueryResult};
use jmap::protocol::json::JSONValue;
use jmap::request::query::QueryRequest;
use mail_parser::parsers::header::{parse_header_name, HeaderParserResult};
use mail_parser::RfcHeader;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::core::JMAPIdPrefix;
use store::nlp::Language;
use store::read::comparator::{Comparator, DocumentSetComparator, FieldComparator};
use store::read::filter::{FieldValue, Filter, TextQuery};
use store::read::QueryFilterMap;
use store::DocumentId;
use store::{roaring::RoaringBitmap, AccountId, JMAPId, JMAPStore, Store};

pub struct QueryMail<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    store: &'y JMAPStore<T>,
    account_id: AccountId,
    is_immutable_filter: bool,
    is_immutable_sort: bool,
    collapse_threads: bool,
    seen_threads: HashSet<DocumentId>,
}

impl<'y, T> QueryFilterMap for QueryMail<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn filter_map_id(&mut self, document_id: store::DocumentId) -> store::Result<Option<JMAPId>> {
        Ok(
            if let Some(thread_id) = self.store.get_document_value(
                self.account_id,
                Collection::Mail,
                document_id,
                MessageField::ThreadId.into(),
            )? {
                if self.collapse_threads && !self.seen_threads.insert(thread_id) {
                    None
                } else {
                    Some(JMAPId::from_parts(thread_id, document_id))
                }
            } else {
                None
            },
        )
    }
}

impl<'y, T> QueryObject<'y, T> for QueryMail<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn new(store: &'y JMAPStore<T>, request: &QueryRequest) -> jmap::Result<Self> {
        Ok(QueryMail {
            store,
            account_id: request.account_id,
            is_immutable_filter: true,
            is_immutable_sort: true,
            seen_threads: HashSet::new(),
            collapse_threads: request
                .arguments
                .get("collapseThreads")
                .and_then(|v| v.to_bool())
                .unwrap_or(false),
        })
    }

    fn parse_filter(&mut self, cond: HashMap<String, JSONValue>) -> jmap::Result<Filter> {
        if let Some((cond_name, cond_value)) = cond.into_iter().next() {
            Ok(match cond_name.as_str() {
                "inMailbox" => {
                    if self.is_immutable_filter {
                        self.is_immutable_filter = false;
                    }
                    Filter::eq(
                        MessageField::Mailbox.into(),
                        FieldValue::Tag(Tag::Id(cond_value.parse_document_id()?)),
                    )
                }
                "inMailboxOtherThan" => {
                    if self.is_immutable_filter {
                        self.is_immutable_filter = false;
                    }
                    Filter::not(
                        cond_value
                            .parse_array_items(false)?
                            .unwrap()
                            .into_iter()
                            .map(|mailbox| {
                                Filter::eq(
                                    MessageField::Mailbox.into(),
                                    FieldValue::Tag(Tag::Id(mailbox)),
                                )
                            })
                            .collect::<Vec<Filter>>(),
                    )
                }
                "before" => Filter::lt(
                    MessageField::ReceivedAt.into(),
                    FieldValue::LongInteger(cond_value.parse_utc_date(false)?.unwrap() as u64),
                ),
                "after" => Filter::gt(
                    MessageField::ReceivedAt.into(),
                    FieldValue::LongInteger(cond_value.parse_utc_date(false)?.unwrap() as u64),
                ),
                "minSize" => Filter::ge(
                    MessageField::Size.into(),
                    FieldValue::LongInteger(cond_value.parse_unsigned_int(false)?.unwrap() as u64),
                ),
                "maxSize" => Filter::le(
                    MessageField::Size.into(),
                    FieldValue::LongInteger(cond_value.parse_unsigned_int(false)?.unwrap() as u64),
                ),
                "hasAttachment" => {
                    let filter: Filter = Filter::eq(
                        MessageField::Attachment.into(),
                        FieldValue::Tag(Tag::Static(0)),
                    );
                    if !cond_value.parse_bool()? {
                        Filter::not(vec![filter])
                    } else {
                        filter
                    }
                }
                "from" => Filter::eq(
                    RfcHeader::From.into(),
                    FieldValue::Text(cond_value.parse_string()?),
                ),
                "to" => Filter::eq(
                    RfcHeader::To.into(),
                    FieldValue::Text(cond_value.parse_string()?),
                ),
                "cc" => Filter::eq(
                    RfcHeader::Cc.into(),
                    FieldValue::Text(cond_value.parse_string()?),
                ),
                "bcc" => Filter::eq(
                    RfcHeader::Bcc.into(),
                    FieldValue::Text(cond_value.parse_string()?),
                ),
                "subject" => Filter::eq(
                    RfcHeader::Subject.into(),
                    FieldValue::FullText(TextQuery::query(
                        cond_value.parse_string()?,
                        Language::English,
                    )),
                ),
                "body" => Filter::eq(
                    MessageField::Body.into(),
                    FieldValue::FullText(TextQuery::query(
                        cond_value.parse_string()?,
                        Language::English,
                    )),
                ),
                "text" => {
                    let text = cond_value.parse_string()?;
                    Filter::or(vec![
                        Filter::eq(RfcHeader::From.into(), FieldValue::Text(text.clone())),
                        Filter::eq(RfcHeader::To.into(), FieldValue::Text(text.clone())),
                        Filter::eq(RfcHeader::Cc.into(), FieldValue::Text(text.clone())),
                        Filter::eq(RfcHeader::Bcc.into(), FieldValue::Text(text.clone())),
                        Filter::eq(
                            RfcHeader::Subject.into(),
                            FieldValue::FullText(TextQuery::query(text.clone(), Language::English)),
                        ),
                        Filter::eq(
                            MessageField::Body.into(),
                            FieldValue::FullText(TextQuery::query(
                                text,
                                Language::English, //TODO detect language
                            )),
                        ),
                    ])
                }
                "header" => {
                    let mut cond_value = cond_value.unwrap_array().ok_or_else(|| {
                        MethodError::InvalidArguments("Expected array.".to_string())
                    })?;
                    let (value, header) = match cond_value.len() {
                        1 => (None, cond_value.pop().unwrap().parse_string()?),
                        2 => (
                            Some(cond_value.pop().unwrap().parse_string()?),
                            cond_value.pop().unwrap().parse_string()?,
                        ),
                        _ => {
                            return Err(MethodError::InvalidArguments(
                                "Expected array of length 1 or 2.".to_string(),
                            ));
                        }
                    };
                    let header = match parse_header_name(header.as_bytes()) {
                        (_, HeaderParserResult::Rfc(rfc_header)) => rfc_header,
                        _ => {
                            return Err(MethodError::InvalidArguments(format!(
                                "Querying non-RFC header '{}' is not allowed.",
                                header
                            )))
                        }
                    };

                    // TODO special case for message references
                    if let Some(value) = value {
                        Filter::eq(header.into(), FieldValue::Text(value))
                    } else {
                        Filter::eq(
                            MessageField::HasHeader.into(),
                            FieldValue::Tag(Tag::Static(header.into())),
                        )
                    }
                }
                "hasKeyword" => {
                    if self.is_immutable_filter {
                        self.is_immutable_filter = false;
                    }
                    Filter::eq(
                        MessageField::Keyword.into(),
                        FieldValue::Tag(Keyword::from_jmap(cond_value.parse_string()?)),
                    )
                }
                "notKeyword" => {
                    if self.is_immutable_filter {
                        self.is_immutable_filter = false;
                    }
                    Filter::not(vec![Filter::eq(
                        MessageField::Keyword.into(),
                        FieldValue::Tag(Keyword::from_jmap(cond_value.parse_string()?)),
                    )])
                }
                "allInThreadHaveKeyword" => {
                    if self.is_immutable_filter {
                        self.is_immutable_filter = false;
                    }
                    Filter::DocumentSet(self.get_thread_keywords(
                        Keyword::from_jmap(cond_value.parse_string()?),
                        true,
                    )?)
                }
                "someInThreadHaveKeyword" => {
                    if self.is_immutable_filter {
                        self.is_immutable_filter = false;
                    }
                    Filter::DocumentSet(self.get_thread_keywords(
                        Keyword::from_jmap(cond_value.parse_string()?),
                        false,
                    )?)
                }
                "noneInThreadHaveKeyword" => {
                    if self.is_immutable_filter {
                        self.is_immutable_filter = false;
                    }
                    Filter::not(vec![Filter::DocumentSet(self.get_thread_keywords(
                        Keyword::from_jmap(cond_value.parse_string()?),
                        false,
                    )?)])
                }
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
        mut arguments: HashMap<String, JSONValue>,
    ) -> jmap::Result<Comparator> {
        Ok(match property.as_ref() {
            "receivedAt" => Comparator::Field(FieldComparator {
                field: MessageField::ReceivedAt.into(),
                ascending: is_ascending,
            }),
            "size" => Comparator::Field(FieldComparator {
                field: MessageField::Size.into(),
                ascending: is_ascending,
            }),
            "from" => Comparator::Field(FieldComparator {
                field: RfcHeader::From.into(),
                ascending: is_ascending,
            }),
            "to" => Comparator::Field(FieldComparator {
                field: RfcHeader::To.into(),
                ascending: is_ascending,
            }),
            "subject" => Comparator::Field(FieldComparator {
                field: MessageField::ThreadName.into(),
                ascending: is_ascending,
            }),
            "sentAt" => Comparator::Field(FieldComparator {
                field: RfcHeader::Date.into(),
                ascending: is_ascending,
            }),
            "hasKeyword" => {
                if self.is_immutable_sort {
                    self.is_immutable_sort = false;
                }
                Comparator::DocumentSet(DocumentSetComparator {
                    set: self
                        .store
                        .get_tag(
                            self.account_id,
                            Collection::Mail,
                            MessageField::Keyword.into(),
                            Keyword::from_jmap(
                                arguments
                                    .remove("keyword")
                                    .ok_or_else(|| {
                                        MethodError::InvalidArguments(
                                            "Missing 'keyword' property.".to_string(),
                                        )
                                    })?
                                    .parse_string()?,
                            ),
                        )?
                        .unwrap_or_else(RoaringBitmap::new),
                    ascending: is_ascending,
                })
            }
            "allInThreadHaveKeyword" => {
                if self.is_immutable_sort {
                    self.is_immutable_sort = false;
                }
                Comparator::DocumentSet(DocumentSetComparator {
                    set: self.get_thread_keywords(
                        Keyword::from_jmap(
                            arguments
                                .remove("keyword")
                                .ok_or_else(|| {
                                    MethodError::InvalidArguments(
                                        "Missing 'keyword' property.".to_string(),
                                    )
                                })?
                                .parse_string()?,
                        ),
                        true,
                    )?,
                    ascending: is_ascending,
                })
            }
            "someInThreadHaveKeyword" => {
                if self.is_immutable_sort {
                    self.is_immutable_sort = false;
                }
                Comparator::DocumentSet(DocumentSetComparator {
                    set: self.get_thread_keywords(
                        Keyword::from_jmap(
                            arguments
                                .remove("keyword")
                                .ok_or_else(|| {
                                    MethodError::InvalidArguments(
                                        "Missing 'keyword' property.".to_string(),
                                    )
                                })?
                                .parse_string()?,
                        ),
                        false,
                    )?,
                    ascending: is_ascending,
                })
            }
            _ => {
                return Err(MethodError::UnsupportedSort(format!(
                    "Unsupported sort property '{}'.",
                    property
                )))
            }
        })
    }

    fn has_more_filters(&self) -> bool {
        false
    }

    fn apply_filters(&mut self, _results: Vec<JMAPId>) -> jmap::Result<Vec<JMAPId>> {
        unreachable!()
    }

    fn is_immutable(&self) -> bool {
        self.is_immutable_filter && self.is_immutable_sort
    }

    fn collection() -> Collection {
        Collection::Mail
    }
}

impl<'y, T> QueryMail<'y, T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_thread_keywords(&self, keyword: Tag, match_all: bool) -> store::Result<RoaringBitmap> {
        if let Some(tagged_doc_ids) = self.store.get_tag(
            self.account_id,
            Collection::Mail,
            MessageField::Keyword.into(),
            keyword,
        )? {
            let mut not_matched_ids = RoaringBitmap::new();
            let mut matched_ids = RoaringBitmap::new();

            for tagged_doc_id in tagged_doc_ids.clone().into_iter() {
                if matched_ids.contains(tagged_doc_id) || not_matched_ids.contains(tagged_doc_id) {
                    continue;
                }

                if let Some(thread_doc_ids) = self.store.get_tag(
                    self.account_id,
                    Collection::Mail,
                    MessageField::ThreadId.into(),
                    Tag::Id(
                        self.store
                            .get_document_value(
                                self.account_id,
                                Collection::Mail,
                                tagged_doc_id,
                                MessageField::ThreadId.into(),
                            )?
                            .ok_or_else(|| {
                                StoreError::InternalError(format!(
                                    "Thread id for document {} not found.",
                                    tagged_doc_id
                                ))
                            })?,
                    ),
                )? {
                    let mut thread_tag_intersection = thread_doc_ids.clone();
                    thread_tag_intersection &= &tagged_doc_ids;

                    if (match_all && thread_tag_intersection == thread_doc_ids)
                        || (!match_all && !thread_tag_intersection.is_empty())
                    {
                        matched_ids |= &thread_doc_ids;
                    } else if !thread_tag_intersection.is_empty() {
                        not_matched_ids |= &thread_tag_intersection;
                    }
                }
            }
            Ok(matched_ids)
        } else {
            Ok(RoaringBitmap::new())
        }
    }
}

pub trait JMAPQueryMail {
    fn mail_query(&self, request: QueryRequest) -> jmap::Result<QueryResult>;
}

impl<T> JMAPQueryMail for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_query(&self, request: QueryRequest) -> jmap::Result<QueryResult> {
        JMAPQuery::query::<QueryMail<T>>(self, request)
    }
}
