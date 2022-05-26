use std::collections::HashSet;

use crate::mail::MessageField;
use jmap::error::method::MethodError;
use jmap::jmap_store::query::{ExtraFilterFnc, QueryHelper, QueryObject};
use jmap::request::query::{QueryRequest, QueryResponse};
use jmap::types::jmap::JMAPId;
use mail_parser::parsers::header::{parse_header_name, HeaderParserResult};
use mail_parser::RfcHeader;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::core::tag::Tag;
use store::nlp::Language;
use store::read::comparator::{self, DocumentSetComparator, FieldComparator};
use store::read::filter::{self, FieldValue, TextQuery};
use store::LongInteger;
use store::{roaring::RoaringBitmap, AccountId, JMAPStore, Store};

use super::schema::{Comparator, Email, Filter};

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct QueryArguments {
    #[serde(rename = "collapseThreads")]
    collapse_threads: Option<bool>,
}

impl QueryObject for Email {
    type QueryArguments = QueryArguments;

    type Filter = Filter;

    type Comparator = Comparator;
}

pub trait JMAPMailQuery<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_query(&self, request: QueryRequest<Email>) -> jmap::Result<QueryResponse>;
    fn get_thread_keywords(
        &self,
        account_id: AccountId,
        keyword: Tag,
        match_all: bool,
    ) -> store::Result<RoaringBitmap>;
}

impl<T> JMAPMailQuery<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_query(&self, request: QueryRequest<Email>) -> jmap::Result<QueryResponse> {
        let mut helper = QueryHelper::new(self, request)?;
        let account_id = helper.account_id;
        let collapse_threads = helper.request.arguments.collapse_threads.unwrap_or(false);
        let mut is_immutable_filter = true;
        let mut is_immutable_sort = true;

        helper.parse_filter(|filter| {
            Ok(match filter {
                Filter::InMailbox { value } => {
                    if is_immutable_filter {
                        is_immutable_filter = false;
                    }
                    filter::Filter::eq(
                        MessageField::Mailbox.into(),
                        FieldValue::Tag(Tag::Id(value.get_document_id())),
                    )
                }
                Filter::InMailboxOtherThan { value } => {
                    if is_immutable_filter {
                        is_immutable_filter = false;
                    }
                    filter::Filter::not(
                        value
                            .into_iter()
                            .map(|mailbox| {
                                filter::Filter::eq(
                                    MessageField::Mailbox.into(),
                                    FieldValue::Tag(Tag::Id(mailbox.get_document_id())),
                                )
                            })
                            .collect::<Vec<filter::Filter>>(),
                    )
                }
                Filter::Before { value } => filter::Filter::lt(
                    MessageField::ReceivedAt.into(),
                    FieldValue::LongInteger(value.timestamp() as LongInteger),
                ),
                Filter::After { value } => filter::Filter::gt(
                    MessageField::ReceivedAt.into(),
                    FieldValue::LongInteger(value.timestamp() as LongInteger),
                ),
                Filter::MinSize { value } => filter::Filter::ge(
                    MessageField::Size.into(),
                    FieldValue::LongInteger(value as LongInteger),
                ),
                Filter::MaxSize { value } => filter::Filter::le(
                    MessageField::Size.into(),
                    FieldValue::LongInteger(value as LongInteger),
                ),
                Filter::AllInThreadHaveKeyword { value } => {
                    if is_immutable_filter {
                        is_immutable_filter = false;
                    }
                    filter::Filter::DocumentSet(
                        self.get_thread_keywords(account_id, value.tag, true)?,
                    )
                }
                Filter::SomeInThreadHaveKeyword { value } => {
                    if is_immutable_filter {
                        is_immutable_filter = false;
                    }
                    filter::Filter::DocumentSet(
                        self.get_thread_keywords(account_id, value.tag, false)?,
                    )
                }
                Filter::NoneInThreadHaveKeyword { value } => {
                    if is_immutable_filter {
                        is_immutable_filter = false;
                    }
                    filter::Filter::not(vec![filter::Filter::DocumentSet(
                        self.get_thread_keywords(account_id, value.tag, false)?,
                    )])
                }
                Filter::HasKeyword { value } => {
                    if is_immutable_filter {
                        is_immutable_filter = false;
                    }
                    filter::Filter::eq(MessageField::Keyword.into(), FieldValue::Tag(value.tag))
                }
                Filter::NotKeyword { value } => {
                    if is_immutable_filter {
                        is_immutable_filter = false;
                    }
                    filter::Filter::not(vec![filter::Filter::eq(
                        MessageField::Keyword.into(),
                        FieldValue::Tag(value.tag),
                    )])
                }
                Filter::HasAttachment { value } => {
                    let filter = filter::Filter::eq(
                        MessageField::Attachment.into(),
                        FieldValue::Tag(Tag::Static(0)),
                    );
                    if !value {
                        filter::Filter::not(vec![filter])
                    } else {
                        filter
                    }
                }
                Filter::Text { value } => filter::Filter::or(vec![
                    filter::Filter::eq(RfcHeader::From.into(), FieldValue::Text(value.clone())),
                    filter::Filter::eq(RfcHeader::To.into(), FieldValue::Text(value.clone())),
                    filter::Filter::eq(RfcHeader::Cc.into(), FieldValue::Text(value.clone())),
                    filter::Filter::eq(RfcHeader::Bcc.into(), FieldValue::Text(value.clone())),
                    filter::Filter::eq(
                        RfcHeader::Subject.into(),
                        FieldValue::FullText(TextQuery::query(value.clone(), Language::English)),
                    ),
                    filter::Filter::eq(
                        MessageField::Body.into(),
                        FieldValue::FullText(TextQuery::query(
                            value,
                            Language::English, //TODO detect language
                        )),
                    ),
                ]),
                Filter::From { value } => {
                    filter::Filter::eq(RfcHeader::From.into(), FieldValue::Text(value))
                }
                Filter::To { value } => {
                    filter::Filter::eq(RfcHeader::To.into(), FieldValue::Text(value))
                }
                Filter::Cc { value } => {
                    filter::Filter::eq(RfcHeader::Cc.into(), FieldValue::Text(value))
                }
                Filter::Bcc { value } => {
                    filter::Filter::eq(RfcHeader::Bcc.into(), FieldValue::Text(value))
                }
                Filter::Subject { value } => filter::Filter::eq(
                    RfcHeader::Subject.into(), //TODO detect language
                    FieldValue::FullText(TextQuery::query(value, Language::English)),
                ),
                Filter::Body { value } => filter::Filter::eq(
                    MessageField::Body.into(),
                    FieldValue::FullText(TextQuery::query(value, Language::English)),
                ),
                Filter::Header { mut value } => {
                    let (value, header) = match value.len() {
                        1 => (None, value.pop().unwrap()),
                        2 => (Some(value.pop().unwrap()), value.pop().unwrap()),
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
                        filter::Filter::eq(header.into(), FieldValue::Keyword(value))
                    } else {
                        filter::Filter::eq(
                            MessageField::HasHeader.into(),
                            FieldValue::Tag(Tag::Static(header.into())),
                        )
                    }
                }
                Filter::Unsupported { value } => {
                    return Err(MethodError::UnsupportedFilter(value));
                }
            })
        })?;

        helper.parse_comparator(|comparator| {
            Ok(match comparator.property {
                Comparator::ReceivedAt => comparator::Comparator::Field(FieldComparator {
                    field: MessageField::ReceivedAt.into(),
                    ascending: comparator.is_ascending,
                }),
                Comparator::Size => comparator::Comparator::Field(FieldComparator {
                    field: MessageField::Size.into(),
                    ascending: comparator.is_ascending,
                }),
                Comparator::From => comparator::Comparator::Field(FieldComparator {
                    field: RfcHeader::From.into(),
                    ascending: comparator.is_ascending,
                }),
                Comparator::To => comparator::Comparator::Field(FieldComparator {
                    field: RfcHeader::To.into(),
                    ascending: comparator.is_ascending,
                }),
                Comparator::Subject => comparator::Comparator::Field(FieldComparator {
                    field: MessageField::ThreadName.into(),
                    ascending: comparator.is_ascending,
                }),
                Comparator::SentAt => comparator::Comparator::Field(FieldComparator {
                    field: RfcHeader::Date.into(),
                    ascending: comparator.is_ascending,
                }),
                Comparator::HasKeyword { keyword } => {
                    if is_immutable_sort {
                        is_immutable_sort = false;
                    }
                    comparator::Comparator::DocumentSet(DocumentSetComparator {
                        set: self
                            .get_tag(
                                account_id,
                                Collection::Mail,
                                MessageField::Keyword.into(),
                                keyword.tag,
                            )?
                            .unwrap_or_else(RoaringBitmap::new),
                        ascending: comparator.is_ascending,
                    })
                }
                Comparator::AllInThreadHaveKeyword { keyword } => {
                    if is_immutable_sort {
                        is_immutable_sort = false;
                    }
                    comparator::Comparator::DocumentSet(DocumentSetComparator {
                        set: self.get_thread_keywords(account_id, keyword.tag, true)?,
                        ascending: comparator.is_ascending,
                    })
                }
                Comparator::SomeInThreadHaveKeyword { keyword } => {
                    if is_immutable_sort {
                        is_immutable_sort = false;
                    }
                    comparator::Comparator::DocumentSet(DocumentSetComparator {
                        set: self.get_thread_keywords(account_id, keyword.tag, false)?,
                        ascending: comparator.is_ascending,
                    })
                }
            })
        })?;

        let mut seen_threads = HashSet::new();
        helper
            .query(
                |document_id| {
                    Ok(
                        if let Some(thread_id) = self.get_document_value(
                            account_id,
                            Collection::Mail,
                            document_id,
                            MessageField::ThreadId.into(),
                        )? {
                            if collapse_threads && !seen_threads.insert(thread_id) {
                                None
                            } else {
                                Some(JMAPId::from_parts(thread_id, document_id).into())
                            }
                        } else {
                            None
                        },
                    )
                },
                None::<ExtraFilterFnc>,
            )
            .map(|mut r| {
                r.is_immutable = is_immutable_filter && is_immutable_sort;
                r
            })
    }

    fn get_thread_keywords(
        &self,
        account_id: AccountId,
        keyword: Tag,
        match_all: bool,
    ) -> store::Result<RoaringBitmap> {
        if let Some(tagged_doc_ids) = self.get_tag(
            account_id,
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

                if let Some(thread_doc_ids) = self.get_tag(
                    account_id,
                    Collection::Mail,
                    MessageField::ThreadId.into(),
                    Tag::Id(
                        self.get_document_value(
                            account_id,
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
