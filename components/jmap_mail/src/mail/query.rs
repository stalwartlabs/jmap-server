use std::collections::{HashMap, HashSet};

use jmap::error::method::MethodError;
use jmap::jmap_store::changes::JMAPChanges;
use jmap::protocol::json::JSONValue;
use jmap::request::query::{QueryRequest, QueryResult};
use mail_parser::parsers::header::{parse_header_name, HeaderParserResult};
use mail_parser::RfcHeader;
use nlp::Language;
use store::{
    roaring::RoaringBitmap, AccountId, Comparator, DocumentSetComparator, FieldComparator,
    FieldValue, Filter, JMAPId, JMAPStore, Store, StoreError, Tag, TextQuery,
};
use store::{Collection, JMAPIdPrefix};

use crate::mail::{Keyword, MessageField};

pub trait JMAPMailQuery {
    fn mail_query(&self, request: QueryRequest) -> jmap::Result<JSONValue>;

    fn mail_query_ext(&self, request: QueryRequest) -> jmap::Result<QueryResult>;

    fn get_thread_keywords(
        &self,
        account: AccountId,
        keyword: Tag,
        match_all: bool,
    ) -> store::Result<RoaringBitmap>;
}

impl<T> JMAPMailQuery for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_query(&self, request: QueryRequest) -> jmap::Result<JSONValue> {
        self.mail_query_ext(request).map(|r| r.result)
    }

    fn mail_query_ext(&self, mut request: QueryRequest) -> jmap::Result<QueryResult> {
        let mut is_immutable_filter = true;
        let mut is_immutable_sort = true;
        let account_id = request.account_id;

        let cond_fnc = |cond: HashMap<String, JSONValue>| {
            if let Some((cond_name, cond_value)) = cond.into_iter().next() {
                Ok(match cond_name.as_str() {
                    "inMailbox" => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::eq(
                            MessageField::Mailbox.into(),
                            FieldValue::Tag(Tag::Id(cond_value.parse_document_id()?)),
                        )
                    }
                    "inMailboxOtherThan" => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
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
                        FieldValue::LongInteger(
                            cond_value.parse_unsigned_int(false)?.unwrap() as u64
                        ),
                    ),
                    "maxSize" => Filter::le(
                        MessageField::Size.into(),
                        FieldValue::LongInteger(
                            cond_value.parse_unsigned_int(false)?.unwrap() as u64
                        ),
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
                                FieldValue::FullText(TextQuery::query(
                                    text.clone(),
                                    Language::English,
                                )),
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
                            1 => ("".to_string(), cond_value.pop().unwrap().parse_string()?),
                            2 => (
                                cond_value.pop().unwrap().parse_string()?,
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
                        // TODO implement empty header matching
                        Filter::eq(header.into(), FieldValue::Text(value))
                    }
                    "hasKeyword" => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::eq(
                            MessageField::Keyword.into(),
                            FieldValue::Tag(Keyword::from_jmap(cond_value.parse_string()?)),
                        )
                    }
                    "notKeyword" => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::not(vec![Filter::eq(
                            MessageField::Keyword.into(),
                            FieldValue::Tag(Keyword::from_jmap(cond_value.parse_string()?)),
                        )])
                    }
                    "allInThreadHaveKeyword" => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::DocumentSet(self.get_thread_keywords(
                            account_id,
                            Keyword::from_jmap(cond_value.parse_string()?),
                            true,
                        )?)
                    }
                    "someInThreadHaveKeyword" => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::DocumentSet(self.get_thread_keywords(
                            account_id,
                            Keyword::from_jmap(cond_value.parse_string()?),
                            false,
                        )?)
                    }
                    "noneInThreadHaveKeyword" => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::not(vec![Filter::DocumentSet(self.get_thread_keywords(
                            account_id,
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
        };

        let sort_fnc = |mut comp: jmap::request::query::Comparator| {
            Ok(match comp.property.as_ref() {
                "receivedAt" => Comparator::Field(FieldComparator {
                    field: MessageField::ReceivedAt.into(),
                    ascending: comp.is_ascending,
                }),
                "size" => Comparator::Field(FieldComparator {
                    field: MessageField::Size.into(),
                    ascending: comp.is_ascending,
                }),
                "from" => Comparator::Field(FieldComparator {
                    field: RfcHeader::From.into(),
                    ascending: comp.is_ascending,
                }),
                "to" => Comparator::Field(FieldComparator {
                    field: RfcHeader::To.into(),
                    ascending: comp.is_ascending,
                }),
                "subject" => Comparator::Field(FieldComparator {
                    field: MessageField::ThreadName.into(),
                    ascending: comp.is_ascending,
                }),
                "sentAt" => Comparator::Field(FieldComparator {
                    field: RfcHeader::Date.into(),
                    ascending: comp.is_ascending,
                }),
                "hasKeyword" => {
                    if is_immutable_sort {
                        is_immutable_sort = false;
                    }
                    Comparator::DocumentSet(DocumentSetComparator {
                        set: self
                            .get_tag(
                                account_id,
                                Collection::Mail,
                                MessageField::Keyword.into(),
                                Keyword::from_jmap(
                                    comp.arguments
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
                        ascending: comp.is_ascending,
                    })
                }
                "allInThreadHaveKeyword" => {
                    if is_immutable_sort {
                        is_immutable_sort = false;
                    }
                    Comparator::DocumentSet(DocumentSetComparator {
                        set: self.get_thread_keywords(
                            account_id,
                            Keyword::from_jmap(
                                comp.arguments
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
                        ascending: comp.is_ascending,
                    })
                }
                "someInThreadHaveKeyword" => {
                    if is_immutable_sort {
                        is_immutable_sort = false;
                    }
                    Comparator::DocumentSet(DocumentSetComparator {
                        set: self.get_thread_keywords(
                            account_id,
                            Keyword::from_jmap(
                                comp.arguments
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
                        ascending: comp.is_ascending,
                    })
                }
                _ => {
                    return Err(MethodError::UnsupportedSort(format!(
                        "Unsupported sort property '{}'.",
                        comp.property
                    )))
                }
            })
        };

        let mut seen_threads = HashSet::new();
        let collapse_threads = request
            .arguments
            .get("collapseThreads")
            .and_then(|v| v.to_bool())
            .unwrap_or(false);
        let filter_map_fnc = Some(|document_id| {
            Ok(
                if let Some(thread_id) = self.get_document_tag_id(
                    account_id,
                    Collection::Mail,
                    document_id,
                    MessageField::ThreadId.into(),
                )? {
                    if collapse_threads && !seen_threads.insert(thread_id) {
                        None
                    } else {
                        Some(JMAPId::from_parts(thread_id, document_id))
                    }
                } else {
                    None
                },
            )
        });

        if request.limit == 0 || request.limit > self.config.query_max_results {
            request.limit = self.config.query_max_results;
        }

        let query = request.build_query(Collection::Mail, cond_fnc, sort_fnc, filter_map_fnc)?;

        Ok(QueryResult {
            is_immutable: is_immutable_filter && is_immutable_sort,
            result: request.into_response(
                self.query(query)?,
                self.get_state(account_id, Collection::Mail)?,
            )?,
        })
    }

    fn get_thread_keywords(
        &self,
        account: AccountId,
        keyword: Tag,
        match_all: bool,
    ) -> store::Result<RoaringBitmap> {
        if let Some(tagged_doc_ids) = self.get_tag(
            account,
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
                    account,
                    Collection::Mail,
                    MessageField::ThreadId.into(),
                    Tag::Id(
                        self.get_document_tag_id(
                            account,
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
