use std::collections::HashSet;

use jmap_store::query::paginate_results;
use jmap_store::{
    changes::JMAPChanges, query::build_query, JMAPQueryRequest, JMAPQueryResponse, JMAP_MAIL,
};
use mail_parser::RfcHeader;
use nlp::Language;
use store::JMAPIdPrefix;
use store::{
    query::JMAPStoreQuery, roaring::RoaringBitmap, AccountId, Comparator, DocumentSetComparator,
    FieldComparator, FieldValue, Filter, JMAPId, JMAPStore, Store, StoreError, Tag, TextQuery,
    ThreadId,
};

use crate::MessageField;

pub type MailboxId = u32;

#[derive(Debug, Clone)]
pub enum JMAPMailFilterCondition {
    InMailbox(MailboxId),
    InMailboxOtherThan(Vec<MailboxId>),
    Before(u64),
    After(u64),
    MinSize(usize),
    MaxSize(usize),
    AllInThreadHaveKeyword(String),
    SomeInThreadHaveKeyword(String),
    NoneInThreadHaveKeyword(String),
    HasKeyword(String),
    NotKeyword(String),
    HasAttachment(bool),
    Text(String),
    From(String),
    To(String),
    Cc(String),
    Bcc(String),
    Subject(String),
    Body(String),
    Header((RfcHeader, Option<String>)),
}

#[derive(Debug, Clone)]
pub enum JMAPMailComparator {
    ReceivedAt,
    Size,
    From,
    To,
    Subject,
    SentAt,
    HasKeyword(String),
    AllInThreadHaveKeyword(String),
    SomeInThreadHaveKeyword(String),
}

#[derive(Debug, Clone)]
pub struct JMAPMailQueryArguments {
    pub collapse_threads: bool,
}

pub trait JMAPMailQuery {
    fn mail_query(
        &self,
        request: JMAPQueryRequest<
            JMAPMailFilterCondition,
            JMAPMailComparator,
            JMAPMailQueryArguments,
        >,
    ) -> jmap_store::Result<JMAPQueryResponse>;

    fn get_thread_keywords(
        &self,
        account: AccountId,
        keyword: String,
        match_all: bool,
    ) -> store::Result<RoaringBitmap>;
}

impl<T> JMAPMailQuery for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_query(
        &self,
        request: JMAPQueryRequest<
            JMAPMailFilterCondition,
            JMAPMailComparator,
            JMAPMailQueryArguments,
        >,
    ) -> jmap_store::Result<JMAPQueryResponse> {
        let mut is_immutable_filter = true;
        let mut is_immutable_sort = true;
        let account_id = request.account_id;

        let (filter, sort) = build_query(
            request.filter,
            request.sort,
            |cond| {
                Ok(match cond {
                    JMAPMailFilterCondition::InMailbox(mailbox) => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::eq(
                            MessageField::Mailbox.into(),
                            FieldValue::Tag(Tag::Id(mailbox)),
                        )
                    }
                    JMAPMailFilterCondition::InMailboxOtherThan(mailboxes) => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::not(
                            mailboxes
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
                    JMAPMailFilterCondition::Before(timestamp) => Filter::lt(
                        MessageField::ReceivedAt.into(),
                        FieldValue::LongInteger(timestamp),
                    ),
                    JMAPMailFilterCondition::After(timestamp) => Filter::gt(
                        MessageField::ReceivedAt.into(),
                        FieldValue::LongInteger(timestamp),
                    ),
                    JMAPMailFilterCondition::MinSize(size) => Filter::ge(
                        MessageField::Size.into(),
                        FieldValue::LongInteger(size as u64),
                    ),
                    JMAPMailFilterCondition::MaxSize(size) => Filter::le(
                        MessageField::Size.into(),
                        FieldValue::LongInteger(size as u64),
                    ),
                    JMAPMailFilterCondition::HasAttachment(has_attachment) => {
                        let filter: Filter = Filter::eq(
                            MessageField::Attachment.into(),
                            FieldValue::Tag(Tag::Static(0)),
                        );
                        if !has_attachment {
                            Filter::not(vec![filter])
                        } else {
                            filter
                        }
                    }
                    JMAPMailFilterCondition::From(from) => {
                        Filter::eq(RfcHeader::From.into(), FieldValue::Text(from))
                    }
                    JMAPMailFilterCondition::To(to) => {
                        Filter::eq(RfcHeader::To.into(), FieldValue::Text(to))
                    }
                    JMAPMailFilterCondition::Cc(cc) => {
                        Filter::eq(RfcHeader::Cc.into(), FieldValue::Text(cc))
                    }
                    JMAPMailFilterCondition::Bcc(bcc) => {
                        Filter::eq(RfcHeader::Bcc.into(), FieldValue::Text(bcc))
                    }
                    JMAPMailFilterCondition::Subject(subject) => Filter::eq(
                        RfcHeader::Subject.into(),
                        FieldValue::FullText(TextQuery::query(subject, Language::English)),
                    ),
                    JMAPMailFilterCondition::Body(body) => Filter::eq(
                        MessageField::Body.into(),
                        FieldValue::FullText(TextQuery::query(body, Language::English)),
                    ),
                    JMAPMailFilterCondition::Text(text) => {
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
                    JMAPMailFilterCondition::Header((header, value)) => {
                        // TODO special case for message references
                        // TODO implement empty header matching
                        Filter::eq(
                            header.into(),
                            FieldValue::Text(value.unwrap_or_else(|| "".into())),
                        )
                    }
                    JMAPMailFilterCondition::HasKeyword(keyword) => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        // TODO text to id conversion
                        Filter::eq(
                            MessageField::Keyword.into(),
                            FieldValue::Tag(Tag::Text(keyword)),
                        )
                    }
                    JMAPMailFilterCondition::NotKeyword(keyword) => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::not(vec![Filter::eq(
                            MessageField::Keyword.into(),
                            FieldValue::Tag(Tag::Text(keyword)),
                        )])
                    }
                    JMAPMailFilterCondition::AllInThreadHaveKeyword(keyword) => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::DocumentSet(self.get_thread_keywords(account_id, keyword, true)?)
                    }
                    JMAPMailFilterCondition::SomeInThreadHaveKeyword(keyword) => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::DocumentSet(self.get_thread_keywords(account_id, keyword, false)?)
                    }
                    JMAPMailFilterCondition::NoneInThreadHaveKeyword(keyword) => {
                        if is_immutable_filter {
                            is_immutable_filter = false;
                        }
                        Filter::not(vec![Filter::DocumentSet(
                            self.get_thread_keywords(account_id, keyword, false)?,
                        )])
                    }
                })
            },
            |comp| {
                Ok(match comp.property {
                    JMAPMailComparator::ReceivedAt => Comparator::Field(FieldComparator {
                        field: MessageField::ReceivedAt.into(),
                        ascending: comp.is_ascending,
                    }),
                    JMAPMailComparator::Size => Comparator::Field(FieldComparator {
                        field: MessageField::Size.into(),
                        ascending: comp.is_ascending,
                    }),
                    JMAPMailComparator::From => Comparator::Field(FieldComparator {
                        field: RfcHeader::From.into(),
                        ascending: comp.is_ascending,
                    }),
                    JMAPMailComparator::To => Comparator::Field(FieldComparator {
                        field: RfcHeader::To.into(),
                        ascending: comp.is_ascending,
                    }),
                    JMAPMailComparator::Subject => Comparator::Field(FieldComparator {
                        field: MessageField::ThreadName.into(),
                        ascending: comp.is_ascending,
                    }),
                    JMAPMailComparator::SentAt => Comparator::Field(FieldComparator {
                        field: RfcHeader::Date.into(),
                        ascending: comp.is_ascending,
                    }),
                    JMAPMailComparator::HasKeyword(keyword) => {
                        if is_immutable_sort {
                            is_immutable_sort = false;
                        }
                        Comparator::DocumentSet(DocumentSetComparator {
                            set: self
                                .get_tag(
                                    account_id,
                                    JMAP_MAIL,
                                    MessageField::Keyword.into(),
                                    Tag::Text(keyword),
                                )?
                                .unwrap_or_else(RoaringBitmap::new),
                            ascending: comp.is_ascending,
                        })
                    }
                    JMAPMailComparator::AllInThreadHaveKeyword(keyword) => {
                        if is_immutable_sort {
                            is_immutable_sort = false;
                        }
                        Comparator::DocumentSet(DocumentSetComparator {
                            set: self.get_thread_keywords(account_id, keyword, true)?,
                            ascending: comp.is_ascending,
                        })
                    }
                    JMAPMailComparator::SomeInThreadHaveKeyword(keyword) => {
                        if is_immutable_sort {
                            is_immutable_sort = false;
                        }
                        Comparator::DocumentSet(DocumentSetComparator {
                            set: self.get_thread_keywords(account_id, keyword, false)?,
                            ascending: comp.is_ascending,
                        })
                    }
                })
            },
        )?;

        let mut seen_threads = HashSet::new();
        let collapse_threads = request.arguments.collapse_threads;
        let filter_map_fnc = Some(|document_id| {
            Ok(
                if let Some(thread_id) = self.get_document_value::<ThreadId>(
                    account_id,
                    JMAP_MAIL,
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

        let query_state = self.get_state(request.account_id, JMAP_MAIL)?;
        let results = self.query(JMAPStoreQuery {
            account_id,
            collection_id: JMAP_MAIL,
            filter_map_fnc,
            filter,
            sort,
        })?;
        let total_results = results.len();

        let (results, start_position) = paginate_results(
            results,
            total_results,
            request.limit,
            request.position,
            request.anchor,
            request.anchor_offset,
        )?;

        Ok(JMAPQueryResponse {
            account_id: request.account_id,
            include_total: request.calculate_total,
            query_state,
            position: start_position,
            total: total_results,
            limit: request.limit,
            ids: results,
            is_immutable: is_immutable_filter && is_immutable_sort,
        })
    }

    fn get_thread_keywords(
        &self,
        account: AccountId,
        keyword: String,
        match_all: bool,
    ) -> store::Result<RoaringBitmap> {
        if let Some(tagged_doc_ids) = self.get_tag(
            account,
            JMAP_MAIL,
            MessageField::Keyword.into(),
            Tag::Text(keyword),
        )? {
            let mut not_matched_ids = RoaringBitmap::new();
            let mut matched_ids = RoaringBitmap::new();

            for tagged_doc_id in tagged_doc_ids.clone().into_iter() {
                if matched_ids.contains(tagged_doc_id) || not_matched_ids.contains(tagged_doc_id) {
                    continue;
                }

                if let Some(thread_doc_ids) = self.get_tag(
                    account,
                    JMAP_MAIL,
                    MessageField::ThreadId.into(),
                    Tag::Id(
                        self.get_document_value(
                            account,
                            JMAP_MAIL,
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
