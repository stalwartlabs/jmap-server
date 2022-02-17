use std::collections::HashSet;

use crate::{JMAPMailIdImpl, JMAPMailQuery};
use jmap_store::{
    changes::JMAPLocalChanges,
    local_store::JMAPLocalStore,
    query::{build_query, paginate_results},
    JMAPId, JMAPQuery, JMAPQueryResponse, JMAP_MAIL,
};
use mail_parser::RfcHeader;
use nlp::Language;
use store::{
    AccountId, Comparator, DocumentId, DocumentSet, DocumentSetBitOps, DocumentSetComparator,
    FieldComparator, FieldValue, Filter, Store, StoreError, Tag, TextQuery, ThreadId,
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

impl<'x, T> JMAPMailQuery<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn mail_query(
        &'x self,
        request: JMAPQuery<JMAPMailFilterCondition, JMAPMailComparator, JMAPMailQueryArguments>,
    ) -> jmap_store::Result<JMAPQueryResponse> {
        let mut is_immutable_filter = true;
        let mut is_immutable_sort = true;
        let account_id = request.account_id;

        let doc_ids = build_query(
            &self.store,
            account_id,
            JMAP_MAIL,
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
                                .collect::<Vec<Filter<T::Set>>>(),
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
                        let filter: Filter<T::Set> = Filter::eq(
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
                                .store
                                .get_tag(
                                    account_id,
                                    JMAP_MAIL,
                                    MessageField::Keyword.into(),
                                    Tag::Text(keyword),
                                )?
                                .unwrap_or_else(DocumentSet::new),
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

        let query_state = self.get_state(request.account_id, JMAP_MAIL)?;
        let num_results = doc_ids.len();
        let mut seen_threads = HashSet::with_capacity(num_results);
        let collapse_threads = request.arguments.collapse_threads;

        let (results, start_position) = paginate_results(
            doc_ids,
            num_results,
            request.limit,
            request.position,
            request.anchor,
            request.anchor_offset,
            collapse_threads,
            Some(|doc_id| {
                Ok(
                    if let Some(thread_id) = self.store.get_document_value::<ThreadId>(
                        account_id,
                        JMAP_MAIL,
                        doc_id,
                        MessageField::ThreadId.into(),
                    )? {
                        if collapse_threads {
                            if seen_threads.contains(&thread_id) {
                                return Ok(None);
                            }
                            seen_threads.insert(thread_id);
                        }
                        Some(JMAPId::from_email(thread_id, doc_id))
                    } else {
                        None
                    },
                )
            }),
            Some(|doc_ids: Vec<DocumentId>| {
                Ok(self
                    .store
                    .get_multi_document_value(
                        account_id,
                        JMAP_MAIL,
                        doc_ids.iter().copied(),
                        MessageField::ThreadId.into(),
                    )?
                    .into_iter()
                    .zip(doc_ids.into_iter())
                    .filter_map(|(thread_id, doc_id)| JMAPId::from_email(thread_id?, doc_id).into())
                    .collect())
            }),
        )?;

        Ok(JMAPQueryResponse {
            account_id: request.account_id,
            include_total: request.calculate_total,
            query_state,
            position: start_position,
            total: num_results,
            limit: request.limit,
            ids: results,
            is_immutable: is_immutable_filter && is_immutable_sort,
        })
    }
}

pub trait JMAPThreadKeywords<'x, T>
where
    T: Store<'x>,
{
    fn get_thread_keywords(
        &self,
        account: AccountId,
        keyword: String,
        match_all: bool,
    ) -> store::Result<T::Set>;
}

impl<'x, T> JMAPThreadKeywords<'x, T> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn get_thread_keywords(
        &self,
        account: AccountId,
        keyword: String,
        match_all: bool,
    ) -> store::Result<T::Set> {
        if let Some(tagged_doc_ids) = self.store.get_tag(
            account,
            JMAP_MAIL,
            MessageField::Keyword.into(),
            Tag::Text(keyword),
        )? {
            let mut not_matched_ids = T::Set::new();
            let mut matched_ids = T::Set::new();

            for tagged_doc_id in tagged_doc_ids.clone().into_iter() {
                if matched_ids.contains(tagged_doc_id) || not_matched_ids.contains(tagged_doc_id) {
                    continue;
                }

                if let Some(thread_doc_ids) = self.store.get_tag(
                    account,
                    JMAP_MAIL,
                    MessageField::ThreadId.into(),
                    Tag::Id(
                        self.store
                            .get_document_value(
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
                    thread_tag_intersection.intersection(&tagged_doc_ids);

                    if (match_all && thread_tag_intersection == thread_doc_ids)
                        || (!match_all && !thread_tag_intersection.is_empty())
                    {
                        matched_ids.union(&thread_doc_ids);
                    } else if !thread_tag_intersection.is_empty() {
                        not_matched_ids.union(&thread_tag_intersection);
                    }
                }
            }
            Ok(matched_ids)
        } else {
            Ok(T::Set::new())
        }
    }
}
