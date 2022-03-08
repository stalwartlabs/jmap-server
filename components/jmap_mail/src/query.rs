use std::sync::Arc;

use jmap_store::{
    async_trait::async_trait, changes::JMAPChanges, query::build_query, JMAPQueryRequest,
    JMAPQueryResponse, JMAP_MAIL,
};
use mail_parser::RfcHeader;
use nlp::Language;
use store::{
    parking_lot::Mutex,
    query::{JMAPPrefix, JMAPStoreQuery},
    roaring::RoaringBitmap,
    AccountId, Comparator, DocumentSetComparator, FieldComparator, FieldValue, Filter, JMAPStore,
    Store, StoreError, Tag, TextQuery,
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

#[async_trait]
pub trait JMAPMailQuery {
    async fn mail_query(
        &self,
        request: JMAPQueryRequest<
            JMAPMailFilterCondition,
            JMAPMailComparator,
            JMAPMailQueryArguments,
        >,
    ) -> jmap_store::Result<JMAPQueryResponse>;

    async fn get_thread_keywords(
        &self,
        account: AccountId,
        keyword: String,
        match_all: bool,
    ) -> store::Result<RoaringBitmap>;
}

#[async_trait]
impl<T> JMAPMailQuery for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    async fn mail_query(
        &self,
        request: JMAPQueryRequest<
            JMAPMailFilterCondition,
            JMAPMailComparator,
            JMAPMailQueryArguments,
        >,
    ) -> jmap_store::Result<JMAPQueryResponse> {
        //TODO improve this
        let mut _is_immutable_filter = Arc::new(Mutex::new(false));
        let mut _is_immutable_sort = Arc::new(Mutex::new(false));
        let account_id = request.account_id;

        let is_immutable_filter = _is_immutable_filter.clone();
        let is_immutable_sort = _is_immutable_sort.clone();

        let (filter, sort) = build_query(
            request.filter,
            request.sort,
            |cond| async {
                Ok(match cond {
                    JMAPMailFilterCondition::InMailbox(mailbox) => {
                        *is_immutable_filter.lock() = false;
                        Filter::eq(
                            MessageField::Mailbox.into(),
                            FieldValue::Tag(Tag::Id(mailbox)),
                        )
                    }
                    JMAPMailFilterCondition::InMailboxOtherThan(mailboxes) => {
                        *is_immutable_filter.lock() = false;
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
                        *is_immutable_filter.lock() = false;
                        // TODO text to id conversion
                        Filter::eq(
                            MessageField::Keyword.into(),
                            FieldValue::Tag(Tag::Text(keyword)),
                        )
                    }
                    JMAPMailFilterCondition::NotKeyword(keyword) => {
                        *is_immutable_filter.lock() = false;
                        Filter::not(vec![Filter::eq(
                            MessageField::Keyword.into(),
                            FieldValue::Tag(Tag::Text(keyword)),
                        )])
                    }
                    JMAPMailFilterCondition::AllInThreadHaveKeyword(keyword) => {
                        *is_immutable_filter.lock() = false;
                        Filter::DocumentSet(
                            self.get_thread_keywords(account_id, keyword, true).await?,
                        )
                    }
                    JMAPMailFilterCondition::SomeInThreadHaveKeyword(keyword) => {
                        *is_immutable_filter.lock() = false;
                        Filter::DocumentSet(
                            self.get_thread_keywords(account_id, keyword, false).await?,
                        )
                    }
                    JMAPMailFilterCondition::NoneInThreadHaveKeyword(keyword) => {
                        *is_immutable_filter.lock() = false;
                        Filter::not(vec![Filter::DocumentSet(
                            self.get_thread_keywords(account_id, keyword, false).await?,
                        )])
                    }
                })
            },
            |comp| async {
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
                        *is_immutable_sort.lock() = false;

                        Comparator::DocumentSet(DocumentSetComparator {
                            set: self
                                .get_tag(
                                    account_id,
                                    JMAP_MAIL,
                                    MessageField::Keyword.into(),
                                    Tag::Text(keyword),
                                )
                                .await?
                                .unwrap_or_else(RoaringBitmap::new),
                            ascending: comp.is_ascending,
                        })
                    }
                    JMAPMailComparator::AllInThreadHaveKeyword(keyword) => {
                        *is_immutable_sort.lock() = false;
                        Comparator::DocumentSet(DocumentSetComparator {
                            set: self.get_thread_keywords(account_id, keyword, true).await?,
                            ascending: comp.is_ascending,
                        })
                    }
                    JMAPMailComparator::SomeInThreadHaveKeyword(keyword) => {
                        *is_immutable_sort.lock() = false;
                        Comparator::DocumentSet(DocumentSetComparator {
                            set: self.get_thread_keywords(account_id, keyword, false).await?,
                            ascending: comp.is_ascending,
                        })
                    }
                })
            },
        )
        .await?;

        let query_state = self.get_state(request.account_id, JMAP_MAIL).await?;
        let result = self
            .query(JMAPStoreQuery {
                account_id,
                collection_id: JMAP_MAIL,
                jmap_prefix: JMAPPrefix {
                    collection_id: JMAP_MAIL,
                    field_id: MessageField::ThreadId.into(),
                    unique: request.arguments.collapse_threads,
                }
                .into(),
                limit: request.limit,
                position: request.position,
                anchor: request.anchor,
                anchor_offset: request.anchor_offset,
                filter,
                sort,
            })
            .await?;

        let filter_lock = _is_immutable_filter.lock();
        let sort_lock = _is_immutable_sort.lock();

        Ok(JMAPQueryResponse {
            account_id: request.account_id,
            include_total: request.calculate_total,
            query_state,
            position: result.start_position,
            total: result.total_results,
            limit: request.limit,
            ids: result.results,
            is_immutable: *filter_lock && *sort_lock,
        })
    }

    async fn get_thread_keywords(
        &self,
        account: AccountId,
        keyword: String,
        match_all: bool,
    ) -> store::Result<RoaringBitmap> {
        if let Some(tagged_doc_ids) = self
            .get_tag(
                account,
                JMAP_MAIL,
                MessageField::Keyword.into(),
                Tag::Text(keyword),
            )
            .await?
        {
            let mut not_matched_ids = RoaringBitmap::new();
            let mut matched_ids = RoaringBitmap::new();

            for tagged_doc_id in tagged_doc_ids.clone().into_iter() {
                if matched_ids.contains(tagged_doc_id) || not_matched_ids.contains(tagged_doc_id) {
                    continue;
                }

                if let Some(thread_doc_ids) = self
                    .get_tag(
                        account,
                        JMAP_MAIL,
                        MessageField::ThreadId.into(),
                        Tag::Id(
                            self.get_document_value(
                                account,
                                JMAP_MAIL,
                                tagged_doc_id,
                                MessageField::ThreadId.into(),
                            )
                            .await?
                            .ok_or_else(|| {
                                StoreError::InternalError(format!(
                                    "Thread id for document {} not found.",
                                    tagged_doc_id
                                ))
                            })?,
                        ),
                    )
                    .await?
                {
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
