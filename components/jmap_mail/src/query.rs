use std::{borrow::Cow, collections::HashSet};

use crate::{JMAPMailIdImpl, JMAPMailQuery};
use jmap_store::{
    changes::JMAPLocalChanges, local_store::JMAPLocalStore, JMAPError, JMAPFilter, JMAPId,
    JMAPLogicalOperator, JMAPQuery, JMAPQueryResponse, JMAP_MAIL,
};
use mail_parser::RfcHeader;
use nlp::Language;
use store::{
    AccountId, Comparator, DocumentId, DocumentSet, DocumentSetBitOps, DocumentSetComparator,
    FieldComparator, FieldValue, Filter, FilterOperator, LogicalOperator, Store, StoreError, Tag,
    TextQuery, ThreadId,
};

use crate::MessageField;

pub type MailboxId = u32;

#[derive(Debug, Clone)]
pub enum JMAPMailFilterCondition<'x> {
    InMailbox(MailboxId),
    InMailboxOtherThan(Vec<MailboxId>),
    Before(u64),
    After(u64),
    MinSize(usize),
    MaxSize(usize),
    AllInThreadHaveKeyword(Cow<'x, str>),
    SomeInThreadHaveKeyword(Cow<'x, str>),
    NoneInThreadHaveKeyword(Cow<'x, str>),
    HasKeyword(Cow<'x, str>),
    NotKeyword(Cow<'x, str>),
    HasAttachment(bool),
    Text(Cow<'x, str>),
    From(Cow<'x, str>),
    To(Cow<'x, str>),
    Cc(Cow<'x, str>),
    Bcc(Cow<'x, str>),
    Subject(Cow<'x, str>),
    Body(Cow<'x, str>),
    Header((RfcHeader, Option<Cow<'x, str>>)),
}

#[derive(Debug, Clone)]
pub enum JMAPMailComparator<'x> {
    ReceivedAt,
    Size,
    From,
    To,
    Subject,
    SentAt,
    HasKeyword(Cow<'x, str>),
    AllInThreadHaveKeyword(Cow<'x, str>),
    SomeInThreadHaveKeyword(Cow<'x, str>),
}

struct QueryState<'x, T>
where
    T: DocumentSet,
{
    op: JMAPLogicalOperator,
    terms: Vec<Filter<'x, T>>,
    it: std::vec::IntoIter<JMAPFilter<JMAPMailFilterCondition<'x>>>,
}

impl<'x, T> JMAPMailQuery<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn mail_query(
        &'x self,
        mut query: JMAPQuery<JMAPMailFilterCondition<'x>, JMAPMailComparator<'x>>,
        collapse_threads: bool,
    ) -> jmap_store::Result<JMAPQueryResponse> {
        let mut is_immutable = true;
        let state: Option<QueryState<T::Set>> = match query.filter {
            JMAPFilter::Operator(op) => Some(QueryState {
                op: op.operator,
                terms: Vec::with_capacity(op.conditions.len()),
                it: op.conditions.into_iter(),
            }),
            JMAPFilter::None => None,
            cond => Some(QueryState {
                op: JMAPLogicalOperator::And,
                it: vec![cond].into_iter(),
                terms: Vec::with_capacity(1),
            }),
        };

        let filter: Filter<T::Set> = if let Some(mut state) = state {
            let mut state_stack = Vec::new();
            let mut filter;

            'outer: loop {
                while let Some(term) = state.it.next() {
                    match term {
                        JMAPFilter::Condition(cond) => {
                            match cond {
                                JMAPMailFilterCondition::InMailbox(mailbox) => {
                                    state.terms.push(Filter::eq(
                                        MessageField::Mailbox.into(),
                                        FieldValue::Tag(Tag::Id(mailbox)),
                                    ));
                                    if is_immutable {
                                        is_immutable = false;
                                    }
                                }
                                JMAPMailFilterCondition::InMailboxOtherThan(mailboxes) => {
                                    state.terms.push(Filter::not(
                                        mailboxes
                                            .into_iter()
                                            .map(|mailbox| {
                                                Filter::eq(
                                                    MessageField::Mailbox.into(),
                                                    FieldValue::Tag(Tag::Id(mailbox)),
                                                )
                                            })
                                            .collect::<Vec<Filter<T::Set>>>(),
                                    ));
                                    if is_immutable {
                                        is_immutable = false;
                                    }
                                }
                                JMAPMailFilterCondition::Before(timestamp) => {
                                    state.terms.push(Filter::lt(
                                        MessageField::ReceivedAt.into(),
                                        FieldValue::LongInteger(timestamp),
                                    ));
                                }
                                JMAPMailFilterCondition::After(timestamp) => {
                                    state.terms.push(Filter::gt(
                                        MessageField::ReceivedAt.into(),
                                        FieldValue::LongInteger(timestamp),
                                    ));
                                }
                                JMAPMailFilterCondition::MinSize(size) => {
                                    state.terms.push(Filter::ge(
                                        MessageField::Size.into(),
                                        FieldValue::LongInteger(size as u64),
                                    ));
                                }
                                JMAPMailFilterCondition::MaxSize(size) => {
                                    state.terms.push(Filter::le(
                                        MessageField::Size.into(),
                                        FieldValue::LongInteger(size as u64),
                                    ));
                                }
                                JMAPMailFilterCondition::HasAttachment(has_attachment) => {
                                    let filter: Filter<T::Set> = Filter::eq(
                                        MessageField::Attachment.into(),
                                        FieldValue::Tag(Tag::Static(0)),
                                    );
                                    state.terms.push(if !has_attachment {
                                        Filter::not(vec![filter])
                                    } else {
                                        filter
                                    });
                                }
                                JMAPMailFilterCondition::From(from) => {
                                    state.terms.push(Filter::eq(
                                        RfcHeader::From.into(),
                                        FieldValue::Text(from),
                                    ));
                                }
                                JMAPMailFilterCondition::To(to) => {
                                    state.terms.push(Filter::eq(
                                        RfcHeader::To.into(),
                                        FieldValue::Text(to),
                                    ));
                                }
                                JMAPMailFilterCondition::Cc(cc) => {
                                    state.terms.push(Filter::eq(
                                        RfcHeader::Cc.into(),
                                        FieldValue::Text(cc),
                                    ));
                                }
                                JMAPMailFilterCondition::Bcc(bcc) => {
                                    state.terms.push(Filter::eq(
                                        RfcHeader::Bcc.into(),
                                        FieldValue::Text(bcc),
                                    ));
                                }
                                JMAPMailFilterCondition::Subject(subject) => {
                                    state.terms.push(Filter::eq(
                                        RfcHeader::Subject.into(),
                                        FieldValue::FullText(TextQuery::query(
                                            subject,
                                            Language::English,
                                        )),
                                    ));
                                }
                                JMAPMailFilterCondition::Body(body) => {
                                    state.terms.push(Filter::eq(
                                        MessageField::Body.into(),
                                        FieldValue::FullText(TextQuery::query(
                                            body,
                                            Language::English,
                                        )),
                                    ));
                                }
                                JMAPMailFilterCondition::Text(text) => {
                                    state.terms.push(Filter::or(vec![
                                        Filter::eq(
                                            RfcHeader::From.into(),
                                            FieldValue::Text(text.clone()),
                                        ),
                                        Filter::eq(
                                            RfcHeader::To.into(),
                                            FieldValue::Text(text.clone()),
                                        ),
                                        Filter::eq(
                                            RfcHeader::Cc.into(),
                                            FieldValue::Text(text.clone()),
                                        ),
                                        Filter::eq(
                                            RfcHeader::Bcc.into(),
                                            FieldValue::Text(text.clone()),
                                        ),
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
                                                text.clone(),
                                                Language::English, //TODO detect language
                                            )),
                                        ),
                                    ]));
                                }
                                JMAPMailFilterCondition::Header((header, value)) => {
                                    // TODO special case for message references
                                    // TODO implement empty header matching
                                    state.terms.push(Filter::eq(
                                        header.into(),
                                        FieldValue::Text(value.unwrap_or_else(|| "".into())),
                                    ));
                                }
                                JMAPMailFilterCondition::HasKeyword(keyword) => {
                                    // TODO text to id conversion
                                    state.terms.push(Filter::eq(
                                        MessageField::Keyword.into(),
                                        FieldValue::Tag(Tag::Text(keyword)),
                                    ));
                                    if is_immutable {
                                        is_immutable = false;
                                    }
                                }
                                JMAPMailFilterCondition::NotKeyword(keyword) => {
                                    state.terms.push(Filter::not(vec![Filter::eq(
                                        MessageField::Keyword.into(),
                                        FieldValue::Tag(Tag::Text(keyword)),
                                    )]));
                                    if is_immutable {
                                        is_immutable = false;
                                    }
                                }
                                JMAPMailFilterCondition::AllInThreadHaveKeyword(keyword) => {
                                    state.terms.push(Filter::DocumentSet(
                                        self.get_thread_keywords(query.account_id, keyword, true)?,
                                    ));
                                    if is_immutable {
                                        is_immutable = false;
                                    }
                                }
                                JMAPMailFilterCondition::SomeInThreadHaveKeyword(keyword) => {
                                    state.terms.push(Filter::DocumentSet(
                                        self.get_thread_keywords(query.account_id, keyword, false)?,
                                    ));
                                    if is_immutable {
                                        is_immutable = false;
                                    }
                                }
                                JMAPMailFilterCondition::NoneInThreadHaveKeyword(keyword) => {
                                    state.terms.push(Filter::not(vec![Filter::DocumentSet(
                                        self.get_thread_keywords(query.account_id, keyword, false)?,
                                    )]));
                                    if is_immutable {
                                        is_immutable = false;
                                    }
                                }
                            }
                        }
                        JMAPFilter::Operator(op) => {
                            let new_state = QueryState {
                                op: op.operator,
                                terms: Vec::with_capacity(op.conditions.len()),
                                it: op.conditions.into_iter(),
                            };
                            state_stack.push(state);
                            state = new_state;
                        }
                        JMAPFilter::None => {}
                    }
                }

                filter = Filter::Operator(FilterOperator {
                    operator: match state.op {
                        JMAPLogicalOperator::And => LogicalOperator::And,
                        JMAPLogicalOperator::Or => LogicalOperator::Or,
                        JMAPLogicalOperator::Not => LogicalOperator::Not,
                    },
                    conditions: state.terms,
                });

                if let Some(prev_state) = state_stack.pop() {
                    state = prev_state;
                    state.terms.push(filter);
                } else {
                    break 'outer;
                }
            }

            filter
        } else {
            Filter::None
        };

        let sort = if !query.sort.is_empty() {
            let mut terms: Vec<Comparator<T::Set>> = Vec::with_capacity(query.sort.len());
            for comp in query.sort {
                terms.push(match comp.property {
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
                        if is_immutable {
                            is_immutable = false;
                        }
                        Comparator::DocumentSet(DocumentSetComparator {
                            set: self
                                .store
                                .get_tag(
                                    query.account_id,
                                    JMAP_MAIL,
                                    MessageField::Keyword.into(),
                                    Tag::Text(keyword),
                                )?
                                .unwrap_or_else(DocumentSet::new),
                            ascending: comp.is_ascending,
                        })
                    }
                    JMAPMailComparator::AllInThreadHaveKeyword(keyword) => {
                        if is_immutable {
                            is_immutable = false;
                        }
                        Comparator::DocumentSet(DocumentSetComparator {
                            set: self.get_thread_keywords(query.account_id, keyword, true)?,
                            ascending: comp.is_ascending,
                        })
                    }
                    JMAPMailComparator::SomeInThreadHaveKeyword(keyword) => {
                        if is_immutable {
                            is_immutable = false;
                        }
                        Comparator::DocumentSet(DocumentSetComparator {
                            set: self.get_thread_keywords(query.account_id, keyword, false)?,
                            ascending: comp.is_ascending,
                        })
                    }
                });
            }
            Comparator::List(terms)
        } else {
            Comparator::None
        };

        let doc_ids = self
            .store
            .query(query.account_id, JMAP_MAIL, filter, sort)?;
        let query_state = self.get_state(query.account_id, JMAP_MAIL)?;
        let num_results = doc_ids.len();

        let results: Vec<JMAPId> = if collapse_threads || query.anchor.is_some() {
            let has_anchor = query.anchor.is_some();
            let results_len = if query.limit > 0 {
                query.limit
            } else {
                num_results
            };
            let mut results = Vec::with_capacity(results_len);
            let mut anchor_found = false;
            let mut seen_threads = HashSet::with_capacity(results_len);

            for doc_id in doc_ids {
                if let Some(thread_id) = self.store.get_document_value::<ThreadId>(
                    query.account_id,
                    JMAP_MAIL,
                    doc_id,
                    MessageField::ThreadId.into(),
                )? {
                    if collapse_threads {
                        if seen_threads.contains(&thread_id) {
                            continue;
                        }
                        seen_threads.insert(thread_id);
                    }
                    let result = JMAPId::from_email(thread_id, doc_id);

                    if !has_anchor {
                        if query.position >= 0 {
                            if query.position > 0 {
                                query.position -= 1;
                            } else {
                                results.push(result);
                                if query.limit > 0 && results.len() == query.limit {
                                    break;
                                }
                            }
                        } else {
                            results.push(result);
                        }
                    } else if query.anchor_offset >= 0 {
                        if !anchor_found {
                            if &result != query.anchor.as_ref().unwrap() {
                                continue;
                            }
                            anchor_found = true;
                        }

                        if query.anchor_offset > 0 {
                            query.anchor_offset -= 1;
                        } else {
                            results.push(result);
                            if query.limit > 0 && results.len() == query.limit {
                                break;
                            }
                        }
                    } else {
                        anchor_found = &result == query.anchor.as_ref().unwrap();
                        results.push(result);

                        if !anchor_found {
                            continue;
                        }

                        query.position = query.anchor_offset;

                        break;
                    }
                }
            }

            if !has_anchor || anchor_found {
                if query.position >= 0 {
                    results
                } else {
                    let position = query.position.abs() as usize;
                    let start_offset = if position < results.len() {
                        results.len() - position
                    } else {
                        0
                    };
                    let end_offset = if query.limit > 0 {
                        std::cmp::min(start_offset + query.limit, results.len())
                    } else {
                        results.len()
                    };

                    results[start_offset..end_offset].to_vec()
                }
            } else {
                return Err(JMAPError::AnchorNotFound);
            }
        } else {
            let doc_ids = if query.position != 0 && query.limit > 0 {
                doc_ids
                    .skip(if query.position > 0 {
                        query.position as usize
                    } else {
                        let position = query.position.abs();
                        if num_results > position as usize {
                            num_results - position as usize
                        } else {
                            0
                        }
                    })
                    .take(query.limit)
                    .collect::<Vec<DocumentId>>()
            } else if query.limit > 0 {
                doc_ids.take(query.limit).collect::<Vec<DocumentId>>()
            } else if query.position != 0 {
                doc_ids
                    .skip(if query.position > 0 {
                        query.position as usize
                    } else {
                        let position = query.position.abs();
                        if num_results > position as usize {
                            num_results - position as usize
                        } else {
                            0
                        }
                    })
                    .collect::<Vec<DocumentId>>()
            } else {
                doc_ids.collect::<Vec<DocumentId>>()
            };

            self.store
                .get_multi_document_value(
                    query.account_id,
                    JMAP_MAIL,
                    doc_ids.iter().copied(),
                    MessageField::ThreadId.into(),
                )?
                .into_iter()
                .zip(doc_ids.into_iter())
                .filter_map(|(thread_id, doc_id)| JMAPId::from_email(thread_id?, doc_id).into())
                .collect()
        };

        Ok(JMAPQueryResponse {
            query_state,
            total: num_results,
            ids: results,
            is_immutable,
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
        keyword: Cow<'x, str>,
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
        keyword: Cow<'x, str>,
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
