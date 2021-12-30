use std::{borrow::Cow, collections::HashSet};

use jmap_store::{
    local_store::JMAPLocalStore, JMAPFilter, JMAPLogicalOperator, JMAPQuery, JMAPQueryResponse,
    JMAP_MAIL,
};
use mail_parser::HeaderName;
use nlp::Language;
use store::{
    AccountId, Comparator, DocumentId, DocumentSet, DocumentSetComparator, FieldComparator,
    FieldValue, Filter, FilterOperator, LogicalOperator, Store, StoreError, Tag, TextQuery,
    ThreadId,
};

use crate::{JMAPMailId, JMAPMailStoreQuery, MessageField};

pub type MailboxId = u64;

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
    Header((HeaderName, Option<Cow<'x, str>>)),
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

impl<'x, T> JMAPMailStoreQuery<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    type Set = T::Set;

    fn mail_query(
        &'x self,
        mut query: JMAPQuery<JMAPMailFilterCondition<'x>, JMAPMailComparator<'x>, JMAPMailId>,
        collapse_threads: bool,
    ) -> store::Result<JMAPQueryResponse<JMAPMailId>> {
        let state: Option<QueryState<Self::Set>> = match query.filter {
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

        let filter: Filter<Self::Set> = if let Some(mut state) = state {
            let mut state_stack = Vec::new();
            let mut filter;

            'outer: loop {
                while let Some(term) = state.it.next() {
                    match term {
                        JMAPFilter::Condition(cond) => match cond {
                            JMAPMailFilterCondition::InMailbox(mailbox) => {
                                state.terms.push(Filter::eq(
                                    MessageField::Mailbox.into(),
                                    FieldValue::Tag(Tag::Id(mailbox)),
                                ));
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
                                        .collect::<Vec<Filter<Self::Set>>>(),
                                ));
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
                                let filter: Filter<Self::Set> = Filter::eq(
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
                                    HeaderName::From.into(),
                                    FieldValue::Text(from),
                                ));
                            }
                            JMAPMailFilterCondition::To(to) => {
                                state
                                    .terms
                                    .push(Filter::eq(HeaderName::To.into(), FieldValue::Text(to)));
                            }
                            JMAPMailFilterCondition::Cc(cc) => {
                                state
                                    .terms
                                    .push(Filter::eq(HeaderName::Cc.into(), FieldValue::Text(cc)));
                            }
                            JMAPMailFilterCondition::Bcc(bcc) => {
                                state.terms.push(Filter::eq(
                                    HeaderName::Bcc.into(),
                                    FieldValue::Text(bcc),
                                ));
                            }
                            JMAPMailFilterCondition::Subject(subject) => {
                                state.terms.push(Filter::eq(
                                    HeaderName::Subject.into(),
                                    FieldValue::FullText(TextQuery::query(
                                        subject,
                                        Language::English,
                                    )),
                                ));
                            }
                            JMAPMailFilterCondition::Body(body) => {
                                state.terms.push(Filter::eq(
                                    MessageField::Body.into(),
                                    FieldValue::FullText(TextQuery::query(body, Language::English)),
                                ));
                            }
                            JMAPMailFilterCondition::Text(text) => {
                                state.terms.push(Filter::or(vec![
                                    Filter::eq(
                                        HeaderName::From.into(),
                                        FieldValue::Text(text.clone()),
                                    ),
                                    Filter::eq(
                                        HeaderName::To.into(),
                                        FieldValue::Text(text.clone()),
                                    ),
                                    Filter::eq(
                                        HeaderName::Cc.into(),
                                        FieldValue::Text(text.clone()),
                                    ),
                                    Filter::eq(
                                        HeaderName::Bcc.into(),
                                        FieldValue::Text(text.clone()),
                                    ),
                                    Filter::eq(
                                        HeaderName::Subject.into(),
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
                            }
                            JMAPMailFilterCondition::NotKeyword(keyword) => {
                                state.terms.push(Filter::not(vec![Filter::eq(
                                    MessageField::Keyword.into(),
                                    FieldValue::Tag(Tag::Text(keyword)),
                                )]));
                            }
                            JMAPMailFilterCondition::AllInThreadHaveKeyword(keyword) => {
                                state.terms.push(Filter::DocumentSet(get_thread_keywords(
                                    self.get_store(),
                                    query.account_id,
                                    keyword,
                                    true,
                                )?));
                            }
                            JMAPMailFilterCondition::SomeInThreadHaveKeyword(keyword) => {
                                state.terms.push(Filter::DocumentSet(get_thread_keywords(
                                    self.get_store(),
                                    query.account_id,
                                    keyword,
                                    false,
                                )?));
                            }
                            JMAPMailFilterCondition::NoneInThreadHaveKeyword(keyword) => {
                                state.terms.push(Filter::not(vec![Filter::DocumentSet(
                                    get_thread_keywords(
                                        self.get_store(),
                                        query.account_id,
                                        keyword,
                                        false,
                                    )?,
                                )]));
                            }
                        },
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
            let mut terms: Vec<Comparator<Self::Set>> = Vec::with_capacity(query.sort.len());
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
                        field: HeaderName::From.into(),
                        ascending: comp.is_ascending,
                    }),
                    JMAPMailComparator::To => Comparator::Field(FieldComparator {
                        field: HeaderName::To.into(),
                        ascending: comp.is_ascending,
                    }),
                    JMAPMailComparator::Subject => Comparator::Field(FieldComparator {
                        field: MessageField::ThreadName.into(),
                        ascending: comp.is_ascending,
                    }),
                    JMAPMailComparator::SentAt => Comparator::Field(FieldComparator {
                        field: HeaderName::Date.into(),
                        ascending: comp.is_ascending,
                    }),
                    JMAPMailComparator::HasKeyword(keyword) => {
                        Comparator::DocumentSet(DocumentSetComparator {
                            set: self
                                .store
                                .get_tag(
                                    query.account_id,
                                    JMAP_MAIL,
                                    MessageField::Keyword.into(),
                                    Tag::Text(keyword),
                                )?
                                .unwrap_or_else(Self::Set::new),
                            ascending: comp.is_ascending,
                        })
                    }
                    JMAPMailComparator::AllInThreadHaveKeyword(keyword) => {
                        Comparator::DocumentSet(DocumentSetComparator {
                            set: get_thread_keywords(
                                self.get_store(),
                                query.account_id,
                                keyword,
                                true,
                            )?,
                            ascending: comp.is_ascending,
                        })
                    }
                    JMAPMailComparator::SomeInThreadHaveKeyword(keyword) => {
                        Comparator::DocumentSet(DocumentSetComparator {
                            set: get_thread_keywords(
                                self.get_store(),
                                query.account_id,
                                keyword,
                                false,
                            )?,
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
        let num_results = doc_ids.size_hint().0;

        let results: Vec<JMAPMailId> = if collapse_threads || query.anchor.is_some() {
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
                    0,
                )? {
                    if collapse_threads {
                        if seen_threads.contains(&thread_id) {
                            continue;
                        }
                        seen_threads.insert(thread_id);
                    }
                    let result = JMAPMailId::new(thread_id, doc_id);

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
                return Err(StoreError::InternalError("Anchor not found".into()));
                // TODO use correct error type
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
                .filter_map(|(thread_id, doc_id)| JMAPMailId::new(thread_id?, doc_id).into())
                .collect()
        };

        Ok(JMAPQueryResponse {
            query_state: "".to_string(),
            total: num_results,
            ids: results,
        })
    }
}

fn get_thread_keywords<'x, T>(
    store: &T,
    account: AccountId,
    keyword: Cow<'x, str>,
    match_all: bool,
) -> store::Result<T::Set>
where
    T: Store<'x>,
{
    if let Some(tagged_doc_ids) = store.get_tag(
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

            if let Some(thread_doc_ids) = store.get_tag(
                account,
                JMAP_MAIL,
                MessageField::ThreadId.into(),
                Tag::Id(
                    store
                        .get_document_value(
                            account,
                            JMAP_MAIL,
                            tagged_doc_id,
                            MessageField::ThreadId.into(),
                            0,
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
