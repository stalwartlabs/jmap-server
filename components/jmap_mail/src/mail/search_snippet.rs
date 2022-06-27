use std::sync::Arc;

use jmap::{
    error::method::MethodError,
    request::{
        query::{self, Filter},
        ACLEnforce, MaybeResultReference, ResultReference,
    },
    types::jmap::JMAPId,
};
use mail_parser::{decoders::html::html_to_text, Message, RfcHeader};
use serde::{Deserialize, Serialize};
use store::{
    blob::BlobId,
    core::{
        acl::{ACLToken, ACL},
        collection::Collection,
        document::MAX_TOKEN_LENGTH,
        error::StoreError,
    },
    nlp::{search_snippet::generate_snippet, stemmer::Stemmer, tokenizers::Tokenizer, Language},
    read::filter::LogicalOperator,
    tracing::error,
    JMAPStore, Store,
};

use super::{
    parse::get_message_part, sharing::JMAPShareMail, MessageData, MessageField, MimePartType,
};

#[derive(Debug, Clone, Deserialize)]
pub struct SearchSnippetGetRequest {
    #[serde(skip)]
    pub acl: Option<Arc<ACLToken>>,

    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "filter")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter<super::schema::Filter>>,

    #[serde(rename = "emailIds", alias = "#emailIds")]
    pub email_ids: MaybeResultReference<Vec<JMAPId>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SearchSnippetGetResponse {
    #[serde(rename = "accountId")]
    pub account_id: JMAPId,

    #[serde(rename = "list")]
    pub list: Vec<SearchSnippet>,

    #[serde(rename = "notFound")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub not_found: Option<Vec<JMAPId>>,
}

#[derive(Serialize, Clone, Debug)]
pub struct SearchSnippet {
    #[serde(rename = "emailId")]
    pub email_id: JMAPId,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<String>,
}

impl SearchSnippet {
    pub fn empty(email_id: JMAPId) -> Self {
        SearchSnippet {
            email_id,
            subject: None,
            preview: None,
        }
    }
}

impl SearchSnippetGetRequest {
    pub fn eval_result_references(
        &mut self,
        mut fnc: impl FnMut(&ResultReference) -> Option<Vec<u64>>,
    ) -> jmap::Result<()> {
        if let MaybeResultReference::Reference(rr) = &self.email_ids {
            if let Some(ids) = fnc(rr) {
                self.email_ids =
                    MaybeResultReference::Value(ids.into_iter().map(Into::into).collect());
            } else {
                return Err(MethodError::InvalidResultReference(
                    "Failed to evaluate #ids result reference.".to_string(),
                ));
            }
        }
        Ok(())
    }
}

struct QueryState {
    op: LogicalOperator,
    it: std::vec::IntoIter<query::Filter<super::schema::Filter>>,
}

pub trait JMAPMailSearchSnippet<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_search_snippet(
        &self,
        request: SearchSnippetGetRequest,
    ) -> jmap::Result<SearchSnippetGetResponse>;
}

impl<T> JMAPMailSearchSnippet<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_search_snippet(
        &self,
        request: SearchSnippetGetRequest,
    ) -> jmap::Result<SearchSnippetGetResponse> {
        let account_id = request.account_id.get_document_id();
        let email_ids = request.email_ids.unwrap_value().unwrap_or_default();
        let acl = request.acl.unwrap();

        let mut terms = Vec::new();

        let mut list = Vec::with_capacity(email_ids.len());
        let mut not_found = Vec::new();

        // Fetch document ids
        let document_ids = if acl.is_member(account_id) {
            Arc::new(self.get_document_ids(account_id, Collection::Mail)?)
        } else {
            self.mail_shared_messages(account_id, &acl.member_of, ACL::ReadItems)?
        };

        // Obtain text terms
        if let Some(filter) = request.filter {
            let mut state = match filter {
                query::Filter::FilterOperator(op) => QueryState {
                    op: op.operator.into(),
                    it: op.conditions.into_iter(),
                },
                condition => QueryState {
                    op: LogicalOperator::And,
                    it: vec![condition].into_iter(),
                },
            };

            let mut state_stack = Vec::new();

            'outer: loop {
                while let Some(term) = state.it.next() {
                    match term {
                        query::Filter::FilterOperator(op) => {
                            state_stack.push(state);
                            state = QueryState {
                                op: op.operator.into(),
                                it: op.conditions.into_iter(),
                            };
                        }
                        query::Filter::FilterCondition(
                            super::schema::Filter::Text { value }
                            | super::schema::Filter::Subject { value }
                            | super::schema::Filter::Body { value },
                        ) => {
                            let mut include_term = true;
                            for state in &state_stack {
                                if state.op == LogicalOperator::Not {
                                    include_term = !include_term;
                                }
                            }
                            if state.op == LogicalOperator::Not {
                                include_term = !include_term;
                            }
                            if include_term {
                                terms.push(value);
                            }
                        }
                        _ => (),
                    }
                }

                if let Some(prev_state) = state_stack.pop() {
                    state = prev_state;
                } else {
                    break 'outer;
                }
            }
        }

        for email_id in email_ids {
            let document_id = email_id.get_document_id();
            if document_ids
                .as_ref()
                .as_ref()
                .map_or(true, |b| !b.contains(document_id))
            {
                not_found.push(email_id);
                continue;
            }

            if terms.is_empty() {
                list.push(SearchSnippet::empty(email_id));
                continue;
            }

            // Fetch message data
            let message_data = MessageData::from_metadata(
                &self
                    .blob_get(
                        &self
                            .get_document_value::<BlobId>(
                                account_id,
                                Collection::Mail,
                                document_id,
                                MessageField::Metadata.into(),
                            )?
                            .ok_or(StoreError::DataCorruption)?,
                    )?
                    .ok_or(StoreError::DataCorruption)?,
            )
            .ok_or(StoreError::DataCorruption)?;

            // Fetch term index
            let term_index = self
                .get_term_index(account_id, Collection::Mail, document_id)?
                .ok_or_else(|| {
                    StoreError::InternalError(format!(
                        "Term index not found for email {}/{}",
                        account_id, document_id
                    ))
                })?;
            let mut match_terms = Vec::new();
            let mut match_phrase = false;

            // Tokenize and stem terms
            for term in &terms {
                let language = Language::English; //TODO detect
                let match_phrase_term = (term.starts_with('"') && term.ends_with('"'))
                    || (term.starts_with('\'') && term.ends_with('\''));
                if !match_phrase_term {
                    for token in Stemmer::new(term, language, MAX_TOKEN_LENGTH) {
                        match_terms.push(term_index.get_match_term(
                            token.word.as_ref(),
                            token.stemmed_word.as_ref().map(|w| w.as_ref()),
                        ));
                    }
                } else {
                    match_phrase = true;
                    for token in Tokenizer::new(term, language, MAX_TOKEN_LENGTH) {
                        match_terms.push(term_index.get_match_term(token.word.as_ref(), None));
                    }
                }
            }

            let mut subject = None;
            let mut preview = None;

            for term_group in term_index
                .match_terms(&match_terms, None, match_phrase, true, true)
                .map_err(|err| match err {
                    store::nlp::term_index::Error::InvalidArgument => {
                        MethodError::UnsupportedFilter("Too many search terms.".to_string())
                    }
                    err => {
                        error!("Failed to generate search snippet: {:?}", err);
                        MethodError::UnsupportedFilter(
                            "Failed to generate search snippet.".to_string(),
                        )
                    }
                })?
                .unwrap_or_default()
            {
                if term_group.part_id == 0 {
                    // Generate subject snippent
                    subject = generate_snippet(
                        &term_group.terms,
                        message_data
                            .headers
                            .get(&RfcHeader::Subject)
                            .and_then(|value| value.last())
                            .and_then(|value| value.as_text())
                            .unwrap_or(""),
                    );
                } else if term_group.part_id < message_data.mime_parts.len() as u32 {
                    // Generate snippet of a body part
                    let part = &message_data.mime_parts[term_group.part_id as usize];

                    if let Some(blob_id) = &part.blob_id {
                        let mut text = String::from_utf8(
                            self.blob_get(blob_id)?.ok_or(StoreError::DataCorruption)?,
                        )
                        .map_or_else(
                            |err| String::from_utf8_lossy(err.as_bytes()).into_owned(),
                            |s| s,
                        );
                        if part.mime_type == MimePartType::Html {
                            text = html_to_text(&text);
                        }
                        preview = generate_snippet(&term_group.terms, &text);
                    } else {
                        error!(
                            "Corrupted term index for email {}/{}: MIME part does not contain a blob.",
                            account_id, document_id
                        );
                    }
                } else {
                    // Generate snippet of an attached email subpart
                    let part_id = term_group.part_id >> 16;
                    let subpart_id = term_group.part_id & (u16::MAX as u32);

                    if part_id < message_data.mime_parts.len() as u32 {
                        if let Some(blob_id) = &message_data.mime_parts[part_id as usize].blob_id {
                            let blob = self.blob_get(blob_id)?.ok_or(StoreError::DataCorruption)?;
                            let message =
                                Message::parse(&blob).ok_or(StoreError::DataCorruption)?;
                            if subpart_id == 0 {
                                preview = generate_snippet(
                                    &term_group.terms,
                                    message.get_subject().unwrap_or(""),
                                );
                            } else if let Some(bytes) =
                                get_message_part(message, subpart_id - 1, true)
                            {
                                preview = generate_snippet(
                                    &term_group.terms,
                                    &String::from_utf8_lossy(&bytes),
                                );
                            } else {
                                error!(
                                    "Corrupted term index for email {}/{}: Could not find subpart {}/{}.",
                                    account_id, document_id, part_id, subpart_id
                                );
                            }
                        } else {
                            //TODO errors are not displayed from components (not just here)
                            error!(
                                "Corrupted term index for email {}/{}: Could not find message attachment {}.",
                                account_id, document_id, part_id
                            );
                        }
                    }
                }

                if preview.is_some() {
                    break;
                }
            }

            list.push(SearchSnippet {
                email_id,
                subject,
                preview,
            });
        }

        Ok(SearchSnippetGetResponse {
            account_id: request.account_id,
            list,
            not_found: if !not_found.is_empty() {
                not_found.into()
            } else {
                None
            },
        })
    }
}
