use std::collections::HashMap;

use jmap_store::{
    json::JSONValue, local_store::JMAPLocalStore, JMAPError, JMAPGet, JMAPGetResponse, JMAPId,
    JMAPIdSerialize, JMAP_MAIL,
};
use mail_parser::{HeaderName, RfcHeaders};
use store::{AccountId, DocumentId, Store, StoreError};

use crate::{
    import::bincode_deserialize, JMAPMailHeaderForm, JMAPMailHeaderProperty, JMAPMailHeaders,
    JMAPMailIdImpl, JMAPMailProperties, JMAPMailStoreGet, JMAPMailStoreGetArguments, MessageField,
    MESSAGE_HEADERS,
};

//TODO make parameter configurable
const MAX_RESULTS: usize = 100;

impl<'x, T> JMAPMailStoreGet<'x> for JMAPLocalStore<T>
where
    T: Store<'x>,
{
    fn get_headers_rfc(
        &'x self,
        account: AccountId,
        document: DocumentId,
    ) -> jmap_store::Result<RfcHeaders> {
        bincode::deserialize(
            &self
                .store
                .get_document_value::<Vec<u8>>(
                    account,
                    JMAP_MAIL,
                    document,
                    MessageField::Internal.into(),
                    crate::MESSAGE_HEADERS,
                )?
                .ok_or_else(|| {
                    JMAPError::InternalError(StoreError::InternalError(format!(
                        "Headers for doc_id {} not found",
                        document
                    )))
                })?,
        )
        .map_err(|e| JMAPError::InternalError(StoreError::InternalError(e.to_string())))
        // TODO all errors have to include more info about context
    }

    fn mail_get(
        &self,
        request: JMAPGet<JMAPMailProperties<'x>>,
        arguments: JMAPMailStoreGetArguments,
    ) -> jmap_store::Result<jmap_store::JMAPGetResponse<'x, JMAPMailProperties<'x>>> {
        let mut rfc_headers = Vec::new();
        let mut other_headers = Vec::new();
        let mut raw_headers = Vec::new();
        let mut message_parts = Vec::new();
        let mut other_parts = Vec::new();

        let properties = request.properties.unwrap_or_else(|| {
            vec![
                JMAPMailProperties::Id,
                JMAPMailProperties::BlobId,
                JMAPMailProperties::ThreadId,
                JMAPMailProperties::MailboxIds,
                JMAPMailProperties::Keywords,
                JMAPMailProperties::Size,
                JMAPMailProperties::ReceivedAt,
                JMAPMailProperties::MessageId,
                JMAPMailProperties::InReplyTo,
                JMAPMailProperties::References,
                JMAPMailProperties::Sender,
                JMAPMailProperties::From,
                JMAPMailProperties::To,
                JMAPMailProperties::Cc,
                JMAPMailProperties::Bcc,
                JMAPMailProperties::ReplyTo,
                JMAPMailProperties::Subject,
                JMAPMailProperties::SentAt,
                JMAPMailProperties::HasAttachment,
                JMAPMailProperties::Preview,
                JMAPMailProperties::BodyValues,
                JMAPMailProperties::TextBody,
                JMAPMailProperties::HtmlBody,
                JMAPMailProperties::Attachments,
            ]
        });

        for property in properties {
            match property {
                JMAPMailProperties::Id
                | JMAPMailProperties::BlobId
                | JMAPMailProperties::ThreadId
                | JMAPMailProperties::MailboxIds
                | JMAPMailProperties::Keywords
                | JMAPMailProperties::BodyStructure => {
                    other_parts.push(property);
                }

                JMAPMailProperties::HasAttachment
                | JMAPMailProperties::Attachments
                | JMAPMailProperties::Preview
                | JMAPMailProperties::BodyValues
                | JMAPMailProperties::TextBody
                | JMAPMailProperties::HtmlBody
                | JMAPMailProperties::Size
                | JMAPMailProperties::ReceivedAt => {
                    message_parts.push(property);
                }

                JMAPMailProperties::RfcHeader(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    ..
                })
                | JMAPMailProperties::OtherHeader(JMAPMailHeaderProperty {
                    form: JMAPMailHeaderForm::Raw,
                    ..
                }) => {
                    raw_headers.push(property);
                }

                JMAPMailProperties::MessageId => {
                    rfc_headers.push(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::MessageIds,
                        header: HeaderName::MessageId,
                        all: false,
                    });
                }
                JMAPMailProperties::InReplyTo => {
                    rfc_headers.push(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::MessageIds,
                        header: HeaderName::InReplyTo,
                        all: false,
                    });
                }
                JMAPMailProperties::References => {
                    rfc_headers.push(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::MessageIds,
                        header: HeaderName::References,
                        all: false,
                    });
                }
                JMAPMailProperties::Sender => {
                    rfc_headers.push(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::Addresses,
                        header: HeaderName::Sender,
                        all: false,
                    });
                }
                JMAPMailProperties::From => {
                    rfc_headers.push(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::Addresses,
                        header: HeaderName::From,
                        all: false,
                    });
                }
                JMAPMailProperties::To => {
                    rfc_headers.push(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::Addresses,
                        header: HeaderName::To,
                        all: false,
                    });
                }
                JMAPMailProperties::Cc => {
                    rfc_headers.push(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::Addresses,
                        header: HeaderName::Cc,
                        all: false,
                    });
                }
                JMAPMailProperties::Bcc => {
                    rfc_headers.push(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::Addresses,
                        header: HeaderName::Bcc,
                        all: false,
                    });
                }
                JMAPMailProperties::ReplyTo => {
                    rfc_headers.push(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::Addresses,
                        header: HeaderName::ReplyTo,
                        all: false,
                    });
                }
                JMAPMailProperties::Subject => {
                    rfc_headers.push(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::Text,
                        header: HeaderName::Subject,
                        all: false,
                    });
                }
                JMAPMailProperties::SentAt => {
                    rfc_headers.push(JMAPMailHeaderProperty {
                        form: JMAPMailHeaderForm::Date,
                        header: HeaderName::Date,
                        all: false,
                    });
                }
                JMAPMailProperties::RfcHeader(rfc_header) => {
                    rfc_headers.push(rfc_header);
                }

                JMAPMailProperties::OtherHeader(_) => {
                    other_headers.push(property);
                }

                // Ignore, used internally
                JMAPMailProperties::Name
                | JMAPMailProperties::Email
                | JMAPMailProperties::Addresses => (),
            }
        }

        let request_ids = if let Some(request_ids) = request.ids {
            if request_ids.len() > MAX_RESULTS {
                return Err(JMAPError::RequestTooLarge);
            } else {
                request_ids
            }
        } else {
            let document_ids = self
                .store
                .get_document_ids(request.account_id, JMAP_MAIL)?
                .into_iter()
                .take(MAX_RESULTS)
                .collect::<Vec<DocumentId>>();
            if !document_ids.is_empty() {
                self.store
                    .get_multi_document_value(
                        request.account_id,
                        JMAP_MAIL,
                        document_ids.iter().copied(),
                        MessageField::ThreadId.into(),
                    )?
                    .into_iter()
                    .zip(document_ids)
                    .filter_map(|(thread_id, document_id)| {
                        JMAPId::from_email(thread_id?, document_id).into()
                    })
                    .collect::<Vec<u64>>()
            } else {
                Vec::new()
            }
        };

        let mut not_found = Vec::new();
        let mut results = Vec::with_capacity(request_ids.len());

        for jmap_id in request_ids {
            let mut result = HashMap::new();

            if !rfc_headers.is_empty() {
                let mut jmap_headers = if let Some(jmap_headers) =
                    self.store.get_document_value::<Vec<u8>>(
                        request.account_id,
                        JMAP_MAIL,
                        jmap_id.get_document_id(),
                        MessageField::Internal.into(),
                        MESSAGE_HEADERS,
                    )? {
                    bincode_deserialize::<JMAPMailHeaders>(&jmap_headers)?
                } else {
                    not_found.push(jmap_id);
                    continue;
                };

                for rfc_header in &rfc_headers {
                    let value = match rfc_header {
                        JMAPMailHeaderProperty {
                            header: header @ (HeaderName::Date | HeaderName::ResentDate),
                            form: JMAPMailHeaderForm::Date,
                            all,
                        }
                        | JMAPMailHeaderProperty {
                            header:
                                header
                                @
                                (HeaderName::Subject
                                | HeaderName::Comments
                                | HeaderName::Keywords
                                | HeaderName::ListId),
                            form: JMAPMailHeaderForm::Text,
                            all,
                        } => match jmap_headers.remove(header).unwrap_or_default() {
                            JSONValue::Array(mut list) => {
                                if !*all {
                                    list.pop().unwrap_or_default()
                                } else {
                                    JSONValue::Array(list)
                                }
                            }
                            value @ JSONValue::String(_) => {
                                if !*all {
                                    value
                                } else {
                                    JSONValue::Array(vec![value])
                                }
                            }
                            _ => JSONValue::Null,
                        },
                        JMAPMailHeaderProperty {
                            header:
                                header
                                @
                                (HeaderName::MessageId
                                | HeaderName::References
                                | HeaderName::ResentMessageId
                                | HeaderName::InReplyTo),
                            form: JMAPMailHeaderForm::MessageIds,
                            all,
                        }
                        | JMAPMailHeaderProperty {
                            header:
                                header
                                @
                                (HeaderName::ListArchive
                                | HeaderName::ListHelp
                                | HeaderName::ListOwner
                                | HeaderName::ListPost
                                | HeaderName::ListSubscribe
                                | HeaderName::ListUnsubscribe),
                            form: JMAPMailHeaderForm::URLs,
                            all,
                        } => {
                            if let JSONValue::Array(mut list) =
                                jmap_headers.remove(header).unwrap_or_default()
                            {
                                let is_collection =
                                    matches!(list.get(0), Some(JSONValue::Array(_)));
                                if !all {
                                    if !is_collection {
                                        JSONValue::Array(list)
                                    } else {
                                        list.pop().unwrap_or_default()
                                    }
                                } else if is_collection {
                                    JSONValue::Array(list)
                                } else {
                                    JSONValue::Array(vec![JSONValue::Array(list)])
                                }
                            } else {
                                JSONValue::Null
                            }
                        }
                        JMAPMailHeaderProperty {
                            header:
                                header
                                @
                                (HeaderName::From
                                | HeaderName::To
                                | HeaderName::Cc
                                | HeaderName::Bcc
                                | HeaderName::ReplyTo
                                | HeaderName::Sender
                                | HeaderName::ResentTo
                                | HeaderName::ResentFrom
                                | HeaderName::ResentBcc
                                | HeaderName::ResentCc
                                | HeaderName::ResentSender),
                            form:
                                form
                                @
                                (JMAPMailHeaderForm::Addresses
                                | JMAPMailHeaderForm::GroupedAddresses),
                            all,
                        } => {
                            if let JSONValue::Array(mut list) =
                                jmap_headers.remove(header).unwrap_or_default()
                            {
                                let grouped = matches!(form, JMAPMailHeaderForm::GroupedAddresses);
                                let (is_collection, is_grouped) = match list.get(0) {
                                    Some(JSONValue::Array(list)) => (
                                        true,
                                        matches!(list.get(0), Some(JSONValue::Properties(obj)) if obj.contains_key(&JMAPMailProperties::Addresses)),
                                    ),
                                    Some(JSONValue::Properties(obj)) => {
                                        (false, obj.contains_key(&JMAPMailProperties::Addresses))
                                    }
                                    _ => (false, false),
                                };

                                if ((grouped && is_grouped) || (!grouped && !is_grouped))
                                    && ((is_collection && *all) || (!is_collection && !*all))
                                {
                                    JSONValue::Array(list)
                                } else if (grouped && is_grouped) || (!grouped && !is_grouped) {
                                    if *all && !is_collection {
                                        JSONValue::Array(vec![JSONValue::Array(list)])
                                    } else {
                                        // !*all && is_collection
                                        list.pop().unwrap_or_default()
                                    }
                                } else {
                                    let list = if *all && !is_collection {
                                        vec![JSONValue::Array(list)]
                                    } else if !*all && is_collection {
                                        if let JSONValue::Array(list) =
                                            list.pop().unwrap_or_default()
                                        {
                                            list
                                        } else {
                                            vec![]
                                        }
                                    } else {
                                        list
                                    };

                                    if grouped && !is_grouped {
                                        let list_to_group = |list: JSONValue<'x, JMAPMailProperties<'x>>| -> JSONValue<'x, JMAPMailProperties<'x>> {
                                            let mut group = HashMap::new();
                                            group.insert(JMAPMailProperties::Name, JSONValue::Null);
                                            group.insert(JMAPMailProperties::Addresses, list);
                                            JSONValue::Properties(group)
                                        };
                                        JSONValue::Array(if !*all {
                                            list.into_iter().map(list_to_group).collect()
                                        } else {
                                            list.into_iter()
                                                .map(|field| {
                                                    if let JSONValue::Array(list) = field {
                                                        JSONValue::Array(
                                                            list.into_iter()
                                                                .map(list_to_group)
                                                                .collect(),
                                                        )
                                                    } else {
                                                        field
                                                    }
                                                })
                                                .collect()
                                        })
                                    } else {
                                        // !grouped && is_grouped
                                        let flatten_group = |list: Vec<JSONValue<'x, JMAPMailProperties<'x>>>| -> Vec<JSONValue<'x, JMAPMailProperties<'x>>> {
                                            let mut addresses = Vec::with_capacity(list.len() * 2);
                                            for group in list {
                                                if let JSONValue::Properties(mut group) = group {
                                                    if let Some(JSONValue::Array(mut group_addresses)) = group.remove(&JMAPMailProperties::Addresses) {
                                                        addresses.append(&mut group_addresses);
                                                    }
                                                }
                                            }
                                            addresses
                                        };
                                        JSONValue::Array(if !*all {
                                            flatten_group(list)
                                        } else {
                                            list.into_iter()
                                                .map(|field| {
                                                    if let JSONValue::Array(list) = field {
                                                        JSONValue::Array(flatten_group(list))
                                                    } else {
                                                        field
                                                    }
                                                })
                                                .collect()
                                        })
                                    }
                                }
                            } else {
                                JSONValue::Null
                            }
                        }
                        _ => return Err(JMAPError::InvalidArguments),
                    };

                    if !matches!(value, JSONValue::Null) {
                        result.insert(JMAPMailProperties::RfcHeader(rfc_header.clone()), value);
                    }
                }

                result.insert(
                    JMAPMailProperties::Id,
                    JSONValue::String(jmap_id.to_jmap_string().into()),
                );
                results.push(JSONValue::Properties(result));
            }
        }

        Ok(JMAPGetResponse {
            state: self.get_state(request.account_id, JMAP_MAIL)?,
            list: if !results.is_empty() {
                JSONValue::Array(results)
            } else {
                JSONValue::Null
            },
            not_found: if not_found.is_empty() {
                None
            } else {
                not_found.into()
            },
        })
    }
}
