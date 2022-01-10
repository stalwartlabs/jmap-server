use std::collections::HashMap;

use jmap_store::{
    json::JSONValue, JMAPError, JMAPGet, JMAPGetResponse, JMAPId, JMAPIdSerialize, JMAP_MAIL,
};
use mail_parser::{
    parsers::{
        fields::{
            address::parse_address, date::parse_date, id::parse_id,
            unstructured::parse_unstructured,
        },
        message::MessageStream,
    },
    HeaderName, HeaderOffsetName, HeaderValue,
};
use store::{BlobEntry, DocumentId, Store};

use crate::{
    changes::JMAPMailLocalStoreChanges,
    import::bincode_deserialize,
    parse::{
        header_to_jmap_address, header_to_jmap_date, header_to_jmap_id, header_to_jmap_text,
        header_to_jmap_url,
    },
    JMAPMailHeaderForm, JMAPMailHeaderProperty, JMAPMailHeaders, JMAPMailIdImpl,
    JMAPMailProperties, JMAPMailStoreGetArguments, MessageField, MessageRawHeaders,
    MESSAGE_HEADERS, MESSAGE_HEADERS_RAW,
};

pub trait JMAPMailLocalStoreGet<'x>: JMAPMailLocalStoreChanges<'x> + Store<'x> {
    fn mail_get(
        &self,
        request: JMAPGet<JMAPMailProperties<'x>>,
        arguments: JMAPMailStoreGetArguments,
    ) -> jmap_store::Result<jmap_store::JMAPGetResponse<'x, JMAPMailProperties<'x>>> {
        let mut rfc_headers = Vec::new();
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
                    header,
                    all,
                }) => {
                    raw_headers.push((HeaderOffsetName::Rfc(header), JMAPMailHeaderForm::Raw, all));
                }
                JMAPMailProperties::OtherHeader(header) => {
                    raw_headers.push((
                        HeaderOffsetName::Other(header.header),
                        header.form,
                        header.all,
                    ));
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

                // Ignore, used internally
                JMAPMailProperties::Name
                | JMAPMailProperties::Email
                | JMAPMailProperties::Addresses => (),
            }
        }

        let request_ids = if let Some(request_ids) = request.ids {
            if request_ids.len() > self.get_config().jmap_mail_options.get_max_results {
                return Err(JMAPError::RequestTooLarge);
            } else {
                request_ids
            }
        } else {
            let document_ids = self
                .get_document_ids(request.account_id, JMAP_MAIL)?
                .into_iter()
                .take(self.get_config().jmap_mail_options.get_max_results)
                .collect::<Vec<DocumentId>>();
            if !document_ids.is_empty() {
                self.get_multi_document_value(
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
                let mut jmap_headers = bincode_deserialize::<JMAPMailHeaders>(
                    &self
                        .get_document_blob_entry(
                            request.account_id,
                            JMAP_MAIL,
                            jmap_id.get_document_id(),
                            BlobEntry::new(MESSAGE_HEADERS),
                        )?
                        .value,
                )?;

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
                        } => transform_json_string(
                            jmap_headers.remove(header).unwrap_or_default(),
                            *all,
                        ),
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
                        } => transform_json_stringlist(
                            jmap_headers.remove(header).unwrap_or_default(),
                            *all,
                        ),
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
                        } => transform_json_emailaddress(
                            jmap_headers.remove(header).unwrap_or_default(),
                            matches!(form, JMAPMailHeaderForm::GroupedAddresses),
                            *all,
                        ),
                        _ => return Err(JMAPError::InvalidArguments),
                    };

                    if !matches!(value, JSONValue::Null) {
                        result.insert(JMAPMailProperties::RfcHeader(rfc_header.clone()), value);
                    }
                }
            }

            if !raw_headers.is_empty() {
                let mut jmap_headers = bincode_deserialize::<MessageRawHeaders>(
                    &self
                        .get_document_blob_entry(
                            request.account_id,
                            JMAP_MAIL,
                            jmap_id.get_document_id(),
                            BlobEntry::new(MESSAGE_HEADERS_RAW),
                        )?
                        .value,
                )?;

                let header_bytes: Vec<u8> = Vec::new();

                for (header_name, form, all) in &raw_headers {
                    result.insert(
                        match header_name {
                            HeaderOffsetName::Rfc(rfc_header) => {
                                JMAPMailProperties::RfcHeader(JMAPMailHeaderProperty {
                                    form: form.clone(),
                                    header: *rfc_header,
                                    all: *all,
                                })
                            }
                            HeaderOffsetName::Other(other_header) => {
                                JMAPMailProperties::OtherHeader(JMAPMailHeaderProperty {
                                    form: form.clone(),
                                    header: other_header.clone(),
                                    all: *all,
                                })
                            }
                        },
                        if let Some(offsets) = jmap_headers.headers.remove(header_name) {
                            let mut header_values: Vec<HeaderValue> = offsets
                                .iter()
                                .skip(if !*all && offsets.len() > 1 {
                                    offsets.len() - 1
                                } else {
                                    0
                                })
                                .map(|offset| {
                                    (header_bytes.get(offset.start..offset.end).map_or(
                                        HeaderValue::Empty,
                                        |bytes| match form {
                                            JMAPMailHeaderForm::Raw => HeaderValue::Text(
                                                std::str::from_utf8(bytes).map_or_else(
                                                    |_| {
                                                        String::from_utf8_lossy(bytes)
                                                            .trim()
                                                            .to_string()
                                                            .into()
                                                    },
                                                    |str| str.trim().to_string().into(),
                                                ),
                                            ),
                                            JMAPMailHeaderForm::Text => {
                                                parse_unstructured(&mut MessageStream::new(bytes))
                                            }
                                            JMAPMailHeaderForm::Addresses => {
                                                parse_address(&mut MessageStream::new(bytes))
                                            }
                                            JMAPMailHeaderForm::GroupedAddresses => {
                                                parse_address(&mut MessageStream::new(bytes))
                                            }
                                            JMAPMailHeaderForm::MessageIds => {
                                                parse_id(&mut MessageStream::new(bytes))
                                            }
                                            JMAPMailHeaderForm::Date => {
                                                parse_date(&mut MessageStream::new(bytes))
                                            }
                                            JMAPMailHeaderForm::URLs => {
                                                parse_address(&mut MessageStream::new(bytes))
                                            }
                                        },
                                    ))
                                    .into_owned()
                                })
                                .collect();
                            let header_values = if *all {
                                HeaderValue::Collection(header_values)
                            } else {
                                header_values.pop().unwrap_or_default()
                            };
                            match form {
                                JMAPMailHeaderForm::Raw | JMAPMailHeaderForm::Text => {
                                    header_to_jmap_text(header_values)
                                }
                                JMAPMailHeaderForm::Addresses => transform_json_emailaddress(
                                    header_to_jmap_address(header_values, false),
                                    false,
                                    *all,
                                ),
                                JMAPMailHeaderForm::GroupedAddresses => {
                                    transform_json_emailaddress(
                                        header_to_jmap_address(header_values, false),
                                        true,
                                        *all,
                                    )
                                }
                                JMAPMailHeaderForm::MessageIds => header_to_jmap_id(header_values),
                                JMAPMailHeaderForm::Date => header_to_jmap_date(header_values),
                                JMAPMailHeaderForm::URLs => header_to_jmap_url(header_values),
                            }
                        } else {
                            JSONValue::Null
                        },
                    );
                }
            }

            result.insert(
                JMAPMailProperties::Id,
                JSONValue::String(jmap_id.to_jmap_string().into()),
            );
            results.push(JSONValue::Properties(result));
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

pub fn transform_json_emailaddress<'x>(
    value: JSONValue<'x, JMAPMailProperties<'x>>,
    as_grouped: bool,
    as_collection: bool,
) -> JSONValue<'x, JMAPMailProperties<'x>> {
    if let JSONValue::Array(mut list) = value {
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

        if ((as_grouped && is_grouped) || (!as_grouped && !is_grouped))
            && ((is_collection && as_collection) || (!is_collection && !as_collection))
        {
            JSONValue::Array(list)
        } else if (as_grouped && is_grouped) || (!as_grouped && !is_grouped) {
            if as_collection && !is_collection {
                JSONValue::Array(vec![JSONValue::Array(list)])
            } else {
                // !as_collection && is_collection
                list.pop().unwrap_or_default()
            }
        } else {
            let mut list = if as_collection && !is_collection {
                vec![JSONValue::Array(list)]
            } else if !as_collection && is_collection {
                if let JSONValue::Array(list) = list.pop().unwrap_or_default() {
                    list
                } else {
                    vec![]
                }
            } else {
                list
            };

            if as_grouped && !is_grouped {
                let list_to_group = |list: Vec<JSONValue<'x, JMAPMailProperties<'x>>>| -> JSONValue<'x, JMAPMailProperties<'x>> {
                let mut group = HashMap::new();
                group.insert(JMAPMailProperties::Name, JSONValue::Null);
                group.insert(JMAPMailProperties::Addresses, JSONValue::Array(list));
                JSONValue::Properties(group)
            };
                JSONValue::Array(if !as_collection {
                    vec![list_to_group(list)]
                } else {
                    list.iter_mut().for_each(|field| {
                        if let JSONValue::Array(list) = field {
                            *field = JSONValue::Array(vec![list_to_group(std::mem::take(list))]);
                        }
                    });
                    list
                })
            } else {
                // !as_grouped && is_grouped
                let flatten_group = |list: Vec<JSONValue<'x, JMAPMailProperties<'x>>>| -> Vec<JSONValue<'x, JMAPMailProperties<'x>>> {
                let mut addresses = Vec::with_capacity(list.len() * 2);
                list.into_iter().for_each(|group| {
                    if let JSONValue::Properties(mut group) = group {
                        if let Some(JSONValue::Array(mut group_addresses)) = group.remove(&JMAPMailProperties::Addresses) {
                            addresses.append(&mut group_addresses);
                        }
                    }
                });
                addresses
            };
                JSONValue::Array(if !as_collection {
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

pub fn transform_json_stringlist<'x>(
    value: JSONValue<'x, JMAPMailProperties<'x>>,
    as_collection: bool,
) -> JSONValue<'x, JMAPMailProperties<'x>> {
    if let JSONValue::Array(mut list) = value {
        let is_collection = matches!(list.get(0), Some(JSONValue::Array(_)));
        if !as_collection {
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

pub fn transform_json_string<'x>(
    value: JSONValue<'x, JMAPMailProperties<'x>>,
    as_collection: bool,
) -> JSONValue<'x, JMAPMailProperties<'x>> {
    match value {
        JSONValue::Array(mut list) => {
            if !as_collection {
                list.pop().unwrap_or_default()
            } else {
                JSONValue::Array(list)
            }
        }
        value @ JSONValue::String(_) => {
            if !as_collection {
                value
            } else {
                JSONValue::Array(vec![value])
            }
        }
        _ => JSONValue::Null,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use jmap_store::json::JSONValue;

    use crate::JMAPMailProperties;

    #[test]
    fn test_json_transform() {
        for (value, expected_result, expected_result_all) in [
            (
                JSONValue::String("hello".into()),
                JSONValue::String("hello".into()),
                JSONValue::Array::<JMAPMailProperties>(vec![JSONValue::String("hello".into())]),
            ),
            (
                JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
                JSONValue::String("world".into()),
                JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
            ),
        ] {
            assert_eq!(
                super::transform_json_string(value.clone(), false),
                expected_result
            );
            assert_eq!(
                super::transform_json_string(value, true),
                expected_result_all
            );
        }

        for (value, expected_result, expected_result_all) in [
            (
                JSONValue::Array::<JMAPMailProperties>(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
                JSONValue::Array::<JMAPMailProperties>(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ]),
                JSONValue::Array::<JMAPMailProperties>(vec![JSONValue::Array(vec![
                    JSONValue::String("hello".into()),
                    JSONValue::String("world".into()),
                ])]),
            ),
            (
                JSONValue::Array(vec![
                    JSONValue::Array::<JMAPMailProperties>(vec![
                        JSONValue::String("hello".into()),
                        JSONValue::String("world".into()),
                    ]),
                    JSONValue::Array::<JMAPMailProperties>(vec![
                        JSONValue::String("hola".into()),
                        JSONValue::String("mundo".into()),
                    ]),
                ]),
                JSONValue::Array::<JMAPMailProperties>(vec![
                    JSONValue::String("hola".into()),
                    JSONValue::String("mundo".into()),
                ]),
                JSONValue::Array(vec![
                    JSONValue::Array::<JMAPMailProperties>(vec![
                        JSONValue::String("hello".into()),
                        JSONValue::String("world".into()),
                    ]),
                    JSONValue::Array::<JMAPMailProperties>(vec![
                        JSONValue::String("hola".into()),
                        JSONValue::String("mundo".into()),
                    ]),
                ]),
            ),
        ] {
            assert_eq!(
                super::transform_json_stringlist(value.clone(), false),
                expected_result
            );
            assert_eq!(
                super::transform_json_stringlist(value, true),
                expected_result_all
            );
        }

        fn make_email<'x>(name: &str, addr: &str) -> JSONValue<'x, JMAPMailProperties<'x>> {
            let mut email = HashMap::new();
            email.insert(
                JMAPMailProperties::Name,
                JSONValue::String(name.to_string().into()),
            );
            email.insert(
                JMAPMailProperties::Email,
                JSONValue::String(addr.to_string().into()),
            );
            JSONValue::Properties(email)
        }

        fn make_group<'x>(
            name: Option<&str>,
            addresses: JSONValue<'x, JMAPMailProperties<'x>>,
        ) -> JSONValue<'x, JMAPMailProperties<'x>> {
            let mut email = HashMap::new();
            email.insert(
                JMAPMailProperties::Name,
                name.map_or(JSONValue::Null, |name| {
                    JSONValue::String(name.to_string().into())
                }),
            );
            email.insert(JMAPMailProperties::Addresses, addresses);
            JSONValue::Properties(email)
        }

        fn make_list<'x>(
            value1: JSONValue<'x, JMAPMailProperties<'x>>,
            value2: JSONValue<'x, JMAPMailProperties<'x>>,
        ) -> JSONValue<'x, JMAPMailProperties<'x>> {
            JSONValue::Array(vec![value1, value2])
        }

        fn make_list_many<'x>(
            value1: JSONValue<'x, JMAPMailProperties<'x>>,
            value2: JSONValue<'x, JMAPMailProperties<'x>>,
            value3: JSONValue<'x, JMAPMailProperties<'x>>,
            value4: JSONValue<'x, JMAPMailProperties<'x>>,
        ) -> JSONValue<'x, JMAPMailProperties<'x>> {
            JSONValue::Array(vec![value1, value2, value3, value4])
        }

        fn make_list_single<'x>(
            value: JSONValue<'x, JMAPMailProperties<'x>>,
        ) -> JSONValue<'x, JMAPMailProperties<'x>> {
            JSONValue::Array(vec![value])
        }

        for (
            value,
            expected_result_single_addr,
            expected_result_all_addr,
            expected_result_single_group,
            expected_result_all_group,
        ) in [
            (
                make_list(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                ),
                make_list(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                ),
                make_list_single(make_list(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                )),
                make_list_single(make_group(
                    None,
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                )),
                make_list_single(make_list_single(make_group(
                    None,
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                ))),
            ),
            (
                make_list(
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                    make_list(
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                ),
                make_list(
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                ),
                make_list(
                    make_list(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                    ),
                    make_list(
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                ),
                make_list_single(make_group(
                    None,
                    make_list(
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                )),
                make_list(
                    make_list_single(make_group(
                        None,
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    )),
                    make_list_single(make_group(
                        None,
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    )),
                ),
            ),
            (
                make_list(
                    make_group(
                        "Group 1".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 2".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                ),
                make_list_many(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                ),
                make_list_single(make_list_many(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                )),
                make_list(
                    make_group(
                        "Group 1".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 2".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                ),
                make_list_single(make_list(
                    make_group(
                        "Group 1".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 2".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                )),
            ),
            (
                make_list(
                    make_list(
                        make_group(
                            "Group 1".into(),
                            make_list(
                                make_email("Tim Hortons", "tim@hortos.com"),
                                make_email("Ronald McDowell", "ronnie@mac.com"),
                            ),
                        ),
                        make_group(
                            "Group 2".into(),
                            make_list(
                                make_email("Wendy D", "wendy@d.com"),
                                make_email("Kentucky Frango", "kentucky@frango.com"),
                            ),
                        ),
                    ),
                    make_list(
                        make_group(
                            "Group 3".into(),
                            make_list(
                                make_email("John Doe", "jdoe@domain.com"),
                                make_email("Jane Smith", "jsmith@test.com"),
                            ),
                        ),
                        make_group(
                            "Group 4".into(),
                            make_list(
                                make_email("Juan Gomez", "jgomez@dominio.com"),
                                make_email("Juanita Perez", "jperez@prueba.com"),
                            ),
                        ),
                    ),
                ),
                make_list_many(
                    make_email("John Doe", "jdoe@domain.com"),
                    make_email("Jane Smith", "jsmith@test.com"),
                    make_email("Juan Gomez", "jgomez@dominio.com"),
                    make_email("Juanita Perez", "jperez@prueba.com"),
                ),
                make_list(
                    make_list_many(
                        make_email("Tim Hortons", "tim@hortos.com"),
                        make_email("Ronald McDowell", "ronnie@mac.com"),
                        make_email("Wendy D", "wendy@d.com"),
                        make_email("Kentucky Frango", "kentucky@frango.com"),
                    ),
                    make_list_many(
                        make_email("John Doe", "jdoe@domain.com"),
                        make_email("Jane Smith", "jsmith@test.com"),
                        make_email("Juan Gomez", "jgomez@dominio.com"),
                        make_email("Juanita Perez", "jperez@prueba.com"),
                    ),
                ),
                make_list(
                    make_group(
                        "Group 3".into(),
                        make_list(
                            make_email("John Doe", "jdoe@domain.com"),
                            make_email("Jane Smith", "jsmith@test.com"),
                        ),
                    ),
                    make_group(
                        "Group 4".into(),
                        make_list(
                            make_email("Juan Gomez", "jgomez@dominio.com"),
                            make_email("Juanita Perez", "jperez@prueba.com"),
                        ),
                    ),
                ),
                make_list(
                    make_list(
                        make_group(
                            "Group 1".into(),
                            make_list(
                                make_email("Tim Hortons", "tim@hortos.com"),
                                make_email("Ronald McDowell", "ronnie@mac.com"),
                            ),
                        ),
                        make_group(
                            "Group 2".into(),
                            make_list(
                                make_email("Wendy D", "wendy@d.com"),
                                make_email("Kentucky Frango", "kentucky@frango.com"),
                            ),
                        ),
                    ),
                    make_list(
                        make_group(
                            "Group 3".into(),
                            make_list(
                                make_email("John Doe", "jdoe@domain.com"),
                                make_email("Jane Smith", "jsmith@test.com"),
                            ),
                        ),
                        make_group(
                            "Group 4".into(),
                            make_list(
                                make_email("Juan Gomez", "jgomez@dominio.com"),
                                make_email("Juanita Perez", "jperez@prueba.com"),
                            ),
                        ),
                    ),
                ),
            ),
        ] {
            assert_eq!(
                super::transform_json_emailaddress(value.clone(), false, false),
                expected_result_single_addr,
                "single+address"
            );
            assert_eq!(
                super::transform_json_emailaddress(value.clone(), false, true),
                expected_result_all_addr,
                "all+address"
            );
            assert_eq!(
                super::transform_json_emailaddress(value.clone(), true, false),
                expected_result_single_group,
                "single+group"
            );
            assert_eq!(
                super::transform_json_emailaddress(value.clone(), true, true),
                expected_result_all_group,
                "all+group"
            );
        }
    }
}
