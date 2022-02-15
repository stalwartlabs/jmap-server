use std::{collections::HashMap, fs, path::PathBuf};

use jmap_mail::{
    get::JMAPMailStoreGetArguments,
    import::JMAPMailLocalStoreImport,
    parse::{get_message_blob, JMAPMailParseRequest},
    JMAPMailBodyProperties, JMAPMailGet, JMAPMailHeaderForm, JMAPMailHeaderProperty, JMAPMailParse,
    JMAPMailProperties,
};
use jmap_store::{
    blob::JMAPLocalBlobStore,
    id::{BlobId, JMAPIdSerialize},
    json::JSONValue,
    local_store::JMAPLocalStore,
    JMAPGet,
};
use mail_parser::{HeaderName, RfcHeader};
use store::Store;

use crate::jmap_mail_get::UntaggedJSONValue;

pub fn test_jmap_mail_parse<T>(mail_store: JMAPLocalStore<T>)
where
    T: for<'x> Store<'x>,
{
    let mut test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_dir.push("resources");
    test_dir.push("jmap_mail_parse");

    // Test parsing an email attachment
    for test_name in ["attachment.eml", "attachment_b64.eml"] {
        let mut test_file = test_dir.clone();
        test_file.push(test_name);

        let blob_id = BlobId::from_jmap_string(
            &mail_store
                .mail_get(
                    JMAPGet {
                        account_id: 0,
                        ids: vec![mail_store
                            .mail_import_blob(
                                0,
                                &fs::read(&test_file).unwrap(),
                                vec![],
                                vec![],
                                None,
                            )
                            .unwrap()
                            .unwrap_object()
                            .unwrap()
                            .get("id")
                            .unwrap()
                            .to_jmap_id()
                            .unwrap()]
                        .into(),
                        properties: vec![JMAPMailProperties::Attachments].into(),
                    },
                    JMAPMailStoreGetArguments {
                        body_properties: vec![JMAPMailBodyProperties::BlobId],
                        fetch_text_body_values: false,
                        fetch_html_body_values: false,
                        fetch_all_body_values: false,
                        max_body_value_bytes: 100,
                    },
                )
                .unwrap()
                .list
                .unwrap_array()
                .unwrap()
                .pop()
                .unwrap()
                .unwrap_object()
                .unwrap()
                .remove("attachments")
                .unwrap()
                .unwrap_array()
                .unwrap()
                .pop()
                .unwrap()
                .unwrap_object()
                .unwrap()
                .remove("blobId")
                .unwrap()
                .unwrap_string()
                .unwrap(),
        )
        .unwrap();

        let result = mail_store
            .mail_parse(JMAPMailParseRequest {
                account_id: 0,
                blob_ids: vec![blob_id.clone()],
                properties: vec![
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
                    JMAPMailProperties::BodyStructure,
                ],
                arguments: JMAPMailStoreGetArguments {
                    body_properties: vec![
                        JMAPMailBodyProperties::PartId,
                        JMAPMailBodyProperties::BlobId,
                        JMAPMailBodyProperties::Size,
                        JMAPMailBodyProperties::Name,
                        JMAPMailBodyProperties::Type,
                        JMAPMailBodyProperties::Charset,
                        JMAPMailBodyProperties::Headers,
                        JMAPMailBodyProperties::Disposition,
                        JMAPMailBodyProperties::Cid,
                        JMAPMailBodyProperties::Language,
                        JMAPMailBodyProperties::Location,
                    ],
                    fetch_text_body_values: true,
                    fetch_html_body_values: true,
                    fetch_all_body_values: true,
                    max_body_value_bytes: 100,
                },
            })
            .unwrap();

        assert_eq!(result.not_found, JSONValue::Null);
        assert_eq!(result.not_parsable, JSONValue::Null);

        let result = result
            .parsed
            .unwrap_object()
            .unwrap()
            .remove(&blob_id.to_jmap_string())
            .unwrap();

        for part_name in ["textBody", "htmlBody", "attachments"] {
            for part in result
                .to_object()
                .unwrap()
                .get(part_name)
                .unwrap()
                .to_array()
                .unwrap()
            {
                let part = part.to_object().unwrap();

                let inner_blob = mail_store
                    .download_blob(
                        0,
                        &BlobId::from_jmap_string(part.get("blobId").unwrap().to_string().unwrap())
                            .unwrap(),
                        get_message_blob,
                    )
                    .unwrap()
                    .unwrap();

                test_file.set_extension(format!(
                    "part{}",
                    part.get("partId").unwrap().to_number().unwrap()
                ));

                //fs::write(&test_file, inner_blob).unwrap();

                assert_eq!(inner_blob, fs::read(&test_file).unwrap());
            }
        }

        test_file.set_extension("json");

        /*fs::write(
            test_file,
            &serde_json::to_string_pretty(&UntaggedJSONValue::from(result)).unwrap(),
        )
        .unwrap();*/

        assert_eq!(
            UntaggedJSONValue::from(result),
            serde_json::from_slice::<UntaggedJSONValue>(&fs::read(&test_file).unwrap()).unwrap()
        );
    }

    // Test header parsing on a temporary blob
    let mut test_file = test_dir;
    test_file.push("headers.eml");
    let blob_id = mail_store
        .upload_blob(0, &fs::read(&test_file).unwrap())
        .unwrap();

    let mut properties = vec![
        JMAPMailProperties::Id,
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
        JMAPMailProperties::Preview,
        JMAPMailProperties::TextBody,
        JMAPMailProperties::HtmlBody,
        JMAPMailProperties::Attachments,
    ];

    for header in [
        HeaderName::Rfc(RfcHeader::From),
        HeaderName::Rfc(RfcHeader::To),
        HeaderName::Rfc(RfcHeader::Cc),
        HeaderName::Rfc(RfcHeader::Bcc),
        HeaderName::Other("X-Address-Single".into()),
        HeaderName::Other("X-Address".into()),
        HeaderName::Other("X-AddressList-Single".into()),
        HeaderName::Other("X-AddressList".into()),
        HeaderName::Other("X-AddressesGroup-Single".into()),
        HeaderName::Other("X-AddressesGroup".into()),
    ] {
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Addresses,
            header: header.clone(),
            all: true,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Addresses,
            header: header.clone(),
            all: false,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::GroupedAddresses,
            header: header.clone(),
            all: true,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::GroupedAddresses,
            header: header.clone(),
            all: false,
        }));
    }

    for header in [
        HeaderName::Rfc(RfcHeader::ListPost),
        HeaderName::Rfc(RfcHeader::ListSubscribe),
        HeaderName::Rfc(RfcHeader::ListUnsubscribe),
        HeaderName::Rfc(RfcHeader::ListOwner),
        HeaderName::Other("X-List-Single".into()),
        HeaderName::Other("X-List".into()),
    ] {
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::URLs,
            header: header.clone(),
            all: true,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::URLs,
            header: header.clone(),
            all: false,
        }));
    }

    for header in [
        HeaderName::Rfc(RfcHeader::Date),
        HeaderName::Rfc(RfcHeader::ResentDate),
        HeaderName::Other("X-Date-Single".into()),
        HeaderName::Other("X-Date".into()),
    ] {
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Date,
            header: header.clone(),
            all: true,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Date,
            header: header.clone(),
            all: false,
        }));
    }

    for header in [
        HeaderName::Rfc(RfcHeader::MessageId),
        HeaderName::Rfc(RfcHeader::References),
        HeaderName::Other("X-Id-Single".into()),
        HeaderName::Other("X-Id".into()),
    ] {
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::MessageIds,
            header: header.clone(),
            all: true,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::MessageIds,
            header: header.clone(),
            all: false,
        }));
    }

    for header in [
        HeaderName::Rfc(RfcHeader::Subject),
        HeaderName::Rfc(RfcHeader::Keywords),
        HeaderName::Other("X-Text-Single".into()),
        HeaderName::Other("X-Text".into()),
    ] {
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Text,
            header: header.clone(),
            all: true,
        }));
        properties.push(JMAPMailProperties::Header(JMAPMailHeaderProperty {
            form: JMAPMailHeaderForm::Text,
            header: header.clone(),
            all: false,
        }));
    }

    let mut result = HashMap::new();
    for property in properties {
        result.extend(
            mail_store
                .mail_parse(JMAPMailParseRequest {
                    account_id: 0,
                    blob_ids: vec![blob_id.clone()],
                    properties: vec![property],
                    arguments: JMAPMailStoreGetArguments {
                        body_properties: vec![
                            JMAPMailBodyProperties::Size,
                            JMAPMailBodyProperties::Name,
                            JMAPMailBodyProperties::Type,
                            JMAPMailBodyProperties::Charset,
                            JMAPMailBodyProperties::Disposition,
                            JMAPMailBodyProperties::Cid,
                            JMAPMailBodyProperties::Language,
                            JMAPMailBodyProperties::Location,
                            JMAPMailBodyProperties::Header(JMAPMailHeaderProperty::new_other(
                                "X-Custom-Header".into(),
                                JMAPMailHeaderForm::Raw,
                                false,
                            )),
                            JMAPMailBodyProperties::Header(JMAPMailHeaderProperty::new_other(
                                "X-Custom-Header-2".into(),
                                JMAPMailHeaderForm::Raw,
                                false,
                            )),
                        ],
                        fetch_text_body_values: true,
                        fetch_html_body_values: true,
                        fetch_all_body_values: true,
                        max_body_value_bytes: 100,
                    },
                })
                .unwrap()
                .parsed
                .unwrap_object()
                .unwrap()
                .remove(&blob_id.to_jmap_string())
                .unwrap()
                .unwrap_object()
                .unwrap(),
        );
    }

    test_file.set_extension("json");

    /*fs::write(
        test_file,
        &serde_json::to_string_pretty(&UntaggedJSONValue::from(JSONValue::Object(result))).unwrap(),
    )
    .unwrap();*/

    assert_eq!(
        UntaggedJSONValue::from(JSONValue::Object(result)),
        serde_json::from_slice::<UntaggedJSONValue>(&fs::read(&test_file).unwrap()).unwrap()
    );
}
