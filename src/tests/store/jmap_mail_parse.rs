use std::{collections::HashMap, fs, path::PathBuf};

use jmap::{
    id::{blob::JMAPBlob, JMAPIdSerialize},
    jmap_store::{blob::JMAPBlobStore, get::JMAPGet, parse::JMAPParse},
    protocol::json::JSONValue,
    request::{get::GetRequest, parse::ParseRequest},
};
use jmap_mail::{
    mail::{
        get::GetMail,
        import::JMAPMailImport,
        parse::{get_message_part, ParseMail},
        HeaderName, MailBodyProperty, MailHeaderForm, MailHeaderProperty, MailProperty,
    },
    mail_parser::RfcHeader,
};

use store::{AccountId, JMAPStore, Store};

use crate::tests::store::jmap_mail_get::{build_mail_get_arguments, SortedJSONValue};

pub fn jmap_mail_parse<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
    let mut test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_dir.push("resources");
    test_dir.push("jmap_mail_parse");

    // Test parsing an email attachment
    for test_name in ["attachment.eml", "attachment_b64.eml"] {
        let mut test_file = test_dir.clone();
        test_file.push(test_name);

        let raw_blob = fs::read(&test_file).unwrap();
        let raw_blob_id = mail_store.blob_store(&raw_blob).unwrap();

        let blob_id = JMAPBlob::from_jmap_string(
            &JSONValue::from(
                mail_store
                    .get::<GetMail<T>>(GetRequest {
                        account_id,
                        ids: vec![
                            mail_store
                                .mail_import(
                                    account_id,
                                    raw_blob_id,
                                    &raw_blob,
                                    vec![],
                                    vec![],
                                    None,
                                )
                                .unwrap()
                                .id,
                        ]
                        .into(),
                        properties: vec![MailProperty::Attachments.to_string().into()].into(),
                        arguments: build_mail_get_arguments(
                            vec![MailBodyProperty::BlobId],
                            false,
                            false,
                            false,
                            100,
                        ),
                    })
                    .unwrap(),
            )
            .eval_unwrap_string("/list/0/attachments/0/blobId"),
        )
        .unwrap();

        let mut arguments = build_mail_get_arguments(
            vec![
                MailBodyProperty::PartId,
                MailBodyProperty::BlobId,
                MailBodyProperty::Size,
                MailBodyProperty::Name,
                MailBodyProperty::Type,
                MailBodyProperty::Charset,
                MailBodyProperty::Headers,
                MailBodyProperty::Disposition,
                MailBodyProperty::Cid,
                MailBodyProperty::Language,
                MailBodyProperty::Location,
            ],
            true,
            true,
            true,
            100,
        );

        arguments.insert(
            "properties".to_string(),
            vec![
                MailProperty::Id,
                MailProperty::BlobId,
                MailProperty::ThreadId,
                MailProperty::MailboxIds,
                MailProperty::Keywords,
                MailProperty::Size,
                MailProperty::ReceivedAt,
                MailProperty::MessageId,
                MailProperty::InReplyTo,
                MailProperty::References,
                MailProperty::Sender,
                MailProperty::From,
                MailProperty::To,
                MailProperty::Cc,
                MailProperty::Bcc,
                MailProperty::ReplyTo,
                MailProperty::Subject,
                MailProperty::SentAt,
                MailProperty::HasAttachment,
                MailProperty::Preview,
                MailProperty::BodyValues,
                MailProperty::TextBody,
                MailProperty::HtmlBody,
                MailProperty::Attachments,
                MailProperty::BodyStructure,
            ]
            .into_iter()
            .map(|p| p.to_string().into())
            .collect::<Vec<_>>()
            .into(),
        );

        let result: JSONValue = mail_store
            .parse::<ParseMail>(ParseRequest {
                account_id,
                arguments,
                blob_ids: vec![blob_id.clone()],
            })
            .unwrap()
            .into();

        assert_eq!(result.eval("/notFound").unwrap(), vec![].into());
        assert_eq!(result.eval("/notParsable").unwrap(), vec![].into());

        for part_name in ["textBody", "htmlBody", "attachments"] {
            for part in result.eval_unwrap_array(&format!(
                "/parsed/{}/{}",
                blob_id.to_jmap_string(),
                part_name
            )) {
                let inner_blob = mail_store
                    .blob_jmap_get(
                        account_id,
                        &part.eval_unwrap_blob("/blobId"),
                        get_message_part,
                    )
                    .unwrap()
                    .unwrap();

                test_file.set_extension(format!("part{}", part.eval_unwrap_string("/partId")));

                //fs::write(&test_file, inner_blob).unwrap();
                let expected_inner_blob = fs::read(&test_file).unwrap();

                assert_eq!(
                    inner_blob,
                    expected_inner_blob,
                    "file: {}",
                    test_file.display()
                );
            }
        }

        test_file.set_extension("json");

        /*fs::write(
            test_file,
            &serde_json::to_string_pretty(&SortedJSONValue::from(result)).unwrap(),
        )
        .unwrap();*/

        assert_eq!(
            SortedJSONValue::from(
                result
                    .eval(&format!("/parsed/{}", blob_id.to_jmap_string()))
                    .unwrap(),
            ),
            serde_json::from_slice::<SortedJSONValue>(&fs::read(&test_file).unwrap()).unwrap(),
            "({}) {}",
            test_file.display(),
            serde_json::to_string_pretty(&SortedJSONValue::from(
                result
                    .eval(&format!("/parsed/{}", blob_id.to_jmap_string()))
                    .unwrap()
            ))
            .unwrap()
        );
    }

    // Test header parsing on a temporary blob
    let mut test_file = test_dir;
    test_file.push("headers.eml");
    let blob_id = mail_store
        .blob_store_ephimeral(account_id, &fs::read(&test_file).unwrap())
        .unwrap();

    let mut properties = vec![
        MailProperty::Id,
        MailProperty::MessageId,
        MailProperty::InReplyTo,
        MailProperty::References,
        MailProperty::Sender,
        MailProperty::From,
        MailProperty::To,
        MailProperty::Cc,
        MailProperty::Bcc,
        MailProperty::ReplyTo,
        MailProperty::Subject,
        MailProperty::SentAt,
        MailProperty::Preview,
        MailProperty::TextBody,
        MailProperty::HtmlBody,
        MailProperty::Attachments,
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
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Addresses,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Addresses,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::GroupedAddresses,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::GroupedAddresses,
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
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::URLs,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::URLs,
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
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Date,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Date,
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
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::MessageIds,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::MessageIds,
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
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Text,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperty::Header(MailHeaderProperty {
            form: MailHeaderForm::Text,
            header: header.clone(),
            all: false,
        }));
    }

    let mut result = HashMap::new();
    for property in properties {
        let mut arguments = build_mail_get_arguments(
            vec![
                MailBodyProperty::Size,
                MailBodyProperty::Name,
                MailBodyProperty::Type,
                MailBodyProperty::Charset,
                MailBodyProperty::Disposition,
                MailBodyProperty::Cid,
                MailBodyProperty::Language,
                MailBodyProperty::Location,
                MailBodyProperty::Header(MailHeaderProperty::new_other(
                    "X-Custom-Header".into(),
                    MailHeaderForm::Raw,
                    false,
                )),
                MailBodyProperty::Header(MailHeaderProperty::new_other(
                    "X-Custom-Header-2".into(),
                    MailHeaderForm::Raw,
                    false,
                )),
            ],
            true,
            true,
            true,
            100,
        );

        arguments.insert(
            "properties".to_string(),
            vec![property.to_string().into()].into(),
        );

        result.extend(
            JSONValue::from(
                mail_store
                    .parse::<ParseMail>(ParseRequest {
                        account_id,
                        arguments,
                        blob_ids: vec![blob_id.clone()],
                    })
                    .unwrap(),
            )
            .eval_unwrap_object(&format!("/parsed/{}", blob_id.to_jmap_string())),
        );
    }

    test_file.set_extension("json");

    /*fs::write(
        test_file,
        &serde_json::to_string_pretty(&SortedJSONValue::from(JSONValue::Object(result))).unwrap(),
    )
    .unwrap();*/

    assert_eq!(
        SortedJSONValue::from(JSONValue::Object(result)),
        serde_json::from_slice::<SortedJSONValue>(&fs::read(&test_file).unwrap()).unwrap(),
    );
}
