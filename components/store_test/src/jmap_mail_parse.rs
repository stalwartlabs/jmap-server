use std::{collections::HashMap, fs, path::PathBuf};

use jmap::{
    id::{blob::BlobId, JMAPIdSerialize},
    jmap_store::{blob::JMAPBlobStore, get::JMAPGet, parse::JMAPParse},
    protocol::json::JSONValue,
    request::{get::GetRequest, parse::ParseRequest},
};
use jmap_mail::mail::{
    get::GetMail,
    import::JMAPMailImport,
    parse::{get_message_blob, ParseMail},
    HeaderName, MailBodyProperties, MailHeaderForm, MailHeaderProperty, MailProperties,
};
use mail_parser::RfcHeader;
use store::{AccountId, JMAPStore, Store};

use crate::jmap_mail_get::{build_mail_get_arguments, SortedJSONValue};

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

        let blob_id = BlobId::from_jmap_string(
            &JSONValue::from(
                mail_store
                    .get::<GetMail<T>>(GetRequest {
                        account_id,
                        ids: vec![mail_store
                            .mail_import_blob(
                                account_id,
                                fs::read(&test_file).unwrap(),
                                vec![],
                                vec![],
                                None,
                            )
                            .unwrap()
                            .eval_unwrap_jmap_id("/id")]
                        .into(),
                        properties: vec![MailProperties::Attachments.to_string().into()].into(),
                        arguments: build_mail_get_arguments(
                            vec![MailBodyProperties::BlobId],
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
                MailBodyProperties::PartId,
                MailBodyProperties::BlobId,
                MailBodyProperties::Size,
                MailBodyProperties::Name,
                MailBodyProperties::Type,
                MailBodyProperties::Charset,
                MailBodyProperties::Headers,
                MailBodyProperties::Disposition,
                MailBodyProperties::Cid,
                MailBodyProperties::Language,
                MailBodyProperties::Location,
            ],
            true,
            true,
            true,
            100,
        );

        arguments.insert(
            "properties".to_string(),
            vec![
                MailProperties::Id,
                MailProperties::BlobId,
                MailProperties::ThreadId,
                MailProperties::MailboxIds,
                MailProperties::Keywords,
                MailProperties::Size,
                MailProperties::ReceivedAt,
                MailProperties::MessageId,
                MailProperties::InReplyTo,
                MailProperties::References,
                MailProperties::Sender,
                MailProperties::From,
                MailProperties::To,
                MailProperties::Cc,
                MailProperties::Bcc,
                MailProperties::ReplyTo,
                MailProperties::Subject,
                MailProperties::SentAt,
                MailProperties::HasAttachment,
                MailProperties::Preview,
                MailProperties::BodyValues,
                MailProperties::TextBody,
                MailProperties::HtmlBody,
                MailProperties::Attachments,
                MailProperties::BodyStructure,
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
                    .download_blob(
                        account_id,
                        &part.eval_unwrap_blob_id("/blobId"),
                        get_message_blob,
                    )
                    .unwrap()
                    .unwrap();

                test_file
                    .set_extension(format!("part{}", part.eval_unwrap_unsigned_int("/partId")));

                //fs::write(&test_file, inner_blob).unwrap();

                assert_eq!(inner_blob, fs::read(&test_file).unwrap());
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
                    .unwrap()
            ),
            serde_json::from_slice::<SortedJSONValue>(&fs::read(&test_file).unwrap()).unwrap()
        );
    }

    // Test header parsing on a temporary blob
    let mut test_file = test_dir;
    test_file.push("headers.eml");
    let blob_id = mail_store
        .upload_blob(account_id, &fs::read(&test_file).unwrap())
        .unwrap();

    let mut properties = vec![
        MailProperties::Id,
        MailProperties::MessageId,
        MailProperties::InReplyTo,
        MailProperties::References,
        MailProperties::Sender,
        MailProperties::From,
        MailProperties::To,
        MailProperties::Cc,
        MailProperties::Bcc,
        MailProperties::ReplyTo,
        MailProperties::Subject,
        MailProperties::SentAt,
        MailProperties::Preview,
        MailProperties::TextBody,
        MailProperties::HtmlBody,
        MailProperties::Attachments,
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
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Addresses,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Addresses,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::GroupedAddresses,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
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
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::URLs,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
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
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Date,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
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
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::MessageIds,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
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
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Raw,
            header: header.clone(),
            all: false,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Text,
            header: header.clone(),
            all: true,
        }));
        properties.push(MailProperties::Header(MailHeaderProperty {
            form: MailHeaderForm::Text,
            header: header.clone(),
            all: false,
        }));
    }

    let mut result = HashMap::new();
    for property in properties {
        let mut arguments = build_mail_get_arguments(
            vec![
                MailBodyProperties::Size,
                MailBodyProperties::Name,
                MailBodyProperties::Type,
                MailBodyProperties::Charset,
                MailBodyProperties::Disposition,
                MailBodyProperties::Cid,
                MailBodyProperties::Language,
                MailBodyProperties::Location,
                MailBodyProperties::Header(MailHeaderProperty::new_other(
                    "X-Custom-Header".into(),
                    MailHeaderForm::Raw,
                    false,
                )),
                MailBodyProperties::Header(MailHeaderProperty::new_other(
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
        serde_json::from_slice::<SortedJSONValue>(&fs::read(&test_file).unwrap()).unwrap()
    );
}
