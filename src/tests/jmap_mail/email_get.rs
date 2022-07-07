use std::{fs, path::PathBuf};

use actix_web::web;
use jmap::types::jmap::JMAPId;
use jmap_client::{
    client::Client,
    email::{self, Header, HeaderForm},
    mailbox::Role,
};
use jmap_mail::mail_parser::RfcHeader;
use store::Store;

use crate::{tests::store::utils::StoreCompareWith, JMAPServer};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Running Email Get tests...");

    let mut test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_dir.push("src");
    test_dir.push("tests");
    test_dir.push("resources");
    test_dir.push("jmap_mail_get");

    let mailbox_id = client
        .set_default_account_id(JMAPId::new(1).to_string())
        .mailbox_create("JMAP Get", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();

    for file_name in fs::read_dir(&test_dir).unwrap() {
        let mut file_name = file_name.as_ref().unwrap().path();
        if file_name.extension().map_or(true, |e| e != "eml") {
            continue;
        }
        let is_headers_test = file_name.file_name().unwrap() == "headers.eml";

        let blob = fs::read(&file_name).unwrap();
        let blob_len = blob.len();
        let email = client
            .email_import(
                blob,
                [mailbox_id.clone()],
                ["tag".to_string()].into(),
                ((blob_len * 1000000) as i64).into(),
            )
            .await
            .unwrap();

        let mut request = client.build();
        request
            .get_email()
            .ids([email.id().unwrap()])
            .properties([
                email::Property::Id,
                email::Property::BlobId,
                email::Property::ThreadId,
                email::Property::MailboxIds,
                email::Property::Keywords,
                email::Property::Size,
                email::Property::ReceivedAt,
                email::Property::MessageId,
                email::Property::InReplyTo,
                email::Property::References,
                email::Property::Sender,
                email::Property::From,
                email::Property::To,
                email::Property::Cc,
                email::Property::Bcc,
                email::Property::ReplyTo,
                email::Property::Subject,
                email::Property::SentAt,
                email::Property::HasAttachment,
                email::Property::Preview,
                email::Property::BodyValues,
                email::Property::TextBody,
                email::Property::HtmlBody,
                email::Property::Attachments,
                email::Property::BodyStructure,
            ])
            .arguments()
            .body_properties(if !is_headers_test {
                [
                    email::BodyProperty::PartId,
                    email::BodyProperty::BlobId,
                    email::BodyProperty::Size,
                    email::BodyProperty::Name,
                    email::BodyProperty::Type,
                    email::BodyProperty::Charset,
                    email::BodyProperty::Headers,
                    email::BodyProperty::Disposition,
                    email::BodyProperty::Cid,
                    email::BodyProperty::Language,
                    email::BodyProperty::Location,
                ]
            } else {
                [
                    email::BodyProperty::PartId,
                    email::BodyProperty::Size,
                    email::BodyProperty::Name,
                    email::BodyProperty::Type,
                    email::BodyProperty::Charset,
                    email::BodyProperty::Disposition,
                    email::BodyProperty::Cid,
                    email::BodyProperty::Language,
                    email::BodyProperty::Location,
                    email::BodyProperty::Header(Header {
                        name: "X-Custom-Header".into(),
                        form: HeaderForm::Raw,
                        all: false,
                    }),
                    email::BodyProperty::Header(Header {
                        name: "X-Custom-Header-2".into(),
                        form: HeaderForm::Raw,
                        all: false,
                    }),
                ]
            })
            .fetch_all_body_values(true)
            .max_body_value_bytes(100);

        let mut result = request
            .send_get_email()
            .await
            .unwrap()
            .unwrap_list()
            .pop()
            .unwrap()
            .into_test();

        if is_headers_test {
            for property in all_headers() {
                let mut request = client.build();
                request
                    .get_email()
                    .ids([email.id().unwrap()])
                    .properties([property]);
                result.headers.extend(
                    request
                        .send_get_email()
                        .await
                        .unwrap()
                        .unwrap_list()
                        .pop()
                        .unwrap()
                        .into_test()
                        .headers,
                );
            }
        }

        let result = serde_json::to_string_pretty(&result).unwrap();

        file_name.set_extension("json");

        //fs::write(&file_name, &result).unwrap();

        assert_eq!(
            &String::from_utf8(fs::read(&file_name).unwrap()).unwrap(),
            &result,
            "{} ({})",
            result,
            file_name.to_str().unwrap()
        );
    }

    client.mailbox_destroy(&mailbox_id, true).await.unwrap();

    server.store.assert_is_empty();
}

pub fn all_headers() -> Vec<email::Property> {
    let mut properties = Vec::new();

    for header in [
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::From),
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::To),
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::Cc),
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::Bcc),
        jmap_mail::mail::HeaderName::Other("X-Address-Single".into()),
        jmap_mail::mail::HeaderName::Other("X-Address".into()),
        jmap_mail::mail::HeaderName::Other("X-AddressList-Single".into()),
        jmap_mail::mail::HeaderName::Other("X-AddressList".into()),
        jmap_mail::mail::HeaderName::Other("X-AddressesGroup-Single".into()),
        jmap_mail::mail::HeaderName::Other("X-AddressesGroup".into()),
    ] {
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Raw,
            name: header.to_string(),
            all: true,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Raw,
            name: header.to_string(),
            all: false,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Addresses,
            name: header.to_string(),
            all: true,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Addresses,
            name: header.to_string(),
            all: false,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::GroupedAddresses,
            name: header.to_string(),
            all: true,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::GroupedAddresses,
            name: header.to_string(),
            all: false,
        }));
    }

    for header in [
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::ListPost),
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::ListSubscribe),
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::ListUnsubscribe),
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::ListOwner),
        jmap_mail::mail::HeaderName::Other("X-List-Single".into()),
        jmap_mail::mail::HeaderName::Other("X-List".into()),
    ] {
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Raw,
            name: header.to_string(),
            all: true,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Raw,
            name: header.to_string(),
            all: false,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::URLs,
            name: header.to_string(),
            all: true,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::URLs,
            name: header.to_string(),
            all: false,
        }));
    }

    for header in [
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::Date),
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::ResentDate),
        jmap_mail::mail::HeaderName::Other("X-Date-Single".into()),
        jmap_mail::mail::HeaderName::Other("X-Date".into()),
    ] {
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Raw,
            name: header.to_string(),
            all: true,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Raw,
            name: header.to_string(),
            all: false,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Date,
            name: header.to_string(),
            all: true,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Date,
            name: header.to_string(),
            all: false,
        }));
    }

    for header in [
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::MessageId),
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::References),
        jmap_mail::mail::HeaderName::Other("X-Id-Single".into()),
        jmap_mail::mail::HeaderName::Other("X-Id".into()),
    ] {
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Raw,
            name: header.to_string(),
            all: true,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Raw,
            name: header.to_string(),
            all: false,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::MessageIds,
            name: header.to_string(),
            all: true,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::MessageIds,
            name: header.to_string(),
            all: false,
        }));
    }

    for header in [
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::Subject),
        jmap_mail::mail::HeaderName::Rfc(RfcHeader::Keywords),
        jmap_mail::mail::HeaderName::Other("X-Text-Single".into()),
        jmap_mail::mail::HeaderName::Other("X-Text".into()),
    ] {
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Raw,
            name: header.to_string(),
            all: true,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Raw,
            name: header.to_string(),
            all: false,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Text,
            name: header.to_string(),
            all: true,
        }));
        properties.push(email::Property::Header(Header {
            form: HeaderForm::Text,
            name: header.to_string(),
            all: false,
        }));
    }

    properties
}
