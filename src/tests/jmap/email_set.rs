use std::{fs, path::PathBuf};

use actix_web::web;
use jmap::id::JMAPIdSerialize;
use jmap_client::{
    client::Client,
    core::set::{SetError, SetErrorType},
    email::{self, Email},
    mailbox::Role,
    Error, Set,
};
use store::Store;

use crate::{tests::store::utils::StoreCompareWith, JMAPServer};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    let mailbox_id = client
        .set_default_account_id(1u64.to_jmap_string())
        .mailbox_create("JMAP Set", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();

    create(client, &mailbox_id).await;
    update(client, &mailbox_id).await;

    client.mailbox_destroy(&mailbox_id, true).await.unwrap();

    server.store.assert_is_empty();
}

async fn create(client: &mut Client, mailbox_id: &str) {
    let mut test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_dir.push("src");
    test_dir.push("tests");
    test_dir.push("resources");
    test_dir.push("jmap_mail_set");

    for file_name in fs::read_dir(&test_dir).unwrap() {
        let mut file_name = file_name.as_ref().unwrap().path();
        if file_name.extension().map_or(true, |e| e != "json") {
            continue;
        }

        // Upload blobs
        let mut json_request = String::from_utf8(fs::read(&file_name).unwrap()).unwrap();
        let blob_values = find_values(&json_request, "\"blobId\"");
        if !blob_values.is_empty() {
            let mut blob_ids = Vec::with_capacity(blob_values.len());
            for blob_value in &blob_values {
                let blob_value = blob_value.replace("\\r", "\r").replace("\\n", "\n");
                blob_ids.push(
                    client
                        .upload(blob_value.into_bytes(), None)
                        .await
                        .unwrap()
                        .unwrap_blob_id(),
                );
            }
            json_request = replace_values(json_request, &blob_values, &blob_ids);
        }

        // Create message and obtain its blobId
        let mut request = client.build();
        let mut create_item =
            serde_json::from_slice::<Email<Set>>(json_request.as_bytes()).unwrap();
        create_item.mailbox_ids([mailbox_id]);
        let create_id = request.set_email().create_item(create_item);
        let created_email = request
            .send_set_email()
            .await
            .unwrap()
            .created(&create_id)
            .unwrap();

        // Download raw message
        let raw_message = client.download(created_email.blob_id()).await.unwrap();

        // Fetch message
        let mut request = client.build();
        request
            .get_email()
            .ids([created_email.id()])
            .properties([
                email::Property::Id,
                email::Property::BlobId,
                email::Property::ThreadId,
                email::Property::MailboxIds,
                email::Property::Keywords,
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
            .body_properties([
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
            ])
            .fetch_all_body_values(true)
            .max_body_value_bytes(100);
        let email = request
            .send_get_email()
            .await
            .unwrap()
            .pop()
            .unwrap()
            .into_test();

        // Compare response
        file_name.set_extension("jmap");

        assert_diff(
            &replace_boundaries(serde_json::to_string_pretty(&email).unwrap()),
            &String::from_utf8(fs::read(&file_name).unwrap()).unwrap(),
            file_name.to_str().unwrap(),
        );

        /*fs::write(
            file_name.clone(),
            replace_boundaries(serde_json::to_string_pretty(&email).unwrap()),
        )
        .unwrap();*/

        // Compare raw message
        file_name.set_extension("eml");

        assert_diff(
            &replace_boundaries(String::from_utf8(raw_message).unwrap()),
            &String::from_utf8(fs::read(&file_name).unwrap()).unwrap(),
            file_name.to_str().unwrap(),
        );

        /*fs::write(
            file_name,
            replace_boundaries(String::from_utf8(raw_message).unwrap()),
        )
        .unwrap();*/
    }
}

async fn update(client: &mut Client, root_mailbox_id: &str) {
    // Obtain all messageIds previously created
    let mailbox = client
        .email_query(
            email::query::Filter::in_mailbox(root_mailbox_id).into(),
            None,
        )
        .await
        .unwrap();

    // Create two test mailboxes
    let test_mailbox1_id = client
        .set_default_account_id(1u64.to_jmap_string())
        .mailbox_create("Test 1", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();
    let test_mailbox2_id = client
        .set_default_account_id(1u64.to_jmap_string())
        .mailbox_create("Test 2", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();

    // Set keywords and mailboxes
    let mut request = client.build();
    request
        .set_email()
        .update(mailbox.id(0))
        .mailbox_ids([&test_mailbox1_id, &test_mailbox2_id])
        .keywords(["test1", "test2"]);
    request
        .send_set_email()
        .await
        .unwrap()
        .updated(mailbox.id(0))
        .unwrap();
    assert_email_properties(
        client,
        mailbox.id(0),
        &[&test_mailbox1_id, &test_mailbox2_id],
        &["test1", "test2"],
    )
    .await;

    // Patch keywords and mailboxes
    let mut request = client.build();
    request
        .set_email()
        .update(mailbox.id(0))
        .mailbox_id(&test_mailbox1_id, false)
        .keyword("test1", true)
        .keyword("test2", false)
        .keyword("test3", true);
    request
        .send_set_email()
        .await
        .unwrap()
        .updated(mailbox.id(0))
        .unwrap();
    assert_email_properties(
        client,
        mailbox.id(0),
        &[&test_mailbox2_id],
        &["test1", "test3"],
    )
    .await;

    // Orphan messages should not be permitted
    let mut request = client.build();
    request
        .set_email()
        .update(mailbox.id(0))
        .mailbox_id(&test_mailbox2_id, false);
    assert!(matches!(
        request
            .send_set_email()
            .await
            .unwrap()
            .updated(mailbox.id(0)),
        Err(Error::Set(SetError {
            type_: SetErrorType::InvalidProperties,
            ..
        }))
    ));

    // Updating and destroying the same item should not be allowed
    let mut request = client.build();
    let set_email_request = request.set_email();
    set_email_request
        .update(mailbox.id(0))
        .mailbox_id(&test_mailbox2_id, false);
    set_email_request.destroy([mailbox.id(0)]);
    assert!(matches!(
        request
            .send_set_email()
            .await
            .unwrap()
            .updated(mailbox.id(0)),
        Err(Error::Set(SetError {
            type_: SetErrorType::WillDestroy,
            ..
        }))
    ));

    // Delete some messages
    let mut request = client.build();
    request.set_email().destroy([mailbox.id(1), mailbox.id(2)]);
    assert_eq!(
        request
            .send_set_email()
            .await
            .unwrap()
            .destroyed_ids()
            .unwrap()
            .count(),
        2
    );
    let mut request = client.build();
    request.get_email().ids([mailbox.id(1), mailbox.id(2)]);
    assert_eq!(request.send_get_email().await.unwrap().not_found().len(), 2);

    // Destroy test mailboxes
    client
        .mailbox_destroy(&test_mailbox1_id, true)
        .await
        .unwrap();
    client
        .mailbox_destroy(&test_mailbox2_id, true)
        .await
        .unwrap();
}

async fn assert_email_properties(
    client: &mut Client,
    message_id: &str,
    mailbox_ids: &[&str],
    keywords: &[&str],
) {
    let result = client
        .email_get(
            message_id,
            [email::Property::MailboxIds, email::Property::Keywords].into(),
        )
        .await
        .unwrap()
        .unwrap();

    let mut mailbox_ids_ = result.mailbox_ids().to_vec();
    let mut keywords_ = result.keywords().to_vec();
    mailbox_ids_.sort_unstable();
    keywords_.sort_unstable();

    assert_eq!(mailbox_ids_, mailbox_ids);
    assert_eq!(keywords_, keywords);
}

fn find_values(string: &str, name: &str) -> Vec<String> {
    let mut last_pos = 0;
    let mut values = Vec::new();

    while let Some(pos) = string[last_pos..].find(name) {
        let mut value = string[last_pos + pos + name.len()..]
            .split('"')
            .nth(1)
            .unwrap();
        if value.ends_with('\\') {
            value = &value[..value.len() - 1];
        }
        values.push(value.to_string());
        last_pos += pos + name.len();
    }

    values
}

fn replace_values(mut string: String, find: &[String], replace: &[String]) -> String {
    for (find, replace) in find.iter().zip(replace.iter()) {
        string = string.replace(find, replace);
    }
    string
}

fn replace_boundaries(string: String) -> String {
    let values = find_values(&string, "boundary=");
    if !values.is_empty() {
        replace_values(
            string,
            &values,
            &(0..values.len())
                .map(|i| format!("boundary_{}", i))
                .collect::<Vec<_>>(),
        )
    } else {
        string
    }
}

fn assert_diff(str1: &str, str2: &str, filename: &str) {
    for ((pos1, ch1), (pos2, ch2)) in str1.char_indices().zip(str2.char_indices()) {
        if ch1 != ch2 {
            panic!(
                "{:?} != {:?} ({})",
                &str1[if pos1 >= 10 { pos1 - 10 } else { pos1 }..pos1 + 10],
                &str2[if pos2 >= 10 { pos2 - 10 } else { pos2 }..pos2 + 10],
                filename
            );
        }
    }

    assert_eq!(str1.len(), str2.len(), "{}", filename);
}
