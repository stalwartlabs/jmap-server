use std::collections::HashMap;

use actix_web::web;
use jmap::types::jmap::JMAPId;
use jmap_client::{
    client::{Client, Credentials},
    email::{import::EmailImportResponse, query::Filter, Property},
    mailbox,
    principal::ACL,
};
use jmap_mail::{INBOX_ID, TRASH_ID};
use store::Store;

use crate::{tests::jmap::authorization::assert_forbidden, JMAPServer};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, admin_client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Running Authorization tests...");

    // Create a domain name and three test accounts
    let inbox_id = JMAPId::new(INBOX_ID as u64).to_string();
    let trash_id = JMAPId::new(TRASH_ID as u64).to_string();
    let domain_id = admin_client
        .set_default_account_id(JMAPId::new(0))
        .domain_create("example.com")
        .await
        .unwrap()
        .unwrap_id();
    let john_id = admin_client
        .individual_create("jdoe@example.com", "12345", "John Doe")
        .await
        .unwrap()
        .unwrap_id();
    let jane_id = admin_client
        .individual_create("jane.smith@example.com", "abcde", "Jane Smith")
        .await
        .unwrap()
        .unwrap_id();
    let bill_id = admin_client
        .individual_create("bill@example.com", "098765", "Bill Foobar")
        .await
        .unwrap()
        .unwrap_id();
    let sales_id = admin_client
        .group_create("sales@example.com", "Sales Group", Vec::<String>::new())
        .await
        .unwrap()
        .unwrap_id();

    // Authenticate all accounts
    let mut john_client = Client::connect(
        admin_client.session_url(),
        Credentials::basic("jdoe@example.com", "12345"),
    )
    .await
    .unwrap();
    let mut jane_client = Client::connect(
        admin_client.session_url(),
        Credentials::basic("jane.smith@example.com", "abcde"),
    )
    .await
    .unwrap();
    let mut bill_client = Client::connect(
        admin_client.session_url(),
        Credentials::basic("bill@example.com", "098765"),
    )
    .await
    .unwrap();

    // Insert two emails in each account
    let mut email_ids = HashMap::new();
    for (client, account_id, name) in [
        (&mut john_client, &john_id, "john"),
        (&mut jane_client, &jane_id, "jane"),
        (&mut bill_client, &bill_id, "bill"),
        (admin_client, &sales_id, "sales"),
    ] {
        let user_name = client.session().username().to_string();
        let mut ids = Vec::with_capacity(2);
        for (mailbox_id, mailbox_name) in [(&inbox_id, "inbox"), (&trash_id, "trash")] {
            ids.push(
                client
                    .set_default_account_id(account_id)
                    .email_import(
                        format!(
                            concat!(
                                "From: acl_test@example.com\r\n",
                                "To: {}\r\n",
                                "Subject: Owned by {} in {}\r\n",
                                "\r\n",
                                "This message is owned by {}.",
                            ),
                            user_name, name, mailbox_name, name
                        )
                        .into_bytes(),
                        [mailbox_id],
                        None::<Vec<&str>>,
                        None,
                    )
                    .await
                    .unwrap()
                    .unwrap_id(),
            );
        }
        email_ids.insert(name, ids);
    }

    // John should have access to his emails only
    assert_eq!(
        john_client
            .email_get(
                email_ids.get("john").unwrap().first().unwrap(),
                [Property::Subject].into(),
            )
            .await
            .unwrap()
            .unwrap()
            .subject()
            .unwrap(),
        "Owned by john in inbox"
    );
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .email_get(
                email_ids.get("jane").unwrap().first().unwrap(),
                [Property::Subject].into(),
            )
            .await,
    );
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .mailbox_get(&inbox_id, None::<Vec<_>>)
            .await,
    );
    assert_forbidden(
        john_client
            .set_default_account_id(&sales_id)
            .email_get(
                email_ids.get("sales").unwrap().first().unwrap(),
                [Property::Subject].into(),
            )
            .await,
    );
    assert_forbidden(
        john_client
            .set_default_account_id(&sales_id)
            .mailbox_get(&inbox_id, None::<Vec<_>>)
            .await,
    );
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .email_query(None::<Filter>, None::<Vec<_>>)
            .await,
    );

    // Jane grants Inbox ReadItems access to John
    jane_client
        .mailbox_update_acl(&inbox_id, &john_id, [ACL::ReadItems])
        .await
        .unwrap();

    // John shoud have ReadItems access to Inbox
    assert_eq!(
        john_client
            .set_default_account_id(&jane_id)
            .email_get(
                email_ids.get("jane").unwrap().first().unwrap(),
                [Property::Subject].into(),
            )
            .await
            .unwrap()
            .unwrap()
            .subject()
            .unwrap(),
        "Owned by jane in inbox"
    );
    assert_eq!(
        john_client
            .set_default_account_id(&jane_id)
            .email_query(None::<Filter>, None::<Vec<_>>)
            .await
            .unwrap()
            .ids(),
        [email_ids.get("jane").unwrap().first().unwrap().as_str()]
    );

    // John's session resource should contain Jane's account details
    john_client.refresh_session().await.unwrap();
    assert_eq!(
        john_client.session().account(&jane_id).unwrap().name(),
        "Jane Smith"
    );

    // John should not have access to emails in Jane's Trash folder
    assert!(john_client
        .set_default_account_id(&jane_id)
        .email_get(
            email_ids.get("jane").unwrap().last().unwrap(),
            [Property::Subject].into(),
        )
        .await
        .unwrap()
        .is_none());

    // John only has ReadItems access to Inbox but no Read access
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .mailbox_get(&inbox_id, [mailbox::Property::MyRights].into())
            .await,
    );

    // Grant access and try again
    jane_client
        .mailbox_update_acl(&inbox_id, &john_id, [ACL::Read, ACL::ReadItems])
        .await
        .unwrap();
    assert_eq!(
        john_client
            .set_default_account_id(&jane_id)
            .mailbox_get(&inbox_id, [mailbox::Property::MyRights].into())
            .await
            .unwrap()
            .unwrap()
            .my_rights()
            .unwrap()
            .acl_list(),
        vec![ACL::ReadItems]
    );

    // Try to add items using import and copy
    let blob_id = john_client
        .set_default_account_id(&john_id)
        .upload(
            concat!(
                "From: acl_test@example.com\r\n",
                "To: jane.smith@example.com\r\n",
                "Subject: Created by john in jane's inbox\r\n",
                "\r\n",
                "This message is owned by jane.",
            )
            .as_bytes()
            .to_vec(),
            None,
        )
        .await
        .unwrap()
        .unwrap_blob_id();
    let mut request = john_client.set_default_account_id(&jane_id).build();
    let email_id = request
        .import_email()
        .email(&blob_id)
        .mailbox_ids([&inbox_id])
        .create_id();
    assert_forbidden(
        request
            .send_single::<EmailImportResponse>()
            .await
            .unwrap()
            .created(&email_id),
    );
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .email_copy(
                &john_id,
                email_ids.get("john").unwrap().last().unwrap(),
                [&inbox_id],
                None::<Vec<&str>>,
                None,
            )
            .await,
    );

    // Grant access and try again
    jane_client
        .mailbox_update_acl(
            &inbox_id,
            &john_id,
            [ACL::Read, ACL::ReadItems, ACL::AddItems],
        )
        .await
        .unwrap();

    let mut request = john_client.set_default_account_id(&jane_id).build();
    let email_id = request
        .import_email()
        .email(&blob_id)
        .mailbox_ids([&inbox_id])
        .create_id();
    let email_id = request
        .send_single::<EmailImportResponse>()
        .await
        .unwrap()
        .created(&email_id)
        .unwrap()
        .unwrap_id();
    let email_id_2 = john_client
        .set_default_account_id(&jane_id)
        .email_copy(
            &john_id,
            email_ids.get("john").unwrap().last().unwrap(),
            [&inbox_id],
            None::<Vec<&str>>,
            None,
        )
        .await
        .unwrap()
        .unwrap_id();

    assert_eq!(
        jane_client
            .email_get(&email_id, [Property::Subject].into(),)
            .await
            .unwrap()
            .unwrap()
            .subject()
            .unwrap(),
        "Created by john in jane's inbox"
    );
    assert_eq!(
        jane_client
            .email_get(&email_id_2, [Property::Subject].into(),)
            .await
            .unwrap()
            .unwrap()
            .subject()
            .unwrap(),
        "Owned by john in trash"
    );

    // Try to remove items
    // Try to set seen
    // Try to set keywords
    // Try to create child
    // Try to rename
    // Try to delete
    // Try to change ACL

    // TODO test groups
    /*println!(
        "{}",
        serde_json::to_string_pretty(john_client.session()).unwrap()
    );*/
    /*assert_forbidden(
        john_client
            .set_default_account_id(&sales_id)
            .email_get(
                email_ids.get("sales").unwrap().first().unwrap(),
                [Property::Subject].into(),
            )
            .await,
    );*/
}
