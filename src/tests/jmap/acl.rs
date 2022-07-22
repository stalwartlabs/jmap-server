use std::collections::HashMap;

use actix_web::web;
use jmap::{types::jmap::JMAPId, SUPERUSER_ID};
use jmap_client::{
    client::{Client, Credentials},
    email::{import::EmailImportResponse, query::Filter, Property},
    mailbox::{self, Role},
    principal::ACL,
};
use jmap_mail::{INBOX_ID, TRASH_ID};
use store::Store;

use crate::{
    tests::{jmap::authorization::assert_forbidden, store::utils::StoreCompareWith},
    JMAPServer,
};

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
        .take_id();
    let john_id = admin_client
        .individual_create("jdoe@example.com", "12345", "John Doe")
        .await
        .unwrap()
        .take_id();
    let jane_id = admin_client
        .individual_create("jane.smith@example.com", "abcde", "Jane Smith")
        .await
        .unwrap()
        .take_id();
    let bill_id = admin_client
        .individual_create("bill@example.com", "098765", "Bill Foobar")
        .await
        .unwrap()
        .take_id();
    let sales_id = admin_client
        .group_create("sales@example.com", "Sales Group", Vec::<String>::new())
        .await
        .unwrap()
        .take_id();

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
                    .take_id(),
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
        .mailbox_update_acl(&inbox_id, "jdoe@example.com", [ACL::ReadItems])
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

    // John should only be able to copy blobs he has access to
    let blob_id = jane_client
        .email_get(
            email_ids.get("jane").unwrap().first().unwrap(),
            [Property::BlobId].into(),
        )
        .await
        .unwrap()
        .unwrap()
        .take_blob_id();
    john_client
        .set_default_account_id(&john_id)
        .blob_copy(&jane_id, &blob_id)
        .await
        .unwrap();
    let blob_id = jane_client
        .email_get(
            email_ids.get("jane").unwrap().last().unwrap(),
            [Property::BlobId].into(),
        )
        .await
        .unwrap()
        .unwrap()
        .take_blob_id();
    assert_forbidden(
        john_client
            .set_default_account_id(&john_id)
            .blob_copy(&jane_id, &blob_id)
            .await,
    );

    // John only has ReadItems access to Inbox but no Read access
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .mailbox_get(&inbox_id, [mailbox::Property::MyRights].into())
            .await,
    );
    jane_client
        .mailbox_update_acl(&inbox_id, "jdoe@example.com", [ACL::Read, ACL::ReadItems])
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
            Some(&john_id),
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
        .take_blob_id();
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
            "jdoe@example.com",
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
        .take_id();
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
        .take_id();

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

    // Try removing items
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .email_destroy(&email_id)
            .await,
    );
    jane_client
        .mailbox_update_acl(
            &inbox_id,
            "jdoe@example.com",
            [ACL::Read, ACL::ReadItems, ACL::AddItems, ACL::RemoveItems],
        )
        .await
        .unwrap();
    john_client
        .set_default_account_id(&jane_id)
        .email_destroy(&email_id)
        .await
        .unwrap();

    // Try to set keywords
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .email_set_keyword(&email_id_2, "$seen", true)
            .await,
    );
    jane_client
        .mailbox_update_acl(
            &inbox_id,
            "jdoe@example.com",
            [
                ACL::Read,
                ACL::ReadItems,
                ACL::AddItems,
                ACL::RemoveItems,
                ACL::ModifyItems,
            ],
        )
        .await
        .unwrap();
    john_client
        .set_default_account_id(&jane_id)
        .email_set_keyword(&email_id_2, "$seen", true)
        .await
        .unwrap();
    john_client
        .set_default_account_id(&jane_id)
        .email_set_keyword(&email_id_2, "my-keyword", true)
        .await
        .unwrap();

    // Try to create child
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .mailbox_create("John's mailbox", None::<&str>, Role::None)
            .await,
    );
    jane_client
        .mailbox_update_acl(
            &inbox_id,
            "jdoe@example.com",
            [
                ACL::Read,
                ACL::ReadItems,
                ACL::AddItems,
                ACL::RemoveItems,
                ACL::ModifyItems,
                ACL::CreateChild,
            ],
        )
        .await
        .unwrap();
    let mailbox_id = john_client
        .set_default_account_id(&jane_id)
        .mailbox_create("John's mailbox", None::<&str>, Role::None)
        .await
        .unwrap()
        .take_id();

    // Try renaming a mailbox
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .mailbox_rename(&mailbox_id, "John's private mailbox")
            .await,
    );
    jane_client
        .mailbox_update_acl(
            &mailbox_id,
            "jdoe@example.com",
            [ACL::Read, ACL::ReadItems, ACL::Modify],
        )
        .await
        .unwrap();
    john_client
        .set_default_account_id(&jane_id)
        .mailbox_rename(&mailbox_id, "John's private mailbox")
        .await
        .unwrap();

    // Try moving a message
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .email_set_mailbox(&email_id_2, &mailbox_id, true)
            .await,
    );
    jane_client
        .mailbox_update_acl(
            &mailbox_id,
            "jdoe@example.com",
            [ACL::Read, ACL::ReadItems, ACL::Modify, ACL::AddItems],
        )
        .await
        .unwrap();
    john_client
        .set_default_account_id(&jane_id)
        .email_set_mailbox(&email_id_2, &mailbox_id, true)
        .await
        .unwrap();

    // Try deleting a mailbox
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .mailbox_destroy(&mailbox_id, true)
            .await,
    );
    jane_client
        .mailbox_update_acl(
            &mailbox_id,
            "jdoe@example.com",
            [
                ACL::Read,
                ACL::ReadItems,
                ACL::Modify,
                ACL::AddItems,
                ACL::Delete,
            ],
        )
        .await
        .unwrap();
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .mailbox_destroy(&mailbox_id, true)
            .await,
    );
    jane_client
        .mailbox_update_acl(
            &mailbox_id,
            "jdoe@example.com",
            [
                ACL::Read,
                ACL::ReadItems,
                ACL::Modify,
                ACL::AddItems,
                ACL::Delete,
                ACL::RemoveItems,
            ],
        )
        .await
        .unwrap();
    john_client
        .set_default_account_id(&jane_id)
        .mailbox_destroy(&mailbox_id, true)
        .await
        .unwrap();

    // Try changing ACL
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .mailbox_update_acl(&inbox_id, "bill@example.com", [ACL::Read, ACL::ReadItems])
            .await,
    );
    assert_forbidden(
        bill_client
            .set_default_account_id(&jane_id)
            .email_query(None::<Filter>, None::<Vec<_>>)
            .await,
    );
    jane_client
        .mailbox_update_acl(
            &inbox_id,
            "jdoe@example.com",
            [
                ACL::Read,
                ACL::ReadItems,
                ACL::AddItems,
                ACL::RemoveItems,
                ACL::ModifyItems,
                ACL::CreateChild,
                ACL::Modify,
                ACL::Administer,
            ],
        )
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
        vec![
            ACL::ReadItems,
            ACL::AddItems,
            ACL::RemoveItems,
            ACL::ModifyItems,
            ACL::CreateChild,
            ACL::Modify
        ]
    );
    john_client
        .set_default_account_id(&jane_id)
        .mailbox_update_acl(&inbox_id, "bill@example.com", [ACL::Read, ACL::ReadItems])
        .await
        .unwrap();
    assert_eq!(
        bill_client
            .set_default_account_id(&jane_id)
            .email_query(None::<Filter>, None::<Vec<_>>)
            .await
            .unwrap()
            .ids(),
        [email_ids.get("jane").unwrap().first().unwrap().as_str()]
    );

    // Revoke all access to John
    jane_client
        .mailbox_update_acl(&inbox_id, "jdoe@example.com", [])
        .await
        .unwrap();
    assert_forbidden(
        john_client
            .set_default_account_id(&jane_id)
            .email_get(
                email_ids.get("jane").unwrap().first().unwrap(),
                [Property::Subject].into(),
            )
            .await,
    );
    john_client.refresh_session().await.unwrap();
    assert!(john_client.session().account(&jane_id).is_none());
    assert_eq!(
        bill_client
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

    // Add John and Jane to the Sales group
    admin_client
        .set_default_account_id(JMAPId::new(SUPERUSER_ID as u64).to_string())
        .principal_set_members(&sales_id, [&jane_id, &john_id].into())
        .await
        .unwrap();
    john_client.refresh_session().await.unwrap();
    jane_client.refresh_session().await.unwrap();
    bill_client.refresh_session().await.unwrap();
    assert_eq!(
        john_client.session().account(&sales_id).unwrap().name(),
        "Sales Group"
    );
    assert!(!john_client
        .session()
        .account(&sales_id)
        .unwrap()
        .is_personal());
    assert_eq!(
        jane_client.session().account(&sales_id).unwrap().name(),
        "Sales Group"
    );
    assert!(bill_client.session().account(&sales_id).is_none());

    // Insert a message in Sales's inbox
    let blob_id = john_client
        .set_default_account_id(&sales_id)
        .upload(
            Some(&sales_id),
            concat!(
                "From: acl_test@example.com\r\n",
                "To: sales@example.com\r\n",
                "Subject: Created by john in sales\r\n",
                "\r\n",
                "This message is owned by sales.",
            )
            .as_bytes()
            .to_vec(),
            None,
        )
        .await
        .unwrap()
        .take_blob_id();
    let mut request = john_client.build();
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
        .take_id();

    // Both Jane and John should be able to see this message, but not Bill
    assert_eq!(
        john_client
            .set_default_account_id(&sales_id)
            .email_get(&email_id, [Property::Subject].into(),)
            .await
            .unwrap()
            .unwrap()
            .subject()
            .unwrap(),
        "Created by john in sales"
    );
    assert_eq!(
        jane_client
            .set_default_account_id(&sales_id)
            .email_get(&email_id, [Property::Subject].into(),)
            .await
            .unwrap()
            .unwrap()
            .subject()
            .unwrap(),
        "Created by john in sales"
    );
    assert_forbidden(
        bill_client
            .set_default_account_id(&sales_id)
            .email_get(&email_id, [Property::Subject].into())
            .await,
    );

    // Remove John from the sales group
    admin_client
        .set_default_account_id(JMAPId::new(SUPERUSER_ID as u64).to_string())
        .principal_set_members(&sales_id, [&jane_id].into())
        .await
        .unwrap();
    assert_forbidden(
        john_client
            .set_default_account_id(&sales_id)
            .email_get(&email_id, [Property::Subject].into())
            .await,
    );

    // Destroy test accounts
    for user_id in [jane_id, john_id, bill_id, sales_id, domain_id] {
        admin_client
            .set_default_account_id(JMAPId::new(SUPERUSER_ID as u64))
            .principal_destroy(&user_id)
            .await
            .unwrap();
    }
    server.store.assert_is_empty();
}
