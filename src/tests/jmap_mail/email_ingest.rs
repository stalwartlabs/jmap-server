use actix_web::web;
use jmap::{types::jmap::JMAPId, SUPERUSER_ID};
use jmap_client::{
    client::Client,
    core::set::{SetError, SetErrorType},
};
use store::{core::collection::Collection, Store};

use crate::{
    api::ingest::{DeliveryStatus, Dsn},
    tests::{jmap_mail::ingest_message, store::utils::StoreCompareWith},
    JMAPServer,
};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Running Email ingest tests...");

    // Domain names need to exist before creating an account
    assert!(matches!(
        client
            .individual_create("jdoe@example.com", "12345", "John Doe")
            .await,
        Err(jmap_client::Error::Set(SetError {
            type_: SetErrorType::InvalidProperties,
            ..
        }))
    ));

    // Create a domain name and a test account
    let domain_id = client
        .set_default_account_id(JMAPId::new(0))
        .domain_create("example.com")
        .await
        .unwrap()
        .take_id();
    let account_id_1 = client
        .individual_create("jdoe@example.com", "12345", "John Doe")
        .await
        .unwrap()
        .take_id();
    client
        .principal_set_aliases(&account_id_1, ["john.doe@example.com"].into())
        .await
        .unwrap();
    let account_id_2 = client
        .individual_create("jane@example.com", "abcdef", "Jane Smith")
        .await
        .unwrap()
        .take_id();
    let account_id_3 = client
        .individual_create("bill@example.com", "12345", "Bill Foobar")
        .await
        .unwrap()
        .take_id();

    assert!(matches!(
        client
            .individual_create("jdoe@example.com", "12345", "John Doe")
            .await,
        Err(jmap_client::Error::Set(SetError {
            type_: SetErrorType::InvalidProperties,
            ..
        }))
    ));

    // Mailing list should contain existing ids belonging to individuals
    assert!(matches!(
        client
            .list_create(
                "members@example.com",
                "Mailing List",
                [JMAPId::new(u64::MAX), JMAPId::new(12345678)],
            )
            .await,
        Err(jmap_client::Error::Set(SetError {
            type_: SetErrorType::InvalidProperties,
            ..
        }))
    ));
    assert!(matches!(
        client
            .list_create("members@example.com", "Mailing List", [&domain_id],)
            .await,
        Err(jmap_client::Error::Set(SetError {
            type_: SetErrorType::InvalidProperties,
            ..
        }))
    ));

    // Create a mailing list
    let list_id = client
        .list_create(
            "members@example.com",
            "Mailing List",
            [&account_id_1, &account_id_2, &account_id_3],
        )
        .await
        .unwrap()
        .take_id();

    // Delivering to individuals
    ingest_message(
        concat!(
            "From: bill@example.com\r\n",
            "To: jdoe@example.com\r\n",
            "Subject: TPS Report\r\n",
            "\r\n",
            "I'm going to need those TPS reports ASAP. ",
            "So, if you could do that, that'd be great."
        )
        .as_bytes()
        .to_vec(),
        &["jdoe@example.com"],
    )
    .await;
    assert_eq!(
        server
            .store
            .get_document_ids(
                JMAPId::parse(&account_id_1).unwrap().get_document_id(),
                Collection::Mail
            )
            .unwrap()
            .unwrap()
            .len(),
        1
    );

    // Delivering to individuals' aliases
    ingest_message(
        concat!(
            "From: bill@example.com\r\n",
            "To: john.doe@example.com\r\n",
            "Subject: Fwd: TPS Report\r\n",
            "\r\n",
            "--- Forwarded Message ---\r\n\r\n ",
            "I'm going to need those TPS reports ASAP. ",
            "So, if you could do that, that'd be great."
        )
        .as_bytes()
        .to_vec(),
        &["john.doe@example.com"],
    )
    .await;
    assert_eq!(
        server
            .store
            .get_document_ids(
                JMAPId::parse(&account_id_1).unwrap().get_document_id(),
                Collection::Mail
            )
            .unwrap()
            .unwrap()
            .len(),
        2
    );

    // Delivering to a mailing list
    ingest_message(
        concat!(
            "From: bill@example.com\r\n",
            "To: members@example.com\r\n",
            "Subject: WFH policy\r\n",
            "\r\n",
            "We need the entire staff back in the office, ",
            "TPS reports cannot be filed properly from home."
        )
        .as_bytes()
        .to_vec(),
        &["members@example.com"],
    )
    .await;
    for (account_id, num_messages) in [(&account_id_1, 3), (&account_id_2, 1), (&account_id_3, 1)] {
        assert_eq!(
            server
                .store
                .get_document_ids(
                    JMAPId::parse(account_id).unwrap().get_document_id(),
                    Collection::Mail
                )
                .unwrap()
                .unwrap()
                .len(),
            num_messages,
            "for {}",
            account_id
        );
    }

    // Removing members from the mailing list
    client
        .principal_set_members(&list_id, [&account_id_2, &account_id_3].into())
        .await
        .unwrap();
    ingest_message(
        concat!(
            "From: bill@example.com\r\n",
            "To: members@example.com\r\n",
            "Subject: WFH policy (reminder)\r\n",
            "\r\n",
            "This is a reminter that we need the entire staff back in the office, ",
            "TPS reports cannot be filed properly from home."
        )
        .as_bytes()
        .to_vec(),
        &["members@example.com"],
    )
    .await;
    for (account_id, num_messages) in [(&account_id_1, 3), (&account_id_2, 2), (&account_id_3, 2)] {
        assert_eq!(
            server
                .store
                .get_document_ids(
                    JMAPId::parse(account_id).unwrap().get_document_id(),
                    Collection::Mail
                )
                .unwrap()
                .unwrap()
                .len(),
            num_messages,
            "for {}",
            account_id
        );
    }

    // Deduplication of recipients
    ingest_message(
        concat!(
            "From: bill@example.com\r\n",
            "Bcc: Undisclosed recipients;\r\n",
            "Subject: Holidays\r\n",
            "\r\n",
            "Remember to file your TPS reports before ",
            "going on holidays."
        )
        .as_bytes()
        .to_vec(),
        &[
            "members@example.com",
            "jdoe@example.com",
            "john.doe@example.com",
            "jane@example.com",
            "bill@example.com",
        ],
    )
    .await;
    for (account_id, num_messages) in [(&account_id_1, 4), (&account_id_2, 3), (&account_id_3, 3)] {
        assert_eq!(
            server
                .store
                .get_document_ids(
                    JMAPId::parse(account_id).unwrap().get_document_id(),
                    Collection::Mail
                )
                .unwrap()
                .unwrap()
                .len(),
            num_messages,
            "for {}",
            account_id
        );
    }

    // Rejection of unknown recipients
    assert_eq!(
        ingest_message(
            concat!(
                "From: bill@example.com\r\n",
                "To: unknown@example.com\r\n",
                "Subject: Holidays\r\n",
                "\r\n",
                "Remember to file your TPS reports before ",
                "going on holidays."
            )
            .as_bytes()
            .to_vec(),
            &["unknown@example.com"],
        )
        .await,
        vec![Dsn {
            to: "unknown@example.com".to_string(),
            status: DeliveryStatus::Failure,
            reason: "Recipient does not exist.".to_string().into()
        }]
    );

    // Remove test data
    for account_id in [&account_id_1, &account_id_2, &account_id_3] {
        client
            .set_default_account_id(JMAPId::new(SUPERUSER_ID as u64))
            .principal_destroy(account_id)
            .await
            .unwrap();
    }
    client.principal_destroy(&list_id).await.unwrap();
    client.principal_destroy(&domain_id).await.unwrap();
    server.store.assert_is_empty();
}
