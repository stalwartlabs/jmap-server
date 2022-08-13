use std::sync::Arc;

use actix_web::web;
use jmap::types::jmap::JMAPId;
use jmap_client::{client::Client, mailbox::Role};
use store::{
    chrono::{Duration, Utc},
    RecipientType, Store,
};

use crate::{
    tests::{
        jmap_mail::{
            email_submission::{
                assert_message_delivery, expect_nothing, spawn_mock_smtp_server, MockMessage,
            },
            ingest_message,
        },
        store::utils::StoreCompareWith,
    },
    JMAPServer,
};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Running Vacation Response tests...");

    // Create INBOX
    let mailbox_id = client
        .set_default_account_id(JMAPId::new(1).to_string())
        .mailbox_create("Inbox", None::<String>, Role::Inbox)
        .await
        .unwrap()
        .take_id();
    server.store.recipients.insert(
        "jdoe@example.com".to_string(),
        Arc::new(RecipientType::Individual(1)),
    );

    // Start mock SMTP server
    let (mut smtp_rx, smtp_settings) = spawn_mock_smtp_server();

    // Let people know that we'll be down in Kokomo
    client
        .vacation_response_create(
            "Off the Florida Keys there's a place called Kokomo",
            "That's where you wanna go to get away from it all".into(),
            "That's where <b>you wanna go</b> to get away from it all".into(),
        )
        .await
        .unwrap();

    // Send a message
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

    // Await vacation response
    assert_message_delivery(
        &mut smtp_rx,
        MockMessage::new("<jdoe@example.com>", ["<bill@example.com>"], "@Kokomo"),
        false,
    )
    .await;

    // Further messages from the same recipient should not
    // trigger a vacation response
    ingest_message(
        concat!(
            "From: bill@example.com\r\n",
            "To: jdoe@example.com\r\n",
            "Subject: TPS Report -- friendly reminder\r\n",
            "\r\n",
            "Listen, are you gonna have those TPS reports for us this afternoon?",
        )
        .as_bytes()
        .to_vec(),
        &["jdoe@example.com"],
    )
    .await;
    expect_nothing(&mut smtp_rx).await;

    // Vacation responses should honor the configured date ranges
    client
        .vacation_response_set_dates((Utc::now() + Duration::days(1)).timestamp().into(), None)
        .await
        .unwrap();
    ingest_message(
        concat!(
            "From: jane_smith@example.com\r\n",
            "To: jdoe@example.com\r\n",
            "Subject: When were you going on holidays?\r\n",
            "\r\n",
            "I'm asking because Bill really wants those TPS reports.",
        )
        .as_bytes()
        .to_vec(),
        &["jdoe@example.com"],
    )
    .await;
    expect_nothing(&mut smtp_rx).await;

    client
        .vacation_response_set_dates((Utc::now() - Duration::days(1)).timestamp().into(), None)
        .await
        .unwrap();
    smtp_settings.lock().do_stop = true;
    ingest_message(
        concat!(
            "From: jane_smith@example.com\r\n",
            "To: jdoe@example.com\r\n",
            "Subject: When were you going on holidays?\r\n",
            "\r\n",
            "I'm asking because Bill really wants those TPS reports.",
        )
        .as_bytes()
        .to_vec(),
        &["jdoe@example.com"],
    )
    .await;
    assert_message_delivery(
        &mut smtp_rx,
        MockMessage::new(
            "<jdoe@example.com>",
            ["<jane_smith@example.com>"],
            "@Kokomo",
        ),
        false,
    )
    .await;

    // Delete vacation response
    client.mailbox_destroy(&mailbox_id, true).await.unwrap();
    client.vacation_response_destroy().await.unwrap();
    server.store.assert_is_empty();
}
