/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use actix_web::web;
use jmap::{types::jmap::JMAPId, SUPERUSER_ID};
use jmap_client::client::Client;
use jmap_sharing::principal::set::JMAPSetPrincipal;
use store::{
    chrono::{Duration, Utc},
    Store,
};

use crate::{
    tests::{
        jmap_mail::{
            email_submission::{
                assert_message_delivery, expect_nothing, spawn_mock_smtp_server, MockMessage,
            },
            lmtp::SmtpConnection,
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
    let domain_id = client
        .set_default_account_id(JMAPId::new(SUPERUSER_ID as u64))
        .domain_create("example.com")
        .await
        .unwrap()
        .take_id();
    let account_id = client
        .individual_create("jdoe@example.com", "12345", "John Doe")
        .await
        .unwrap()
        .take_id();

    // Start mock SMTP server
    let (mut smtp_rx, smtp_settings) = spawn_mock_smtp_server();

    // Let people know that we'll be down in Kokomo
    client
        .set_default_account_id(&account_id)
        .vacation_response_create(
            "Off the Florida Keys there's a place called Kokomo",
            "That's where you wanna go to get away from it all".into(),
            "That's where <b>you wanna go</b> to get away from it all".into(),
        )
        .await
        .unwrap();

    // Connect to LMTP service
    let mut lmtp = SmtpConnection::connect().await;

    // Send a message
    lmtp.ingest(
        "bill@example.com",
        &["jdoe@example.com"],
        concat!(
            "From: bill@example.com\r\n",
            "To: jdoe@example.com\r\n",
            "Subject: TPS Report\r\n",
            "\r\n",
            "I'm going to need those TPS reports ASAP. ",
            "So, if you could do that, that'd be great."
        ),
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
    lmtp.ingest(
        "bill@example.com",
        &["jdoe@example.com"],
        concat!(
            "From: bill@example.com\r\n",
            "To: jdoe@example.com\r\n",
            "Subject: TPS Report -- friendly reminder\r\n",
            "\r\n",
            "Listen, are you gonna have those TPS reports for us this afternoon?",
        ),
    )
    .await;

    expect_nothing(&mut smtp_rx).await;

    // Messages from MAILER-DAEMON should not
    // trigger a vacation response
    lmtp.ingest(
        "MAILER-DAEMON@example.com",
        &["jdoe@example.com"],
        concat!(
            "From: MAILER-DAEMON@example.com\r\n",
            "To: jdoe@example.com\r\n",
            "Subject: Delivery Failure\r\n",
            "\r\n",
            "I tried so hard and got so far but in the end it wasn't delivered.",
        ),
    )
    .await;

    expect_nothing(&mut smtp_rx).await;

    // Vacation responses should honor the configured date ranges
    client
        .vacation_response_set_dates((Utc::now() + Duration::days(1)).timestamp().into(), None)
        .await
        .unwrap();
    lmtp.ingest(
        "jane_smith@example.com",
        &["jdoe@example.com"],
        concat!(
            "From: jane_smith@example.com\r\n",
            "To: jdoe@example.com\r\n",
            "Subject: When were you going on holidays?\r\n",
            "\r\n",
            "I'm asking because Bill really wants those TPS reports.",
        ),
    )
    .await;

    expect_nothing(&mut smtp_rx).await;

    client
        .vacation_response_set_dates((Utc::now() - Duration::days(1)).timestamp().into(), None)
        .await
        .unwrap();
    smtp_settings.lock().do_stop = true;
    lmtp.ingest(
        "jane_smith@example.com",
        &["jdoe@example.com"],
        concat!(
            "From: jane_smith@example.com\r\n",
            "To: jdoe@example.com\r\n",
            "Subject: When were you going on holidays?\r\n",
            "\r\n",
            "I'm asking because Bill really wants those TPS reports.",
        ),
    )
    .await;
    lmtp.quit().await;

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

    // Remove test data
    for account_id in [&account_id, &domain_id] {
        client
            .set_default_account_id(JMAPId::new(SUPERUSER_ID as u64))
            .principal_destroy(account_id)
            .await
            .unwrap();
    }
    server.store.principal_purge().unwrap();
    server.store.assert_is_empty();
}
