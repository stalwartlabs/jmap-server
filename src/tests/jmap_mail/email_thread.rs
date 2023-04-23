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

use jmap::types::jmap::JMAPId;
use jmap_client::{client::Client, mailbox::Role};
use store::Store;

use crate::{tests::store::utils::StoreCompareWith, JMAPServer};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Running Email Thread tests...");

    let mailbox_id = client
        .set_default_account_id(JMAPId::new(1).to_string())
        .mailbox_create("JMAP Get", None::<String>, Role::None)
        .await
        .unwrap()
        .take_id();

    // A simple thread that uses in-reply-to to link messages together
    let thread_1 = vec![
        client.email_import(
"Message-ID: <t1-msg1>
From: test1@example.com
To: test2@example.com
Subject: my thread

message here!".into(),
            [&mailbox_id],
            None::<Vec<String>>,
            Some(1),
        ).await.unwrap(),

        client.email_import(
"Message-ID: <t1-msg2>
From: test2@example.com
To: test1@example.com
In-Reply-To: <t1-msg1>
Subject: Re: my thread

reply here!".into(),
            [&mailbox_id],
            None::<Vec<String>>,
            Some(2),
        ).await.unwrap(),

        client.email_import(
"Message-ID: <t1-msg3>
From: test1@example.com
To: test2@example.com
In-Reply-To: <t1-msg2>
Subject: Re: my thread

last reply".into(),
            [&mailbox_id],
            None::<Vec<String>>,
            Some(3),
        ).await.unwrap(),
    ];

    // Another simple thread, but this time with a shared reference header instead
    let thread_2 = vec![
        client.email_import(
"Message-ID: <t2-msg1>
From: test1@example.com
To: test2@example.com
Subject: my thread

message here!".into(),
            [&mailbox_id],
            None::<Vec<String>>,
            Some(1),
        ).await.unwrap(),

        client.email_import(
"Message-ID: <t2-msg2>
References: <t2-msg1>
From: test2@example.com
To: test1@example.com
Subject: my thread

reply here!".into(),
            [&mailbox_id],
            None::<Vec<String>>,
            Some(2),
        ).await.unwrap(),

        client.email_import(
"Message-ID: <t2-msg3>
References: <t2-msg1>
From: test1@example.com
To: test2@example.com
Subject: my thread

reply here!".into(),
            [&mailbox_id],
            None::<Vec<String>>,
            Some(3),
        ).await.unwrap(),
    ];

    // Make sure none of the separate threads end up with the same thread ID
    assert_ne!(
        thread_1.first().unwrap().thread_id().unwrap(),
        thread_2.first().unwrap().thread_id().unwrap(),
        "Making sure thread 1 and thread 2 have different thread IDs"
    );

    // Make sure each message in each thread ends up with the right thread ID
    assert_thread_ids_match(client, &thread_1, "thread chained with In-Reply-To header").await;
    assert_thread_ids_match(client, &thread_2, "thread with References header").await;

    client.mailbox_destroy(&mailbox_id, true).await.unwrap();

    server.store.assert_is_empty();
}

async fn assert_thread_ids_match(client: &mut Client, emails: &Vec<jmap_client::email::Email>, description: &str) {
    let thread_id = emails.first().unwrap().thread_id().unwrap();

    println!("    Testing {}...", description);

    // First, make sure the thread ID is the same for all messages in the thread
    for email in emails {
        assert_eq!(
            email.thread_id().unwrap(),
            thread_id,
            "Comparing thread IDs of messages in: {}",
            description
        );
    }

    // Next, make sure querying the thread yields the same messages
    let full_thread = client.thread_get(thread_id).await.unwrap().unwrap();
    let mut email_ids_in_fetched_thread = full_thread.email_ids().iter().map(|x| x.clone()).collect::<Vec<_>>();
    email_ids_in_fetched_thread.sort();

    let mut expected_email_ids = emails.iter().map(|email| email.id().unwrap()).collect::<Vec<_>>();
    expected_email_ids.sort();

    assert_eq!(
        email_ids_in_fetched_thread,
        expected_email_ids,
        "Comparing email IDs in: {}",
        description
    );
}
