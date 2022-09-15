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

use std::{sync::Arc, time::Duration};

use actix_web::web;
use futures::future::join_all;
use jmap::{orm::serialize::JMAPOrm, types::jmap::JMAPId};
use jmap_client::{
    client::Client,
    core::set::{SetErrorType, SetObject},
    mailbox::{self, Mailbox, Role},
};
use jmap_mail::{
    mail::{schema::Email, MessageField},
    mailbox::get::JMAPGetMailbox,
};
use store::{
    ahash::AHashSet,
    core::{collection::Collection, tag::Tag},
    rand::{self, Rng},
    serialize::key::BitmapKey,
    AccountId, ColumnFamily, Store,
};
use store_rocksdb::RocksDB;

use crate::{
    tests::store::utils::{destroy_temp_dir, StoreCompareWith},
    JMAPServer,
};

use super::init_jmap_tests;

const TEST_USER_ID: AccountId = 1;
const NUM_PASSES: usize = 1;

#[actix_web::test]
#[ignore]
async fn jmap_stress_tests() {
    let (server, client, temp_dir) = init_jmap_tests::<RocksDB>("jmap_stress_tests").await;

    let client = Arc::new(client);

    email_tests(server.clone(), client.clone()).await;
    mailbox_tests(server.clone(), client.clone()).await;

    destroy_temp_dir(&temp_dir);
}

async fn email_tests<T>(server: web::Data<JMAPServer<T>>, client: Arc<Client>)
where
    T: for<'x> Store<'x> + 'static,
{
    for pass in 0..NUM_PASSES {
        println!("----------------- PASS {} -----------------", pass);
        let mailboxes = Arc::new(vec![
            client
                .mailbox_create("Inbox", None::<String>, Role::Inbox)
                .await
                .unwrap()
                .take_id(),
            client
                .mailbox_create("Trash", None::<String>, Role::Trash)
                .await
                .unwrap()
                .take_id(),
            client
                .mailbox_create("Archive", None::<String>, Role::Archive)
                .await
                .unwrap()
                .take_id(),
        ]);
        let mut futures = Vec::new();

        for num in 0..1000 {
            match rand::thread_rng().gen_range(0..3) {
                0 => {
                    let client = client.clone();
                    let mailboxes = mailboxes.clone();
                    futures.push(tokio::spawn(async move {
                        let mailbox_num =
                            rand::thread_rng().gen_range::<usize, _>(0..mailboxes.len());
                        let _message_id = client
                            .email_import(
                                format!(
                                    concat!(
                                        "From: test@test.com\n",
                                        "To: test@test.com\r\n",
                                        "Subject: test {}\r\n\r\ntest {}\r\n"
                                    ),
                                    num, num
                                )
                                .into_bytes(),
                                [&mailboxes[mailbox_num]],
                                None::<Vec<String>>,
                                None,
                            )
                            .await
                            .unwrap()
                            .take_id();
                        //println!("Inserted message {}.", message_id);
                    }));
                }

                1 => {
                    let client = client.clone();
                    futures.push(tokio::spawn(async move {
                        let mut req = client.build();
                        req.query_email();
                        let ids = req.send_query_email().await.unwrap().take_ids();
                        if !ids.is_empty() {
                            let message_id = &ids[rand::thread_rng().gen_range(0..ids.len())];
                            //println!("Deleting message {}.", message_id);
                            match client.email_destroy(message_id).await {
                                Ok(_) => (),
                                Err(jmap_client::Error::Set(err)) => match err.error() {
                                    SetErrorType::NotFound => {}
                                    _ => {
                                        panic!("Unexpected error: {:?}", err);
                                    }
                                },
                                Err(err) => {
                                    panic!("Unexpected error: {:?}", err);
                                }
                            }
                        }
                    }));
                }
                _ => {
                    let client = client.clone();
                    let mailboxes = mailboxes.clone();
                    futures.push(tokio::spawn(async move {
                        let mut req = client.build();
                        let ref_id = req.query_email().result_reference();
                        req.get_email()
                            .ids_ref(ref_id)
                            .properties([jmap_client::email::Property::MailboxIds]);
                        let emails = req
                            .send()
                            .await
                            .unwrap()
                            .unwrap_method_responses()
                            .pop()
                            .unwrap()
                            .unwrap_get_email()
                            .unwrap()
                            .take_list();

                        if !emails.is_empty() {
                            let message = &emails[rand::thread_rng().gen_range(0..emails.len())];
                            let message_id = message.id().unwrap();
                            let mailbox_ids = message.mailbox_ids();
                            assert_eq!(mailbox_ids.len(), 1, "{:#?}", message);
                            let mailbox_id = mailbox_ids.last().unwrap();
                            loop {
                                let new_mailbox_id =
                                    &mailboxes[rand::thread_rng().gen_range(0..mailboxes.len())];
                                if new_mailbox_id != mailbox_id {
                                    /*println!(
                                        "Moving message {} from {} to {}.",
                                        message_id, mailbox_id, new_mailbox_id
                                    );*/
                                    let mut req = client.build();
                                    req.set_email()
                                        .update(message_id)
                                        .mailbox_ids([new_mailbox_id]);
                                    req.send_set_email().await.unwrap();

                                    break;
                                }
                            }
                        }
                    }));
                }
            }
            tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(5..10))).await;
        }

        join_all(futures).await;

        server.store.db.compact(ColumnFamily::Bitmaps).unwrap();

        let email_ids = server
            .store
            .get_document_ids(TEST_USER_ID, Collection::Mail)
            .unwrap()
            .unwrap_or_default();
        let mailbox_ids = server
            .store
            .get_document_ids(TEST_USER_ID, Collection::Mailbox)
            .unwrap()
            .unwrap_or_default();
        assert_eq!(mailbox_ids.len(), 3);

        for mailbox in mailboxes.iter() {
            let mailbox_id = JMAPId::parse(mailbox).unwrap().get_document_id();
            let email_ids_in_mailbox = server
                .store
                .mailbox_tags(TEST_USER_ID, mailbox_id)
                .unwrap()
                .unwrap_or_default();
            let mut email_ids_check = email_ids_in_mailbox.clone();
            email_ids_check &= &email_ids;
            assert_eq!(email_ids_in_mailbox, email_ids_check);

            //println!("Emails {:?}", email_ids_in_mailbox);

            for email_id in &email_ids_in_mailbox {
                let email = server
                    .store
                    .get_orm::<Email>(TEST_USER_ID, email_id)
                    .unwrap();

                if let Some(email) = email {
                    if let Some(mailbox_tags) =
                        email.get_tags(&jmap_mail::mail::schema::Property::MailboxIds)
                    {
                        if mailbox_tags.len() != 1 {
                            panic!(
                            "Email ORM has more than one mailbox {:?}! Id {} in mailbox {} with messages {:?}",
                            mailbox_tags, email_id, mailbox_id, email_ids_in_mailbox
                        );
                        }
                        let mailbox_tag = mailbox_tags.iter().next().unwrap().as_id();
                        if mailbox_tag != mailbox_id {
                            panic!(
                                concat!(
                                    "Email ORM has an unexpected mailbox tag {}! Id {} in ",
                                    "mailbox {} with messages {:?} and key {:?}"
                                ),
                                mailbox_tag,
                                email_id,
                                mailbox_id,
                                email_ids_in_mailbox,
                                BitmapKey::serialize_tag(
                                    TEST_USER_ID,
                                    Collection::Mail,
                                    MessageField::Mailbox.into(),
                                    &Tag::Id(mailbox_id)
                                )
                            );
                        }
                    } else {
                        panic!(
                            "Email ORM has no tags! Id {} in mailbox {} with messages {:?}",
                            email_id, mailbox_id, email_ids_in_mailbox
                        );
                    }
                } else {
                    panic!(
                        "Email ORM not found! Id {} in mailbox {} with messages {:?}",
                        email_id, mailbox_id, email_ids_in_mailbox
                    );
                }
            }
        }

        for mailbox_id in mailboxes.iter() {
            client.mailbox_destroy(mailbox_id, true).await.unwrap();
        }

        server.store.assert_is_empty();
    }
}

async fn mailbox_tests<T>(server: web::Data<JMAPServer<T>>, client: Arc<Client>)
where
    T: for<'x> Store<'x> + 'static,
{
    let mailboxes = Arc::new(vec![
        "test/test1/test2/test3".to_string(),
        "test1/test2/test3".to_string(),
        "test2/test3/test4".to_string(),
        "test3/test4/test5".to_string(),
        "test4".to_string(),
        "test5".to_string(),
    ]);
    let mut futures = Vec::new();

    for _ in 0..1000 {
        match rand::thread_rng().gen_range(0..=3) {
            0 => {
                for pos in 0..mailboxes.len() {
                    let client = client.clone();
                    let mailboxes = mailboxes.clone();
                    futures.push(tokio::spawn(async move {
                        create_mailbox(&client, &mailboxes[pos]).await;
                    }));
                }
            }

            1 => {
                let client = client.clone();
                futures.push(tokio::spawn(async move {
                    query_mailboxes(&client).await;
                }));
            }

            2 => {
                let client = client.clone();
                futures.push(tokio::spawn(async move {
                    for mailbox_id in client
                        .mailbox_query(None::<mailbox::query::Filter>, None::<Vec<_>>)
                        .await
                        .unwrap()
                        .take_ids()
                    {
                        let client = client.clone();
                        tokio::spawn(async move {
                            delete_mailbox(&client, &mailbox_id).await;
                        });
                    }
                }));
            }

            _ => {
                let client = client.clone();
                futures.push(tokio::spawn(async move {
                    let mut ids = client
                        .mailbox_query(None::<mailbox::query::Filter>, None::<Vec<_>>)
                        .await
                        .unwrap()
                        .take_ids();
                    if !ids.is_empty() {
                        let id = ids.swap_remove(rand::thread_rng().gen_range(0..ids.len()));
                        let sort_order = rand::thread_rng().gen_range(0..100);
                        client.mailbox_update_sort_order(&id, sort_order).await.ok();
                    }
                }));
            }
        }
        tokio::time::sleep(Duration::from_millis(rand::thread_rng().gen_range(5..10))).await;
    }

    join_all(futures).await;

    let mailbox_ids = client
        .mailbox_query(None::<mailbox::query::Filter>, None::<Vec<_>>)
        .await
        .unwrap()
        .take_ids();
    //println!("Will delete {:?}", mailbox_ids);

    let mut deleted_ids = AHashSet::new();
    while mailbox_ids.len() != deleted_ids.len() {
        for mailbox_id in &mailbox_ids {
            if !deleted_ids.contains(mailbox_id) {
                match client.mailbox_destroy(mailbox_id, true).await {
                    Ok(_) => {
                        deleted_ids.insert(mailbox_id);
                    }
                    Err(jmap_client::Error::Set(err)) => match err.error() {
                        SetErrorType::NotFound => {
                            deleted_ids.insert(mailbox_id);
                        }
                        SetErrorType::MailboxHasChild => (),
                        _ => {
                            panic!("Unexpected error: {:?}", err);
                        }
                    },
                    Err(err) => {
                        panic!("Unexpected error: {:?}", err);
                    }
                }
            }
        }
    }

    assert_eq!(
        client
            .mailbox_query(None::<mailbox::query::Filter>, None::<Vec<_>>)
            .await
            .unwrap()
            .take_ids(),
        Vec::<String>::new()
    );

    server.store.assert_is_empty();
}

async fn create_mailbox(client: &Client, mailbox: &str) -> Vec<String> {
    let mut request = client.build();
    let mut create_ids: Vec<String> = Vec::new();
    let set_request = request.set_mailbox();
    for path_item in mailbox.split('/') {
        let create_item = set_request.create().name(path_item);
        if let Some(create_id) = create_ids.last() {
            create_item.parent_id_ref(create_id);
        }
        create_ids.push(create_item.create_id().unwrap());
    }
    let mut response = request.send_set_mailbox().await.unwrap();
    let mut ids = Vec::with_capacity(create_ids.len());
    for create_id in create_ids {
        if let Ok(mut id) = response.created(&create_id) {
            ids.push(id.take_id());
        }
    }
    ids
}

async fn query_mailboxes(client: &Client) -> Vec<Mailbox> {
    let mut request = client.build();
    let query_result = request
        .query_mailbox()
        .calculate_total(true)
        .result_reference();
    request.get_mailbox().ids_ref(query_result).properties([
        jmap_client::mailbox::Property::Id,
        jmap_client::mailbox::Property::Name,
        jmap_client::mailbox::Property::IsSubscribed,
        jmap_client::mailbox::Property::ParentId,
        jmap_client::mailbox::Property::Role,
        jmap_client::mailbox::Property::TotalEmails,
        jmap_client::mailbox::Property::UnreadEmails,
    ]);

    request
        .send()
        .await
        .unwrap()
        .unwrap_method_responses()
        .pop()
        .unwrap()
        .unwrap_get_mailbox()
        .unwrap()
        .take_list()
}

async fn delete_mailbox(client: &Client, mailbox_id: &str) {
    match client.mailbox_destroy(mailbox_id, true).await {
        Ok(_) => (),
        Err(err) => match err {
            jmap_client::Error::Set(_) => (),
            _ => panic!("Failed: {:?}", err),
        },
    }
}
