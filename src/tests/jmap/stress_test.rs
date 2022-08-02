use std::{sync::Arc, time::Duration};

use futures::future::join_all;
use jmap_client::{
    client::Client,
    core::set::{SetErrorType, SetObject},
    mailbox::{self, Mailbox, Property},
};
use store::{
    ahash::AHashSet,
    rand::{self, Rng},
};
use store_rocksdb::RocksDB;

use crate::tests::store::utils::{destroy_temp_dir, StoreCompareWith};

use super::init_jmap_tests;

#[actix_web::test]
async fn jmap_stress_tests() {
    let (server, client, temp_dir) = init_jmap_tests::<RocksDB>("jmap_stress_tests").await;

    let mailboxes = Arc::new(vec![
        "test/test1/test2/test3".to_string(),
        "test1/test2/test3".to_string(),
        "test2/test3/test4".to_string(),
        "test3/test4/test5".to_string(),
        "test4".to_string(),
        "test5".to_string(),
    ]);
    let client = Arc::new(client);
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

    destroy_temp_dir(temp_dir);
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
        Property::Id,
        Property::Name,
        Property::IsSubscribed,
        Property::ParentId,
        Property::Role,
        Property::TotalEmails,
        Property::UnreadEmails,
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
