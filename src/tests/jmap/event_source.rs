use std::{ops::Deref, sync::Arc, time::Duration};

use actix_web::web;
use futures::{pin_mut, StreamExt};
use jmap::id::JMAPIdSerialize;
use jmap_client::{client::Client, event_source::Changes, mailbox::Role};
use store::{parking_lot::Mutex, Store};
use tokio::time::sleep;

use crate::{tests::store::utils::StoreCompareWith, JMAPServer};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    let changes = client
        .event_source(None::<Vec<_>>, false, 1.into(), None)
        .await
        .unwrap();
    let events = Arc::new(Mutex::new(Vec::new()));

    let _events = events.clone();
    tokio::spawn(async move {
        pin_mut!(changes);

        while let Some(change) = changes.next().await {
            _events.lock().push(change.unwrap());
        }
    });

    let mailbox_id = client
        .set_default_account_id(1u64.to_jmap_string())
        .mailbox_create("EventSource Test", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();

    sleep(Duration::from_millis(2000)).await;

    client
        .email_import(
            b"From: test@test.com\nSubject: hey\n\ntest".to_vec(),
            [&mailbox_id],
            None::<Vec<&str>>,
            None,
        )
        .await
        .unwrap();

    client.mailbox_destroy(&mailbox_id, true).await.unwrap();

    sleep(Duration::from_millis(3000)).await;

    assert_eq!(
        &serde_json::from_slice::<Vec<Changes>>(
            br#"[
            {
              "changes": {
                "i01": {
                  "Mailbox": "s00"
                }
              }
            },
            {
              "changes": {
                "ping": {}
              }
            },
            {
              "changes": {
                "ping": {}
              }
            },
            {
              "changes": {
                "i01": {
                  "Email": "s01"
                }
              }
            },
            {
              "changes": {
                "i01": {
                  "Mailbox": "s02"
                }
              }
            },
            {
              "changes": {
                "ping": {}
              }
            },
            {
              "changes": {
                "ping": {}
              }
            }
          ]"#
        )
        .unwrap(),
        events.lock().deref(),
    );

    server.store.assert_is_empty();
}
