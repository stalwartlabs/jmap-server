use std::time::Duration;

use actix_web::web;
use futures::{pin_mut, StreamExt};
use jmap::id::JMAPIdSerialize;
use jmap_client::{client::Client, event_source::Changes, mailbox::Role, TypeState};
use store::Store;
use tokio::{
    sync::mpsc,
    time::{self},
};

use crate::{tests::store::utils::StoreCompareWith, JMAPServer};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    let changes = client
        .event_source(None::<Vec<_>>, false, 1.into(), None)
        .await
        .unwrap();

    let (event_tx, mut event_rx) = mpsc::channel::<Changes>(100);

    tokio::spawn(async move {
        pin_mut!(changes);

        while let Some(change) = changes.next().await {
            event_tx.send(change.unwrap()).await.unwrap();
        }
    });

    let mailbox_id = client
        .set_default_account_id(1u64.to_jmap_string())
        .mailbox_create("EventSource Test", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();

    assert_state(&mut event_rx, TypeState::Mailbox).await;
    assert_ping(&mut event_rx).await; // Pings are only received in cfg(test)

    client
        .email_import(
            b"From: test@test.com\nSubject: hey\n\ntest".to_vec(),
            [&mailbox_id],
            None::<Vec<&str>>,
            None,
        )
        .await
        .unwrap();

    assert_state(&mut event_rx, TypeState::Email).await;

    client.mailbox_destroy(&mailbox_id, true).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;

    assert_state(&mut event_rx, TypeState::Mailbox).await;
    assert_ping(&mut event_rx).await;
    assert_ping(&mut event_rx).await;

    server.store.assert_is_empty();
}

async fn assert_state(event_rx: &mut mpsc::Receiver<Changes>, state: TypeState) {
    match time::timeout(Duration::from_millis(100), event_rx.recv()).await {
        Ok(Some(changes)) => {
            //println!("received {:?}", changes);
            assert_eq!(
                changes
                    .changes(&1u64.to_jmap_string())
                    .unwrap()
                    .next()
                    .unwrap()
                    .0,
                &state
            );
        }
        result => {
            panic!("Timeout waiting for event {:?}: {:?}", state, result);
        }
    }
}

async fn assert_ping(event_rx: &mut mpsc::Receiver<Changes>) {
    match time::timeout(Duration::from_millis(1100), event_rx.recv()).await {
        Ok(Some(changes)) => {
            //println!("received {:?}", changes);
            assert!(changes.changes("ping").is_some(),);
        }
        _ => {
            panic!("Did not receive ping.");
        }
    }
}
