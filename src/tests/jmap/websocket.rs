use std::time::Duration;

use actix_web::web;
use futures::StreamExt;
use jmap::id::JMAPIdSerialize;
use jmap_client::{
    client::Client,
    client_ws::WebSocketMessage,
    core::{
        response::{MethodResponse, Response},
        set::Create,
    },
    TypeState,
};
use store::Store;
use tokio::sync::mpsc;

use crate::{tests::store::utils::StoreCompareWith, JMAPServer};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    let mut ws_stream = client.connect_ws().await.unwrap();

    let (stream_tx, mut stream_rx) = mpsc::channel::<WebSocketMessage>(100);

    tokio::spawn(async move {
        while let Some(change) = ws_stream.next().await {
            stream_tx.send(change.unwrap()).await.unwrap();
        }
    });

    // Create mailbox
    let mut request = client.set_default_account_id(1u64.to_jmap_string()).build();
    let create_id = request
        .set_mailbox()
        .create()
        .name("WebSocket Test")
        .create_id()
        .unwrap();
    let request_id = request.send_ws().await.unwrap();
    let response = expect_response(&mut stream_rx).await;
    assert_eq!(request_id, response.request_id().unwrap());
    let mailbox_id = response
        .unwrap_method_response()
        .unwrap_set_mailbox()
        .unwrap()
        .created(&create_id)
        .unwrap()
        .unwrap_id();

    // Enable push notifications
    client
        .enable_push_ws(None::<Vec<_>>, None::<&str>)
        .await
        .unwrap();

    // Make changes over standard HTTP and expect a push notification via WebSockets
    client
        .mailbox_update_sort_order(&mailbox_id, 1)
        .await
        .unwrap();
    assert_state(&mut stream_rx, TypeState::Mailbox).await;

    // Multiple changes should be grouped and delivered in intervals
    for num in 0..5 {
        client
            .mailbox_update_sort_order(&mailbox_id, num)
            .await
            .unwrap();
    }
    assert_state(&mut stream_rx, TypeState::Mailbox).await;
    expect_nothing(&mut stream_rx).await;

    // Disable push notifications
    client.disable_push_ws().await.unwrap();

    // No more changes should be received
    let mut request = client.build();
    request.set_mailbox().destroy([&mailbox_id]);
    request.send_ws().await.unwrap();
    expect_response(&mut stream_rx)
        .await
        .unwrap_method_response()
        .unwrap_set_mailbox()
        .unwrap()
        .destroyed(&mailbox_id)
        .unwrap();
    expect_nothing(&mut stream_rx).await;

    server.store.assert_is_empty();
}

async fn expect_response(
    stream_rx: &mut mpsc::Receiver<WebSocketMessage>,
) -> Response<MethodResponse> {
    match tokio::time::timeout(Duration::from_millis(100), stream_rx.recv()).await {
        Ok(Some(message)) => match message {
            WebSocketMessage::Response(response) => response,
            _ => panic!("Expected response, got: {:?}", message),
        },
        result => {
            panic!("Timeout waiting for websocket: {:?}", result);
        }
    }
}

async fn assert_state(stream_rx: &mut mpsc::Receiver<WebSocketMessage>, state: TypeState) {
    match tokio::time::timeout(Duration::from_millis(700), stream_rx.recv()).await {
        Ok(Some(message)) => match message {
            WebSocketMessage::StateChange(changes) => {
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
            _ => panic!("Expected state change, got: {:?}", message),
        },
        result => {
            panic!("Timeout waiting for websocket: {:?}", result);
        }
    }
}

async fn expect_nothing(stream_rx: &mut mpsc::Receiver<WebSocketMessage>) {
    match tokio::time::timeout(Duration::from_millis(1000), stream_rx.recv()).await {
        Err(_) => {}
        message => {
            panic!("Received a message when expecting nothing: {:?}", message);
        }
    }
}
