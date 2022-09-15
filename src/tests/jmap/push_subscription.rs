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

use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, Ordering},
    time::Duration,
};

use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer};
use ece::EcKeyComponents;
use jmap::{
    base64,
    types::{jmap::JMAPId, type_state::TypeState},
};
use jmap_client::{client::Client, mailbox::Role, push_subscription::Keys};
use reqwest::header::CONTENT_ENCODING;
use store::{ahash::AHashSet, Store};
use tokio::sync::mpsc;

use crate::{
    api::StateChangeResponse, cluster::rpc::tls::load_tls_server_config,
    tests::store::utils::StoreCompareWith, JMAPServer,
};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Running Push Subscription tests...");

    // Create channels
    let (event_tx, mut event_rx) = mpsc::channel::<PushMessage>(100);

    // Create subscription keys
    let (keypair, auth_secret) = ece::generate_keypair_and_auth_secret().unwrap();
    let pubkey = keypair.pub_as_raw().unwrap();
    let keys = Keys::new(&pubkey, &auth_secret);

    let push_server = web::Data::new(PushServer {
        keypair: keypair.raw_components().unwrap(),
        auth_secret: auth_secret.to_vec(),
        tx: event_tx,
        fail_requests: false.into(),
    });
    let data = push_server.clone();

    // Start mock push server
    actix_web::rt::spawn(async move {
        let mut pem_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pem_dir.push("src");
        pem_dir.push("tests");
        pem_dir.push("resources");
        pem_dir.push("cert.pem");
        let cert = pem_dir.to_str().unwrap().to_string();
        pem_dir.set_file_name("key.pem");
        let key = pem_dir.to_str().unwrap().to_string();

        HttpServer::new(move || {
            App::new()
                .wrap(middleware::Logger::default())
                .app_data(data.clone())
                .route("/push", web::post().to(handle_push))
        })
        .bind_rustls("127.0.0.1:9000", load_tls_server_config(&cert, &key))?
        .run()
        .await
    });

    // Register push notification (no encryption)
    let push_id = client
        .push_subscription_create("123", "https://127.0.0.1:9000/push", None)
        .await
        .unwrap()
        .take_id();

    // Expect push verification
    let verification = expect_push(&mut event_rx).await.unwrap_verification();
    assert_eq!(verification.push_subscription_id, push_id);

    // Update verification code
    client
        .push_subscription_verify(&push_id, verification.verification_code)
        .await
        .unwrap();

    // Create a mailbox and expect a state change
    let mailbox_id = client
        .set_default_account_id(JMAPId::new(1).to_string())
        .mailbox_create("PushSubscription Test", None::<String>, Role::None)
        .await
        .unwrap()
        .take_id();

    assert_state(&mut event_rx, &[TypeState::Mailbox]).await;

    // Receive states just for the requested types
    client
        .push_subscription_update_types(&push_id, [jmap_client::TypeState::Email].into())
        .await
        .unwrap();
    client
        .mailbox_update_sort_order(&mailbox_id, 123)
        .await
        .unwrap();
    expect_nothing(&mut event_rx).await;

    // Destroy subscription
    client.push_subscription_destroy(&push_id).await.unwrap();

    // Only one verification per minute is allowed
    let push_id = client
        .push_subscription_create("invalid", "https://127.0.0.1:9000/push", None)
        .await
        .unwrap()
        .take_id();
    expect_nothing(&mut event_rx).await;
    client.push_subscription_destroy(&push_id).await.unwrap();

    // Register push notification (with encryption)
    let push_id = client
        .push_subscription_create(
            "123",
            "https://127.0.0.1:9000/push?skip_checks=true", // skip_checks only works in cfg(test)
            keys.into(),
        )
        .await
        .unwrap()
        .take_id();

    // Expect push verification
    let verification = expect_push(&mut event_rx).await.unwrap_verification();
    assert_eq!(verification.push_subscription_id, push_id);

    // Update verification code
    client
        .push_subscription_verify(&push_id, verification.verification_code)
        .await
        .unwrap();

    // Failed deliveries should be re-attempted
    push_server.fail_requests.store(true, Ordering::Relaxed);
    client
        .mailbox_update_sort_order(&mailbox_id, 101)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    push_server.fail_requests.store(false, Ordering::Relaxed);
    assert_state(&mut event_rx, &[TypeState::Mailbox]).await;

    // Make a mailbox change and expect state change
    client
        .mailbox_rename(&mailbox_id, "My Mailbox")
        .await
        .unwrap();
    assert_state(&mut event_rx, &[TypeState::Mailbox]).await;

    // Multiple change updates should be grouped and pushed in intervals
    for num in 0..50 {
        client
            .mailbox_update_sort_order(&mailbox_id, num)
            .await
            .unwrap();
    }
    assert_state(&mut event_rx, &[TypeState::Mailbox]).await;
    expect_nothing(&mut event_rx).await;

    // Destroy mailbox
    client.push_subscription_destroy(&push_id).await.unwrap();
    client.mailbox_destroy(&mailbox_id, true).await.unwrap();
    expect_nothing(&mut event_rx).await;

    server.store.assert_is_empty();
}

struct PushServer {
    keypair: EcKeyComponents,
    auth_secret: Vec<u8>,
    tx: mpsc::Sender<PushMessage>,
    fail_requests: AtomicBool,
}

#[derive(serde::Deserialize, Debug)]
#[serde(untagged)]
enum PushMessage {
    StateChange(StateChangeResponse),
    Verification(PushVerification),
}

impl PushMessage {
    pub fn unwrap_state_change(self) -> StateChangeResponse {
        match self {
            PushMessage::StateChange(state_change) => state_change,
            _ => panic!("Expected StateChange"),
        }
    }

    pub fn unwrap_verification(self) -> PushVerification {
        match self {
            PushMessage::Verification(verification) => verification,
            _ => panic!("Expected Verification"),
        }
    }
}

#[derive(serde::Deserialize, Debug)]
enum PushVerificationType {
    PushVerification,
}

#[derive(serde::Deserialize, Debug)]
struct PushVerification {
    #[serde(rename = "@type")]
    _type: PushVerificationType,
    #[serde(rename = "pushSubscriptionId")]
    pub push_subscription_id: String,
    #[serde(rename = "verificationCode")]
    pub verification_code: String,
}

async fn handle_push(
    payload: web::Bytes,
    request: HttpRequest,
    data: web::Data<PushServer>,
) -> HttpResponse {
    if data.fail_requests.load(Ordering::Relaxed) {
        return HttpResponse::InternalServerError().finish();
    }

    let is_encrypted = request
        .headers()
        .get(CONTENT_ENCODING)
        .map_or(false, |encoding| encoding.to_str().unwrap() == "aes128gcm");

    let message = serde_json::from_slice::<PushMessage>(&if is_encrypted {
        ece::decrypt(
            &data.keypair,
            &data.auth_secret,
            &base64::decode_config(payload, base64::URL_SAFE).unwrap(),
        )
        .unwrap()
    } else {
        payload.to_vec()
    })
    .unwrap();

    //println!("Push received ({}): {:?}", is_encrypted, message);

    data.tx.send(message).await.unwrap();

    HttpResponse::Ok().body("")
}

async fn expect_push(event_rx: &mut mpsc::Receiver<PushMessage>) -> PushMessage {
    match tokio::time::timeout(Duration::from_millis(1500), event_rx.recv()).await {
        Ok(Some(push)) => push,
        result => {
            panic!("Timeout waiting for push: {:?}", result);
        }
    }
}

async fn expect_nothing(event_rx: &mut mpsc::Receiver<PushMessage>) {
    match tokio::time::timeout(Duration::from_millis(1000), event_rx.recv()).await {
        Err(_) => {}
        message => {
            panic!("Received a message when expecting nothing: {:?}", message);
        }
    }
}

async fn assert_state(event_rx: &mut mpsc::Receiver<PushMessage>, state: &[TypeState]) {
    assert_eq!(
        expect_push(event_rx)
            .await
            .unwrap_state_change()
            .changed
            .get(&JMAPId::new(1))
            .unwrap()
            .iter()
            .map(|x| x.0)
            .collect::<AHashSet<&TypeState>>(),
        state.iter().collect::<AHashSet<&TypeState>>()
    );
}
