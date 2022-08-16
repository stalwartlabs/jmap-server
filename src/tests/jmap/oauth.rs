use std::time::Duration;

use actix_web::web::{self, Bytes};
use jmap::{types::jmap::JMAPId, SUPERUSER_ID};
use jmap_client::{
    client::{Client, Credentials},
    mailbox::query::Filter,
};
use jmap_sharing::principal::set::JMAPSetPrincipal;
use serde::de::DeserializeOwned;
use store::{ahash::AHashMap, Store};

use crate::{
    authorization::oauth::{DeviceAuthResponse, ErrorType, OAuthMetadata, TokenResponse},
    tests::store::utils::StoreCompareWith,
    JMAPServer,
};

pub async fn test<T>(server: web::Data<JMAPServer<T>>, admin_client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Running OAuth tests...");

    // Create test account
    let domain_id = admin_client
        .set_default_account_id(JMAPId::from(SUPERUSER_ID).to_string())
        .domain_create("example.com")
        .await
        .unwrap()
        .take_id();
    let john_id = admin_client
        .individual_create("jdoe@example.com", "abcde", "John Doe")
        .await
        .unwrap()
        .take_id();

    // Obtain OAuth metadata
    let metadata: OAuthMetadata = get(&format!(
        "{}/.well-known/oauth-authorization-server",
        server.base_session.base_url()
    ))
    .await;
    //println!("OAuth metadata: {:#?}", metadata);

    // Request a device code
    let device_code_params = AHashMap::from_iter([("client_id".to_string(), "1234".to_string())]);
    let device_response: DeviceAuthResponse =
        post(&metadata.device_authorization_endpoint, &device_code_params).await;
    //println!("Device response: {:#?}", device_response);

    // Status should be pending
    let mut token_params = AHashMap::from_iter([
        ("client_id".to_string(), "1234".to_string()),
        (
            "grant_type".to_string(),
            "urn:ietf:params:oauth:grant-type:device_code".to_string(),
        ),
        (
            "device_code".to_string(),
            device_response.device_code.to_string(),
        ),
    ]);
    assert_eq!(
        post::<TokenResponse>(&metadata.token_endpoint, &token_params).await,
        TokenResponse::Error {
            error: ErrorType::AuthorizationPending
        }
    );

    // Invalidate the code by having too many unsuccessful attempts
    assert_client_auth(
        "jdoe@example.com",
        "wrongpass",
        &device_response,
        "Incorrect",
    )
    .await;
    assert_client_auth(
        "jdoe@example.com",
        "wrongpass",
        &device_response,
        "Invalid or expired authentication code.",
    )
    .await;
    assert_eq!(
        post::<TokenResponse>(&metadata.token_endpoint, &token_params).await,
        TokenResponse::Error {
            error: ErrorType::AccessDenied
        }
    );

    // Request a new device code
    let device_response: DeviceAuthResponse =
        post(&metadata.device_authorization_endpoint, &device_code_params).await;
    token_params.insert(
        "device_code".to_string(),
        device_response.device_code.to_string(),
    );

    // Let the code expire and make sure it's invalidated
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_client_auth(
        "jdoe@example.com",
        "abcde",
        &device_response,
        "Invalid or expired authentication code.",
    )
    .await;
    assert_eq!(
        post::<TokenResponse>(&metadata.token_endpoint, &token_params).await,
        TokenResponse::Error {
            error: ErrorType::ExpiredToken
        }
    );

    // Authenticate account using a valid code
    let device_response: DeviceAuthResponse =
        post(&metadata.device_authorization_endpoint, &device_code_params).await;
    token_params.insert(
        "device_code".to_string(),
        device_response.device_code.to_string(),
    );
    assert_client_auth("jdoe@example.com", "abcde", &device_response, "successful").await;

    // Obtain token
    let (token, refresh_token, _) =
        unwrap_token_response(post(&metadata.token_endpoint, &token_params).await);
    let refresh_token = refresh_token.unwrap();

    // Authorization codes can only be used once
    assert_eq!(
        post::<TokenResponse>(&metadata.token_endpoint, &token_params).await,
        TokenResponse::Error {
            error: ErrorType::ExpiredToken
        }
    );

    // Connect to account using token and attempt to search
    let john_client = Client::new()
        .credentials(Credentials::bearer(&token))
        .connect(admin_client.session_url())
        .await
        .unwrap();
    assert_eq!(john_client.default_account_id(), john_id);
    assert!(!john_client
        .mailbox_query(None::<Filter>, None::<Vec<_>>)
        .await
        .unwrap()
        .ids()
        .is_empty());

    // Connecting using the refresh token should not work
    assert_unauthorized(admin_client, &refresh_token).await;

    // Refreshing a token using the access token should not work
    assert_eq!(
        post::<TokenResponse>(
            &metadata.token_endpoint,
            &AHashMap::from_iter([
                ("client_id".to_string(), "1234".to_string()),
                ("grant_type".to_string(), "refresh_token".to_string()),
                ("refresh_token".to_string(), token),
            ]),
        )
        .await,
        TokenResponse::Error {
            error: ErrorType::InvalidGrant
        }
    );

    // Refreshing the access token before expiration should not include a new refresh token
    let refresh_params = AHashMap::from_iter([
        ("client_id".to_string(), "1234".to_string()),
        ("grant_type".to_string(), "refresh_token".to_string()),
        ("refresh_token".to_string(), refresh_token),
    ]);
    let (token, new_refresh_token, _) =
        unwrap_token_response(post(&metadata.token_endpoint, &refresh_params).await);
    assert_eq!(new_refresh_token, None);

    // Wait 1 second and make sure the access token expired
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_unauthorized(admin_client, &token).await;

    // Wait another second for the refresh token to be about to expire
    // and expect a new refresh token
    tokio::time::sleep(Duration::from_secs(1)).await;
    let (_, new_refresh_token, _) =
        unwrap_token_response(post(&metadata.token_endpoint, &refresh_params).await);
    //println!("New refresh token: {:?}", new_refresh_token);
    assert_ne!(new_refresh_token, None);

    // Wait another second and make sure the refresh token expired
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        post::<TokenResponse>(&metadata.token_endpoint, &refresh_params).await,
        TokenResponse::Error {
            error: ErrorType::InvalidGrant
        }
    );

    // Destroy test accounts
    for principal_id in [john_id, domain_id] {
        admin_client.principal_destroy(&principal_id).await.unwrap();
    }
    server.store.principal_purge().unwrap();
    server.store.assert_is_empty();
}

async fn post_bytes(url: &str, params: &AHashMap<String, String>) -> Bytes {
    reqwest::Client::builder()
        .timeout(Duration::from_millis(200))
        .danger_accept_invalid_certs(true)
        .build()
        .unwrap_or_default()
        .post(url)
        .form(params)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
}

async fn post<T: DeserializeOwned>(url: &str, params: &AHashMap<String, String>) -> T {
    serde_json::from_slice(&post_bytes(url, params).await).unwrap()
}

async fn get<T: DeserializeOwned>(url: &str) -> T {
    serde_json::from_slice(
        &reqwest::Client::builder()
            .timeout(Duration::from_millis(200))
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap_or_default()
            .get(url)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap()
}

async fn assert_client_auth(
    email: &str,
    pass: &str,
    device_response: &DeviceAuthResponse,
    expect: &str,
) {
    let html_response = String::from_utf8_lossy(
        &post_bytes(
            &device_response.verification_uri,
            &AHashMap::from_iter([
                ("email".to_string(), email.to_string()),
                ("password".to_string(), pass.to_string()),
                ("code".to_string(), device_response.user_code.to_string()),
            ]),
        )
        .await,
    )
    .into_owned();
    assert!(html_response.contains(expect), "{:#?}", html_response);
}

async fn assert_unauthorized(client: &Client, token: &str) {
    match Client::new()
        .credentials(Credentials::bearer(token))
        .connect(client.session_url())
        .await
    {
        Ok(_) => panic!("Expected unauthorized access."),
        Err(err) => {
            let err = err.to_string();
            assert!(err.contains("Unauthorized"), "{}", err);
        }
    }
}

fn unwrap_token_response(response: TokenResponse) -> (String, Option<String>, u64) {
    match response {
        TokenResponse::Granted {
            access_token,
            token_type,
            expires_in,
            refresh_token,
            ..
        } => {
            assert_eq!(token_type, "bearer");
            (access_token, refresh_token, expires_in)
        }
        TokenResponse::Error { error } => panic!("Expected granted, got {:?}", error),
    }
}
