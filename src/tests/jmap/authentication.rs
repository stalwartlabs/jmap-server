use std::{sync::Arc, time::Duration};

use actix_web::web;
use jmap::types::jmap::JMAPId;
use jmap_client::{
    client::{Client, Credentials},
    core::error::{MethodError, MethodErrorType, ProblemDetails},
    mailbox::{self},
};
use store::Store;

use crate::JMAPServer;

pub async fn test<T>(server: web::Data<JMAPServer<T>>, admin_client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Running Authentication tests...");

    // Create a domain name and a test account
    let domain_id = admin_client
        .set_default_account_id(JMAPId::new(0))
        .domain_create("example.com")
        .await
        .unwrap()
        .unwrap_id();
    let account_id = admin_client
        .individual_create("jdoe@example.com", "12345", "John Doe")
        .await
        .unwrap()
        .unwrap_id();

    // Incorrect passwords should be rejected with a 401 error
    assert!(matches!(
        Client::connect(
            admin_client.session_url(),
            Credentials::basic("jdoe@example.com", "abcde"),
        )
        .await,
        Err(jmap_client::Error::Problem(ProblemDetails {
            status: Some(401),
            ..
        }))
    ));

    // Requests should be rate limited
    let mut n_401 = 0;
    let mut n_429 = 0;
    for n in 0..110 {
        if let Err(jmap_client::Error::Problem(problem)) = Client::connect(
            admin_client.session_url(),
            Credentials::basic("jdoe@example.com", &format!("brute_force{}", n)),
        )
        .await
        {
            if problem.status().unwrap() == 401 {
                n_401 += 1;
                if n_401 > 100 {
                    panic!("Rate limiter failed.");
                }
            } else if problem.status().unwrap() == 429 {
                n_429 += 1;
                if n_429 > 11 {
                    panic!("Rate limiter too restrictive.");
                }
            } else {
                panic!("Unexpected error status {}", problem.status().unwrap());
            }
        } else {
            panic!("Unaexpected response.");
        }
    }

    // Limit should be restored after 1 second
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Login with the correct credentials
    let client = Client::connect(
        admin_client.session_url(),
        Credentials::basic("jdoe@example.com", "12345"),
    )
    .await
    .unwrap();
    assert_eq!(client.session().username(), "jdoe@example.com");
    assert_eq!(
        client.session().account(&account_id).unwrap().name(),
        "John Doe"
    );
    assert!(client.session().account(&account_id).unwrap().is_personal());

    // Users should not be allowed to create, read, modify or delete principals
    assert!(matches!(
        client
            .individual_create("jane.doe@example.com", "0987654", "Jane Doe")
            .await,
        Err(jmap_client::Error::Method(MethodError {
            p_type: MethodErrorType::Forbidden
        }))
    ));
    assert!(matches!(
        client.principal_get(&domain_id, None::<Vec<_>>).await,
        Err(jmap_client::Error::Method(MethodError {
            p_type: MethodErrorType::Forbidden
        }))
    ));
    assert!(matches!(
        client
            .principal_set_name(&domain_id, "otherdomain.com")
            .await,
        Err(jmap_client::Error::Method(MethodError {
            p_type: MethodErrorType::Forbidden
        }))
    ));
    assert!(matches!(
        client.principal_destroy(&account_id).await,
        Err(jmap_client::Error::Method(MethodError {
            p_type: MethodErrorType::Forbidden
        }))
    ));

    // Concurrent requests check
    let client = Arc::new(client);
    for _ in 0..8 {
        let client_ = client.clone();
        tokio::spawn(async move {
            client_
                .mailbox_query(
                    mailbox::query::Filter::name("__sleep").into(),
                    [mailbox::query::Comparator::name()].into(),
                )
                .await
                .unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        client
            .mailbox_query(
                mailbox::query::Filter::name("__sleep").into(),
                [mailbox::query::Comparator::name()].into(),
            )
            .await,
        Err(jmap_client::Error::Problem(ProblemDetails {
            status: Some(400),
            ..
        }))
    ));

    // Wait for sleep to be done
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Concurrent upload test
    for _ in 0..4 {
        let client_ = client.clone();
        tokio::spawn(async move {
            client_.upload(b"sleep".to_vec(), None).await.unwrap();
        });
    }
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        client.upload(b"sleep".to_vec(), None).await,
        Err(jmap_client::Error::Problem(ProblemDetails {
            status: Some(400),
            ..
        }))
    ));
}
