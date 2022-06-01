use std::{collections::HashMap, sync::Arc, time::Duration};

use actix_web::web;
use jmap::types::jmap::JMAPId;
use jmap_client::{
    client::Client,
    core::set::{SetError, SetErrorType, SetObject},
    email_submission::{Address, Delivered, DeliveryStatus, Displayed, UndoStatus},
    mailbox::Role,
    Error,
};
use store::{chrono::DateTime, parking_lot::Mutex, Store};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::mpsc,
};

use crate::{
    tests::{jmap::email_set::assert_email_properties, store::utils::StoreCompareWith},
    JMAPServer,
};

#[derive(Default, Debug, PartialEq, Eq)]
pub struct MockMessage {
    pub mail_from: String,
    pub rcpt_to: Vec<String>,
    pub message: String,
}

impl MockMessage {
    pub fn new<T, U>(mail_from: T, rcpt_to: U, message: T) -> Self
    where
        T: Into<String>,
        U: IntoIterator<Item = T>,
    {
        Self {
            mail_from: mail_from.into(),
            rcpt_to: rcpt_to.into_iter().map(|s| s.into()).collect(),
            message: message.into(),
        }
    }
}

#[derive(Default)]
pub struct MockSMTPSettings {
    pub fail_mail_from: bool,
    pub fail_rcpt_to: bool,
    pub fail_message: bool,
}

pub async fn test<T>(server: web::Data<JMAPServer<T>>, client: &mut Client)
where
    T: for<'x> Store<'x> + 'static,
{
    println!("Running E-mail submissions tests...");
    // Start mock SMTP server
    let (mut smtp_rx, smtp_settings) = spawn_mock_smtp_server();

    // Create mailbox
    let mailbox_id = client
        .set_default_account_id(JMAPId::new(1).to_string())
        .mailbox_create("JMAP EmailSubmission", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();
    let mailbox_id_2 = client
        .mailbox_create("JMAP EmailSubmission 2", None::<String>, Role::None)
        .await
        .unwrap()
        .unwrap_id();

    // Create an identity
    let identity_id = client
        .identity_create("John Doe", "jdoe@example.com")
        .await
        .unwrap()
        .unwrap_id();

    // Import an email without any recipients
    let email_id = client
        .email_import(
            b"From: jdoe@example.com\nSubject: hey\n\ntest".to_vec(),
            [&mailbox_id],
            None::<Vec<&str>>,
            None,
        )
        .await
        .unwrap()
        .unwrap_id();

    // Submission without a valid emailId or identityId should fail
    assert!(matches!(
        client
            .email_submission_create(JMAPId::new(123456).to_string(), &identity_id)
            .await,
        Err(Error::Set(SetError {
            type_: SetErrorType::InvalidProperties,
            ..
        }))
    ));
    assert!(matches!(
        client
            .email_submission_create(&email_id, JMAPId::new(123456).to_string())
            .await,
        Err(Error::Set(SetError {
            type_: SetErrorType::InvalidProperties,
            ..
        }))
    ));

    // Submissions of e-mails without any recipients should fail
    assert!(matches!(
        client
            .email_submission_create(&email_id, &identity_id)
            .await,
        Err(Error::Set(SetError {
            type_: SetErrorType::InvalidProperties,
            ..
        }))
    ));

    // Submissions with an envelope that does not match
    // the identity from address should fail
    assert!(matches!(
        client
            .email_submission_create_envelope(
                &email_id,
                &identity_id,
                "other_address@example.com",
                Vec::<Address>::new(),
            )
            .await,
        Err(Error::Set(SetError {
            type_: SetErrorType::InvalidProperties,
            ..
        }))
    ));

    // Submit a valid message submission
    let email_body = "From: jdoe@example.com\nTo: jane_smith@example.com\nSubject: hey\n\ntest";
    let email_id = client
        .email_import(
            email_body.as_bytes().to_vec(),
            [&mailbox_id],
            None::<Vec<&str>>,
            None,
        )
        .await
        .unwrap()
        .unwrap_id();
    client
        .email_submission_create(&email_id, &identity_id)
        .await
        .unwrap();

    // Confirm that the message has been delivered
    assert_message_delivery(
        &mut smtp_rx,
        MockMessage::new(
            "<jdoe@example.com>",
            ["<jane_smith@example.com>"],
            email_body,
        ),
    )
    .await;

    // Manually add recipients to the envelope and confirm submission
    let email_submission_id = client
        .email_submission_create_envelope(
            &email_id,
            &identity_id,
            "jdoe@example.com",
            [
                "tim@foobar.com", // Should be de-duplicated
                "tim@foobar.com",
                "tim@foobar.com  ",
                " james@other_domain.com ", // Should be sanitized
                "  secret_rcpt@test.com  ",
            ],
        )
        .await
        .unwrap()
        .unwrap_id();

    assert_message_delivery(
        &mut smtp_rx,
        MockMessage::new(
            "<jdoe@example.com>",
            [
                "<james@other_domain.com>",
                "<secret_rcpt@test.com>",
                "<tim@foobar.com>",
            ],
            email_body,
        ),
    )
    .await;

    // Confirm that the email submission status was updated
    tokio::time::sleep(Duration::from_millis(100)).await;
    let email_submission = client
        .email_submission_get(&email_submission_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(email_submission.undo_status(), &UndoStatus::Final);
    assert_eq!(
        email_submission.delivery_status().unwrap(),
        &HashMap::from_iter([
            (
                "tim@foobar.com".to_string(),
                DeliveryStatus::new("250 OK", Delivered::Queued, Displayed::Unknown)
            ),
            (
                "secret_rcpt@test.com".to_string(),
                DeliveryStatus::new("250 OK", Delivered::Queued, Displayed::Unknown)
            ),
            (
                "james@other_domain.com".to_string(),
                DeliveryStatus::new("250 OK", Delivered::Queued, Displayed::Unknown)
            ),
        ])
    );

    // SMTP rejects some of the recipients
    smtp_settings.lock().fail_rcpt_to = true;
    let email_submission_id = client
        .email_submission_create_envelope(
            &email_id,
            &identity_id,
            "jdoe@example.com",
            ["tim@foobar.com", "james@other_domain.com", "jane@test.com"],
        )
        .await
        .unwrap()
        .unwrap_id();
    assert_message_delivery(
        &mut smtp_rx,
        MockMessage::new("<jdoe@example.com>", ["<tim@foobar.com>"], email_body),
    )
    .await;

    // Confirm that all delivery failures were included
    tokio::time::sleep(Duration::from_millis(100)).await;
    let email_submission = client
        .email_submission_get(&email_submission_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(email_submission.undo_status(), &UndoStatus::Final);
    assert_eq!(
        email_submission.delivery_status().unwrap(),
        &HashMap::from_iter([
            (
                "james@other_domain.com".to_string(),
                DeliveryStatus::new(
                    "550 I refuse to accept that recipient.",
                    Delivered::No,
                    Displayed::Unknown
                )
            ),
            (
                "jane@test.com".to_string(),
                DeliveryStatus::new(
                    "550 I refuse to accept that recipient.",
                    Delivered::No,
                    Displayed::Unknown
                )
            ),
            (
                "tim@foobar.com".to_string(),
                DeliveryStatus::new("250 OK", Delivered::Queued, Displayed::Unknown)
            ),
        ])
    );
    smtp_settings.lock().fail_rcpt_to = false;

    // SMTP rejects the message
    smtp_settings.lock().fail_message = true;
    let email_submission_id = client
        .email_submission_create_envelope(
            &email_id,
            &identity_id,
            "jdoe@example.com",
            ["tim@foobar.com", "james@other_domain.com", "jane@test.com"],
        )
        .await
        .unwrap()
        .unwrap_id();
    expect_nothing(&mut smtp_rx).await;

    // Confirm that all delivery failures were included
    tokio::time::sleep(Duration::from_millis(100)).await;
    let email_submission = client
        .email_submission_get(&email_submission_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(email_submission.undo_status(), &UndoStatus::Canceled);
    assert_eq!(
        email_submission.delivery_status().unwrap(),
        &HashMap::from_iter([
            (
                "james@other_domain.com".to_string(),
                DeliveryStatus::new(
                    "503 Thank you but I am saving myself for dessert.",
                    Delivered::No,
                    Displayed::Unknown
                )
            ),
            (
                "jane@test.com".to_string(),
                DeliveryStatus::new(
                    "503 Thank you but I am saving myself for dessert.",
                    Delivered::No,
                    Displayed::Unknown
                )
            ),
            (
                "tim@foobar.com".to_string(),
                DeliveryStatus::new(
                    "503 Thank you but I am saving myself for dessert.",
                    Delivered::No,
                    Displayed::Unknown
                )
            ),
        ])
    );
    smtp_settings.lock().fail_message = false;

    // Confirm that the sendAt property is updated when using FUTURERELEASE
    let email_submission_id = client
        .email_submission_create_envelope(
            &email_id,
            &identity_id,
            Address::new("jdoe@example.com").parameter("HOLDUNTIL", Some("2079-11-20T05:00:00Z")),
            ["jane_smith@example.com"],
        )
        .await
        .unwrap()
        .unwrap_id();
    assert_message_delivery(
        &mut smtp_rx,
        MockMessage::new(
            "<jdoe@example.com> HOLDUNTIL=2079-11-20T05:00:00Z",
            ["<jane_smith@example.com>"],
            email_body,
        ),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    let email_submission = client
        .email_submission_get(&email_submission_id, None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        email_submission.send_at(),
        DateTime::parse_from_rfc3339("2079-11-20T05:00:00Z")
            .unwrap()
            .timestamp()
    );

    // Verify onSuccessUpdateEmail action
    let mut request = client.build();
    let set_request = request.set_email_submission();
    let create_id = set_request
        .create()
        .email_id(&email_id)
        .identity_id(&identity_id)
        .create_id()
        .unwrap();
    set_request
        .arguments()
        .on_success_update_email(&create_id)
        .keyword("$draft", true)
        .mailbox_id(&mailbox_id, false)
        .mailbox_id(&mailbox_id_2, true);
    request.send().await.unwrap().unwrap_method_responses();

    assert_email_properties(client, &email_id, &[&mailbox_id_2], &["$draft"]).await;

    // Verify onSuccessDestroyEmail action
    let mut request = client.build();
    let set_request = request.set_email_submission();
    let create_id = set_request
        .create()
        .email_id(&email_id)
        .identity_id(&identity_id)
        .create_id()
        .unwrap();
    set_request.arguments().on_success_destroy_email(&create_id);
    request.send().await.unwrap().unwrap_method_responses();

    assert!(client
        .email_get(&email_id, None::<Vec<_>>)
        .await
        .unwrap()
        .is_none());

    // Destroy mailbox, identity and all submissions
    client.mailbox_destroy(&mailbox_id, true).await.unwrap();
    client.mailbox_destroy(&mailbox_id_2, true).await.unwrap();
    client.identity_destroy(&identity_id).await.unwrap();
    let mut request = client.build();
    let result_ref = request.query_email_submission().result_reference();
    request.set_email_submission().destroy_ref(result_ref);
    let response = request.send().await.unwrap();
    response
        .unwrap_method_responses()
        .pop()
        .unwrap()
        .unwrap_set_email_submission()
        .unwrap();
    server.store.assert_is_empty();
}

pub fn spawn_mock_smtp_server() -> (mpsc::Receiver<MockMessage>, Arc<Mutex<MockSMTPSettings>>) {
    // Create channels
    let (event_tx, event_rx) = mpsc::channel::<MockMessage>(100);
    let _settings = Arc::new(Mutex::new(MockSMTPSettings::default()));
    let settings = _settings.clone();

    // Start mock SMTP server
    tokio::spawn(async move {
        let listener = TcpListener::bind("127.0.0.1:9999")
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to bind mock SMTP server to 127.0.0.1:9999: {}", e);
            });

        while let Ok((mut stream, _)) = listener.accept().await {
            let (rx, mut tx) = stream.split();
            let mut rx = BufReader::new(rx);
            let mut buf = String::with_capacity(128);
            let mut message = MockMessage::default();

            tx.write_all(b"220 [127.0.0.1] Clueless host service ready\r\n")
                .await
                .unwrap();

            while rx.read_line(&mut buf).await.is_ok() {
                print!("-> {}", buf);
                if buf.starts_with("EHLO") {
                    tx.write_all(b"250 Hi there, but I have no extensions to offer :-(\r\n")
                        .await
                        .unwrap();
                } else if buf.starts_with("MAIL FROM") {
                    if settings.lock().fail_mail_from {
                        tx.write_all("552-I do not\r\n552 like that MAIL FROM.\r\n".as_bytes())
                            .await
                            .unwrap();
                    } else {
                        message.mail_from = buf.split_once(':').unwrap().1.trim().to_string();
                        tx.write_all(b"250 OK\r\n").await.unwrap();
                    }
                } else if buf.starts_with("RCPT TO") {
                    if settings.lock().fail_rcpt_to && !buf.contains("foobar.com") {
                        tx.write_all(
                            "550-I refuse to\r\n550 accept that recipient.\r\n".as_bytes(),
                        )
                        .await
                        .unwrap();
                    } else {
                        message
                            .rcpt_to
                            .push(buf.split(':').nth(1).unwrap().trim().to_string());
                        tx.write_all(b"250 OK\r\n").await.unwrap();
                    }
                } else if buf.starts_with("DATA") {
                    if settings.lock().fail_message {
                        tx.write_all(
                            "503-Thank you but I am\r\n503 saving myself for dessert.\r\n"
                                .as_bytes(),
                        )
                        .await
                        .unwrap();
                    } else if !message.mail_from.is_empty() && !message.rcpt_to.is_empty() {
                        tx.write_all(b"354 Start feeding me now some quality content please\r\n")
                            .await
                            .unwrap();
                        buf.clear();
                        while rx.read_line(&mut buf).await.is_ok() {
                            if buf.starts_with('.') {
                                message.message = message.message.trim().to_string();
                                break;
                            } else {
                                message.message += &buf;
                                buf.clear();
                            }
                        }
                        tx.write_all(b"250 Great success!\r\n").await.unwrap();
                        message.rcpt_to.sort_unstable();
                        event_tx.send(message).await.unwrap();
                        message = MockMessage::default();
                    } else {
                        tx.write_all("554 You forgot to tell me a few things.\r\n".as_bytes())
                            .await
                            .unwrap();
                    }
                } else if buf.starts_with("QUIT") {
                    tx.write_all("250 Arrivederci!\r\n".as_bytes())
                        .await
                        .unwrap();
                    break;
                } else if buf.starts_with("RSET") {
                    tx.write_all("250 Your wish is my command.\r\n".as_bytes())
                        .await
                        .unwrap();
                    message = MockMessage::default();
                } else {
                    println!("Unknown command: {}", buf.trim());
                }
                buf.clear();
            }
        }
    });

    (event_rx, _settings)
}

async fn assert_message_delivery(
    event_rx: &mut mpsc::Receiver<MockMessage>,
    expected_message: MockMessage,
) {
    match tokio::time::timeout(Duration::from_millis(3000), event_rx.recv()).await {
        Ok(Some(message)) => {
            assert_eq!(message, expected_message);
        }
        result => {
            panic!(
                "Timeout waiting for message {:?}: {:?}",
                expected_message, result
            );
        }
    }
}

async fn expect_nothing(event_rx: &mut mpsc::Receiver<MockMessage>) {
    match tokio::time::timeout(Duration::from_millis(500), event_rx.recv()).await {
        Err(_) => {}
        message => {
            panic!("Received a message when expecting nothing: {:?}", message);
        }
    }
}
