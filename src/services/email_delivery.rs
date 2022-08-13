use std::{collections::VecDeque, time::Duration};

use actix_web::web;
use jmap::{
    orm::{serialize::JMAPOrm, TinyORM},
    types::type_state::TypeState,
};
use jmap_mail::email_submission::schema::{
    Delivered, DeliveryStatus, Displayed, EmailSubmission, Property, UndoStatus, Value,
};
use jmap_mail::mail_send::{smtp::message::Message, Transport};
use jmap_sharing::principal::get::JMAPGetPrincipal;
use store::{
    ahash::AHashMap,
    blob::BlobId,
    config::env_settings::EnvSettings,
    core::{collection::Collection, document::Document},
    tracing::{debug, log::error},
    write::batch::WriteBatch,
    AccountId, DocumentId, Store,
};
use tokio::sync::mpsc;

use crate::{cluster::IPC_CHANNEL_BUFFER, JMAPServer};

use super::state_change::StateChange;

const DEFAULT_SMTP_TIMEOUT_MS: u64 = 60000;

pub enum Event {
    EmailSubmission {
        account_id: AccountId,
        created_ids: Vec<DocumentId>,
        updated_ids: Vec<DocumentId>,
    },
    VacationResponse {
        from: String,
        to: String,
        message: Vec<u8>,
    },
    RelayReady,
    Reload,
    Start,
    Stop,
}

impl Event {
    pub fn new_submission(
        account_id: AccountId,
        created_ids: Vec<DocumentId>,
        updated_ids: Vec<DocumentId>,
    ) -> Self {
        Event::EmailSubmission {
            account_id,
            created_ids,
            updated_ids,
        }
    }

    pub fn vacation_response(from: String, to: String, message: Vec<u8>) -> Self {
        Event::VacationResponse { from, to, message }
    }
}

// TODO be notified of shutdowns and lost leaderships (all modules)
// TODO on startup load all pending deliveries
pub fn init_email_delivery() -> (mpsc::Sender<Event>, mpsc::Receiver<Event>) {
    mpsc::channel::<Event>(IPC_CHANNEL_BUFFER)
}

pub fn spawn_email_delivery<T>(
    core: web::Data<JMAPServer<T>>,
    settings: &EnvSettings,
    tx: mpsc::Sender<Event>,
    mut rx: mpsc::Receiver<Event>,
) where
    T: for<'x> Store<'x> + 'static,
{
    // Parse SMTP relay
    let relay_tx = if let Some(smtp_relay) = parse_smtp_settings(settings) {
        spawn_email_relay(core, smtp_relay, tx)
    } else {
        return;
    };

    tokio::spawn(async move {
        let mut queue = VecDeque::new();
        let mut is_ready = true;

        while let Some(event) = rx.recv().await {
            match event {
                Event::RelayReady => {
                    if let Some(event) = queue.pop_front() {
                        if let Err(err) = relay_tx.send(event).await {
                            error!("Error sending event to relay: {}", err);
                        }
                    } else {
                        is_ready = true;
                    }
                }
                Event::Stop => {
                    if let Err(err) = relay_tx.send(Event::Reload).await {
                        error!("Error sending event to relay: {}", err);
                    }
                    queue.clear();
                }
                Event::Start => (),
                event => {
                    if is_ready {
                        if let Err(err) = relay_tx.send(event).await {
                            error!("Error sending event to relay: {}", err);
                        }
                    } else {
                        queue.push_back(event);
                    }
                }
            }
        }
    });
}

fn spawn_email_relay<T>(
    core: web::Data<JMAPServer<T>>,
    smtp_relay: SMTPRelay,
    queue_tx: mpsc::Sender<Event>,
) -> mpsc::Sender<Event>
where
    T: for<'x> Store<'x> + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Event>(IPC_CHANNEL_BUFFER);
    tokio::spawn(async move {
        // Setup client
        let mut client = Transport::new(&smtp_relay.hostname).timeout(smtp_relay.timeout);
        if smtp_relay.port > 0 {
            client = client.port(smtp_relay.port);
        }
        if let Some((username, secret)) = &smtp_relay.credentials {
            client = client.credentials(username, secret);
        }
        let is_tls = smtp_relay.tls;
        let mut dkim_map = AHashMap::new();

        while let Some(event) = rx.recv().await {
            match event {
                Event::EmailSubmission {
                    account_id,
                    created_ids,
                    ..
                } => {
                    // Fetch submissions
                    let account_id = account_id;
                    let store = core.store.clone();
                    let messages = match core
                        .spawn_worker(move || {
                            let mut messages = Vec::with_capacity(created_ids.len());

                            for created_id in created_ids {
                                if let Some(email_submission) =
                                    store.get_orm::<EmailSubmission>(account_id, created_id)?
                                {
                                    if let Some(blob_id) = store.get_document_value::<BlobId>(
                                        account_id,
                                        Collection::EmailSubmission,
                                        created_id,
                                        Property::EmailId.into(),
                                    )? {
                                        if let Some(blob) = store.blob_get(&blob_id)? {
                                            messages.push((created_id, email_submission, blob));
                                        }
                                    }
                                }
                            }

                            Ok(messages)
                        })
                        .await
                    {
                        Ok(messages) => {
                            if messages.is_empty() {
                                continue;
                            }
                            messages
                        }
                        Err(err) => {
                            error!("Error getting email submissions: {}", err);
                            continue;
                        }
                    };

                    // Connect to relay server
                    let mut results = Vec::with_capacity(messages.len());
                    match if is_tls {
                        client.clone().connect_tls().await
                    } else {
                        client.clone().connect().await
                    } {
                        Ok(mut client) => {
                            for (email_submission_id, current_email_submission, raw_message) in
                                messages
                            {
                                // Track changes
                                let mut email_submission =
                                    TinyORM::track_changes(&current_email_submission);

                                // Access envelope
                                let envelope = if let Some(envelope) = current_email_submission
                                    .get(&Property::Envelope)
                                    .and_then(|value| {
                                        if let Value::Envelope { value } = value {
                                            Some(value)
                                        } else {
                                            None
                                        }
                                    }) {
                                    envelope
                                } else {
                                    error!(
                                        "Missing envelope for {}/{}",
                                        account_id, email_submission_id
                                    );
                                    continue;
                                };

                                // Fetch dkim settings
                                let domain_name = envelope
                                    .mail_from
                                    .email
                                    .split_once('@')
                                    .unwrap()
                                    .1
                                    .to_string();
                                let dkim = if let Some(dkim) = dkim_map.get(&domain_name) {
                                    dkim
                                } else {
                                    match core.store.dkim_get(domain_name.clone()) {
                                        Ok(dkim) => {
                                            dkim_map.insert(
                                                domain_name.clone(),
                                                if let Some(dkim) = dkim {
                                                    dkim.headers([
                                                        "From",
                                                        "To",
                                                        "Subject",
                                                        "Date",
                                                        "Cc",
                                                        "Bcc",
                                                        "Message-ID",
                                                        "References",
                                                        "In-Reply-To",
                                                    ])
                                                    .into()
                                                } else {
                                                    None
                                                },
                                            );
                                            dkim_map.get(&domain_name).unwrap()
                                        }
                                        Err(err) => {
                                            error!(
                                                "Error getting DKIM settings for domain '{}': {}",
                                                domain_name, err
                                            );
                                            continue;
                                        }
                                    }
                                };

                                // Create delivery status list
                                let mut delivery_status =
                                    AHashMap::with_capacity(envelope.rcpt_to.len());

                                // Send mail-from
                                let undo_status = if let Err(err) = client
                                    .cmd(
                                        format!("MAIL FROM:{}\r\n", &envelope.mail_from).as_bytes(),
                                    )
                                    .await
                                {
                                    let err = err.to_string();
                                    for rcpt in &envelope.rcpt_to {
                                        delivery_status.insert(
                                            rcpt.email.to_string(),
                                            DeliveryStatus::new(
                                                err.clone(),
                                                Delivered::No,
                                                Displayed::Unknown,
                                            ),
                                        );
                                    }
                                    UndoStatus::Canceled
                                } else {
                                    // Send recipients
                                    let mut accepted_rcpt = false;
                                    for rcpt in &envelope.rcpt_to {
                                        match client
                                            .cmd(format!("RCPT TO:{}\r\n", &rcpt).as_bytes())
                                            .await
                                        {
                                            Ok(reply) => {
                                                delivery_status.insert(
                                                    rcpt.email.to_string(),
                                                    DeliveryStatus::new(
                                                        reply.to_string(),
                                                        if reply.is_positive_completion() {
                                                            accepted_rcpt = true;
                                                            Delivered::Queued
                                                        } else {
                                                            Delivered::No
                                                        },
                                                        Displayed::Unknown,
                                                    ),
                                                );
                                            }
                                            Err(err) => {
                                                delivery_status.insert(
                                                    rcpt.email.to_string(),
                                                    DeliveryStatus::new(
                                                        err.to_string(),
                                                        Delivered::No,
                                                        Displayed::Unknown,
                                                    ),
                                                );
                                            }
                                        }
                                    }

                                    // Do not submit message if no recipients were accepted
                                    if accepted_rcpt {
                                        // Sign message
                                        let mut headers = None;
                                        if let Some(dkim) = dkim {
                                            match dkim.sign(&raw_message) {
                                                Ok(signature) => {
                                                    headers = signature.to_header().into();
                                                }
                                                Err(err) => {
                                                    error!(
                                                        "Error signing message for domain '{}': {}",
                                                        domain_name, err
                                                    );
                                                }
                                            }
                                        }

                                        // Send message
                                        let result = if let Some(headers) = headers {
                                            client
                                                .data_with_headers(headers.as_bytes(), &raw_message)
                                                .await
                                        } else {
                                            client.data(&raw_message).await
                                        };

                                        match result {
                                            Ok(_) => UndoStatus::Final,
                                            Err(err) => {
                                                let err = err.to_string();
                                                for rcpt in &envelope.rcpt_to {
                                                    delivery_status.insert(
                                                        rcpt.email.to_string(),
                                                        DeliveryStatus::new(
                                                            err.clone(),
                                                            Delivered::No,
                                                            Displayed::Unknown,
                                                        ),
                                                    );
                                                }
                                                UndoStatus::Canceled
                                            }
                                        }
                                    } else {
                                        UndoStatus::Canceled
                                    }
                                };

                                // Update submission
                                email_submission.set(
                                    Property::UndoStatus,
                                    Value::UndoStatus { value: undo_status },
                                );
                                email_submission.set(
                                    Property::DeliveryStatus,
                                    Value::DeliveryStatus {
                                        value: delivery_status,
                                    },
                                );
                                results.push((
                                    email_submission_id,
                                    current_email_submission,
                                    email_submission,
                                ));
                                client.rset().await.ok();
                            }

                            // Send QUIT
                            client.quit().await.ok();
                        }
                        Err(err) => {
                            // Fail all submissions
                            let err = err.to_string();
                            error!("Failed to connect to relay server: {}", err);

                            for (email_submission_id, current_email_submission, _) in messages {
                                // Track changes
                                let mut email_submission =
                                    TinyORM::track_changes(&current_email_submission);

                                // Access envelope
                                if let Some(envelope) = current_email_submission
                                    .get(&Property::Envelope)
                                    .and_then(|value| {
                                        if let Value::Envelope { value } = value {
                                            Some(value)
                                        } else {
                                            None
                                        }
                                    })
                                {
                                    // Create delivery status list
                                    let mut delivery_status =
                                        AHashMap::with_capacity(envelope.rcpt_to.len());

                                    // Fail all recipients
                                    for rcpt in &envelope.rcpt_to {
                                        delivery_status.insert(
                                            rcpt.email.to_string(),
                                            DeliveryStatus::new(
                                                err.clone(),
                                                Delivered::No,
                                                Displayed::Unknown,
                                            ),
                                        );
                                    }
                                    email_submission.set(
                                        Property::UndoStatus,
                                        Value::UndoStatus {
                                            value: UndoStatus::Canceled,
                                        },
                                    );
                                    email_submission.set(
                                        Property::DeliveryStatus,
                                        Value::DeliveryStatus {
                                            value: delivery_status,
                                        },
                                    );
                                    results.push((
                                        email_submission_id,
                                        current_email_submission,
                                        email_submission,
                                    ));
                                }
                            }
                        }
                    }

                    // Update store with submission results
                    let store = core.store.clone();
                    match core
                        .spawn_worker(move || {
                            let mut batch = WriteBatch::new(account_id);
                            for (email_submission_id, current_email_submission, email_submission) in
                                results
                            {
                                let mut document =
                                    Document::new(Collection::EmailSubmission, email_submission_id);

                                // Merge changes
                                current_email_submission.merge(&mut document, email_submission)?;
                                if !document.is_empty() {
                                    batch.update_document(document);
                                    batch.log_update(
                                        Collection::EmailSubmission,
                                        email_submission_id,
                                    );
                                }
                            }
                            // Write changes
                            store.write(batch)
                        })
                        .await
                    {
                        Ok(Some(changes)) => {
                            // Commit change
                            if core.is_in_cluster() {
                                core.commit_index(changes.change_id).await;
                            }

                            // Notify subscribers
                            if let Err(err) = core
                                .publish_state_change(StateChange {
                                    account_id,
                                    types: vec![(TypeState::EmailSubmission, changes.change_id)],
                                })
                                .await
                            {
                                error!("Failed to publish state change: {}", err);
                            }
                        }
                        Ok(None) => (),
                        Err(err) => {
                            error!("Failed to update email submissions: {}", err);
                        }
                    }
                }
                Event::VacationResponse { from, to, message } => {
                    match if is_tls {
                        client.clone().connect_tls().await
                    } else {
                        client.clone().connect().await
                    } {
                        Ok(mut client) => {
                            if let Err(err) = client
                                .send(Message::empty().from(from).to(to).body(&message))
                                .await
                            {
                                debug!("Failed to send vacation response: {}", err);
                            }
                            client.quit().await.ok();
                        }
                        Err(err) => {
                            error!("Failed to connect to relay server: {}", err);
                        }
                    }
                }
                Event::Reload => {
                    dkim_map.clear();
                }
                _ => (),
            }

            // Notify the queue that the event has been processed
            if let Err(err) = queue_tx.send(Event::RelayReady).await {
                error!("Error sending event to relay: {}", err);
            }
        }
    });
    tx
}

struct SMTPRelay {
    hostname: String,
    port: u16,
    credentials: Option<(String, String)>,
    tls: bool,
    timeout: Duration,
}

fn parse_smtp_settings(settings: &EnvSettings) -> Option<SMTPRelay> {
    let smtp_relay = settings.get("smtp-relay")?;
    let mut parts = smtp_relay.split('@');
    let part_1 = parts.next()?;
    let (hostname, credentials) = if let Some(part_2) = parts.next() {
        let mut parts = part_1.split(':');
        (
            part_2,
            Some((parts.next()?.to_string(), parts.next()?.to_string())),
        )
    } else {
        (part_1, None)
    };

    let mut parts = hostname.split(':');
    let mut tls = true;
    Some(SMTPRelay {
        hostname: parts
            .next()
            .map(|h| {
                if let Some(h) = h.strip_prefix('!') {
                    tls = false;
                    h
                } else {
                    h
                }
            })?
            .to_string(),
        port: parts.next().and_then(|p| p.parse().ok()).unwrap_or(0),
        credentials,
        tls,
        timeout: Duration::from_millis(
            settings
                .parse("smtp-relay-timeout")
                .unwrap_or(DEFAULT_SMTP_TIMEOUT_MS),
        ),
    })
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn notify_email_delivery(&self, event: Event) -> jmap::Result<()> {
        let email_tx = self.email_delivery.clone();
        if let Err(err) = email_tx.clone().send(event).await {
            error!("Channel failure while publishing state change: {}", err);
        }
        Ok(())
    }
}
