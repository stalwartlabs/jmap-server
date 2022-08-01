use actix_web::{
    http::{header::ContentType, StatusCode},
    web, HttpResponse,
};
use jmap::{
    orm::TinyORM,
    sanitize_email,
    types::{jmap::JMAPId, principal::Type, type_state::TypeState},
};
use jmap_mail::{
    mail::{
        import::JMAPMailImport,
        schema::{Email, Property},
    },
    mail_parser::Message,
    vacation_response::get::{JMAPGetVacationResponse, VacationMessage},
    INBOX_ID,
};
use jmap_sharing::principal::account::JMAPAccountStore;
use store::{
    ahash::AHashSet,
    core::{collection::Collection, document::Document, tag::Tag},
    log::changes::ChangeId,
    tracing::{debug, error},
    write::{batch::WriteBatch, update::Changes},
    AccountId, DocumentId, JMAPStore, RecipientType, Store,
};

use crate::{
    services::{email_delivery, state_change::StateChange},
    JMAPServer,
};

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct Params {
    from: Option<String>,
    to: String,
    api_key: String,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Dsn {
    pub to: String,
    pub status: DeliveryStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum DeliveryStatus {
    #[serde(rename = "success")]
    Success,
    #[serde(rename = "failure")]
    Failure,
    #[serde(rename = "temporary_failure")]
    TemporaryFailure,
}

pub enum Status {
    Success {
        email: String,
        account_id: AccountId,
        changes: Changes,
        vacation_response: Option<VacationMessage>,
    },
    Failure {
        email: String,
        permanent: bool,
        reason: String,
    },
}

impl Status {
    pub fn internal_error(email: String) -> Status {
        Status::Failure {
            email,
            permanent: false,
            reason: "Internal error, please try again later.".to_string(),
        }
    }

    pub fn not_found(email: String) -> Status {
        Status::Failure {
            email,
            permanent: true,
            reason: "Recipient does not exist.".to_string(),
        }
    }
}

pub async fn handle_ingest<T>(
    params: web::Query<Params>,
    bytes: web::Bytes,
    core: web::Data<JMAPServer<T>>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    // Validate API key
    if core.store.config.api_key.is_empty() || core.store.config.api_key != params.api_key {
        debug!("Invalid API key");
        return HttpResponse::Unauthorized().finish();
    }

    // Ingest message
    let recipients = params.to.split(',').filter_map(sanitize_email).collect();
    let store = core.store.clone();
    let results = core
        .spawn_worker(move || Ok(store.mail_ingest(recipients, bytes.to_vec())))
        .await
        .unwrap();

    // Prepare response
    let mut response = Vec::with_capacity(params.to.len());
    let mut change_id = ChangeId::MAX;
    let mut status_code = StatusCode::OK;

    for result in results {
        match result {
            Status::Success {
                account_id,
                email,
                changes,
                vacation_response,
            } => {
                // Send vacation response
                if let Some(vacation_response) = vacation_response {
                    if let Err(err) = core
                        .notify_email_delivery(email_delivery::Event::vacation_response(
                            vacation_response.from,
                            vacation_response.to,
                            vacation_response.message,
                        ))
                        .await
                    {
                        error!(
                            "No e-mail delivery configured or something else happened: {}",
                            err
                        );
                    }
                }

                // Update the change id
                change_id = changes.change_id;

                // Publish state change
                let mut types = changes
                    .collections
                    .into_iter()
                    .filter_map(|c| Some((TypeState::try_from(c).ok()?, change_id)))
                    .collect::<Vec<_>>();
                types.push((TypeState::EmailDelivery, change_id));

                if let Err(err) = core
                    .publish_state_change(StateChange::new(account_id, types))
                    .await
                {
                    error!("Failed to publish state change: {}", err);
                }

                response.push(Dsn {
                    to: email,
                    status: DeliveryStatus::Success,
                    reason: None,
                });
            }
            Status::Failure {
                email,
                permanent,
                reason,
            } => {
                response.push(Dsn {
                    to: email,
                    status: if permanent {
                        DeliveryStatus::Failure
                    } else {
                        status_code = StatusCode::SERVICE_UNAVAILABLE;
                        DeliveryStatus::TemporaryFailure
                    },
                    reason: reason.into(),
                });
            }
        }
    }

    // Commit change
    if change_id != ChangeId::MAX && core.is_in_cluster() && !core.commit_index(change_id).await {
        response = response
            .into_iter()
            .map(|r| {
                if let DeliveryStatus::Success = r.status {
                    Dsn {
                        to: r.to,
                        status: DeliveryStatus::TemporaryFailure,
                        reason: "Failed to commit changes to cluster.".to_string().into(),
                    }
                } else {
                    r
                }
            })
            .collect();
        status_code = StatusCode::SERVICE_UNAVAILABLE;
    }

    HttpResponse::build(status_code)
        .insert_header(ContentType::json())
        .json(response)
}

pub trait JMAPMailIngest {
    fn mail_ingest(&self, recipients: Vec<String>, raw_message: Vec<u8>) -> Vec<Status>;
    fn mail_deliver_rcpt(
        &self,
        result: &mut Vec<Status>,
        account_id: AccountId,
        email: String,
        document: &Document,
        return_address: Option<&str>,
    );
}

impl<T> JMAPMailIngest for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_ingest(&self, recipients: Vec<String>, raw_message: Vec<u8>) -> Vec<Status> {
        // Parse message
        let message = if let Some(message) = Message::parse(&raw_message) {
            message
        } else {
            return recipients
                .into_iter()
                .map(|email| Status::Failure {
                    email,
                    permanent: true,
                    reason: "Failed to parse message.".to_string(),
                })
                .collect();
        };

        // Obtain return path for vacation response
        let return_address = message.get_return_address().and_then(|v| {
            let r = sanitize_email(v)?;

            // As per RFC3834
            if !r.starts_with("owner-") && !r.ends_with("-request") && !v.contains("MAILER-DAEMON")
            {
                Some(r)
            } else {
                None
            }
        });

        // Store raw message as a blob
        let blob_id = match self.blob_store(&raw_message) {
            Ok(blob_id) => blob_id,
            Err(err) => {
                error!("Failed to store blob during message ingestion: {}", err);
                return recipients.into_iter().map(Status::internal_error).collect();
            }
        };

        // Build message document
        let mut document = Document::new(Collection::Mail, DocumentId::MAX);
        if let Err(err) = self.mail_parse_item(&mut document, blob_id, message, None) {
            error!("Failed to parse message during ingestion: {}", err);
            return recipients.into_iter().map(Status::internal_error).collect();
        }

        // Deliver message to recipients
        let mut result = Vec::with_capacity(recipients.len());
        let mut delivered_to = AHashSet::with_capacity(recipients.len());
        for email in recipients {
            // Expand recipients
            match self.expand_rcpt(email.clone()) {
                Ok(accounts) => match accounts.as_ref() {
                    RecipientType::Individual(account_id) => {
                        if delivered_to.insert(*account_id) {
                            self.mail_deliver_rcpt(
                                &mut result,
                                *account_id,
                                email,
                                &document,
                                return_address.as_deref(),
                            );
                        }
                    }
                    RecipientType::List(accounts) => {
                        for (account_id, email) in accounts {
                            if delivered_to.insert(*account_id) {
                                self.mail_deliver_rcpt(
                                    &mut result,
                                    *account_id,
                                    email.to_string(),
                                    &document,
                                    return_address.as_deref(),
                                );
                            }
                        }
                    }
                    RecipientType::NotFound => {
                        result.push(Status::not_found(email));
                        continue;
                    }
                },
                Err(err) => {
                    error!("Failed to expand recipients: {}", err);
                    result.push(Status::internal_error(email));
                    continue;
                }
            }
        }

        result
    }

    fn mail_deliver_rcpt(
        &self,
        result: &mut Vec<Status>,
        account_id: AccountId,
        email: String,
        document: &Document,
        return_address: Option<&str>,
    ) {
        // Prepare batch
        let mut batch = WriteBatch::new(account_id);
        let mut document = document.clone();

        // Verify that this account has an Inbox mailbox
        match self.get_document_ids(account_id, Collection::Mailbox) {
            Ok(Some(mailbox_ids)) if mailbox_ids.contains(INBOX_ID) => (),
            _ => {
                error!("Account {} does not have an Inbox configured.", email);
                result.push(Status::internal_error(email));
                return;
            }
        }

        // Add mailbox tags
        let mut orm = TinyORM::<Email>::new();
        batch.log_child_update(Collection::Mailbox, JMAPId::new(INBOX_ID.into()));
        orm.tag(Property::MailboxIds, Tag::Id(INBOX_ID));

        // Serialize ORM
        if let Err(err) = orm.insert(&mut document) {
            error!("Failed to update ORM during ingestion: {}", err);
            result.push(Status::internal_error(email));
            return;
        }

        // Obtain document id
        let document_id = match self.assign_document_id(account_id, Collection::Mail) {
            Ok(document_id) => document_id,
            Err(err) => {
                error!("Failed to assign document id during ingestion: {}", err);
                result.push(Status::internal_error(email));
                return;
            }
        };
        document.document_id = document_id;

        // Build vacation response
        let vacation_response = if let Some(return_address) = &return_address {
            let from_name = self
                .get_account_details(account_id)
                .unwrap_or_else(|_| Some(("".to_string(), "".to_string(), Type::Individual)))
                .map(|a| a.1);

            match self.build_vacation_response(
                account_id,
                from_name.as_deref(),
                &email,
                return_address,
            ) {
                Ok(vr) => vr,
                Err(err) => {
                    error!(
                        "Failed to build vacation response during ingestion: {}",
                        err
                    );
                    None
                }
            }
        } else {
            None
        };

        // Lock account while threads are merged
        let _lock = self.lock_account(account_id, Collection::Mail);

        // Obtain thread Id
        match self.mail_set_thread(&mut batch, &mut document) {
            Ok(thread_id) => {
                // Write document to store
                batch.log_insert(Collection::Mail, JMAPId::from_parts(thread_id, document_id));
                batch.insert_document(document);
                match self.write(batch) {
                    Ok(Some(changes)) => result.push(Status::Success {
                        account_id,
                        email,
                        changes,
                        vacation_response,
                    }),
                    Ok(None) => {
                        error!("Unexpected error during ingestion.");
                        result.push(Status::internal_error(email));
                    }
                    Err(err) => {
                        error!("Failed to write document during ingestion: {}", err);
                        result.push(Status::internal_error(email));
                    }
                }
            }
            Err(err) => {
                error!("Failed to set threadId during ingestion: {}", err);
                result.push(Status::internal_error(email));
            }
        }
    }
}
