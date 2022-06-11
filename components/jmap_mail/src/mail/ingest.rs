use std::sync::Arc;

use jmap::{
    orm::{serialize::JMAPOrm, TinyORM},
    principal::{
        self,
        account::JMAPAccountStore,
        schema::{Principal, Type},
    },
    sanitize_email,
    types::jmap::JMAPId,
    SUPERUSER_ID,
};
use mail_parser::Message;
use store::{
    core::{collection::Collection, document::Document, error::StoreError, tag::Tag, JMAPIdPrefix},
    read::{
        comparator::Comparator,
        filter::{Filter, Query},
        FilterMapper,
    },
    tracing::{debug, error},
    write::{batch::WriteBatch, update::Changes},
    AccountId, DocumentId, JMAPStore, RecipientType, Store,
};

use crate::{
    vacation_response::get::{JMAPGetVacationResponse, VacationMessage},
    INBOX_ID,
};

use super::{
    import::JMAPMailImport,
    schema::{Email, Property},
};

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
    fn mail_expand_rcpt(&self, email: String) -> store::Result<Arc<RecipientType>>;
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
        for email in recipients {
            // Expand recipients
            match self.mail_expand_rcpt(email.clone()) {
                Ok(accounts) => match accounts.as_ref() {
                    RecipientType::Individual(account_id) => {
                        self.mail_deliver_rcpt(
                            &mut result,
                            *account_id,
                            email,
                            &document,
                            return_address.as_deref(),
                        );
                    }
                    RecipientType::List(accounts) => {
                        for (account_id, email) in accounts {
                            self.mail_deliver_rcpt(
                                &mut result,
                                *account_id,
                                email.to_string(),
                                &document,
                                return_address.as_deref(),
                            );
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

    fn mail_expand_rcpt(&self, email: String) -> store::Result<Arc<RecipientType>> {
        self.recipients
            .try_get_with::<_, StoreError>(email.clone(), || {
                Ok(Arc::new(
                    if let Some(account_id) = self
                        .query_store::<FilterMapper>(
                            SUPERUSER_ID,
                            Collection::Principal,
                            Filter::or(vec![
                                Filter::eq(
                                    principal::schema::Property::Email.into(),
                                    Query::Index(email.clone()),
                                ),
                                Filter::eq(
                                    principal::schema::Property::Aliases.into(),
                                    Query::Index(email),
                                ),
                            ]),
                            Comparator::None,
                        )?
                        .into_iter()
                        .next()
                        .map(|id| id.get_document_id())
                    {
                        if let Some(mut fields) =
                            self.get_orm::<Principal>(SUPERUSER_ID, account_id)?
                        {
                            match fields.get(&principal::schema::Property::Type) {
                                Some(principal::schema::Value::Type {
                                    value: principal::schema::Type::List,
                                }) => {
                                    if let Some(principal::schema::Value::Members { value }) =
                                        fields.remove(&principal::schema::Property::Members)
                                    {
                                        if !value.is_empty() {
                                            let mut list = Vec::with_capacity(value.len());
                                            for id in value {
                                                let account_id = id.get_document_id();
                                                match self.get_account_details(account_id)? {
                                                    Some((email, _, ptype))
                                                        if ptype == Type::Individual =>
                                                    {
                                                        list.push((account_id, email));
                                                    }
                                                    _ => (),
                                                }
                                            }
                                            return Ok(Arc::new(RecipientType::List(list)));
                                        }
                                    }
                                    RecipientType::NotFound
                                }
                                _ => RecipientType::Individual(account_id),
                            }
                        } else {
                            debug!(
                                "Rcpt expand failed: ORM for account {} does not exist.",
                                JMAPId::from(account_id)
                            );
                            RecipientType::NotFound
                        }
                    } else {
                        RecipientType::NotFound
                    },
                ))
            })
            .map_err(|e| e.as_ref().clone())
    }
}
