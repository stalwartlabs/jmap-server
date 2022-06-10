use jmap::{
    orm::{serialize::JMAPOrm, TinyORM},
    principal::{self, schema::Principal},
    types::jmap::JMAPId,
    SUPERUSER_ID,
};
use mail_parser::Message;
use store::{
    core::{collection::Collection, document::Document, tag::Tag, JMAPIdPrefix},
    read::{
        comparator::Comparator,
        filter::{Filter, Query},
        FilterMapper,
    },
    tracing::{debug, error},
    write::{batch::WriteBatch, update::Changes},
    AccountId, DocumentId, JMAPStore, Store,
};

use crate::vacation_response::get::{JMAPGetVacationResponse, VacationMessage};

use super::{
    import::JMAPMailImport,
    schema::{Email, Property},
};

pub enum RecipientType {
    Individual(AccountId),
    List(Vec<AccountId>),
    NotFound,
}

pub enum Status {
    Success {
        account_id: AccountId,
        changes: Changes,
        vacation_response: Option<VacationMessage>,
    },
    Failure {
        account_id: AccountId,
        permanent: bool,
        reason: String,
    },
}

impl Status {
    pub fn internal_error(account_id: AccountId) -> Status {
        Status::Failure {
            account_id,
            permanent: false,
            reason: "Internal error, please try again later.".to_string(),
        }
    }
}

pub trait JMAPMailIngest {
    fn mail_ingest(&self, account_ids: Vec<AccountId>, raw_message: Vec<u8>) -> Vec<Status>;
    fn mail_expand_rcpt(&self, email: String) -> store::Result<RecipientType>;
}

impl<T> JMAPMailIngest for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_ingest(&self, account_ids: Vec<AccountId>, raw_message: Vec<u8>) -> Vec<Status> {
        // Parse message
        let message = if let Some(message) = Message::parse(&raw_message) {
            message
        } else {
            return account_ids
                .into_iter()
                .map(|account_id| Status::Failure {
                    account_id,
                    permanent: true,
                    reason: "Failed to parse message.".to_string(),
                })
                .collect();
        };

        // Obtain return path for vacation response
        let return_address = message.get_return_address().and_then(|v| {
            let r = v.trim().to_lowercase();

            // As per RFC3834
            if r.contains('@')
                && !r.starts_with("owner-")
                && !r.ends_with("-request")
                && !v.contains("MAILER-DAEMON")
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
                return account_ids
                    .into_iter()
                    .map(Status::internal_error)
                    .collect();
            }
        };

        // Build message document
        let mut document = Document::new(Collection::Mail, DocumentId::MAX);
        if let Err(err) = self.mail_parse_item(&mut document, blob_id, message, None) {
            error!("Failed to parse message during ingestion: {}", err);
            return account_ids
                .into_iter()
                .map(Status::internal_error)
                .collect();
        }

        // Deliver message to recipients
        let mut result = Vec::with_capacity(account_ids.len());
        for account_id in account_ids {
            // Prepare batch
            let mut batch = WriteBatch::new(account_id);
            let mut document = document.clone();

            // Add mailbox tags
            let mut orm = TinyORM::<Email>::new();
            batch.log_child_update(Collection::Mailbox, JMAPId::new(0));
            orm.tag(Property::MailboxIds, Tag::Id(0));

            // Serialize ORM
            if let Err(err) = orm.insert(&mut document) {
                error!("Failed to update ORM during ingestion: {}", err);
                result.push(Status::internal_error(account_id));
                continue;
            }

            // Obtain document id
            let document_id = match self.assign_document_id(account_id, Collection::Mail) {
                Ok(document_id) => document_id,
                Err(err) => {
                    error!("Failed to assign document id during ingestion: {}", err);
                    result.push(Status::internal_error(account_id));
                    continue;
                }
            };
            document.document_id = document_id;

            // Build vacation response
            let vacation_response = if let Some(return_address) = &return_address {
                // TODO fetch identity
                match self.build_vacation_response(
                    account_id,
                    None,
                    "jdoe@example.com",
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
                            changes,
                            vacation_response,
                        }),
                        Ok(None) => {
                            error!("Unexpected error during ingestion.");
                            result.push(Status::internal_error(account_id));
                        }
                        Err(err) => {
                            error!("Failed to write document during ingestion: {}", err);
                            result.push(Status::internal_error(account_id));
                        }
                    }
                }
                Err(err) => {
                    error!("Failed to set threadId during ingestion: {}", err);
                    result.push(Status::internal_error(account_id));
                }
            }
        }

        result
    }
    fn mail_expand_rcpt(&self, email: String) -> store::Result<RecipientType> {
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
            if let Some(mut fields) = self.get_orm::<Principal>(SUPERUSER_ID, account_id)? {
                match fields.get(&principal::schema::Property::Type) {
                    Some(principal::schema::Value::Type {
                        value: principal::schema::Type::List,
                    }) => {
                        if let Some(principal::schema::Value::Members { value }) =
                            fields.remove(&principal::schema::Property::Members)
                        {
                            if !value.is_empty() {
                                return Ok(RecipientType::List(
                                    value.into_iter().map(|id| id.get_document_id()).collect(),
                                ));
                            }
                        }
                        Ok(RecipientType::NotFound)
                    }
                    _ => Ok(RecipientType::Individual(account_id)),
                }
            } else {
                debug!(
                    "Rcpt expand failed: ORM for account {} does not exist.",
                    JMAPId::from(account_id)
                );
                Ok(RecipientType::NotFound)
            }
        } else {
            Ok(RecipientType::NotFound)
        }
    }
}
