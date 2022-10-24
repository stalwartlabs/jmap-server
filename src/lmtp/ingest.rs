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

use std::{borrow::Cow, sync::Arc};

use jmap::{
    orm::TinyORM,
    sanitize_email,
    types::{jmap::JMAPId, type_state::TypeState},
};
use jmap_mail::{
    mail::{
        import::JMAPMailImport,
        schema::{Email, Property},
    },
    mail_parser::Message,
    INBOX_ID,
};
use jmap_sharing::principal::account::JMAPAccountStore;
use serde::{Deserialize, Serialize};
use store::{
    ahash::{AHashMap, AHashSet},
    blob::BlobId,
    core::{collection::Collection, document::Document, tag::Tag},
    log::changes::ChangeId,
    tracing::{debug, error},
    write::{batch::WriteBatch, update::Changes},
    AccountId, DocumentId, JMAPStore, RecipientType, Store,
};

use crate::{
    cluster::rpc::command::{Command, CommandResponse},
    services::{email_delivery, state_change::StateChange},
    JMAPServer,
};

use super::session::{RcptType, Session};

impl<T> Session<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn ingest_message(&mut self) -> Result<(), ()> {
        // Validate request
        if self.message.is_empty() {
            return self
                .write_bytes(b"554 5.7.7 Empty message not accepted.\r\n")
                .await;
        }
        if self.rcpt_to.is_empty() {
            return self.write_bytes(b"503 5.5.1 Missing RCPT TO.\r\n").await;
        }
        let mail_from = if let Some(mail_from) = self.mail_from.take() {
            mail_from
        } else {
            return self.write_bytes(b"503 5.5.1 Missing MAIL FROM.\r\n").await;
        };
        let rcpt_to = std::mem::take(&mut self.rcpt_to);
        let rcpt_to_ids = std::mem::take(&mut self.rcpt_to_ids);
        let message = std::mem::take(&mut self.message);

        // Ingest
        let result = if self.core.is_leader() {
            self.core.mail_ingest(mail_from, rcpt_to_ids, message).await
        } else {
            // Send request to leader
            match self
                .core
                .rpc_command(Command::IngestMessage {
                    mail_from,
                    rcpt_to: rcpt_to_ids,
                    raw_message: message,
                })
                .await
            {
                Some(CommandResponse::IngestMessage { result }) => result,
                Some(CommandResponse::Error { message }) => {
                    debug!("RPC failed: {}", message);
                    return self.write_bytes(b"450 4.3.2 Temporary Failure.\r\n").await;
                }
                _ => {
                    return self.write_bytes(b"450 4.3.2 Temporary Failure.\r\n").await;
                }
            }
        };

        let delivery_status = match result {
            Ok(delivery_status) => delivery_status,
            Err(err) => {
                return self.write_bytes(err.as_bytes()).await;
            }
        };

        // Build response
        let mut buf = Vec::with_capacity(128);
        for rcpt in &rcpt_to {
            let (code, mailbox, message) = match rcpt {
                RcptType::Mailbox { id, name } => match delivery_status.get(id).unwrap() {
                    DeliveryStatus::Success => (b"250 2.1.5", name.as_bytes(), &b"delivered."[..]),
                    DeliveryStatus::TemporaryFailure { reason } => {
                        (b"451 4.3.0", name.as_bytes(), reason.as_bytes())
                    }
                    DeliveryStatus::PermanentFailure { reason } => {
                        (b"550 5.5.0", name.as_bytes(), reason.as_bytes())
                    }
                },
                RcptType::List { ids, name } => {
                    // Count number of successes and failures
                    let mut success = 0;
                    let mut temp_failures = 0;

                    for id in ids {
                        match delivery_status.get(id).unwrap() {
                            DeliveryStatus::Success => {
                                success += 1;
                            }
                            DeliveryStatus::TemporaryFailure { .. } => {
                                temp_failures += 1;
                            }
                            _ => (),
                        }
                    }

                    if success > 0 {
                        (b"250 2.1.5", name.as_bytes(), &b"delivered."[..])
                    } else if temp_failures > 0 {
                        (b"451 4.3.0", name.as_bytes(), &b"temporary failure."[..])
                    } else {
                        (b"550 5.5.0", name.as_bytes(), &b"permanent failure."[..])
                    }
                }
            };

            buf.extend_from_slice(code);
            buf.extend_from_slice(b" <");
            buf.extend_from_slice(mailbox);
            buf.extend_from_slice(b"> ");
            buf.extend_from_slice(message);
            buf.extend_from_slice(b"\r\n");
        }

        self.write_bytes(&buf).await
    }

    pub async fn expand_rcpt(&self, email: &str) -> Option<Arc<RecipientType>> {
        if let Some(email) = sanitize_email(email) {
            #[cfg(not(test))]
            let is_local = self.core.is_leader() || self.core.is_up_to_date();
            #[cfg(test)]
            let is_local = self.core.is_leader();

            if is_local {
                let store = self.core.store.clone();
                match self
                    .core
                    .spawn_worker(move || store.expand_rcpt(email))
                    .await
                {
                    Ok(rt) => Some(rt),
                    Err(err) => {
                        error!("Failed to expand address: {}", err);
                        None
                    }
                }
            } else {
                // Send request to leader
                match self
                    .core
                    .rpc_command(Command::ExpandRcpt { mailbox: email })
                    .await
                {
                    Some(CommandResponse::ExpandRcpt { rt }) => Some(Arc::new(rt)),
                    Some(CommandResponse::Error { message }) => {
                        debug!("RPC failed: {}", message);
                        None
                    }
                    _ => None,
                }
            }
        } else {
            Some(Arc::new(RecipientType::NotFound))
        }
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn mail_ingest(
        &self,
        mail_from: String,
        rcpt_to: AHashSet<AccountId>,
        raw_message: Vec<u8>,
    ) -> Result<AHashMap<AccountId, DeliveryStatus>, String> {
        // Ingest message
        let store = self.store.clone();
        let (change_id, status) = match self
            .spawn_worker(move || Ok(store.mail_ingest(mail_from, rcpt_to, raw_message)))
            .await
            .unwrap()
        {
            Ok(status) => {
                let mut change_id = ChangeId::MAX;

                for rcpt_status in &status {
                    if let Status::Success { changes, .. } = rcpt_status {
                        change_id = changes.change_id;
                    }
                }

                (change_id, status)
            }
            Err(Status::TemporaryFailure { reason, .. }) => {
                return Err(format!("450 4.3.2 {}.\r\n", reason));
            }
            Err(Status::PermanentFailure { reason, .. }) => {
                return Err(format!("554 5.7.7 {}.\r\n", reason));
            }
            _ => unreachable!(),
        };

        // Wait for message to be committed
        if change_id != ChangeId::MAX && self.is_in_cluster() && !self.commit_index(change_id).await
        {
            return Err("450 4.3.2 Temporary cluster failure.\r\n".to_string());
        }

        // Process delivery status
        let mut delivery_status = AHashMap::with_capacity(status.len());
        for rcpt_status in status {
            match rcpt_status {
                Status::Success {
                    account_id,
                    changes,
                    //vacation_response,
                } => {
                    // Publish state change
                    let mut types = changes
                        .collections
                        .into_iter()
                        .filter_map(|c| Some((TypeState::try_from(c).ok()?, change_id)))
                        .collect::<Vec<_>>();
                    types.push((TypeState::EmailDelivery, change_id));

                    if let Err(err) = self
                        .publish_state_change(StateChange::new(account_id, types))
                        .await
                    {
                        error!("Failed to publish state change: {}", err);
                    }

                    // Send vacation response
                    /*if let Some(vacation_response) = vacation_response {
                        if let Err(err) = self
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
                    }*/

                    delivery_status.insert(account_id, DeliveryStatus::Success);
                }
                Status::TemporaryFailure { account_id, reason } => {
                    delivery_status.insert(account_id, DeliveryStatus::TemporaryFailure { reason });
                }
                Status::PermanentFailure { account_id, reason } => {
                    delivery_status.insert(account_id, DeliveryStatus::PermanentFailure { reason });
                }
            }
        }

        Ok(delivery_status)
    }
}

pub trait JMAPMailIngest {
    fn mail_ingest(
        &self,
        mail_from: String,
        rcpt_to: AHashSet<AccountId>,
        raw_message: Vec<u8>,
    ) -> Result<Vec<Status>, Status>;
    fn mail_deliver_rcpt(
        &self,
        account_id: AccountId,
        document: &Document,
        return_address: Option<&str>,
    ) -> Status;
}

impl<T> JMAPMailIngest for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_ingest(
        &self,
        mail_from: String,
        rcpt_to: AHashSet<AccountId>,
        raw_message: Vec<u8>,
    ) -> Result<Vec<Status>, Status> {
        // Parse message
        let message = if let Some(message) = Message::parse(&raw_message) {
            message
        } else {
            return Err(Status::perm_fail(
                AccountId::MAX,
                "Failed to parse message.",
            ));
        };

        // Obtain return path for vacation response
        let return_address = sanitize_email(
            message
                .get_return_address()
                .unwrap_or_else(|| mail_from.as_ref()),
        )
        .and_then(|r| {
            // As per RFC3834
            if !r.starts_with("owner-")
                && !r.contains("-request@")
                && !r.starts_with("mailer-daemon")
            {
                Some(r)
            } else {
                None
            }
        });

        // Build message document
        let mut document = Document::new(Collection::Mail, DocumentId::MAX);
        let blob_id = BlobId::new_external(&raw_message);
        if let Err(err) = self.mail_parse_item(&mut document, blob_id.clone(), message, None) {
            error!("Failed to parse message during ingestion: {}", err);
            return Err(Status::internal_error(AccountId::MAX));
        }

        // Store raw message as a blob
        if let Err(err) = self.blob_store(&blob_id, raw_message) {
            error!("Failed to store blob during message ingestion: {}", err);
            return Err(Status::internal_error(AccountId::MAX));
        }

        // Deliver message to recipients
        let mut result = Vec::with_capacity(rcpt_to.len());
        for account_id in rcpt_to {
            result.push(self.mail_deliver_rcpt(account_id, &document, return_address.as_deref()));
        }

        Ok(result)
    }

    fn mail_deliver_rcpt(
        &self,
        account_id: AccountId,
        document: &Document,
        return_address: Option<&str>,
    ) -> Status {
        // Prepare batch
        let mut batch = WriteBatch::new(account_id);
        let mut document = document.clone();

        // Verify that this account has an Inbox mailbox
        match self.get_document_ids(account_id, Collection::Mailbox) {
            Ok(Some(mailbox_ids)) if mailbox_ids.contains(INBOX_ID) => (),
            _ => {
                error!("Account {} does not have an Inbox configured.", account_id);
                return Status::perm_fail(account_id, "Account does not have an inbox configured.");
            }
        }

        // Add mailbox tags
        let mut orm = TinyORM::<Email>::new();
        batch.log_child_update(Collection::Mailbox, JMAPId::new(INBOX_ID.into()));
        orm.tag(Property::MailboxIds, Tag::Id(INBOX_ID));

        // Serialize ORM
        if let Err(err) = orm.insert(&mut document) {
            error!("Failed to update ORM during ingestion: {}", err);
            return Status::internal_error(account_id);
        }

        // Obtain document id
        let document_id = match self.assign_document_id(account_id, Collection::Mail) {
            Ok(document_id) => document_id,
            Err(err) => {
                error!("Failed to assign document id during ingestion: {}", err);
                return Status::internal_error(account_id);
            }
        };
        document.document_id = document_id;

        // Build vacation response
        /*let vacation_response = if let Some(return_address) = &return_address {
            match self.get_account_details(account_id) {
                Ok(Some((email, from_name, _))) => {
                    match self.build_vacation_response(
                        account_id,
                        from_name.as_str().into(),
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
                }
                Ok(None) => None,
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
        };*/

        // Lock account while threads are merged
        let _lock = self.lock_collection(account_id, Collection::Mail);

        // Obtain thread Id
        match self.mail_set_thread(&mut batch, &mut document) {
            Ok(thread_id) => {
                // Write document to store
                batch.log_insert(Collection::Mail, JMAPId::from_parts(thread_id, document_id));
                batch.insert_document(document);
                match self.write(batch) {
                    Ok(Some(changes)) => Status::Success {
                        account_id,
                        changes,
                        //vacation_response,
                    },
                    Ok(None) => {
                        error!("Unexpected error during ingestion.");
                        Status::internal_error(account_id)
                    }
                    Err(err) => {
                        error!("Failed to write document during ingestion: {}", err);
                        Status::internal_error(account_id)
                    }
                }
            }
            Err(err) => {
                error!("Failed to set threadId during ingestion: {}", err);
                Status::internal_error(account_id)
            }
        }
    }
}

pub enum Status {
    Success {
        account_id: AccountId,
        changes: Changes,
        //vacation_response: Option<VacationMessage>,
    },
    TemporaryFailure {
        account_id: AccountId,
        reason: Cow<'static, str>,
    },
    PermanentFailure {
        account_id: AccountId,
        reason: Cow<'static, str>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum DeliveryStatus {
    Success,
    TemporaryFailure { reason: Cow<'static, str> },
    PermanentFailure { reason: Cow<'static, str> },
}

impl Status {
    pub fn internal_error(account_id: AccountId) -> Status {
        Status::TemporaryFailure {
            account_id,
            reason: "Internal error, please try again later.".into(),
        }
    }

    pub fn temp_fail(account_id: AccountId, reason: impl Into<Cow<'static, str>>) -> Status {
        Status::TemporaryFailure {
            account_id,
            reason: reason.into(),
        }
    }

    pub fn perm_fail(account_id: AccountId, reason: impl Into<Cow<'static, str>>) -> Status {
        Status::PermanentFailure {
            account_id,
            reason: reason.into(),
        }
    }
}
