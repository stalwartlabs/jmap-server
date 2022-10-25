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
        schema::{Email, Keyword, Property},
    },
    mail_parser::Message,
    INBOX_ID,
};
use jmap_sharing::principal::account::JMAPAccountStore;
use jmap_sieve::{
    sieve_script::{get::JMAPGetSieveScript, schema::Value},
    SeenIdHash,
};
use serde::{Deserialize, Serialize};
use store::{
    ahash::{AHashMap, AHashSet},
    blob::BlobId,
    core::{collection::Collection, document::Document, tag::Tag},
    log::changes::ChangeId,
    sieve::{Event, Input},
    tracing::{debug, error},
    write::{batch::WriteBatch, update::Changes},
    AccountId, DocumentId, JMAPStore, RecipientType, Store,
};

use crate::{
    cluster::rpc::command::{Command, CommandResponse},
    services::{email_delivery, state_change::StateChange},
    JMAPServer,
};

use super::{
    session::{RcptType, Session},
    OutgoingMessage,
};

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
        let message = std::mem::take(&mut self.message);

        // Ingest
        let result = if self.core.is_leader() {
            self.core
                .mail_ingest(mail_from, std::mem::take(&mut self.rcpt_to), message)
                .await
        } else {
            // Send request to leader
            match self
                .core
                .rpc_command(Command::IngestMessage {
                    mail_from,
                    rcpt_to: std::mem::take(&mut self.rcpt_to),
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

        let rcpt_to = match result {
            Ok(rcpt_to) => rcpt_to,
            Err(err) => {
                return self.write_bytes(err.as_bytes()).await;
            }
        };

        // Build response
        let mut buf = Vec::with_capacity(128);
        for rcpt in &rcpt_to {
            if let RcptType::Mailbox { name, status, .. } | RcptType::List { name, status, .. } =
                rcpt
            {
                buf.extend_from_slice(match status {
                    DeliveryStatus::Success => b"250 2.1.5 <",
                    DeliveryStatus::TemporaryFailure { .. } => b"451 4.3.0 <",
                    DeliveryStatus::PermanentFailure { .. } => b"550 5.5.0 <",
                    DeliveryStatus::Duplicated => continue,
                });
                buf.extend_from_slice(name.as_bytes());
                buf.extend_from_slice(b"> ");
                buf.extend_from_slice(match status {
                    DeliveryStatus::Success => b"delivered",
                    DeliveryStatus::TemporaryFailure { reason }
                    | DeliveryStatus::PermanentFailure { reason } => reason.as_bytes(),
                    DeliveryStatus::Duplicated => continue,
                });
                buf.extend_from_slice(b"\r\n");
            }
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
        rcpt_to: Vec<RcptType>,
        raw_message: Vec<u8>,
    ) -> Result<Vec<RcptType>, String> {
        // Ingest message
        let store = self.store.clone();
        let status = match self
            .spawn_worker(move || Ok(store.mail_ingest(mail_from, rcpt_to, raw_message)))
            .await
            .unwrap()
        {
            Ok(status) => status,
            Err(Some(reason)) => {
                return Err(format!("554 5.7.7 {}.\r\n", reason));
            }
            Err(None) => {
                return Err("450 4.3.2 Temporary server failure.\r\n".to_string());
            }
            _ => unreachable!(),
        };

        // Wait for message to be committed
        if status.last_change_id != ChangeId::MAX
            && self.is_in_cluster()
            && !self.commit_index(status.last_change_id).await
        {
            return Err("450 4.3.2 Temporary cluster failure.\r\n".to_string());
        }

        // Publish state changes
        for (account_id, changes) in status.changes {
            let mut types = changes
                .collections
                .into_iter()
                .filter_map(|c| Some((TypeState::try_from(c).ok()?, changes.change_id)))
                .collect::<Vec<_>>();
            types.push((TypeState::EmailDelivery, changes.change_id));

            if let Err(err) = self
                .publish_state_change(StateChange::new(account_id, types))
                .await
            {
                error!("Failed to publish state change: {}", err);
            }
        }

        // Send any outgoing messages
        for message in status.messages {
            if let Err(err) = self
                .notify_email_delivery(email_delivery::Event::outgoing_message(
                    message.mail_from,
                    message.rcpt_to,
                    message.message,
                ))
                .await
            {
                error!(
                    "No e-mail delivery configured or something else happened: {}",
                    err
                );
                break;
            }
        }

        Ok(status.rcpt_to)
    }
}

pub trait JMAPMailIngest {
    fn mail_ingest(
        &self,
        mail_from: String,
        rcpt_to: Vec<RcptType>,
        raw_message: Vec<u8>,
    ) -> Result<IngestResult, Option<&'static str>>;

    fn mail_deliver_rcpt(
        &self,
        result: &mut IngestResult,
        account_id: AccountId,
        raw_message: &[u8],
        blob_id: &BlobId,
        envelope_from: &str,
        envelope_to: &str,
    ) -> Result<(), Option<String>>;

    #[allow(clippy::result_unit_err)]
    fn mail_deliver_mailbox(
        &self,
        result: &mut IngestResult,
        account_id: AccountId,
        message: Message,
        blob_id: &BlobId,
        mailbox_ids: &[DocumentId],
        flags: Vec<String>,
    ) -> Result<(), ()>;
}

impl<T> JMAPMailIngest for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn mail_ingest(
        &self,
        mail_from: String,
        rcpt_to: Vec<RcptType>,
        raw_message: Vec<u8>,
    ) -> Result<IngestResult, Option<&'static str>> {
        // Store raw message as a blob
        let blob_id = BlobId::new_external(&raw_message);
        let raw_message = self.blob_store(&blob_id, raw_message).map_err(|err| {
            error!("Failed to store blob during message ingestion: {}", err);
            None
        })?;

        // Deliver message to recipients
        let mut result = IngestResult {
            rcpt_to: Vec::with_capacity(rcpt_to.len()),
            changes: AHashMap::with_capacity(rcpt_to.len()),
            messages: Vec::new(),
            last_change_id: ChangeId::MAX,
        };
        let mut prev_status = if rcpt_to.iter().any(|s| {
            matches!(
                s,
                RcptType::Mailbox {
                    status: DeliveryStatus::Duplicated,
                    ..
                } | RcptType::List {
                    status: DeliveryStatus::Duplicated,
                    ..
                }
            )
        }) {
            AHashMap::new().into()
        } else {
            None
        };
        for mut recipient in rcpt_to {
            match &mut recipient {
                RcptType::Mailbox { id, name, status } => {
                    if !matches!(status, DeliveryStatus::Duplicated) {
                        match self.mail_deliver_rcpt(
                            &mut result,
                            *id,
                            &raw_message,
                            &blob_id,
                            &mail_from,
                            &*name,
                        ) {
                            Ok(_) => {
                                *status = DeliveryStatus::Success;
                            }
                            Err(Some(err)) => {
                                *status = DeliveryStatus::PermanentFailure { reason: err.into() };
                            }
                            Err(None) => {
                                *status = DeliveryStatus::TemporaryFailure {
                                    reason: "Temporary sever failure".into(),
                                };
                            }
                        }
                        if let Some(prev_status) = &mut prev_status {
                            prev_status.insert(*id, status.clone());
                        }
                    } else {
                        *status = prev_status
                            .as_ref()
                            .unwrap()
                            .get(id)
                            .unwrap_or(&DeliveryStatus::Success)
                            .clone();
                    }
                }
                RcptType::List { ids, name, status } => {
                    if !matches!(status, DeliveryStatus::Duplicated) {
                        // Count number of successes and failures
                        let mut success = 0;
                        let mut temp_failures = 0;

                        for &account_id in ids.iter() {
                            let status = match self.mail_deliver_rcpt(
                                &mut result,
                                account_id,
                                &raw_message,
                                &blob_id,
                                &mail_from,
                                &*name,
                            ) {
                                Ok(_) => {
                                    success += 1;
                                    DeliveryStatus::Success
                                }
                                Err(Some(err)) => {
                                    DeliveryStatus::PermanentFailure { reason: err.into() }
                                }
                                Err(None) => {
                                    temp_failures += 1;
                                    DeliveryStatus::TemporaryFailure {
                                        reason: "Temporary sever failure".into(),
                                    }
                                }
                            };

                            if let Some(prev_status) = &mut prev_status {
                                prev_status.insert(account_id, status);
                            }
                        }

                        *status = if success > 0 {
                            DeliveryStatus::Success
                        } else if temp_failures > 0 {
                            DeliveryStatus::TemporaryFailure {
                                reason: "temporary failure".into(),
                            }
                        } else {
                            DeliveryStatus::PermanentFailure {
                                reason: "permanent failure".into(),
                            }
                        };
                    } else {
                        *status = DeliveryStatus::Success;
                    }
                }
            }

            result.rcpt_to.push(recipient);
        }

        Ok(result)
    }

    fn mail_deliver_rcpt(
        &self,
        result: &mut IngestResult,
        account_id: AccountId,
        raw_message: &[u8],
        blob_id: &BlobId,
        envelope_from: &str,
        envelope_to: &str,
    ) -> Result<(), Option<String>> {
        // Verify that this account has an Inbox mailbox
        if !matches!(self.get_document_ids(account_id, Collection::Mailbox), Ok(Some(mailbox_ids)) if mailbox_ids.contains(INBOX_ID))
        {
            error!("Account {} does not have an Inbox configured.", account_id);
            return Err("Account does not have an inbox configured."
                .to_string()
                .into());
        }

        // Parse message
        let message = if let Some(message) = Message::parse(raw_message) {
            message
        } else {
            return Err("Failed to parse message.".to_string().into());
        };

        if let Some(active_script) = self.sieve_script_get_active(account_id).map_err(|err| {
            error!("Failed to get SieveScript for {}: {}", account_id, err);
            None
        })? {
            let mut instance = self.sieve_runtime.filter_parsed(message);
            let mut input = Input::script(
                if let Some(Value::Text { value }) = active_script
                    .orm
                    .get(&jmap_sieve::sieve_script::schema::Property::Name)
                {
                    value.to_string()
                } else {
                    account_id.to_string()
                },
                active_script.script,
            );
            let mut new_ids = AHashSet::new();

            while let Some(event) = instance.run(input) {
                match event {
                    Ok(event) => match event {
                        Event::IncludeScript { name, optional } => todo!(),
                        Event::MailboxExists {
                            mailboxes,
                            special_use,
                        } => todo!(),
                        Event::DuplicateId { id, expiry, last } => {
                            let id_hash = SeenIdHash::new(&id, expiry);
                            let seen_id = active_script.seen_ids.contains(&id_hash);
                            if !seen_id || last {
                                new_ids.insert(id_hash);
                            }

                            input = seen_id.into();
                        }
                        Event::Discard => {
                            input = true.into();
                        }
                        Event::Reject { extended, reason } => todo!(),
                        Event::Keep { flags, message_id } => todo!(),
                        Event::FileInto {
                            folder,
                            flags,
                            mailbox_id,
                            special_use,
                            create,
                            message_id,
                        } => todo!(),
                        Event::SendMessage {
                            recipient,
                            notify,
                            return_of_content,
                            by_time,
                            message_id,
                        } => todo!(),
                        Event::ListContains { .. }
                        | Event::Execute { .. }
                        | Event::Notify { .. } => {
                            // Not allowed
                            input = false.into();
                        }
                        Event::CreatedMessage {
                            message_id,
                            message,
                        } => todo!(),

                        _ => unreachable!(),
                    },
                    Err(err) => todo!(),
                }
            }

            Ok(())
        } else {
            self.mail_deliver_mailbox(
                result,
                account_id,
                message,
                blob_id,
                &[INBOX_ID],
                Vec::new(),
            )
            .map_err(|_| None)
        }
    }

    fn mail_deliver_mailbox(
        &self,
        result: &mut IngestResult,
        account_id: AccountId,
        message: Message,
        blob_id: &BlobId,
        mailbox_ids: &[DocumentId],
        flags: Vec<String>,
    ) -> Result<(), ()> {
        // Prepare batch
        let mut batch = WriteBatch::new(account_id);

        // Obtain document id
        let document_id = match self.assign_document_id(account_id, Collection::Mail) {
            Ok(document_id) => document_id,
            Err(err) => {
                error!("Failed to assign document id during ingestion: {}", err);
                return Err(());
            }
        };
        let mut document = Document::new(Collection::Mail, document_id);

        // Add mailbox tags
        let mut orm = TinyORM::<Email>::new();
        for mailbox_id in mailbox_ids {
            batch.log_child_update(Collection::Mailbox, *mailbox_id);
            orm.tag(Property::MailboxIds, Tag::Id(*mailbox_id));
        }
        for flag in flags {
            orm.tag(Property::Keywords, Keyword::parse(&flag).tag);
        }

        // Serialize ORM
        if let Err(err) = orm.insert(&mut document) {
            error!("Failed to update ORM during ingestion: {}", err);
            return Err(());
        }

        // Build message document
        if let Err(err) = self.mail_parse_item(&mut document, blob_id.clone(), message, None) {
            error!("Failed to parse message during ingestion: {}", err);
            return Err(());
        }

        // Lock account while threads are merged
        let _lock = self.lock_collection(account_id, Collection::Mail);

        // Obtain thread Id
        match self.mail_set_thread(&mut batch, &mut document) {
            Ok(thread_id) => {
                // Write document to store
                batch.log_insert(Collection::Mail, JMAPId::from_parts(thread_id, document_id));
                batch.insert_document(document);
                match self.write(batch) {
                    Ok(Some(changes)) => {
                        result.last_change_id = changes.change_id;
                        result.changes.insert(account_id, changes);
                        Ok(())
                    }
                    Ok(None) => {
                        error!("Unexpected error during ingestion.");
                        Err(())
                    }
                    Err(err) => {
                        error!("Failed to write document during ingestion: {}", err);
                        Err(())
                    }
                }
            }
            Err(err) => {
                error!("Failed to set threadId during ingestion: {}", err);
                Err(())
            }
        }
    }
}

pub struct IngestResult {
    pub rcpt_to: Vec<RcptType>,
    pub changes: AHashMap<AccountId, Changes>,
    pub last_change_id: ChangeId,
    pub messages: Vec<OutgoingMessage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeliveryStatus {
    Success,
    TemporaryFailure { reason: Cow<'static, str> },
    PermanentFailure { reason: Cow<'static, str> },
    Duplicated,
}
