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

use std::{borrow::Cow, sync::Arc, time::SystemTime};

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
    mailbox::{get::JMAPGetMailbox, is_valid_role, set::JMAPSetMailbox},
    INBOX_ID, TRASH_ID,
};
use jmap_sharing::principal::account::JMAPAccountStore;
use jmap_sieve::{
    sieve_script::{
        get::JMAPGetSieveScript,
        schema::{CompiledScript, Value},
    },
    SeenIdHash, SeenIds,
};
use serde::{Deserialize, Serialize};
use store::{
    ahash::{AHashMap, AHashSet},
    blob::BlobId,
    core::{collection::Collection, document::Document, tag::Tag},
    log::changes::ChangeId,
    sieve::{Compiler, Envelope, Event, Input, Mailbox, Recipient},
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
        self.rcpt_to_dup.clear();

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
            let (RcptType::Mailbox { name, status, .. } | RcptType::List { name, status, .. }) =
                rcpt;
            match status {
                DeliveryStatus::Success => buf.extend_from_slice(b"250 2.1.5 <"),
                DeliveryStatus::TemporaryFailure { .. } => buf.extend_from_slice(b"451 4.3.0 <"),
                DeliveryStatus::PermanentFailure { code, .. } => {
                    buf.extend_from_slice(b"550 ");
                    buf.extend_from_slice(code.as_bytes());
                    buf.extend_from_slice(b" <");
                }
                DeliveryStatus::Duplicated => continue,
            }
            buf.extend_from_slice(name.as_bytes());
            buf.extend_from_slice(b"> ");
            buf.extend_from_slice(match status {
                DeliveryStatus::Success => b"delivered",
                DeliveryStatus::TemporaryFailure { reason }
                | DeliveryStatus::PermanentFailure { reason, .. } => reason.as_bytes(),
                DeliveryStatus::Duplicated => continue,
            });
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
    ) -> DeliveryStatus;

    #[allow(clippy::result_unit_err)]
    fn mail_deliver_mailbox(
        &self,
        result: &mut IngestResult,
        account_id: AccountId,
        message: Message,
        blob_id: &BlobId,
        mailbox_ids: &[DocumentId],
        flags: Vec<Tag>,
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
                        *status = self.mail_deliver_rcpt(
                            &mut result,
                            *id,
                            &raw_message,
                            &blob_id,
                            &mail_from,
                            &*name,
                        );
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
                            let status = self.mail_deliver_rcpt(
                                &mut result,
                                account_id,
                                &raw_message,
                                &blob_id,
                                &mail_from,
                                &*name,
                            );

                            match &status {
                                DeliveryStatus::Success => {
                                    success += 1;
                                }
                                DeliveryStatus::TemporaryFailure { .. } => {
                                    temp_failures += 1;
                                }
                                _ => (),
                            }

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
                                code: "5.5.0".into(),
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
    ) -> DeliveryStatus {
        // Verify that this account has an Inbox mailbox
        let mailbox_ids = match self.get_document_ids(account_id, Collection::Mailbox) {
            Ok(Some(mailbox_ids)) if mailbox_ids.contains(INBOX_ID) => mailbox_ids,
            _ => {
                error!("Account {} does not have an Inbox configured.", account_id);
                return DeliveryStatus::perm_failure("Account does not have an inbox configured.");
            }
        };

        // Parse message
        let message = if let Some(message) = Message::parse(raw_message) {
            message
        } else {
            return DeliveryStatus::perm_failure("Failed to parse message.");
        };

        let mut active_script = match self.sieve_script_get_active(account_id) {
            Ok(None) => {
                return if self
                    .mail_deliver_mailbox(
                        result,
                        account_id,
                        message,
                        blob_id,
                        &[INBOX_ID],
                        Vec::new(),
                    )
                    .is_ok()
                {
                    DeliveryStatus::Success
                } else {
                    DeliveryStatus::internal_error()
                };
            }
            Ok(Some(active_script)) => active_script,
            Err(err) => {
                error!("Failed to get SieveScript for {}: {}", account_id, err);
                return if self
                    .mail_deliver_mailbox(
                        result,
                        account_id,
                        message,
                        blob_id,
                        &[INBOX_ID],
                        Vec::new(),
                    )
                    .is_ok()
                {
                    DeliveryStatus::Success
                } else {
                    DeliveryStatus::internal_error()
                };
            }
        };

        let mut instance = self.sieve_runtime.filter_parsed(message);

        // Set account details
        let mail_from = match self.get_account_details(account_id) {
            Ok(Some((email, name, _))) => {
                let mail_from = email.clone();
                instance.set_user_address(email);
                instance.set_user_full_name(&name);
                mail_from
            }
            _ => {
                error!("Failed to obtain account details for {}.", account_id);
                instance.set_user_address(envelope_to.to_string());
                envelope_to.to_string()
            }
        };

        // Set envelope
        instance.set_envelope(Envelope::From, envelope_from);
        instance.set_envelope(Envelope::To, envelope_to);

        let mut input = Input::script(
            if let Some(Value::Text { value }) = active_script
                .orm
                .get(&jmap_sieve::sieve_script::schema::Property::Name)
            {
                value.to_string()
            } else {
                account_id.to_string()
            },
            active_script.script.clone(),
        );

        let mut do_discard = false;
        let mut do_deliver = false;

        let mut new_ids = AHashSet::new();
        let mut reject_reason = None;
        let mut messages: Vec<SieveMessage> = vec![SieveMessage {
            raw_message: raw_message.into(),
            file_into: Vec::new(),
            flags: Vec::new(),
        }];
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        while let Some(event) = instance.run(input) {
            match event {
                Ok(event) => match event {
                    Event::IncludeScript { name, .. } => {
                        if let Ok(Some(script)) =
                            self.sieve_script_get_by_name(account_id, name.as_str().to_string())
                        {
                            input = Input::script(name, script);
                        } else {
                            input = false.into();
                        }
                    }
                    Event::MailboxExists {
                        mailboxes,
                        special_use,
                    } => {
                        if !mailboxes.is_empty() {
                            let special_use = special_use
                                .into_iter()
                                .map(|role| {
                                    if role.eq_ignore_ascii_case("inbox") {
                                        INBOX_ID
                                    } else if role.eq_ignore_ascii_case("trash") {
                                        TRASH_ID
                                    } else {
                                        let mut mailbox_id = DocumentId::MAX;
                                        let role = role.to_ascii_lowercase();
                                        if is_valid_role(&role) {
                                            if let Ok(Some(mailbox_id_)) =
                                                self.mailbox_get_by_role(account_id, &role)
                                            {
                                                mailbox_id = mailbox_id_;
                                            }
                                        }
                                        mailbox_id
                                    }
                                })
                                .collect::<Vec<_>>();

                            let mut result = true;
                            for mailbox in mailboxes {
                                match mailbox {
                                    Mailbox::Name(name) => {
                                        if !matches!(
                                            self.mailbox_get_by_name(account_id, &name),
                                            Ok(Some(document_id)) if special_use.is_empty() ||
                                                        special_use.contains(&document_id)
                                        ) {
                                            result = false;
                                            break;
                                        }
                                    }
                                    Mailbox::Id(id) => {
                                        if !matches!(JMAPId::parse(&id), Some(id) if
                                                            mailbox_ids.contains(id.get_document_id()) &&
                                                            (special_use.is_empty() ||
                                                             special_use.contains(&id.get_document_id())))
                                        {
                                            result = false;
                                            break;
                                        }
                                    }
                                }
                            }
                            input = result.into();
                        } else if !special_use.is_empty() {
                            let mut result = true;

                            for role in special_use {
                                if !role.eq_ignore_ascii_case("inbox")
                                    && !role.eq_ignore_ascii_case("trash")
                                {
                                    let role = role.to_ascii_lowercase();
                                    if !is_valid_role(&role)
                                        || !matches!(
                                            self.mailbox_get_by_role(account_id, &role),
                                            Ok(Some(_))
                                        )
                                    {
                                        result = false;
                                        break;
                                    }
                                }
                            }
                            input = result.into();
                        } else {
                            input = false.into();
                        }
                    }
                    Event::DuplicateId { id, expiry, last } => {
                        let id_hash = SeenIdHash::new(&id, expiry + now);
                        let seen_id = active_script.seen_ids.contains(&id_hash);
                        if !seen_id || last {
                            new_ids.insert(id_hash);
                        }

                        input = seen_id.into();
                    }
                    Event::Discard => {
                        do_discard = true;
                        input = true.into();
                    }
                    Event::Reject { reason, .. } => {
                        reject_reason = reason.into();
                        do_discard = true;
                        input = true.into();
                    }
                    Event::Keep { flags, message_id } => {
                        if let Some(message) = messages.get_mut(message_id) {
                            message.flags =
                                flags.into_iter().map(|f| Keyword::parse(&f).tag).collect();
                            if !message.file_into.contains(&INBOX_ID) {
                                message.file_into.push(INBOX_ID);
                            }
                            do_deliver = true;
                        } else {
                            error!("Sieve filter failed: Unknown message id {}.", message_id);
                        }
                        input = true.into();
                    }
                    Event::FileInto {
                        folder,
                        flags,
                        mailbox_id,
                        special_use,
                        create,
                        message_id,
                    } => {
                        let mut target_id = DocumentId::MAX;

                        // Find mailbox by Id
                        if let Some(mailbox_id) = mailbox_id.and_then(|m| JMAPId::parse(&m)) {
                            let mailbox_id = mailbox_id.get_document_id();
                            if mailbox_ids.contains(mailbox_id) {
                                target_id = mailbox_id;
                            }
                        }

                        // Find mailbox by role
                        if let Some(special_use) = special_use {
                            if target_id == DocumentId::MAX {
                                if special_use.eq_ignore_ascii_case("inbox") {
                                    target_id = INBOX_ID;
                                } else if special_use.eq_ignore_ascii_case("trash") {
                                    target_id = TRASH_ID;
                                } else {
                                    let role = special_use.to_ascii_lowercase();
                                    if is_valid_role(&role) {
                                        if let Ok(Some(mailbox_id_)) =
                                            self.mailbox_get_by_role(account_id, &role)
                                        {
                                            target_id = mailbox_id_;
                                        }
                                    }
                                }
                            }
                        }

                        // Find mailbox by name
                        if target_id == DocumentId::MAX {
                            if !create {
                                if let Ok(Some(document_id)) =
                                    self.mailbox_get_by_name(account_id, &folder)
                                {
                                    target_id = document_id;
                                }
                            } else if let Ok(Some((document_id, changes))) =
                                self.mailbox_create_path(account_id, &folder)
                            {
                                target_id = document_id;
                                if let Some(changes) = changes {
                                    result.last_change_id = changes.change_id;
                                    result.changes.insert(account_id, changes);
                                }
                            }
                        }

                        // Default to Inbox
                        if target_id == DocumentId::MAX {
                            target_id = INBOX_ID;
                        }

                        if let Some(message) = messages.get_mut(message_id) {
                            message.flags =
                                flags.into_iter().map(|f| Keyword::parse(&f).tag).collect();
                            if !message.file_into.contains(&target_id) {
                                message.file_into.push(target_id);
                            }
                            do_deliver = true;
                        } else {
                            error!("Sieve filter failed: Unknown message id {}.", message_id);
                        }
                        input = true.into();
                    }
                    Event::SendMessage {
                        recipient,
                        message_id,
                        ..
                    } => {
                        input = true.into();

                        result.messages.push(OutgoingMessage {
                            mail_from: mail_from.clone(),
                            rcpt_to: match recipient {
                                Recipient::Address(rcpt) => vec![rcpt],
                                Recipient::Group(rcpts) => rcpts,
                                Recipient::List(_) => {
                                    // Not yet implemented
                                    continue;
                                }
                            },
                            message: if let Some(message) = messages.get(message_id) {
                                message.raw_message.to_vec()
                            } else {
                                error!("Sieve filter failed: Unknown message id {}.", message_id);
                                continue;
                            },
                        });
                    }
                    Event::ListContains { .. } | Event::Execute { .. } | Event::Notify { .. } => {
                        // Not allowed
                        input = false.into();
                    }
                    Event::CreatedMessage { message, .. } => {
                        messages.push(SieveMessage {
                            raw_message: message.into(),
                            file_into: Vec::new(),
                            flags: Vec::new(),
                        });
                        input = true.into();
                    }
                    #[allow(unreachable_patterns)]
                    _ => unreachable!(),
                },

                #[cfg(test)]
                Err(store::sieve::runtime::RuntimeError::ScriptErrorMessage(err)) => {
                    panic!("Sieve test failed: {}", err);
                }

                Err(err) => {
                    debug!("Sieve script runtime error: {}", err);
                    input = true.into();
                }
            }
        }

        for (pos, message) in messages.iter().enumerate() {
            println!(
                "----- message {} {:?} {:?}",
                pos, message.file_into, message.flags
            );
        }

        // Fail-safe, no discard and no keep seen, assume that something went wrong and file anyway.
        if !do_deliver && !do_discard {
            messages[0].file_into.push(INBOX_ID);
        }

        // Deliver messages
        let mut has_temp_errors = false;
        let mut has_delivered = false;
        for (message_id, sieve_message) in messages.into_iter().enumerate() {
            if !sieve_message.file_into.is_empty() {
                // Store newly generated message
                let (raw_message, blob_id) = if message_id > 0 {
                    let blob_id = BlobId::new_external(sieve_message.raw_message.as_ref());
                    match self.blob_store(&blob_id, sieve_message.raw_message.into_owned()) {
                        Ok(raw_message) => (raw_message.into(), blob_id),
                        Err(err) => {
                            error!("Failed to store blob: {}", err);
                            has_temp_errors = true;
                            continue;
                        }
                    }
                } else {
                    (sieve_message.raw_message, blob_id.clone())
                };

                // Parse message if needed
                let message = if message_id == 0 && !instance.has_message_changed() {
                    instance.take_message()
                } else if let Some(message) = Message::parse(raw_message.as_ref()) {
                    message
                } else {
                    debug!("Failed to parse Sieve generated message.");
                    continue;
                };

                // Deliver message
                if self
                    .mail_deliver_mailbox(
                        result,
                        account_id,
                        message,
                        &blob_id,
                        &sieve_message.file_into,
                        sieve_message.flags,
                    )
                    .is_ok()
                {
                    has_delivered = true;
                } else {
                    has_temp_errors = true;
                }
            }
        }

        // Save Sieve script changes
        if active_script.has_changes || !new_ids.is_empty() {
            drop(instance);
            active_script.seen_ids.extend(new_ids);
            let mut changes = TinyORM::track_changes(&active_script.orm);
            changes.set(
                jmap_sieve::sieve_script::schema::Property::SeenIds,
                jmap_sieve::sieve_script::schema::Value::SeenIds {
                    value: SeenIds {
                        ids: active_script.seen_ids,
                        has_changes: true,
                    },
                },
            );
            changes.set(
                jmap_sieve::sieve_script::schema::Property::CompiledScript,
                jmap_sieve::sieve_script::schema::Value::CompiledScript {
                    value: CompiledScript {
                        version: Compiler::VERSION,
                        script: match Arc::try_unwrap(active_script.script) {
                            Ok(script) => script,
                            #[cfg(test)]
                            Err(_) => {
                                panic!("Failed to unwrap Arc<Sieve>");
                            }
                            #[cfg(not(test))]
                            Err(script) => script.as_ref().clone(),
                        }
                        .into(),
                    },
                },
            );
            let mut document = Document::new(Collection::SieveScript, active_script.document_id);
            active_script.orm.merge(&mut document, changes).ok();
            let mut batch = WriteBatch::new(account_id);
            batch.update_document(document);
            batch.log_update(Collection::SieveScript, active_script.document_id);
            match self.write(batch) {
                Ok(Some(changes)) => {
                    result.last_change_id = changes.change_id;
                }
                Ok(None) => (),
                Err(err) => {
                    error!("Failed to write Sieve filter: {}", err);
                }
            }
        }

        if let Some(reject_reason) = reject_reason {
            DeliveryStatus::PermanentFailure {
                code: "5.7.1".into(),
                reason: reject_reason.into(),
            }
        } else if has_delivered || !has_temp_errors {
            DeliveryStatus::Success
        } else {
            // There were problems during delivery
            DeliveryStatus::internal_error()
        }
    }

    fn mail_deliver_mailbox(
        &self,
        result: &mut IngestResult,
        account_id: AccountId,
        message: Message,
        blob_id: &BlobId,
        mailbox_ids: &[DocumentId],
        flags: Vec<Tag>,
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
            orm.tag(Property::Keywords, flag);
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

struct SieveMessage<'x> {
    pub raw_message: Cow<'x, [u8]>,
    pub file_into: Vec<DocumentId>,
    pub flags: Vec<Tag>,
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
    TemporaryFailure {
        reason: Cow<'static, str>,
    },
    PermanentFailure {
        code: Cow<'static, str>,
        reason: Cow<'static, str>,
    },
    Duplicated,
}

impl DeliveryStatus {
    pub fn internal_error() -> Self {
        DeliveryStatus::TemporaryFailure {
            reason: "Temporary sever failure".into(),
        }
    }

    pub fn perm_failure(reason: impl Into<Cow<'static, str>>) -> Self {
        DeliveryStatus::PermanentFailure {
            code: "5.5.0".into(),
            reason: reason.into(),
        }
    }
}
