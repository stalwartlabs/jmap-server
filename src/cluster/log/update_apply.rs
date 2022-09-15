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

use super::DocumentUpdate;
use jmap::jmap_store::RaftObject;
use jmap::orm::serialize::JMAPOrm;
use jmap::orm::TinyORM;
use jmap::principal::schema::Principal;
use jmap::push_subscription::schema::PushSubscription;
use jmap::push_subscription::set::JMAPSetPushSubscription;
use jmap_mail::email_submission::schema::EmailSubmission;
use jmap_mail::email_submission::set::JMAPSetEmailSubmission;
use jmap_mail::identity::schema::Identity;
use jmap_mail::identity::set::JMAPSetIdentity;
use jmap_mail::mail::schema::Email;
use jmap_mail::mail::set::JMAPSetMail;
use jmap_mail::mailbox::schema::Mailbox;
use jmap_mail::mailbox::set::JMAPSetMailbox;
use jmap_mail::vacation_response::schema::VacationResponse;
use jmap_mail::vacation_response::set::JMAPSetVacationResponse;
use jmap_sharing::principal::set::JMAPSetPrincipal;
use store::core::collection::Collection;
use store::core::document::Document;
use store::core::error::StoreError;
use store::core::JMAPIdPrefix;
use store::serialize::StoreDeserialize;
use store::write::batch::WriteBatch;
use store::write::options::IndexOptions;
use store::{DocumentId, JMAPStore, Store};

pub trait RaftStoreApplyUpdate<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn apply_update(
        &self,
        write_batch: &mut WriteBatch,
        collection: Collection,
        update: DocumentUpdate,
    ) -> store::Result<()>;

    fn raft_apply_update<U>(
        &self,
        write_batch: &mut WriteBatch,
        update: DocumentUpdate,
    ) -> store::Result<()>
    where
        U: RaftObject<T> + 'static;

    fn delete_document(
        &self,
        write_batch: &mut WriteBatch,
        collection: Collection,
        document_id: DocumentId,
    ) -> store::Result<()>;
}

impl<T> RaftStoreApplyUpdate<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn apply_update(
        &self,
        write_batch: &mut WriteBatch,
        collection: Collection,
        update: DocumentUpdate,
    ) -> store::Result<()> {
        match collection {
            Collection::Mail => self.raft_apply_update::<Email>(write_batch, update),
            Collection::Mailbox => self.raft_apply_update::<Mailbox>(write_batch, update),
            Collection::Principal => self.raft_apply_update::<Principal>(write_batch, update),
            Collection::PushSubscription => {
                self.raft_apply_update::<PushSubscription>(write_batch, update)
            }
            Collection::Identity => self.raft_apply_update::<Identity>(write_batch, update),
            Collection::EmailSubmission => {
                self.raft_apply_update::<EmailSubmission>(write_batch, update)
            }
            Collection::VacationResponse => {
                self.raft_apply_update::<VacationResponse>(write_batch, update)
            }
            Collection::Thread | Collection::None => {
                debug_assert!(false, "Unsupported update for {:?}", collection);
                Ok(())
            }
        }
    }

    fn raft_apply_update<U>(
        &self,
        write_batch: &mut WriteBatch,
        update: DocumentUpdate,
    ) -> store::Result<()>
    where
        U: RaftObject<T> + 'static,
    {
        match update {
            DocumentUpdate::Insert {
                jmap_id,
                fields,
                blobs,
                term_index,
            } => {
                let document_id = jmap_id.get_document_id();
                let mut document = Document::new(U::collection(), document_id);
                TinyORM::<U>::deserialize(&fields)
                    .ok_or_else(|| {
                        StoreError::InternalError("Failed to deserialize ORM.".to_string())
                    })?
                    .insert(&mut document)?;
                if let Some(term_index) = term_index {
                    document.term_index(term_index, IndexOptions::new());
                }

                U::on_raft_update(self, write_batch, &mut document, jmap_id, blobs.into())?;

                write_batch.insert_document(document);
            }
            DocumentUpdate::Update { jmap_id, fields } => {
                let document_id = jmap_id.get_document_id();
                let mut document = Document::new(U::collection(), document_id);

                self.get_orm::<U>(write_batch.account_id, document_id)?
                    .ok_or_else(|| {
                        StoreError::NotFound(format!(
                            "ORM for document {:?}/{} not found.",
                            U::collection(),
                            document_id
                        ))
                    })?
                    .merge_full(
                        &mut document,
                        TinyORM::<U>::deserialize(&fields).ok_or_else(|| {
                            StoreError::InternalError("Failed to deserialize ORM.".to_string())
                        })?,
                    )?;

                U::on_raft_update(self, write_batch, &mut document, jmap_id, None)?;

                if !document.is_empty() {
                    write_batch.update_document(document);
                }
            }
            DocumentUpdate::Delete { document_id } => {
                // Deletes received via DocumentUpdate only happen during rollbacks
                // so if the item is not found, it is safe to ignore it as it
                // was deleted on this node as well.
                match self.delete_document(write_batch, U::collection(), document_id) {
                    Err(StoreError::NotFound(_)) => {}
                    err => return err,
                }
            }
        }

        Ok(())
    }

    fn delete_document(
        &self,
        write_batch: &mut WriteBatch,
        collection: Collection,
        document_id: DocumentId,
    ) -> store::Result<()> {
        let mut document = Document::new(collection, document_id);
        match collection {
            Collection::Mail => {
                self.mail_delete(write_batch.account_id, None, &mut document)?;
            }
            Collection::Mailbox => {
                self.mailbox_delete(write_batch.account_id, &mut document)?;
            }
            Collection::Principal => self.principal_delete(write_batch, &mut document)?,
            Collection::PushSubscription => {
                self.push_subscription_delete(write_batch.account_id, &mut document)?
            }
            Collection::Identity => self.identity_delete(write_batch.account_id, &mut document)?,
            Collection::EmailSubmission => {
                self.email_submission_delete(write_batch.account_id, &mut document)?
            }
            Collection::VacationResponse => {
                self.vacation_response_delete(write_batch.account_id, &mut document)?
            }
            Collection::Thread | Collection::None => unreachable!(),
        }
        write_batch.delete_document(document);
        Ok(())
    }
}
