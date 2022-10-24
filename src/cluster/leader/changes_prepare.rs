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

use crate::cluster::log::changes_merge::MergedChanges;
use crate::cluster::log::update_prepare::RaftStorePrepareUpdate;
use crate::cluster::log::{DocumentUpdate, Update};
use crate::JMAPServer;
use jmap::principal::schema::Principal;
use jmap::push_subscription::schema::PushSubscription;
use jmap_mail::email_submission::schema::EmailSubmission;
use jmap_mail::identity::schema::Identity;
use jmap_mail::mail::schema::Email;
use jmap_mail::mailbox::schema::Mailbox;
use jmap_sieve::sieve_script::schema::SieveScript;
use store::core::collection::Collection;
use store::core::error::StoreError;
use store::tracing::debug;
use store::{AccountId, Store};

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn prepare_changes(
        &self,
        account_id: AccountId,
        collection: Collection,
        changes: &mut MergedChanges,
        is_follower_rollback: bool,
        max_batch_size: usize,
    ) -> store::Result<Vec<Update>> {
        let mut batch_size = 0;
        let mut updates = Vec::new();

        loop {
            let (document_id, is_insert) = if let Some(document_id) = changes.inserts.min() {
                changes.inserts.remove(document_id);
                (document_id, true)
            } else if let Some(document_id) = changes.updates.min() {
                changes.updates.remove(document_id);
                (document_id, false)
            } else {
                break;
            };

            let store = self.store.clone();
            let item = self
                .spawn_worker(move || match collection {
                    Collection::Mail => {
                        store.raft_prepare_update::<Email>(account_id, document_id, is_insert)
                    }
                    Collection::Mailbox => {
                        store.raft_prepare_update::<Mailbox>(account_id, document_id, is_insert)
                    }
                    Collection::Principal => {
                        store.raft_prepare_update::<Principal>(account_id, document_id, is_insert)
                    }
                    Collection::PushSubscription => store.raft_prepare_update::<PushSubscription>(
                        account_id,
                        document_id,
                        is_insert,
                    ),
                    Collection::Identity => {
                        store.raft_prepare_update::<Identity>(account_id, document_id, is_insert)
                    }
                    Collection::EmailSubmission => store.raft_prepare_update::<EmailSubmission>(
                        account_id,
                        document_id,
                        is_insert,
                    ),
                    Collection::SieveScript => {
                        store.raft_prepare_update::<SieveScript>(account_id, document_id, is_insert)
                    }
                    Collection::Thread | Collection::None => Err(StoreError::InternalError(
                        "Unsupported collection for changes".into(),
                    )),
                })
                .await?;

            if updates.is_empty() {
                updates.push(Update::Begin {
                    account_id,
                    collection,
                });
            }
            if let Some(item) = item {
                batch_size += item.size();
                updates.push(Update::Document { update: item });
            } else if is_follower_rollback {
                updates.push(Update::Document {
                    update: DocumentUpdate::Delete { document_id },
                });
            } else {
                debug!(
                    "Warning: Failed to fetch document {} in collection {:?}",
                    document_id, collection,
                );
            }

            if batch_size >= max_batch_size {
                break;
            }
        }

        if !updates.is_empty() {
            updates.push(Update::Eof);
        }

        Ok(updates)
    }
}
