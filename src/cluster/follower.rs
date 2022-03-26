use std::collections::{hash_map, HashMap};

use jmap_mail::import::JMAPMailImport;
use jmap_mail::mailbox::JMAPMailMailbox;

use store::batch::WriteBatch;
use store::raft::{Entry, MergedChanges, RaftId, RawEntry, TermId};
use store::serialize::LogKey;
use store::tracing::{debug, error};
use store::WriteOperation;
use store::{lz4_flex, AccountId, Collection, ColumnFamily, JMAPStore, Store, StoreError};
use tokio::sync::{mpsc, oneshot};

use crate::cluster::log::{AppendEntriesResponse, UpdateCollection};
use crate::JMAPServer;

use super::log::{AppendEntriesRequest, Change, Event};

use super::{
    rpc::{self, Response},
    Cluster,
};
use super::{PeerId, IPC_CHANNEL_BUFFER};

#[derive(Debug)]
enum State {
    Append {
        commit_id: RaftId,
        pending_entries: Vec<RawEntry>,
    },
    Rollback {
        changes: MergedChanges,
    },
}

impl Default for State {
    fn default() -> Self {
        State::Append {
            commit_id: RaftId::none(),
            pending_entries: Vec::new(),
        }
    }
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn spawn_raft_follower(&self) -> mpsc::Sender<Event> {
        let (tx, mut rx) = mpsc::channel::<Event>(IPC_CHANNEL_BUFFER);
        let core = self.core.clone();
        let local_name = self.addr.to_string();

        debug!("[{}] Starting raft follower process.", local_name);

        tokio::spawn(async move {
            let mut state = match core.next_rollback_change().await {
                Ok(Some(changes)) => State::Rollback { changes },
                Ok(None) => State::default(),
                Err(err) => {
                    error!("Failed to obtain rollback changes: {:?}", err);
                    return;
                }
            };

            while let Some(event) = rx.recv().await {
                //println!("Follower: {:?}", state);

                let response = match (event.request, &mut state) {
                    (AppendEntriesRequest::Match { last_log }, State::Append { commit_id, .. }) => {
                        *commit_id = last_log;
                        if let Some(response) = core.handle_match_log(last_log).await {
                            response
                        } else {
                            break;
                        }
                    }

                    (
                        AppendEntriesRequest::Synchronize {
                            last_log,
                            match_terms,
                        },
                        State::Append { commit_id, .. },
                    ) => {
                        *commit_id = last_log;
                        if let Some(response) = core.handle_synchronize_log(match_terms).await {
                            response
                        } else {
                            break;
                        }
                    }

                    (AppendEntriesRequest::Merge { matched_log }, State::Append { .. }) => {
                        if let Some(response) = core.handle_merge_log(&mut state, matched_log).await
                        {
                            response
                        } else {
                            break;
                        }
                    }
                    (
                        AppendEntriesRequest::UpdateLog { last_log, entries },
                        State::Append {
                            commit_id,
                            pending_entries,
                        },
                    ) => {
                        *commit_id = last_log;
                        core.handle_update_log(last_log, entries, pending_entries)
                            .await
                    }

                    (
                        AppendEntriesRequest::UpdateStore {
                            account_id,
                            collection,
                            changes,
                        },
                        State::Append {
                            commit_id,
                            pending_entries,
                        },
                    ) => {
                        core.handle_update_store(
                            pending_entries,
                            *commit_id,
                            account_id,
                            collection,
                            changes,
                        )
                        .await
                    }

                    (
                        AppendEntriesRequest::UpdateStore {
                            account_id,
                            collection,
                            changes: requested_changes,
                        },
                        State::Rollback {
                            changes: rollback_changes,
                        },
                    ) => {
                        if account_id != rollback_changes.account_id
                            || collection != rollback_changes.collection
                        {
                            error!(
                                "Invalid updateStore request: {}/{:?} != {}/{:?}",
                                rollback_changes.account_id,
                                rollback_changes.collection,
                                account_id,
                                collection
                            );
                            break;
                        }

                        if let Some(response) = core
                            .handle_rollback_changes(&mut state, requested_changes)
                            .await
                        {
                            response
                        } else {
                            break;
                        }
                    }

                    (_, State::Rollback { .. }) => {
                        // Resume rollback process when a new leader is elected.
                        if let Some(response) =
                            core.handle_rollback_changes(&mut state, vec![]).await
                        {
                            response
                        } else {
                            break;
                        }
                    }
                };

                event
                    .response_tx
                    .send(response)
                    .unwrap_or_else(|_| error!("Oneshot response channel closed."));
            }

            debug!("[{}] Raft follower process ended.", local_name);
        });
        tx
    }

    pub async fn handle_become_follower(
        &mut self,
        peer_id: PeerId,
        response_tx: oneshot::Sender<rpc::Response>,
        term: TermId,
        last_log: RaftId,
    ) {
        if self.is_known_peer(peer_id) {
            if self.term < term {
                self.term = term;
            }

            if self.term == term && self.log_is_behind_or_eq(last_log.term, last_log.index) {
                self.follow_leader(peer_id)
                    .send(Event {
                        response_tx,
                        request: AppendEntriesRequest::Match { last_log },
                    })
                    .await
                    .unwrap_or_else(|err| error!("Failed to send event: {}", err));
            } else {
                response_tx
                    .send(Response::StepDown { term: self.term })
                    .unwrap_or_else(|_| error!("Oneshot response channel closed."));
            }
        } else {
            response_tx
                .send(rpc::Response::UnregisteredPeer)
                .unwrap_or_else(|_| error!("Oneshot response channel closed."));
        }
    }

    pub async fn handle_append_entries(
        &mut self,
        peer_id: PeerId,
        response_tx: oneshot::Sender<rpc::Response>,
        term: TermId,
        request: AppendEntriesRequest,
    ) {
        if term > self.term {
            self.term = term;
        }

        match self.is_following_peer(peer_id) {
            Some(tx) => {
                tx.send(Event {
                    response_tx,
                    request,
                })
                .await
                .unwrap_or_else(|err| error!("Failed to send event: {}", err));
            }
            _ => response_tx
                .send(rpc::Response::StepDown { term: self.term })
                .unwrap_or_else(|_| error!("Oneshot response channel closed.")),
        }
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    async fn handle_update_log(
        &self,
        commit_id: RaftId,
        entries: Vec<RawEntry>,
        pending_entries: &mut Vec<RawEntry>,
    ) -> Response {
        debug_assert!(!entries.is_empty());

        let store = self.store.clone();
        match self
            .spawn_worker(move || {
                let mut update_collections = HashMap::new();
                for raw_entry in &entries {
                    let mut entry = Entry::deserialize(&raw_entry.data).ok_or_else(|| {
                        StoreError::InternalError(format!(
                            "Failed to deserialize entry: {:?}",
                            raw_entry
                        ))
                    })?;

                    while let Some((account_id, collections)) = entry.next_account() {
                        for collection in collections {
                            if let hash_map::Entry::Vacant(e) =
                                update_collections.entry((account_id, collection))
                            {
                                e.insert(UpdateCollection {
                                    account_id,
                                    collection,
                                    from_change_id: if let Some(last_change_id) =
                                        store.get_last_change_id(account_id, collection)?
                                    {
                                        if raw_entry.id.index <= last_change_id {
                                            continue;
                                        } else {
                                            Some(last_change_id)
                                        }
                                    } else {
                                        None
                                    },
                                });
                            }
                        }
                    }
                }

                Ok((update_collections, entries))
            })
            .await
        {
            Ok((collections, entries)) => {
                if !collections.is_empty() {
                    self.set_up_to_date(false);
                    *pending_entries = entries;
                    Response::AppendEntries(AppendEntriesResponse::Update {
                        collections: collections.into_values().collect(),
                    })
                } else if self.commit_log(entries, commit_id).await {
                    Response::AppendEntries(AppendEntriesResponse::Continue)
                } else {
                    Response::None
                }
            }
            Err(err) => {
                debug!("Worker failed: {:?}", err);
                Response::None
            }
        }
    }

    async fn handle_update_store(
        &self,
        pending_entries: &mut Vec<RawEntry>,
        commit_id: RaftId,
        account_id: AccountId,
        collection: Collection,
        changes: Vec<Change>,
    ) -> Response {
        //println!("{:#?}", changes);

        let store = self.store.clone();
        match self
            .spawn_worker(move || store.process_changes(account_id, collection, changes))
            .await
        {
            Ok(do_commit) => {
                if do_commit
                    && !self
                        .commit_log(std::mem::take(pending_entries), commit_id)
                        .await
                {
                    Response::None
                } else {
                    Response::AppendEntries(AppendEntriesResponse::Continue)
                }
            }
            Err(err) => {
                debug!("Failed to update store: {:?}", err);
                Response::None
            }
        }
    }

    async fn commit_log(&self, entries: Vec<RawEntry>, commit_id: RaftId) -> bool {
        if !entries.is_empty() {
            let store = self.store.clone();
            let last_log = entries.last().map(|e| e.id);

            match self
                .spawn_worker(move || store.insert_raft_entries(entries))
                .await
            {
                Ok(_) => {
                    // If this peer matches the leader's commit index,
                    // read-only requests can be accepted on this node.
                    if let Some(last_log) = last_log {
                        self.update_raft_index(last_log.index);
                        self.store_changed(last_log).await;

                        if commit_id == last_log {
                            self.set_up_to_date(true);
                        }
                    }
                }
                Err(err) => {
                    error!("Failed to commit pending changes: {:?}", err);
                    return false;
                }
            }
        }

        true
    }

    async fn handle_match_log(&self, last_log: RaftId) -> Option<Response>
    where
        T: for<'x> Store<'x> + 'static,
    {
        Response::AppendEntries(AppendEntriesResponse::Match {
            last_log: match self.get_prev_raft_id(last_log).await {
                Ok(Some(matched)) => {
                    self.set_up_to_date(matched == last_log);
                    matched
                }
                Ok(None) => {
                    if last_log.is_none() {
                        self.set_up_to_date(true);
                    }
                    RaftId::none()
                }
                Err(err) => {
                    debug!("Failed to get prev raft id: {:?}", err);
                    return None;
                }
            },
        })
        .into()
    }

    async fn handle_synchronize_log(&self, match_terms: Vec<RaftId>) -> Option<Response> {
        if match_terms.is_empty() {
            error!("Log sync failed: match terms list is empty.");
            return None;
        }

        let local_match_terms = match self.get_raft_match_terms().await {
            Ok(local_match_terms) => {
                debug_assert!(!local_match_terms.is_empty());
                local_match_terms
            }
            Err(err) => {
                error!("Error getting raft match list: {:?}", err);
                return None;
            }
        };
        //println!("Match terms: {:?}\n{:?}", match_terms, local_match_terms);

        let mut matched_id = RaftId::none();
        for (local_id, remote_id) in local_match_terms.into_iter().zip(match_terms) {
            if local_id == remote_id {
                matched_id = local_id;
            } else {
                break;
            }
        }

        //debug!("[{}] Found matching terms at {:?}.", local_name, matched_id,);

        Response::AppendEntries(AppendEntriesResponse::Synchronize {
            match_indexes: if !matched_id.is_none() {
                match self.get_raft_match_indexes(matched_id.index).await {
                    Ok((_, match_indexes)) => {
                        if !match_indexes.is_empty() {
                            let mut bytes = Vec::with_capacity(match_indexes.serialized_size());
                            if let Err(err) = match_indexes.serialize_into(&mut bytes) {
                                error!("Failed to serialize match indexes: {}", err);
                                return None;
                            }
                            bytes
                        } else {
                            debug_assert!(false);
                            debug!("No match indexes found for match indexes {:?}", matched_id);
                            return None;
                        }
                    }
                    Err(err) => {
                        error!("Error getting raft match indexes: {:?}", err);
                        return None;
                    }
                }
            } else {
                vec![]
            },
        })
        .into()
    }

    async fn handle_merge_log(&self, state: &mut State, matched_log: RaftId) -> Option<Response> {
        if let Err(err) = self.prepare_rollback_changes(matched_log).await {
            error!("Failed to prepare rollback changes: {:?}", err);
            return None;
        }

        *state = State::Rollback {
            changes: match self.next_rollback_change().await {
                Ok(Some(rollback_change)) => rollback_change,
                Ok(None) => {
                    error!("Failed to prepare rollback changes: No changes found.");
                    return None;
                }
                Err(err) => {
                    error!("Failed to obtain rollback changes: {:?}", err);
                    return None;
                }
            },
        };

        self.handle_rollback_changes(state, vec![]).await
    }

    async fn handle_rollback_changes(
        &self,
        state: &mut State,
        mut requested_changes: Vec<Change>,
    ) -> Option<Response> {
        loop {
            let rollback_changes = if let State::Rollback { changes } = state {
                changes
            } else {
                unreachable!();
            };

            // Thread collection does not contain any actual records,
            // it exists solely for change tracking.
            if let Collection::Thread = rollback_changes.collection {
                println!("Skipping thread changes...");
                rollback_changes.inserts.clear();
                rollback_changes.updates.clear();
                rollback_changes.deletes.clear();
            }

            if !rollback_changes.inserts.is_empty() {
                println!(
                    "Deleting from collection {:?} items {:?}",
                    rollback_changes.collection, rollback_changes.inserts
                );
                let mut batch = WriteBatch::new(rollback_changes.account_id);
                for delete_id in &rollback_changes.inserts {
                    batch.delete_document(rollback_changes.collection, delete_id);
                }
                let store = self.store.clone();
                if let Err(err) = self.spawn_worker(move || store.write(batch)).await {
                    error!("Failed to delete documents: {:?}", err);
                    return None;
                }
                rollback_changes.inserts.clear();
            }

            let account_id = rollback_changes.account_id;
            let collection = rollback_changes.collection;

            if !requested_changes.is_empty() {
                let store = self.store.clone();
                match self
                    .spawn_worker(move || {
                        store.process_changes(account_id, collection, requested_changes)
                    })
                    .await
                {
                    Ok(do_commit) => {
                        if do_commit {
                            rollback_changes.updates.clear();
                            rollback_changes.deletes.clear();
                        } else {
                            return Response::AppendEntries(AppendEntriesResponse::Continue).into();
                        }
                    }
                    Err(err) => {
                        debug!("Failed to update store: {:?}", err);
                        return None;
                    }
                }
                requested_changes = vec![];
            }

            if !rollback_changes.deletes.is_empty() || !rollback_changes.updates.is_empty() {
                return Response::AppendEntries(AppendEntriesResponse::Restore {
                    account_id,
                    collection,
                    changes: match rollback_changes.serialize_rollback() {
                        Some(changes) => changes,
                        None => {
                            error!("Failed to serialize bitmap.");
                            return None;
                        }
                    },
                })
                .into();
            } else {
                if let Err(err) = self
                    .remove_rollback_change(
                        rollback_changes.account_id,
                        rollback_changes.collection,
                    )
                    .await
                {
                    error!("Failed to remove rollback change key: {:?}", err);
                    return None;
                }

                match self.next_rollback_change().await {
                    Ok(Some(changes)) => {
                        *state = State::Rollback { changes };
                        continue;
                    }
                    Ok(None) => {
                        *state = State::default();
                        return Response::AppendEntries(AppendEntriesResponse::Match {
                            last_log: match self.get_last_log().await {
                                Ok(Some(last_log)) => last_log,
                                Ok(None) => {
                                    error!("Unexpected error: Last log not found.");
                                    return None;
                                }
                                Err(err) => {
                                    debug!("Failed to get prev raft id: {:?}", err);
                                    return None;
                                }
                            },
                        })
                        .into();
                    }
                    Err(err) => {
                        error!("Failed to obtain rollback changes: {:?}", err);
                        return None;
                    }
                }
            }
        }
    }
}

pub trait JMAPStoreRaftChanges {
    fn process_changes(
        &self,
        account_id: AccountId,
        collection: Collection,
        changes: Vec<Change>,
    ) -> store::Result<bool>;
}

impl<T> JMAPStoreRaftChanges for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn process_changes(
        &self,
        account_id: AccountId,
        collection: Collection,
        changes: Vec<Change>,
    ) -> store::Result<bool> {
        let mut do_commit = false;
        let mut document_batch = WriteBatch::new(account_id);
        let mut log_batch = Vec::with_capacity(changes.len());

        debug!(
            "Inserting {} changes in {}/{:?}...",
            changes.len(),
            account_id,
            collection
        );

        let mut has_pending_deletions = false;

        for change in changes {
            if has_pending_deletions && !matches!(change, Change::Delete { .. }) {
                // Deletions are done first to avoid reused ID collisions.
                self.write(document_batch)?;
                document_batch = WriteBatch::new(account_id);
                has_pending_deletions = false;
            }

            match change {
                Change::InsertMail {
                    jmap_id,
                    keywords,
                    mailboxes,
                    received_at,
                    body,
                } => {
                    self.raft_update_mail(
                        &mut document_batch,
                        account_id,
                        jmap_id,
                        mailboxes,
                        keywords,
                        Some((
                            lz4_flex::decompress_size_prepended(&body).map_err(|err| {
                                StoreError::InternalError(format!(
                                    "Failed to decompress raft update: {}",
                                    err
                                ))
                            })?,
                            received_at,
                        )),
                    )?;
                }
                Change::UpdateMail {
                    jmap_id,
                    keywords,
                    mailboxes,
                } => {
                    self.raft_update_mail(
                        &mut document_batch,
                        account_id,
                        jmap_id,
                        mailboxes,
                        keywords,
                        None,
                    )?;
                }
                Change::UpdateMailbox { jmap_id, mailbox } => {
                    self.raft_update_mailbox(&mut document_batch, account_id, jmap_id, mailbox)?;
                }
                Change::InsertChange { change_id, entry } => {
                    log_batch.push(WriteOperation::set(
                        ColumnFamily::Logs,
                        LogKey::serialize_change(account_id, collection, change_id),
                        entry,
                    ));
                }
                Change::Delete { document_id } => {
                    has_pending_deletions = true;
                    document_batch.delete_document(collection, document_id)
                }
                Change::Commit => {
                    do_commit = true;
                }
            }
        }
        if !document_batch.is_empty() {
            self.write(document_batch)?;
        }
        if !log_batch.is_empty() {
            self.db.write(log_batch)?;
        }

        Ok(do_commit)
    }
}
