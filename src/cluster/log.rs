use std::collections::{hash_map, HashMap, HashSet};
use std::task::Poll;

use actix_web::web;
use futures::poll;
use jmap_mail::import::JMAPMailImport;
use jmap_mail::mailbox::{JMAPMailMailbox, JMAPMailboxProperties, Mailbox};
use jmap_mail::query::MailboxId;
use jmap_mail::{MessageField, MessageOutline, MESSAGE_DATA, MESSAGE_RAW};
use store::batch::WriteBatch;
use store::changes::ChangeId;
use store::leb128::Leb128;
use store::raft::{Entry, LogIndex, MergedChanges, RaftId, RawEntry, TermId};
use store::roaring::{RoaringBitmap, RoaringTreemap};
use store::serialize::{LogKey, StoreDeserialize};
use store::sha2::digest::typenum::Le;
use store::tracing::{debug, error};
use store::{
    lz4_flex, AccountId, Collection, ColumnFamily, DocumentId, JMAPId, Store, StoreError, Tag,
};
use store::{JMAPIdPrefix, WriteOperation};
use tokio::sync::{mpsc, oneshot, watch};

use crate::JMAPServer;

use super::rpc::UpdateCollection;
use super::{
    rpc::{self, Request, Response, RpcEvent},
    Cluster,
};
use super::{Peer, PeerId, IPC_CHANNEL_BUFFER};

const BATCH_MAX_ENTRIES: usize = 10;
const BATCH_MAX_SIZE: usize = 10 * 1024 * 1024;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Change {
    InsertMail {
        jmap_id: JMAPId,
        keywords: HashSet<Tag>,
        mailboxes: HashSet<Tag>,
        received_at: i64,
        body: Vec<u8>,
    },
    UpdateMail {
        jmap_id: JMAPId,
        keywords: HashSet<Tag>,
        mailboxes: HashSet<Tag>,
    },
    UpdateMailbox {
        jmap_id: JMAPId,
        mailbox: Mailbox,
    },
    InsertChange {
        change_id: ChangeId,
        entry: Vec<u8>,
    },
    Delete {
        document_id: DocumentId,
    },
    Commit,
}

#[derive(Debug)]
enum LeaderState {
    BecomeLeader,
    Synchronize,
    Merge {
        matched_log: RaftId,
    },
    AppendEntries,
    PushChanges {
        collections: Vec<UpdateCollection>,
        changes: MergedChanges,
    },
    Wait,
}

#[derive(Debug)]
enum FollowerState {
    Append {
        commit_id: RaftId,
        pending_entries: Vec<RawEntry>,
    },
    Rollback {
        changes: MergedChanges,
    },
}

impl Default for FollowerState {
    fn default() -> Self {
        FollowerState::Append {
            commit_id: RaftId::none(),
            pending_entries: Vec::new(),
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum AppendEntriesRequest {
    Match {
        last_log: RaftId,
    },
    Synchronize {
        last_log: RaftId,
        match_terms: Vec<RaftId>,
    },
    Merge {
        matched_log: RaftId,
    },
    UpdateLog {
        last_log: RaftId,
        entries: Vec<store::raft::RawEntry>,
    },
    UpdateStore {
        account_id: AccountId,
        collection: Collection,
        changes: Vec<Change>,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum AppendEntriesResponse {
    Match {
        last_log: RaftId,
    },
    Synchronize {
        match_indexes: Vec<u8>,
    },
    Update {
        collections: Vec<UpdateCollection>,
    },
    Restore {
        account_id: AccountId,
        collection: Collection,
        changes: Vec<u8>,
    },
    Continue,
}

pub struct Event {
    pub response_tx: oneshot::Sender<rpc::Response>,
    pub request: AppendEntriesRequest,
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn spawn_raft_leader(&self, peer: &Peer, mut log_index_rx: watch::Receiver<LogIndex>) {
        let peer_tx = peer.tx.clone();
        let mut online_rx = peer.online_rx.clone();
        let peer_name = peer.to_string();
        let peer_id = peer.peer_id;
        let local_name = self.addr.to_string();

        let term = self.term;
        let mut last_log = self.last_log;
        let main_tx = self.tx.clone();
        let core = self.core.clone();

        let mut state = LeaderState::BecomeLeader;
        let mut last_committed_id = RaftId::none();
        let mut last_sent_id = RaftId::none();

        tokio::spawn(async move {
            debug!(
                "[{}] Starting raft leader process for peer {}.",
                local_name, peer_name
            );

            'main: loop {
                // Poll the receiver to make sure this node is still the leader.
                match poll!(Box::pin(log_index_rx.changed())) {
                    Poll::Ready(result) => match result {
                        Ok(_) => {
                            last_log.index = *log_index_rx.borrow();
                            last_log.term = term;
                            if matches!(&state, LeaderState::Wait) {
                                state = LeaderState::AppendEntries;
                            }
                        }
                        Err(_) => {
                            debug!(
                                "[{}] Raft leader process for {} exiting.",
                                local_name, peer_name
                            );
                            break;
                        }
                    },
                    Poll::Pending => (),
                }

                let request = match &mut state {
                    LeaderState::BecomeLeader => Request::BecomeFollower { term, last_log },
                    LeaderState::Synchronize => Request::AppendEntries {
                        term,
                        request: AppendEntriesRequest::Synchronize {
                            last_log,
                            match_terms: {
                                match core.get_raft_match_terms().await {
                                    Ok(match_terms) => {
                                        debug_assert!(!match_terms.is_empty());
                                        match_terms
                                    }
                                    Err(err) => {
                                        error!("Error getting raft match list: {:?}", err);
                                        break;
                                    }
                                }
                            },
                        },
                    },
                    LeaderState::Merge { matched_log } => Request::AppendEntries {
                        term,
                        request: AppendEntriesRequest::Merge {
                            matched_log: *matched_log,
                        },
                    },
                    LeaderState::PushChanges {
                        changes,
                        collections,
                    } => {
                        match prepare_changes(&core, term, changes, !collections.is_empty()).await {
                            Ok(request) => request,
                            Err(err) => {
                                error!("Failed to prepare changes: {:?}", err);
                                continue;
                            }
                        }
                    }
                    LeaderState::Wait => {
                        // Wait for the next change
                        if log_index_rx.changed().await.is_ok() {
                            last_log.index = *log_index_rx.borrow();
                            last_log.term = term;
                            debug!("[{}] Received new log index: {:?}", local_name, last_log);
                        } else {
                            debug!(
                                "[{}] Raft leader process for {} exiting.",
                                local_name, peer_name
                            );
                            break;
                        }
                        state = LeaderState::AppendEntries;
                        continue;
                    }
                    LeaderState::AppendEntries => {
                        let _core = core.clone();
                        match core
                            .spawn_worker(move || {
                                _core
                                    .store
                                    .get_raft_entries(last_committed_id, BATCH_MAX_ENTRIES)
                            })
                            .await
                        {
                            Ok(entries) => {
                                if !entries.is_empty() {
                                    last_sent_id = entries.last().unwrap().id;

                                    if last_sent_id.index > last_log.index {
                                        last_log = last_sent_id;
                                    }

                                    Request::AppendEntries {
                                        term,
                                        request: AppendEntriesRequest::UpdateLog {
                                            last_log,
                                            entries,
                                        },
                                    }
                                } else {
                                    debug!(
                                        "[{}] Peer {} is up to date with {:?}.",
                                        local_name, peer_name, last_committed_id
                                    );
                                    state = LeaderState::Wait;
                                    continue;
                                }
                            }
                            Err(err) => {
                                error!("Error getting raft entries: {:?}", err);
                                state = LeaderState::Wait;
                                continue;
                            }
                        }
                    }
                };

                let response = if let Some(response) = send_request(&peer_tx, request).await {
                    match response {
                        Response::StepDown { term: peer_term } => {
                            if let Err(err) = main_tx
                                .send(super::Event::StepDown { term: peer_term })
                                .await
                            {
                                error!("Error sending step down message: {}", err);
                            }
                            debug!("Peer {} requested to step down.", peer_name);
                            break;
                        }
                        Response::None => {
                            // Wait until the peer is back online
                            'online: loop {
                                tokio::select! {
                                    changed = log_index_rx.changed() => {
                                        match changed {
                                            Ok(()) => {
                                                last_log.index = *log_index_rx.borrow();
                                                last_log.term = term;

                                                debug!(
                                                    "[{}] Received new log index while waiting: {:?}",
                                                    local_name, last_log
                                                );
                                            }
                                            Err(_) => {
                                                debug!(
                                                    "[{}] Raft leader process for {} exiting.",
                                                    local_name, peer_name
                                                );
                                                break 'main;
                                            }
                                        }
                                    },
                                    online = online_rx.changed() => {
                                        match online {
                                            Ok(()) => {
                                                if *online_rx.borrow() {
                                                    debug!("Peer {} is back online (rpc).", peer_name);
                                                    break 'online;
                                                } else {
                                                    debug!("Peer {} is still offline (rpc).", peer_name);
                                                    continue 'online;
                                                }
                                            },
                                            Err(_) => {
                                                debug!(
                                                    "[{}] Raft leader process for {} exiting.",
                                                    local_name, peer_name
                                                );
                                                break 'main;
                                            },
                                        }
                                    }
                                };
                            }
                            state = LeaderState::BecomeLeader;
                            continue;
                        }
                        Response::AppendEntries(response) => response,
                        response @ (Response::UpdatePeers { .. }
                        | Response::Vote { .. }
                        | Response::Pong) => {
                            error!(
                                "Unexpected response from peer {}: {:?}",
                                peer_name, response
                            );
                            continue;
                        }
                    }
                } else {
                    debug!(
                        "[{}] Raft leader process for {} exiting (peer_tx channel closed).",
                        local_name, peer_name
                    );
                    break;
                };

                //println!("[{}] {:#?}", peer_name, response);

                match response {
                    AppendEntriesResponse::Match { last_log } => {
                        if !last_log.is_none() {
                            let local_match = match core.get_next_raft_id(last_log).await {
                                Ok(Some(local_match)) => local_match,
                                Ok(None) => {
                                    error!("Log sync failed: local match is null");
                                    break;
                                }
                                Err(err) => {
                                    error!("Error getting next raft id: {:?}", err);
                                    break;
                                }
                            };

                            if local_match == last_log {
                                last_committed_id = last_log;
                                last_sent_id = last_log;

                                main_tx
                                    .send(super::Event::AdvanceCommitIndex {
                                        peer_id,
                                        commit_index: local_match.index,
                                    })
                                    .await
                                    .ok();

                                debug!(
                                    "[{}] Matched index {:?} for peer {}.",
                                    local_name, local_match, peer_name
                                );

                                state = LeaderState::AppendEntries;
                            } else {
                                state = LeaderState::Synchronize;
                            }
                        } else {
                            last_committed_id = last_log;
                            last_sent_id = last_log;
                            state = LeaderState::AppendEntries;
                        }
                    }
                    AppendEntriesResponse::Synchronize { match_indexes } => {
                        let matched_log = if !match_indexes.is_empty() {
                            let matched_indexes =
                                match RoaringTreemap::deserialize_from(&match_indexes[..]) {
                                    Ok(match_terms) => match_terms,
                                    Err(err) => {
                                        error!("Error deserializing match list: {:?}", err);
                                        break;
                                    }
                                };

                            if matched_indexes.is_empty() {
                                error!("Log sync failed: match list is empty");
                                break;
                            }

                            print!("Received match indexes: {:?}", matched_indexes);

                            match core
                                .get_raft_match_indexes(matched_indexes.min().unwrap())
                                .await
                            {
                                Ok((match_term, mut local_match_indexes)) => {
                                    if local_match_indexes.is_empty() {
                                        error!(
                                            "Log sync failed: Could not find a raft index match."
                                        );
                                        state = LeaderState::BecomeLeader;
                                        continue;
                                    }
                                    print!(" & Local {:?}", local_match_indexes);

                                    local_match_indexes &= matched_indexes;

                                    println!(" = {:?}", local_match_indexes);

                                    if local_match_indexes.is_empty() {
                                        error!("Log sync failed: Invalid intersection result.");
                                        state = LeaderState::BecomeLeader;
                                        continue;
                                    }

                                    debug!(
                                        "[{}] Matched indexes {:?} ({}), term {} for peer {}.",
                                        local_name,
                                        local_match_indexes,
                                        local_match_indexes.max().unwrap(),
                                        match_term,
                                        peer_name
                                    );

                                    RaftId::new(match_term, local_match_indexes.max().unwrap())
                                }
                                Err(err) => {
                                    error!("Error getting local match indexes: {:?}", err);
                                    break;
                                }
                            }
                        } else {
                            RaftId::none()
                        };

                        last_committed_id = matched_log;
                        last_sent_id = matched_log;
                        state = LeaderState::Merge { matched_log };
                    }

                    AppendEntriesResponse::Update { collections } => {
                        state = get_next_changes(&core, collections).await;
                    }
                    AppendEntriesResponse::Continue => {
                        let do_commit = match &mut state {
                            LeaderState::PushChanges {
                                changes,
                                collections,
                            } if changes.is_empty() => {
                                if collections.is_empty() {
                                    state = LeaderState::AppendEntries;
                                    true
                                } else {
                                    state =
                                        get_next_changes(&core, std::mem::take(collections)).await;
                                    false
                                }
                            }
                            LeaderState::AppendEntries => true,
                            _ => {
                                debug_assert!(false, "Invalid state: {:?}", state);
                                false
                            }
                        };

                        if do_commit {
                            // Advance commit index
                            last_committed_id = last_sent_id;
                            main_tx
                                .send(super::Event::AdvanceCommitIndex {
                                    peer_id,
                                    commit_index: last_committed_id.index,
                                })
                                .await
                                .ok();
                        }
                    }
                    AppendEntriesResponse::Restore {
                        account_id,
                        collection,
                        changes,
                    } => {
                        let mut changes = if let Some(changes) =
                            MergedChanges::from_rollback_bytes(account_id, collection, &changes)
                        {
                            changes
                        } else {
                            error!("Failed to deserialize rollback bitmaps.");
                            break;
                        };

                        if !changes.deletes.is_empty() {
                            changes.inserts = changes.deletes;
                            changes.deletes = RoaringBitmap::new();
                        }

                        state = LeaderState::PushChanges {
                            collections: vec![],
                            changes,
                        };
                    }
                }
            }
        });
    }

    pub fn spawn_raft_follower(&self) -> mpsc::Sender<Event> {
        let (tx, mut rx) = mpsc::channel::<Event>(IPC_CHANNEL_BUFFER);
        let core = self.core.clone();
        let local_name = self.addr.to_string();

        debug!("[{}] Starting raft follower process.", local_name);

        tokio::spawn(async move {
            let mut state = match core.next_rollback_change().await {
                Ok(Some(changes)) => FollowerState::Rollback { changes },
                Ok(None) => FollowerState::default(),
                Err(err) => {
                    error!("Failed to obtain rollback changes: {:?}", err);
                    return;
                }
            };

            while let Some(event) = rx.recv().await {
                let response = match (event.request, &mut state) {
                    (
                        AppendEntriesRequest::Match { last_log },
                        FollowerState::Append { commit_id, .. },
                    ) => {
                        *commit_id = last_log;
                        if let Some(response) = handle_match_log(&core, last_log).await {
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
                        FollowerState::Append { commit_id, .. },
                    ) => {
                        *commit_id = last_log;
                        if let Some(response) = handle_synchronize_log(&core, match_terms).await {
                            response
                        } else {
                            break;
                        }
                    }

                    (AppendEntriesRequest::Merge { matched_log }, FollowerState::Append { .. }) => {
                        if let Some(response) =
                            handle_merge_log(&core, &mut state, matched_log).await
                        {
                            response
                        } else {
                            break;
                        }
                    }
                    (
                        AppendEntriesRequest::UpdateLog { last_log, entries },
                        FollowerState::Append {
                            commit_id,
                            pending_entries,
                        },
                    ) => {
                        *commit_id = last_log;
                        handle_update_log(&core, last_log, entries, pending_entries).await
                    }

                    (
                        AppendEntriesRequest::UpdateStore {
                            account_id,
                            collection,
                            changes,
                        },
                        FollowerState::Append {
                            commit_id,
                            pending_entries,
                        },
                    ) => {
                        handle_update_store(
                            &core,
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
                        FollowerState::Rollback {
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

                        if let Some(response) =
                            handle_rollback_changes(&core, &mut state, requested_changes).await
                        {
                            response
                        } else {
                            break;
                        }
                    }

                    (_, FollowerState::Rollback { .. }) => {
                        if let Some(response) =
                            handle_rollback_changes(&core, &mut state, vec![]).await
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

async fn handle_update_log<T>(
    core_: &web::Data<JMAPServer<T>>,
    commit_id: RaftId,
    entries: Vec<RawEntry>,
    pending_entries: &mut Vec<RawEntry>,
) -> Response
where
    T: for<'x> Store<'x> + 'static,
{
    debug_assert!(!entries.is_empty());

    let core = core_.clone();
    match core_
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
                                    core.store.get_last_change_id(account_id, collection)?
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
                core_.set_up_to_date(false);
                *pending_entries = entries;
                Response::AppendEntries(AppendEntriesResponse::Update {
                    collections: collections.into_values().collect(),
                })
            } else if commit_log(core_, entries, commit_id).await {
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

async fn handle_update_store<T>(
    core_: &web::Data<JMAPServer<T>>,
    pending_entries: &mut Vec<RawEntry>,
    commit_id: RaftId,
    account_id: AccountId,
    collection: Collection,
    changes: Vec<Change>,
) -> Response
where
    T: for<'x> Store<'x> + 'static,
{
    //println!("{:#?}", changes);

    let core = core_.clone();
    match core_
        .spawn_worker(move || process_changes(core, account_id, collection, changes))
        .await
    {
        Ok(do_commit) => {
            if do_commit && !commit_log(core_, std::mem::take(pending_entries), commit_id).await {
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

fn process_changes<T>(
    core: web::Data<JMAPServer<T>>,
    account_id: AccountId,
    collection: Collection,
    changes: Vec<Change>,
) -> store::Result<bool>
where
    T: for<'x> Store<'x> + 'static,
{
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
            core.store.write(document_batch)?;
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
                core.store.raft_update_mail(
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
                core.store.raft_update_mail(
                    &mut document_batch,
                    account_id,
                    jmap_id,
                    mailboxes,
                    keywords,
                    None,
                )?;
            }
            Change::UpdateMailbox { jmap_id, mailbox } => {
                core.store.raft_update_mailbox(
                    &mut document_batch,
                    account_id,
                    jmap_id,
                    mailbox,
                )?;
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
        core.store.write(document_batch)?;
    }
    if !log_batch.is_empty() {
        core.store.db.write(log_batch)?;
    }

    Ok(do_commit)
}

async fn commit_log<T>(
    core_: &web::Data<JMAPServer<T>>,
    entries: Vec<RawEntry>,
    commit_id: RaftId,
) -> bool
where
    T: for<'x> Store<'x> + 'static,
{
    if !entries.is_empty() {
        let core = core_.clone();
        let last_log = entries.last().map(|e| e.id);

        match core_
            .spawn_worker(move || core.store.insert_raft_entries(entries))
            .await
        {
            Ok(_) => {
                // If this node matches the leader's commit index,
                // read-only requests can be accepted on this node.
                if let Some(last_log) = last_log {
                    core_.update_raft_index(last_log.index);
                    core_.store_changed(last_log).await;

                    if commit_id == last_log {
                        core_.set_up_to_date(true);
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

async fn handle_match_log<T>(core: &web::Data<JMAPServer<T>>, last_log: RaftId) -> Option<Response>
where
    T: for<'x> Store<'x> + 'static,
{
    Response::AppendEntries(AppendEntriesResponse::Match {
        last_log: match core.get_prev_raft_id(last_log).await {
            Ok(Some(matched)) => {
                core.set_up_to_date(matched == last_log);
                matched
            }
            Ok(None) => {
                if last_log.is_none() {
                    core.set_up_to_date(true);
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

async fn handle_synchronize_log<T>(
    core: &web::Data<JMAPServer<T>>,
    match_terms: Vec<RaftId>,
) -> Option<Response>
where
    T: for<'x> Store<'x> + 'static,
{
    if match_terms.is_empty() {
        error!("Log sync failed: match terms list is empty.");
        return None;
    }

    let local_match_terms = match core.get_raft_match_terms().await {
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
            match core.get_raft_match_indexes(matched_id.index).await {
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

async fn handle_merge_log<T>(
    core: &web::Data<JMAPServer<T>>,
    state: &mut FollowerState,
    matched_log: RaftId,
) -> Option<Response>
where
    T: for<'x> Store<'x> + 'static,
{
    if let Err(err) = core.prepare_rollback_changes(matched_log.index).await {
        error!("Failed to obtain rollback changes: {:?}", err);
        return None;
    }

    *state = FollowerState::Rollback {
        changes: match core.next_rollback_change().await {
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

    handle_rollback_changes(&core, state, vec![]).await
}

async fn handle_rollback_changes<T>(
    core_: &web::Data<JMAPServer<T>>,
    state: &mut FollowerState,
    mut requested_changes: Vec<Change>,
) -> Option<Response>
where
    T: for<'x> Store<'x> + 'static,
{
    loop {
        let rollback_changes = if let FollowerState::Rollback { changes } = state {
            changes
        } else {
            unreachable!();
        };

        if !rollback_changes.inserts.is_empty() {
            let mut batch = WriteBatch::new(rollback_changes.account_id);
            for delete_id in &rollback_changes.inserts {
                batch.delete_document(rollback_changes.collection, delete_id);
            }
            let core = core_.clone();
            if let Err(err) = core_.spawn_worker(move || core.store.write(batch)).await {
                error!("Failed to delete documents: {:?}", err);
                return None;
            }
            rollback_changes.inserts.clear();
        }

        let account_id = rollback_changes.account_id;
        let collection = rollback_changes.collection;

        if !requested_changes.is_empty() {
            let core = core_.clone();
            match core_
                .spawn_worker(move || {
                    process_changes(core, account_id, collection, requested_changes)
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
            if let Err(err) = core_
                .remove_rollback_change(rollback_changes.account_id, rollback_changes.collection)
                .await
            {
                error!("Failed to remove rollback change key: {:?}", err);
                return None;
            }

            match core_.next_rollback_change().await {
                Ok(Some(changes)) => {
                    *state = FollowerState::Rollback { changes };
                    continue;
                }
                Ok(None) => {
                    *state = FollowerState::default();
                    return Response::AppendEntries(AppendEntriesResponse::Match {
                        last_log: match core_.get_last_log().await {
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

async fn send_request(peer_tx: &mpsc::Sender<rpc::RpcEvent>, request: Request) -> Option<Response> {
    let (response_tx, rx) = oneshot::channel();
    peer_tx
        .send(RpcEvent::NeedResponse {
            request,
            response_tx,
        })
        .await
        .ok()?;
    rx.await.unwrap_or(Response::None).into()
}

async fn get_next_changes<T>(
    core: &web::Data<JMAPServer<T>>,
    mut collections: Vec<UpdateCollection>,
) -> LeaderState
where
    T: for<'x> Store<'x> + 'static,
{
    loop {
        let collection = if let Some(collection) = collections.pop() {
            collection
        } else {
            return LeaderState::AppendEntries;
        };

        let _core = core.clone();
        match core
            .spawn_worker(move || {
                _core.store.merge_changes(
                    collection.account_id,
                    collection.collection,
                    collection.from_change_id,
                    matches!(collection.collection, Collection::Thread),
                )
            })
            .await
        {
            Ok(changes) => {
                if !changes.is_empty() {
                    return LeaderState::PushChanges {
                        collections,
                        changes,
                    };
                }
            }
            Err(err) => {
                error!("Error getting raft changes: {:?}", err);
                return LeaderState::Synchronize;
            }
        }
    }
}

async fn prepare_changes<T>(
    core: &web::Data<JMAPServer<T>>,
    term: TermId,
    changes: &mut MergedChanges,
    has_more_changes: bool,
) -> store::Result<Request>
where
    T: for<'x> Store<'x> + 'static,
{
    let mut batch_size = 0;
    let mut push_changes = Vec::new();

    loop {
        let item = if let Some(document_id) = changes.deletes.min() {
            // Deletions are always sent first, as IDs can be reused.
            changes.deletes.remove(document_id);
            Some((
                Change::Delete { document_id },
                std::mem::size_of::<Change>(),
            ))
        } else if let Some(document_id) = changes.inserts.min() {
            changes.inserts.remove(document_id);
            fetch_item(
                core,
                changes.account_id,
                changes.collection,
                document_id,
                true,
            )
            .await?
        } else if let Some(document_id) = changes.updates.min() {
            changes.updates.remove(document_id);
            fetch_item(
                core,
                changes.account_id,
                changes.collection,
                document_id,
                false,
            )
            .await?
        } else if let Some(change_id) = changes.changes.min() {
            changes.changes.remove(change_id);
            fetch_raw_change(core, changes.account_id, changes.collection, change_id).await?
        } else {
            break;
        };

        if let Some((item, item_size)) = item {
            push_changes.push(item);
            batch_size += item_size;
        } else {
            debug!(
                "Warning: Failed to fetch item in collection {:?}",
                changes.collection,
            );
        }

        if batch_size >= BATCH_MAX_SIZE {
            break;
        }
    }

    if changes.is_empty() && !has_more_changes {
        push_changes.push(Change::Commit);
    }

    Ok(Request::AppendEntries {
        term,
        request: AppendEntriesRequest::UpdateStore {
            account_id: changes.account_id,
            collection: changes.collection,
            changes: push_changes,
        },
    })
}

async fn fetch_item<T>(
    core: &web::Data<JMAPServer<T>>,
    account_id: AccountId,
    collection: Collection,
    document_id: DocumentId,
    is_insert: bool,
) -> store::Result<Option<(Change, usize)>>
where
    T: for<'x> Store<'x> + 'static,
{
    match collection {
        Collection::Mail => fetch_email(core, account_id, document_id, is_insert).await,
        Collection::Mailbox => fetch_mailbox(core, account_id, document_id).await,
        _ => Err(StoreError::InternalError(
            "Unsupported collection for changes".into(),
        )),
    }
}

async fn fetch_email<T>(
    core: &web::Data<JMAPServer<T>>,
    account_id: AccountId,
    document_id: DocumentId,
    is_insert: bool,
) -> store::Result<Option<(Change, usize)>>
where
    T: for<'x> Store<'x> + 'static,
{
    let _core = core.clone();
    core.spawn_worker(move || {
        let mut item_size = std::mem::size_of::<Change>();

        let mailboxes = if let Some(mailboxes) = _core.store.get_document_tags(
            account_id,
            Collection::Mail,
            document_id,
            MessageField::Mailbox.into(),
        )? {
            item_size += mailboxes.items.len() * std::mem::size_of::<MailboxId>();
            mailboxes.items
        } else {
            return Ok(None);
        };
        let keywords = if let Some(keywords) = _core.store.get_document_tags(
            account_id,
            Collection::Mail,
            document_id,
            MessageField::Keyword.into(),
        )? {
            item_size += keywords.items.iter().map(|tag| tag.len()).sum::<usize>();
            keywords.items
        } else {
            HashSet::new()
        };

        let jmap_id = if let Some(thread_id) = _core.store.get_document_tag_id(
            account_id,
            Collection::Mail,
            document_id,
            MessageField::ThreadId.into(),
        )? {
            JMAPId::from_parts(thread_id, document_id)
        } else {
            return Ok(None);
        };

        Ok(if is_insert {
            if let (Some(body), Some(message_data_bytes)) = (
                _core
                    .store
                    .get_blob(account_id, Collection::Mail, document_id, MESSAGE_RAW)?,
                _core
                    .store
                    .get_blob(account_id, Collection::Mail, document_id, MESSAGE_DATA)?,
            ) {
                // Deserialize the message data
                let (message_data_len, read_bytes) =
                    usize::from_leb128_bytes(&message_data_bytes[..])
                        .ok_or(StoreError::DataCorruption)?;
                let message_outline = MessageOutline::deserialize(
                    &message_data_bytes[read_bytes + message_data_len..],
                )
                .ok_or(StoreError::DataCorruption)?;

                // Compress body
                let body = lz4_flex::compress_prepend_size(&body);
                item_size += body.len();
                Some((
                    Change::InsertMail {
                        jmap_id,
                        keywords,
                        mailboxes,
                        body,
                        received_at: message_outline.received_at,
                    },
                    item_size,
                ))
            } else {
                None
            }
        } else {
            Some((
                Change::UpdateMail {
                    jmap_id,
                    keywords,
                    mailboxes,
                },
                item_size,
            ))
        })
    })
    .await
}

async fn fetch_mailbox<T>(
    core: &web::Data<JMAPServer<T>>,
    account_id: AccountId,
    document_id: DocumentId,
) -> store::Result<Option<(Change, usize)>>
where
    T: for<'x> Store<'x> + 'static,
{
    let _core = core.clone();
    core.spawn_worker(move || {
        Ok(_core
            .store
            .get_document_value::<Mailbox>(
                account_id,
                Collection::Mailbox,
                document_id,
                JMAPMailboxProperties::Id.into(),
            )?
            .map(|mailbox| {
                (
                    Change::UpdateMailbox {
                        jmap_id: document_id as JMAPId,
                        mailbox,
                    },
                    std::mem::size_of::<Mailbox>(),
                )
            }))
    })
    .await
}

async fn fetch_raw_change<T>(
    core: &web::Data<JMAPServer<T>>,
    account_id: AccountId,
    collection: Collection,
    change_id: ChangeId,
) -> store::Result<Option<(Change, usize)>>
where
    T: for<'x> Store<'x> + 'static,
{
    let _core = core.clone();
    core.spawn_worker(move || {
        Ok(_core
            .store
            .db
            .get::<Vec<u8>>(
                ColumnFamily::Logs,
                &LogKey::serialize_change(account_id, collection, change_id),
            )?
            .map(|entry| {
                (
                    Change::InsertChange { change_id, entry },
                    std::mem::size_of::<Change>(),
                )
            }))
    })
    .await
}
