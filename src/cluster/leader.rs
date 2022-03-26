use std::collections::HashSet;
use std::task::Poll;

use futures::poll;

use jmap_mail::mailbox::{JMAPMailboxProperties, Mailbox};
use jmap_mail::query::MailboxId;
use jmap_mail::{MessageField, MessageOutline, MESSAGE_DATA, MESSAGE_RAW};

use store::changes::ChangeId;
use store::leb128::Leb128;
use store::raft::{LogIndex, MergedChanges, RaftId, TermId};
use store::roaring::{RoaringBitmap, RoaringTreemap};
use store::serialize::{LogKey, StoreDeserialize};
use store::tracing::{debug, error};
use store::JMAPIdPrefix;
use store::{lz4_flex, AccountId, Collection, ColumnFamily, DocumentId, JMAPId, Store, StoreError};
use tokio::sync::{mpsc, oneshot, watch};

use crate::cluster::log::{AppendEntriesRequest, AppendEntriesResponse};
use crate::JMAPServer;

use super::log::{Change, UpdateCollection};
use super::Peer;
use super::{
    rpc::{self, Request, Response, RpcEvent},
    Cluster,
};

const BATCH_MAX_ENTRIES: usize = 10; //TODO configure
const BATCH_MAX_SIZE: usize = 10 * 1024 * 1024;

#[derive(Debug)]
enum State {
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

        let mut state = State::BecomeLeader;
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
                            if matches!(&state, State::Wait) {
                                state = State::AppendEntries;
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

                //println!("Leader: {:?}", state);

                let request = match &mut state {
                    State::BecomeLeader => Request::BecomeFollower { term, last_log },
                    State::Synchronize => Request::AppendEntries {
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
                    State::Merge { matched_log } => Request::AppendEntries {
                        term,
                        request: AppendEntriesRequest::Merge {
                            matched_log: *matched_log,
                        },
                    },
                    State::PushChanges {
                        changes,
                        collections,
                    } => {
                        match core
                            .prepare_changes(term, changes, !collections.is_empty())
                            .await
                        {
                            Ok(request) => request,
                            Err(err) => {
                                error!("Failed to prepare changes: {:?}", err);
                                continue;
                            }
                        }
                    }
                    State::Wait => {
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
                        state = State::AppendEntries;
                        continue;
                    }
                    State::AppendEntries => {
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
                                    state = State::Wait;
                                    continue;
                                }
                            }
                            Err(err) => {
                                error!("Error getting raft entries: {:?}", err);
                                state = State::Wait;
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
                            state = State::BecomeLeader;
                            continue;
                        }
                        Response::UnregisteredPeer => {
                            println!("Peer does not know us, retrying");
                            state = State::BecomeLeader;
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

                                state = State::AppendEntries;
                            } else {
                                state = State::Synchronize;
                            }
                        } else {
                            last_committed_id = last_log;
                            last_sent_id = last_log;
                            state = State::AppendEntries;
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

                            //print!("Received match indexes: {:?}", matched_indexes);

                            match core
                                .get_raft_match_indexes(matched_indexes.min().unwrap())
                                .await
                            {
                                Ok((match_term, mut local_match_indexes)) => {
                                    if local_match_indexes.is_empty() {
                                        error!(
                                            "Log sync failed: Could not find a raft index match."
                                        );
                                        state = State::BecomeLeader;
                                        continue;
                                    }
                                    //print!(" & Local {:?}", local_match_indexes);

                                    local_match_indexes &= matched_indexes;

                                    //println!(" = {:?}", local_match_indexes);

                                    if local_match_indexes.is_empty() {
                                        error!("Log sync failed: Invalid intersection result.");
                                        state = State::BecomeLeader;
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
                        state = State::Merge { matched_log };
                    }

                    AppendEntriesResponse::Update { collections } => {
                        state = core.get_next_changes(collections).await;
                    }
                    AppendEntriesResponse::Continue => {
                        let do_commit = match &mut state {
                            State::PushChanges {
                                changes,
                                collections,
                            } if changes.is_empty() => {
                                if collections.is_empty() {
                                    state = State::AppendEntries;
                                    true
                                } else {
                                    state =
                                        core.get_next_changes(std::mem::take(collections)).await;
                                    false
                                }
                            }
                            State::AppendEntries => true,
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

                        // Restore deletions.
                        if !changes.deletes.is_empty() {
                            changes.inserts = changes.deletes;
                            changes.deletes = RoaringBitmap::new();
                        }

                        state = State::PushChanges {
                            collections: vec![],
                            changes,
                        };
                    }
                }
            }
        });
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    async fn get_next_changes(&self, mut collections: Vec<UpdateCollection>) -> State {
        loop {
            let collection = if let Some(collection) = collections.pop() {
                collection
            } else {
                return State::AppendEntries;
            };

            let store = self.store.clone();
            match self
                .spawn_worker(move || {
                    store.merge_changes(
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
                        return State::PushChanges {
                            collections,
                            changes,
                        };
                    }
                }
                Err(err) => {
                    error!("Error getting raft changes: {:?}", err);
                    return State::Synchronize;
                }
            }
        }
    }

    async fn prepare_changes(
        &self,
        term: TermId,
        changes: &mut MergedChanges,
        has_more_changes: bool,
    ) -> store::Result<Request> {
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
                self.fetch_item(changes.account_id, changes.collection, document_id, true)
                    .await?
            } else if let Some(document_id) = changes.updates.min() {
                changes.updates.remove(document_id);
                self.fetch_item(changes.account_id, changes.collection, document_id, false)
                    .await?
            } else if let Some(change_id) = changes.changes.min() {
                changes.changes.remove(change_id);
                self.fetch_raw_change(changes.account_id, changes.collection, change_id)
                    .await?
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

    async fn fetch_item(
        &self,
        account_id: AccountId,
        collection: Collection,
        document_id: DocumentId,
        is_insert: bool,
    ) -> store::Result<Option<(Change, usize)>> {
        match collection {
            Collection::Mail => self.fetch_email(account_id, document_id, is_insert).await,
            Collection::Mailbox => self.fetch_mailbox(account_id, document_id).await,
            _ => Err(StoreError::InternalError(
                "Unsupported collection for changes".into(),
            )),
        }
    }

    async fn fetch_email(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
        is_insert: bool,
    ) -> store::Result<Option<(Change, usize)>> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            let mut item_size = std::mem::size_of::<Change>();

            let mailboxes = if let Some(mailboxes) = store.get_document_tags(
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
            let keywords = if let Some(keywords) = store.get_document_tags(
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

            let jmap_id = if let Some(thread_id) = store.get_document_tag_id(
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
                    store.get_blob(account_id, Collection::Mail, document_id, MESSAGE_RAW)?,
                    store.get_blob(account_id, Collection::Mail, document_id, MESSAGE_DATA)?,
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

    async fn fetch_mailbox(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<(Change, usize)>> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            Ok(store
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

    async fn fetch_raw_change(
        &self,
        account_id: AccountId,
        collection: Collection,
        change_id: ChangeId,
    ) -> store::Result<Option<(Change, usize)>> {
        let store = self.store.clone();
        self.spawn_worker(move || {
            Ok(store
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
