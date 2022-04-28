use std::collections::HashSet;
use std::task::Poll;

use futures::poll;

use jmap::jmap_store::orm::{JMAPOrm, PropertySchema};
use jmap_mail::identity::IdentityProperty;
use jmap_mail::mail::{MessageField, MessageOutline};
use jmap_mail::mailbox::MailboxProperty;

use store::leb128::Leb128;
use store::log::{LogIndex, RaftId};
use store::roaring::{RoaringBitmap, RoaringTreemap};
use store::serialize::{StoreDeserialize, StoreSerialize};
use store::tracing::{debug, error};
use store::Collections;
use store::{lz4_flex, AccountId, Collection, DocumentId, Store, StoreError};
use tokio::sync::{mpsc, oneshot, watch};

use crate::cluster::log::{AppendEntriesRequest, AppendEntriesResponse, RaftStore};
use crate::JMAPServer;

use super::log::{DocumentUpdate, MergedChanges, Update};
use super::Peer;
use super::{
    rpc::{self, Request, Response, RpcEvent},
    Cluster,
};

const BATCH_MAX_SIZE: usize = 10 * 1024 * 1024; //TODO configure

#[derive(Debug)]
enum State {
    BecomeLeader,
    Synchronize,
    Merge {
        matched_log: RaftId,
    },
    AppendLogs {
        pending_changes: Vec<(Collections, Vec<AccountId>)>,
    },
    AppendChanges {
        account_id: AccountId,
        collection: Collection,
        changes: MergedChanges,
    },
    Wait,
}

#[derive(Debug, Clone, Copy)]
pub struct Event {
    pub last_log_index: LogIndex,
    pub uncommitted_index: LogIndex,
}

impl Event {
    pub fn new(last_log_index: LogIndex, uncommitted_index: LogIndex) -> Self {
        Self {
            last_log_index,
            uncommitted_index,
        }
    }
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn spawn_raft_leader(
        &self,
        peer: &Peer,
        mut log_index_rx: watch::Receiver<Event>,
        mut init_rx: Option<watch::Receiver<bool>>,
    ) {
        let peer_tx = peer.tx.clone();
        let mut online_rx = peer.online_rx.clone();
        let peer_name = peer.to_string();
        let peer_id = peer.peer_id;
        let local_name = self.addr.to_string();

        let term = self.term;
        let mut last_log = self.last_log;
        let mut uncommitted_index = self.uncommitted_index;

        let main_tx = self.tx.clone();
        let core = self.core.clone();

        tokio::spawn(async move {
            let mut state = State::BecomeLeader;
            let mut follower_last_index = LogIndex::MAX;

            debug!(
                "[{}] Starting raft leader process for peer {}.",
                local_name, peer_name
            );

            'main: loop {
                // Poll the receiver to make sure this node is still the leader.
                match poll!(Box::pin(log_index_rx.changed())) {
                    Poll::Ready(result) => match result {
                        Ok(_) => {
                            let log_index = *log_index_rx.borrow();
                            last_log.index = log_index.last_log_index;
                            last_log.term = term;
                            uncommitted_index = log_index.uncommitted_index;

                            if matches!(&state, State::Wait) {
                                state = State::AppendLogs {
                                    pending_changes: vec![],
                                };
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

                let request = match state {
                    State::BecomeLeader => {
                        state = State::BecomeLeader;
                        Request::BecomeFollower { term, last_log }
                    }
                    State::Synchronize => {
                        state = State::Synchronize;
                        Request::AppendEntries {
                            term,
                            request: AppendEntriesRequest::Synchronize {
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
                        }
                    }
                    State::Merge { matched_log } => {
                        state = State::Merge { matched_log };
                        Request::AppendEntries {
                            term,
                            request: AppendEntriesRequest::Merge { matched_log },
                        }
                    }
                    State::Wait => {
                        // Wait for the next change
                        if log_index_rx.changed().await.is_ok() {
                            let log_index = *log_index_rx.borrow();
                            last_log.index = log_index.last_log_index;
                            last_log.term = term;
                            uncommitted_index = log_index.uncommitted_index;
                            debug!("[{}] Received new log index: {:?}", local_name, log_index);
                        } else {
                            debug!(
                                "[{}] Raft leader process for {} exiting.",
                                local_name, peer_name
                            );
                            break;
                        }
                        state = State::AppendLogs {
                            pending_changes: vec![],
                        };
                        continue;
                    }
                    State::AppendLogs { pending_changes } => {
                        debug_assert!(uncommitted_index != LogIndex::MAX);

                        if !pending_changes.is_empty() || follower_last_index != uncommitted_index {
                            let _core = core.clone();
                            match core
                                .spawn_worker(move || {
                                    _core.store.get_log_entries(
                                        follower_last_index,
                                        uncommitted_index,
                                        pending_changes,
                                        BATCH_MAX_SIZE,
                                    )
                                })
                                .await
                            {
                                Ok((updates, pending_changes, last_index)) => {
                                    follower_last_index = last_index;
                                    state = State::AppendLogs { pending_changes };
                                    Request::AppendEntries {
                                        term,
                                        request: AppendEntriesRequest::Update {
                                            commit_index: last_log.index,
                                            updates,
                                        },
                                    }
                                }
                                Err(err) => {
                                    error!("Error fetching log entries: {:?}", err);
                                    break;
                                }
                            }
                        } else {
                            debug!(
                                "[{}] No more entries left to send to peer {}.",
                                local_name, peer_name
                            );

                            state = State::Wait;
                            Request::AppendEntries {
                                term,
                                request: AppendEntriesRequest::AdvanceCommitIndex {
                                    commit_index: last_log.index,
                                },
                            }
                        }
                    }
                    State::AppendChanges {
                        account_id,
                        collection,
                        mut changes,
                    } => {
                        match core
                            .prepare_changes(account_id, collection, &mut changes)
                            .await
                        {
                            Ok(updates) => {
                                state = State::AppendChanges {
                                    account_id,
                                    collection,
                                    changes,
                                };
                                Request::AppendEntries {
                                    term,
                                    request: AppendEntriesRequest::Update {
                                        commit_index: last_log.index,
                                        updates,
                                    },
                                }
                            }
                            Err(err) => {
                                error!("Failed to prepare changes: {:?}", err);
                                break;
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
                            debug!(
                                "[{}] Peer {} requested this node to step down.",
                                local_name, peer_name
                            );
                            break;
                        }
                        Response::None => {
                            // Wait until the peer is back online
                            debug!(
                                concat!(
                                    "[{}] Could not send message to {}, ",
                                    "waiting until it is confirmed online."
                                ),
                                local_name, peer_name
                            );
                            'online: loop {
                                tokio::select! {
                                    changed = log_index_rx.changed() => {
                                        match changed {
                                            Ok(()) => {
                                                let log_index = *log_index_rx.borrow();
                                                last_log.index = log_index.last_log_index;
                                                last_log.term = term;
                                                uncommitted_index = log_index.uncommitted_index;

                                                debug!(
                                                    "[{}] Received new log index {:?} while waiting for peer {}.",
                                                    local_name, log_index, peer_name
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
                                                    debug!("[{}] Peer {} is back online (rpc).", local_name, peer_name);
                                                    break 'online;
                                                } else {
                                                    debug!("[{}] Peer {} is still offline (rpc).", local_name, peer_name);
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
                            debug!(
                                "[{}] Peer {} does not know this node, retrying...",
                                local_name, peer_name
                            );
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
                    AppendEntriesResponse::Match { match_log } => {
                        if let Some(mut init_rx) = Option::take(&mut init_rx) {
                            debug!(
                                "[{}] Leader process for peer {} waiting for init...",
                                local_name, peer_name
                            );
                            init_rx.changed().await.ok();
                            if !*init_rx.borrow() {
                                error!(
                                    "[{}] Leader failed to init, exiting process for peer {}.",
                                    local_name, peer_name
                                );
                                break;
                            }
                        }

                        follower_last_index = match_log.index;
                        if !match_log.is_none() {
                            let local_match = match core.get_next_raft_id(match_log).await {
                                Ok(Some(local_match)) => local_match,
                                Ok(None) => {
                                    let last_log = core
                                        .get_last_log()
                                        .await
                                        .unwrap_or(None)
                                        .unwrap_or_else(RaftId::none);
                                    error!("Log sync failed: could not match id {:?}, last local log: {:?}.", match_log, last_log);
                                    break;
                                }
                                Err(err) => {
                                    error!("Error getting next raft id: {:?}", err);
                                    break;
                                }
                            };

                            if local_match == match_log {
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

                                state = State::AppendLogs {
                                    pending_changes: vec![],
                                };
                            } else {
                                state = State::Synchronize;
                            }
                        } else {
                            debug!(
                                "[{}] Peer {} requested all log entries to be sent.",
                                local_name, peer_name
                            );

                            state = if uncommitted_index != LogIndex::MAX {
                                State::AppendLogs {
                                    pending_changes: vec![],
                                }
                            } else {
                                State::Wait
                            };
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

                        follower_last_index = matched_log.index;
                        state = State::Merge { matched_log };
                    }
                    AppendEntriesResponse::Continue => (),
                    AppendEntriesResponse::Done { up_to_index } => {
                        // Advance commit index
                        if up_to_index != LogIndex::MAX {
                            main_tx
                                .send(super::Event::AdvanceCommitIndex {
                                    peer_id,
                                    commit_index: up_to_index,
                                })
                                .await
                                .ok();

                            if up_to_index == last_log.index {
                                debug!(
                                    "[{}] Follower {} is up to date with leader's commit index {}.",
                                    local_name, peer_name, last_log.index
                                );
                            } else {
                                debug!(
                                    concat!(
                                        "[{}] Updating follower {} index to {} ",
                                        "and sending remaining entries up to index {}."
                                    ),
                                    local_name, peer_name, up_to_index, last_log.index
                                );
                            }
                        } else {
                            debug!(
                                "[{}] Resuming append logs for peer {}.",
                                local_name, peer_name
                            );
                        }

                        state = if up_to_index != uncommitted_index {
                            State::AppendLogs {
                                pending_changes: vec![],
                            }
                        } else {
                            State::Wait
                        };
                    }
                    AppendEntriesResponse::Update {
                        account_id,
                        collection,
                        changes,
                    } => {
                        let mut changes = if let Some(changes) = MergedChanges::from_bytes(&changes)
                        {
                            changes
                        } else {
                            error!("Failed to deserialize changes bitmap.");
                            break;
                        };

                        // Restore deletions.
                        if !changes.deletes.is_empty() {
                            changes.inserts = changes.deletes;
                            changes.deletes = RoaringBitmap::new();
                        }

                        debug!(
                            concat!(
                                "[{}] Peer {} requested {} insertions, ",
                                "{} updates for account {}, collection {:?}."
                            ),
                            local_name,
                            peer_name,
                            changes.inserts.len(),
                            changes.updates.len(),
                            account_id,
                            collection
                        );

                        state = State::AppendChanges {
                            account_id,
                            collection,
                            changes,
                        };
                    }
                }
            }
        });
    }

    pub fn spawn_raft_leader_init(
        &self,
        mut log_index_rx: watch::Receiver<Event>,
    ) -> watch::Receiver<bool> {
        let (tx, rx) = watch::channel(false);

        let term = self.term;
        let last_log_index = self.last_log.index;

        let core = self.core.clone();
        tokio::spawn(async move {
            if let Err(err) = core.commit_leader(LogIndex::MAX, true).await {
                error!("Failed to rollback uncommitted entries: {:?}", err);
                return;
            }
            if let Err(err) = core.commit_follower(LogIndex::MAX, true).await {
                error!("Failed to commit pending updates: {:?}", err);
                return;
            }

            // Poll the receiver to make sure this node is still the leader.
            match poll!(Box::pin(log_index_rx.changed())) {
                Poll::Ready(result) => match result {
                    Ok(_) => (),
                    Err(_) => {
                        debug!("This node was asked to step down during initialization.");
                        return;
                    }
                },
                Poll::Pending => (),
            }

            core.update_raft_index(last_log_index);
            if let Err(err) = core.set_leader_commit_index(last_log_index).await {
                error!("Failed to set leader commit index: {:?}", err);
                return;
            }
            core.set_leader(term);

            if tx.send(true).is_err() {
                error!("Failed to send message to raft leader processes.");
            }
        });
        rx
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    async fn prepare_changes(
        &self,
        account_id: AccountId,
        collection: Collection,
        changes: &mut MergedChanges,
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

            if let Some((item, item_size)) = match collection {
                Collection::Mail => self.fetch_email(account_id, document_id, is_insert).await?,
                Collection::Mailbox => {
                    self.fetch_orm::<MailboxProperty>(account_id, document_id)
                        .await?
                }
                Collection::Identity => {
                    self.fetch_orm::<IdentityProperty>(account_id, document_id)
                        .await?
                }
                _ => {
                    return Err(StoreError::InternalError(
                        "Unsupported collection for changes".into(),
                    ))
                }
            } {
                updates.push(item);
                batch_size += item_size;
            } else {
                debug!(
                    "Warning: Failed to fetch item in collection {:?}",
                    collection,
                );
            }

            if batch_size >= BATCH_MAX_SIZE {
                break;
            }
        }

        if changes.is_empty() {
            updates.push(Update::Eof);
        }

        Ok(updates)
    }

    async fn fetch_email(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
        is_insert: bool,
    ) -> store::Result<Option<(Update, usize)>> {
        /*let store = self.store.clone();
        self.spawn_worker(move || {
            let mut item_size = std::mem::size_of::<Update>();

            let mailboxes = if let Some(mailboxes) = store.get_document_tags(
                account_id,
                Collection::Mail,
                document_id,
                MessageField::Mailbox.into(),
            )? {
                item_size += mailboxes.items.len() * std::mem::size_of::<DocumentId>();
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

            let thread_id = if let Some(thread_id) = store.get_document_tag_id(
                account_id,
                Collection::Mail,
                document_id,
                MessageField::ThreadId.into(),
            )? {
                thread_id
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
                        Update::Document {
                            account_id,
                            document_id,
                            update: DocumentUpdate::InsertMail {
                                thread_id,
                                keywords,
                                mailboxes,
                                body,
                                received_at: message_outline.received_at,
                            },
                        },
                        item_size,
                    ))
                } else {
                    None
                }
            } else {
                Some((
                    Update::Document {
                        account_id,
                        document_id,
                        update: DocumentUpdate::UpdateMail {
                            thread_id,
                            keywords,
                            mailboxes,
                        },
                    },
                    item_size,
                ))
            })
        })
        .await */
        Ok(None)
    }

    async fn fetch_orm<U>(
        &self,
        account_id: AccountId,
        document_id: DocumentId,
    ) -> store::Result<Option<(Update, usize)>>
    where
        U: PropertySchema + 'static,
    {
        let store = self.store.clone();
        self.spawn_worker(move || {
            Ok(
                if let Some(orm) = store.get_orm::<U>(account_id, document_id)? {
                    let data = orm.serialize().ok_or_else(|| {
                        StoreError::SerializeError("Failed to serialize ORM.".to_string())
                    })?;
                    let data_len = data.len();
                    (
                        Update::Document {
                            account_id,
                            document_id,
                            update: DocumentUpdate::ORM {
                                collection: U::collection(),
                                data,
                            },
                        },
                        data_len,
                    )
                        .into()
                } else {
                    None
                },
            )
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
