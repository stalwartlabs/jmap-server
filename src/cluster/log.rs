use std::collections::{hash_map, HashMap};
use std::task::Poll;
use std::time::Duration;

use actix_web::web;
use futures::poll;
use jmap_mail::import::{Bincoded, JMAPMailImport};
use jmap_mail::mailbox::{JMAPMailMailbox, JMAPMailboxProperties, Mailbox};
use jmap_mail::query::MailboxId;
use jmap_mail::{MessageField, MessageOutline, MESSAGE_DATA, MESSAGE_RAW};
use store::batch::WriteBatch;
use store::changes::{self, ChangeId};
use store::leb128::Leb128;
use store::raft::{Entry, LogIndex, PendingChanges, RaftId, TermId};
use store::serialize::StoreDeserialize;
use store::tracing::{debug, error};
use store::{
    lz4_flex, AccountId, Collection, ColumnFamily, DocumentId, JMAPId, Store, StoreError, Tag,
    ThreadId,
};
use store::{JMAPIdPrefix, WriteOperation};
use tokio::{
    sync::{mpsc, oneshot, watch},
    time,
};

use crate::JMAPServer;

use super::rpc::UpdateCollection;
use super::{
    rpc::{self, Request, Response, RpcEvent},
    Cluster, Event,
};
use super::{Peer, PeerId};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Change {
    InsertMail {
        jmap_id: JMAPId,
        keywords: Vec<Tag>,
        mailboxes: Vec<MailboxId>,
        received_at: i64,
        body: Vec<u8>,
    },
    UpdateMail {
        jmap_id: JMAPId,
        keywords: Vec<Tag>,
        mailboxes: Vec<MailboxId>,
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

const RETRY_MS: u64 = 30 * 1000;
const BATCH_MAX_ENTRIES: usize = 10;
const BATCH_MAX_SIZE: usize = 10 * 1024 * 1024;

enum State {
    Synchronize,
    AppendEntries,
    PushChanges {
        collections: Vec<UpdateCollection>,
        changes: PendingChanges,
    },
    Wait,
}

pub fn spawn_append_entries<T>(cluster: &Cluster<T>, peer: &Peer, mut rx: watch::Receiver<LogIndex>)
where
    T: for<'x> Store<'x> + 'static,
{
    let peer_tx = peer.tx.clone();
    let peer_name = peer.to_string();

    let term = cluster.term;
    let mut last = RaftId::new(cluster.last_log_term, cluster.last_log_index);
    let main_tx = cluster.tx.clone();
    let core = cluster.core.clone();

    let mut state = State::Synchronize;
    let mut last_commited_id = RaftId::none();
    let mut last_sent_id = RaftId::none();

    tokio::spawn(async move {
        debug!("Starting append entries process for peer {}.", peer_name);

        loop {
            // Poll the receiver to make sure this node is still the leader.
            match poll!(Box::pin(rx.changed())) {
                Poll::Ready(result) => match result {
                    Ok(_) => {
                        last.index = *rx.borrow();
                        if matches!(&state, State::Wait) {
                            state = State::AppendEntries;
                        }
                    }
                    Err(_) => {
                        debug!("Log sync process with {} exiting.", peer_name);
                        break;
                    }
                },
                Poll::Pending => (),
            }

            let request = match &mut state {
                State::Synchronize => Request::SynchronizeLog { term, last },
                State::PushChanges { changes, .. } => match prepare_changes(&core, changes).await {
                    Ok(request) => request,
                    Err(err) => {
                        error!("Failed to prepare changes: {:?}", err);
                        continue;
                    }
                },
                State::Wait => {
                    // Wait for the next change
                    if rx.changed().await.is_ok() {
                        debug!("Received new log index.");
                        last.index = *rx.borrow();
                    } else {
                        debug!("Log sync process with {} exiting.", peer_name);
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
                                .get_raft_entries(last_sent_id, BATCH_MAX_ENTRIES)
                        })
                        .await
                    {
                        Ok(entries) => {
                            if !entries.is_empty() {
                                Request::AppendEntries { term, entries }
                            } else {
                                debug!(
                                    "No entries left to send to {} after {:?}.",
                                    peer_name, last_sent_id
                                );
                                state = State::Wait;
                                continue;
                            }
                        }
                        Err(err) => {
                            error!("Error getting raft entries: {:?}", err);
                            last_sent_id = last_commited_id;
                            state = State::Wait;
                            continue;
                        }
                    }
                }
            };

            match send_request(&peer_tx, request).await {
                Response::SynchronizeLog {
                    term: peer_term,
                    success,
                    matched,
                } => {
                    if !success || peer_term > term {
                        if let Err(err) = main_tx.send(Event::StepDown { term: peer_term }).await {
                            error!("Error sending step down message: {}", err);
                        }
                        break;
                    } else if !matched.is_none() {
                        let local_match = match core.get_next_raft_id(matched).await {
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
                        if local_match != matched {
                            // TODO delete out of sync entries
                            error!(
                                "Failed to match raft logs with {}, local match: {:?}, peer match: {:?}", peer_name,
                                local_match, matched
                            );
                            break;
                        }
                    }

                    last_commited_id = matched;
                    last_sent_id = matched;
                    state = State::AppendEntries;
                }
                Response::None => {
                    // There was a problem delivering the message, wait 30 seconds or until
                    // the next change is received.
                    match time::timeout(Duration::from_millis(RETRY_MS), rx.changed()).await {
                        Ok(Ok(())) => {
                            debug!("Received new log index.");
                            last.index = *rx.borrow();
                        }
                        Ok(Err(_)) => {
                            debug!("Log sync process with {} exiting.", peer_name);
                            break;
                        }
                        Err(_) => (),
                    }
                    state = State::Synchronize;
                }
                Response::NeedUpdates { collections } => {
                    state = get_next_changes(&core, collections).await;
                }
                Response::Continue => match &mut state {
                    State::PushChanges {
                        changes,
                        collections,
                    } if changes.is_empty() => {
                        if collections.is_empty() {
                            last_commited_id = last_sent_id;
                            state = State::AppendEntries;
                        } else {
                            state = get_next_changes(&core, std::mem::take(collections)).await;
                        }
                    }
                    _ => (),
                },

                response @ (Response::UpdatePeers { .. } | Response::Vote { .. }) => {
                    error!(
                        "Unexpected response from peer {}: {:?}",
                        peer_name, response
                    );
                }
            }
        }
    });
}

async fn send_request(peer_tx: &mpsc::Sender<rpc::RpcEvent>, request: Request) -> Response {
    let (response_tx, rx) = oneshot::channel();
    if let Err(err) = peer_tx
        .send(RpcEvent::NeedResponse {
            request,
            response_tx,
        })
        .await
    {
        error!("Channel failed: {}", err);
        return Response::None;
    }
    rx.await.unwrap_or(Response::None)
}

async fn get_next_changes<T>(
    core: &web::Data<JMAPServer<T>>,
    mut collections: Vec<UpdateCollection>,
) -> State
where
    T: for<'x> Store<'x> + 'static,
{
    let collection = if let Some(collection) = collections.pop() {
        collection
    } else {
        error!("Received empty collections list.");
        return State::AppendEntries;
    };

    let _core = core.clone();
    match core
        .spawn_worker(move || {
            _core.store.get_pending_changes(
                collection.account_id,
                collection.collection,
                collection.from_change_id,
                matches!(
                    collection.collection,
                    Collection::MailboxChanges | Collection::Thread
                ),
            )
        })
        .await
    {
        Ok(changes) => State::PushChanges {
            collections,
            changes,
        },
        Err(err) => {
            error!("Error getting raft changes: {:?}", err);
            State::Synchronize
        }
    }
}

async fn prepare_changes<T>(
    core: &web::Data<JMAPServer<T>>,
    changes: &mut PendingChanges,
) -> store::Result<Request>
where
    T: for<'x> Store<'x> + 'static,
{
    let mut batch_size = 0;
    let mut push_changes = Vec::new();

    loop {
        let item = if let Some(document_id) = changes.inserts.min() {
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
        } else if let Some(document_id) = changes.deletes.min() {
            changes.deletes.remove(document_id);
            Some((
                Change::Delete { document_id },
                std::mem::size_of::<Change>(),
            ))
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
                "Failed to fetch item in collection {:?}",
                changes.collection,
            );
        }

        if batch_size >= BATCH_MAX_SIZE {
            break;
        }
    }

    if changes.is_empty() {
        push_changes.push(Change::Commit);
    }

    Ok(Request::UpdateStore {
        account_id: changes.account_id,
        collection: changes.collection,
        changes: push_changes,
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

        let mailboxes = if let Some(mailboxes) =
            _core.store.get_document_value::<Bincoded<Vec<MailboxId>>>(
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
        let keywords = if let Some(keywords) =
            _core.store.get_document_value::<Bincoded<Vec<Tag>>>(
                account_id,
                Collection::Mail,
                document_id,
                MessageField::Keyword.into(),
            )? {
            item_size += keywords.items.iter().map(|tag| tag.len()).sum::<usize>();
            keywords.items
        } else {
            vec![]
        };

        let jmap_id = if let Some(thread_id) = _core.store.get_document_value::<ThreadId>(
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
                &changes::Entry::serialize_key(account_id, collection, change_id),
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

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_synchronize_request(
        &mut self,
        peer_id: PeerId,
        term: TermId,
        last: RaftId,
    ) -> Response {
        if self.term < term {
            self.term = term;
        }

        let (success, matched) =
            if self.term == term && self.log_is_behind_or_eq(last.term, last.index) {
                self.follow_leader(peer_id);
                match self.core.get_prev_raft_id(last).await {
                    Ok(Some(matched)) => (true, matched),
                    Ok(None) => (true, RaftId::none()),
                    Err(err) => {
                        debug!("Failed to get prev raft id: {:?}", err);
                        (false, RaftId::none())
                    }
                }
            } else {
                (false, RaftId::none())
            };

        Response::SynchronizeLog {
            term: self.term,
            success,
            matched,
        }
    }

    pub async fn handle_append_entries(
        &mut self,
        peer_id: PeerId,
        term: TermId,
        entries: Vec<Entry>,
    ) -> Response {
        if term < self.term || !self.is_following_peer(peer_id) {
            return Response::SynchronizeLog {
                term: self.term,
                success: false,
                matched: RaftId::new(self.last_log_term, self.last_log_index),
            };
        }

        let core = self.core.clone();
        match self
            .core
            .spawn_worker(move || {
                let mut collections = HashMap::new();
                for entry in &entries {
                    for change in &entry.changes {
                        if let hash_map::Entry::Vacant(e) =
                            collections.entry((entry.account_id, change.collection))
                        {
                            e.insert(UpdateCollection {
                                account_id: entry.account_id,
                                collection: change.collection,
                                from_change_id: if let Some(last_change_id) = core
                                    .store
                                    .get_last_change_id(entry.account_id, change.collection)?
                                {
                                    if change.change_id <= last_change_id {
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

                Ok((collections, entries))
            })
            .await
        {
            Ok((collections, entries)) => {
                self.pending_changes = entries.into();
                if !collections.is_empty() {
                    Response::NeedUpdates {
                        collections: collections.into_values().collect(),
                    }
                } else if self.commit_pending_changes().await {
                    Response::Continue
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

    pub async fn handle_update_store(
        &mut self,
        peer_id: PeerId,
        account_id: AccountId,
        collection: Collection,
        changes: Vec<Change>,
    ) -> Response {
        if !self.is_following_peer(peer_id) {
            return Response::SynchronizeLog {
                term: self.term,
                success: false,
                matched: RaftId::new(self.last_log_term, self.last_log_index),
            };
        }

        let core = self.core.clone();
        match self
            .core
            .spawn_worker(move || {
                let mut do_commit = false;
                let mut document_batch = Vec::with_capacity(changes.len());
                let mut log_batch = Vec::with_capacity(changes.len());

                for change in changes {
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
                                changes::Entry::serialize_key(account_id, collection, change_id),
                                entry,
                            ));
                        }
                        Change::Delete { document_id } => document_batch.push(WriteBatch::delete(
                            collection,
                            document_id,
                            document_id,
                        )),
                        Change::Commit => {
                            do_commit = true;
                        }
                    }
                }
                if !document_batch.is_empty() {
                    core.store
                        .update_documents(account_id, RaftId::none(), document_batch)?;
                }
                if !log_batch.is_empty() {
                    core.store.db.write(log_batch)?;
                }

                Ok(do_commit)
            })
            .await
        {
            Ok(do_commit) => {
                if do_commit && !self.commit_pending_changes().await {
                    Response::None
                } else {
                    Response::Continue
                }
            }
            Err(err) => {
                debug!("Failed to update store: {:?}", err);
                Response::None
            }
        }
    }

    pub async fn commit_pending_changes(&mut self) -> bool {
        let mut success = true;
        if let Some(pending_changes) = std::mem::take(&mut self.pending_changes) {
            let core = self.core.clone();
            match self
                .core
                .spawn_worker(move || core.store.insert_raft_entries(pending_changes))
                .await
            {
                Ok(_) => (),
                Err(err) => {
                    error!("Failed to commit pending changes: {:?}", err);
                    success = false;
                }
            }
        }
        success
    }
}
