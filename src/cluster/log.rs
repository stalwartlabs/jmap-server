use std::collections::HashSet;

use jmap_mail::mailbox::Mailbox;
use store::{changes::ChangeId, raft::RaftId, AccountId, Collection, DocumentId, JMAPId, Tag};
use tokio::sync::oneshot;

use super::rpc;

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

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct UpdateCollection {
    pub account_id: AccountId,
    pub collection: Collection,
    pub from_change_id: Option<ChangeId>,
}

pub struct Event {
    pub response_tx: oneshot::Sender<rpc::Response>,
    pub request: AppendEntriesRequest,
}
