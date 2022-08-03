pub mod changes_get;
pub mod changes_merge;
pub mod document_delete;
pub mod entries_get;
pub mod index_match;
pub mod rollback_apply;
pub mod rollback_get;
pub mod rollback_prepare;
pub mod rollback_remove;
pub mod update_apply;

use super::rpc;
use jmap::jmap_store::raft::RaftUpdate;
use store::bincode;
use store::blob::BlobId;
use store::core::collection::Collection;
use store::log::raft::{LogIndex, RaftId};
use store::serialize::{StoreDeserialize, StoreSerialize};
use store::{AccountId, DocumentId};
use tokio::sync::oneshot;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Update {
    Begin {
        account_id: AccountId,
        collection: Collection,
    },
    Document {
        update: RaftUpdate,
    },
    Change {
        change: Vec<u8>,
    },
    Blob {
        blob_id: BlobId,
        blob: Vec<u8>,
    },
    Log {
        raft_id: RaftId,
        log: Vec<u8>,
    },
    Eof,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum AppendEntriesRequest {
    Match {
        last_log: RaftId,
    },
    Synchronize {
        match_terms: Vec<RaftId>,
    },
    Merge {
        matched_log: RaftId,
    },
    Update {
        commit_index: LogIndex,
        updates: Vec<Update>,
    },
    AdvanceCommitIndex {
        commit_index: LogIndex,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum AppendEntriesResponse {
    Match {
        match_log: RaftId,
    },
    Synchronize {
        match_indexes: Vec<u8>,
    },
    Update {
        account_id: AccountId,
        collection: Collection,
        changes: Vec<u8>,
    },
    FetchBlobs {
        blob_ids: Vec<BlobId>,
    },
    Continue,
    Done {
        up_to_index: LogIndex,
    },
}

pub struct Event {
    pub response_tx: oneshot::Sender<rpc::Response>,
    pub request: AppendEntriesRequest,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum PendingUpdate {
    Begin {
        account_id: AccountId,
        collection: Collection,
    },
    Update {
        update: RaftUpdate,
    },
    Delete {
        document_ids: Vec<DocumentId>,
    },
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct PendingUpdates {
    pub updates: Vec<PendingUpdate>,
}

impl PendingUpdates {
    pub fn new(updates: Vec<PendingUpdate>) -> Self {
        Self { updates }
    }
}

impl StoreSerialize for PendingUpdates {
    fn serialize(&self) -> Option<Vec<u8>> {
        bincode::serialize(self).ok()
    }
}

impl StoreDeserialize for PendingUpdates {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}
