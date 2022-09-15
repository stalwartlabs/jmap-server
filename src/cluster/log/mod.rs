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

pub mod changes_get;
pub mod changes_merge;
pub mod entries_get;
pub mod index_match;
pub mod rollback_apply;
pub mod rollback_get;
pub mod rollback_prepare;
pub mod rollback_remove;
pub mod update_apply;
pub mod update_prepare;

use super::rpc;
use store::blob::BlobId;
use store::core::collection::Collection;
use store::log::raft::{LogIndex, RaftId};
use store::serialize::{StoreDeserialize, StoreSerialize};
use store::{bincode, JMAPId};
use store::{AccountId, DocumentId};
use tokio::sync::oneshot;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Update {
    Begin {
        account_id: AccountId,
        collection: Collection,
    },
    Document {
        update: DocumentUpdate,
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
pub enum DocumentUpdate {
    Insert {
        jmap_id: JMAPId,
        fields: Vec<u8>,
        blobs: Vec<BlobId>,
        term_index: Option<BlobId>,
    },
    Update {
        jmap_id: JMAPId,
        fields: Vec<u8>,
    },
    Delete {
        document_id: DocumentId,
    },
}

impl DocumentUpdate {
    pub fn size(&self) -> usize {
        match self {
            DocumentUpdate::Insert {
                fields,
                blobs,
                term_index,
                ..
            } => {
                fields.len()
                    + std::mem::size_of::<JMAPId>()
                    + ((blobs.len() + term_index.as_ref().map(|_| 1).unwrap_or(0))
                        * std::mem::size_of::<BlobId>())
            }
            DocumentUpdate::Update { fields, .. } => fields.len() + std::mem::size_of::<JMAPId>(),
            DocumentUpdate::Delete { .. } => std::mem::size_of::<DocumentId>(),
        }
    }
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
        is_rollback: bool,
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
        update: DocumentUpdate,
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
