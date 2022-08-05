pub mod blobs_prepare;
pub mod changes_prepare;
pub mod commit;
pub mod init_leader;
pub mod spawn_leader;

use super::log::changes_merge::MergedChanges;
use super::Peer;
use super::{
    rpc::{self},
    Cluster,
};
use store::blob::BlobId;
use store::core::bitmap::Bitmap;
use store::core::collection::Collection;
use store::log::raft::{LogIndex, RaftId};
use store::AccountId;

const BATCH_MAX_SIZE: usize = 10 * 1024 * 1024; //TODO configure

#[derive(Debug)]
enum State {
    BecomeLeader,
    Synchronize,
    Merge {
        matched_log: RaftId,
    },
    AppendLogs {
        pending_changes: Vec<(Bitmap<Collection>, Vec<AccountId>)>,
    },
    AppendChanges {
        account_id: AccountId,
        collection: Collection,
        changes: MergedChanges,
        is_rollback: bool,
    },
    AppendBlobs {
        pending_blob_ids: Vec<BlobId>,
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
