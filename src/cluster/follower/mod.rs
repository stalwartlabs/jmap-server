pub mod append_entries;
pub mod become_follower;
pub mod blobs;
pub mod commit;
pub mod log_match;
pub mod log_merge;
pub mod log_synchronize;
pub mod log_update;
pub mod spawn_follower;
pub mod updates_check;
pub mod updates_commit;
pub mod updates_pending;
pub mod updates_request;
pub mod updates_rollback;

use super::log::changes_merge::MergedChanges;
use super::log::Update;
use super::{
    rpc::{self},
    Cluster,
};
use super::{PeerId, IPC_CHANNEL_BUFFER};
use store::ahash::{AHashMap, AHashSet};
use store::blob::BlobId;
use store::core::bitmap::Bitmap;
use store::core::collection::Collection;
use store::log::raft::LogIndex;
use store::AccountId;

#[derive(Debug)]
pub enum State {
    Synchronize,
    AppendEntries {
        changed_accounts: AHashMap<AccountId, Bitmap<Collection>>,
    },
    AppendChanges {
        changed_accounts: Vec<(AccountId, Bitmap<Collection>)>,
    },
    AppendBlobs {
        pending_blobs: AHashSet<BlobId>,
        pending_updates: Vec<Update>,
        changed_accounts: Vec<(AccountId, Bitmap<Collection>)>,
    },
    Rollback {
        account_id: AccountId,
        collection: Collection,
        changes: MergedChanges,
    },
}

impl Default for State {
    fn default() -> Self {
        State::Synchronize
    }
}

#[derive(Debug)]
pub struct RaftIndexes {
    leader_commit_index: LogIndex,
    commit_index: LogIndex,
    uncommitted_index: LogIndex,
    merge_index: LogIndex,
    sequence_id: u64,
}
