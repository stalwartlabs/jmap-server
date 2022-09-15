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
