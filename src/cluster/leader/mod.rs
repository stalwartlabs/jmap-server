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
