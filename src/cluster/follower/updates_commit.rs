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

use super::rpc::Response;
use super::{RaftIndexes, State};
use crate::cluster::log::AppendEntriesResponse;
use crate::JMAPServer;
use store::log::raft::LogIndex;
use store::tracing::{debug, error};
use store::Store;

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn commit_updates(&self, indexes: &mut RaftIndexes) -> Option<(State, Response)> {
        // Apply changes
        if indexes.leader_commit_index != LogIndex::MAX
            && indexes.uncommitted_index <= indexes.leader_commit_index
        {
            let last_log = match self.commit_follower(indexes.uncommitted_index, false).await {
                Ok(Some(last_log)) => last_log,
                Ok(None) => {
                    error!(
                        "Raft entry {} not found while committing updates.",
                        indexes.uncommitted_index
                    );
                    return None;
                }
                Err(err) => {
                    error!("Failed to apply changes: {:?}", err);
                    return None;
                }
            };

            indexes.commit_index = indexes.uncommitted_index;
            self.update_last_log(last_log).await;

            // Set up to date
            if indexes.commit_index == indexes.leader_commit_index {
                debug!(
                    "This node is now up to date with the leader's commit index {}.",
                    indexes.leader_commit_index
                );
                self.set_up_to_date(true);
            } else {
                debug!(
                    concat!(
                        "This node is still behind the leader's commit index {}, ",
                        "local commit index is {}."
                    ),
                    indexes.leader_commit_index, indexes.commit_index
                );
            }
        } else {
            debug!(
                concat!(
                    "No changes to apply: leader commit index = {}, ",
                    "local uncommitted index: {}, local committed index: {}."
                ),
                indexes.leader_commit_index, indexes.uncommitted_index, indexes.leader_commit_index
            );
        }
        (
            State::Synchronize,
            Response::AppendEntries(AppendEntriesResponse::Done {
                up_to_index: indexes.uncommitted_index,
            }),
        )
            .into()
    }
}
