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

use super::Cluster;
use super::Event;
use futures::poll;
use std::task::Poll;
use store::log::raft::LogIndex;
use store::tracing::{debug, error};
use store::Store;
use tokio::sync::watch;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
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
            core.set_leader(term).await;

            if tx.send(true).is_err() {
                error!("Failed to send message to raft leader processes.");
            }
        });
        rx
    }
}
