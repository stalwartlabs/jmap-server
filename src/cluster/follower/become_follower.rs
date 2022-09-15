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

use store::log::raft::{RaftId, TermId};
use store::tracing::error;
use store::Store;
use tokio::sync::oneshot;

use crate::cluster::log::{AppendEntriesRequest, Event};

use super::PeerId;
use super::{
    rpc::{self, Response},
    Cluster,
};

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_become_follower(
        &mut self,
        peer_id: PeerId,
        response_tx: oneshot::Sender<rpc::Response>,
        term: TermId,
        last_log: RaftId,
    ) -> store::Result<()> {
        if self.is_known_peer(peer_id) {
            if self.term < term {
                self.term = term;
            }

            if self.term == term && self.log_is_behind_or_eq(last_log.term, last_log.index) {
                self.follow_leader(peer_id)
                    .await?
                    .send(Event {
                        response_tx,
                        request: AppendEntriesRequest::Match { last_log },
                    })
                    .await
                    .unwrap_or_else(|err| error!("Failed to send event: {}", err));
            } else {
                response_tx
                    .send(Response::StepDown { term: self.term })
                    .unwrap_or_else(|_| error!("Oneshot response channel closed."));
            }
        } else {
            response_tx
                .send(rpc::Response::UnregisteredPeer)
                .unwrap_or_else(|_| error!("Oneshot response channel closed."));
        }
        Ok(())
    }
}
