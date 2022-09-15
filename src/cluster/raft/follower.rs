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

use super::{Cluster, PeerId};
use super::{State, RAFT_LOG_BEHIND, RAFT_LOG_LEADER, RAFT_LOG_UPDATED};
use crate::services::{email_delivery, state_change};
use crate::JMAPServer;
use std::sync::atomic::Ordering;
use store::tracing::debug;
use store::Store;
use tokio::sync::mpsc;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn is_following(&self) -> bool {
        matches!(self.state, State::Follower { .. })
    }

    pub fn is_following_peer(
        &self,
        leader_id: PeerId,
    ) -> Option<&mpsc::Sender<crate::cluster::log::Event>> {
        match &self.state {
            State::Follower { peer_id, tx } if peer_id == &leader_id => Some(tx),
            _ => None,
        }
    }

    pub async fn follow_leader(
        &mut self,
        peer_id: PeerId,
    ) -> store::Result<mpsc::Sender<crate::cluster::log::Event>> {
        let tx = self.spawn_raft_follower();
        self.state = State::Follower {
            peer_id,
            tx: tx.clone(),
        };
        self.reset_votes();
        self.core
            .set_follower(self.get_peer(peer_id).unwrap().hostname.clone().into())
            .await;
        debug!(
            "[{}] Following peer {} for term {}.",
            self.addr,
            self.get_peer(peer_id).unwrap(),
            self.term
        );
        Ok(tx)
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn set_follower(&self, leader_hostname: Option<String>) {
        let cluster_ipc = self.cluster.as_ref().unwrap();

        cluster_ipc.state.store(RAFT_LOG_BEHIND, Ordering::Relaxed);
        *cluster_ipc.leader_hostname.lock() = leader_hostname;
        self.store
            .tombstone_deletions
            .store(false, Ordering::Relaxed);
        self.state_change
            .clone()
            .send(state_change::Event::Stop)
            .await
            .ok();
        self.email_delivery
            .clone()
            .send(email_delivery::Event::Stop)
            .await
            .ok();
    }

    pub fn is_up_to_date(&self) -> bool {
        self.cluster
            .as_ref()
            .map(|cluster| {
                [RAFT_LOG_LEADER, RAFT_LOG_UPDATED].contains(&cluster.state.load(Ordering::Relaxed))
            })
            .unwrap_or(true)
    }

    pub fn set_up_to_date(&self, is_up_to_date: bool) {
        self.cluster.as_ref().unwrap().state.store(
            if is_up_to_date {
                RAFT_LOG_UPDATED
            } else {
                RAFT_LOG_BEHIND
            },
            Ordering::Relaxed,
        );
    }
}
