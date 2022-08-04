use super::{Cluster, PeerId};
use super::{State, RAFT_LOG_LEADER};
use crate::services::state_change;
use crate::JMAPServer;
use std::sync::atomic::Ordering;
use store::log::raft::TermId;
use store::tracing::debug;
use store::Store;
use tokio::sync::watch;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn leader_peer_id(&self) -> Option<PeerId> {
        match self.state {
            State::Leader { .. } => Some(self.peer_id),
            State::Follower { peer_id, .. } => Some(peer_id),
            _ => None,
        }
    }

    pub fn is_leading(&self) -> bool {
        matches!(self.state, State::Leader { .. })
    }

    pub async fn become_leader(&mut self) -> store::Result<()> {
        debug!(
            "[{}] This node is the new leader for term {}.",
            self.addr, self.term
        );

        #[cfg(test)]
        {
            let db_index = self
                .core
                .get_last_log()
                .await?
                .unwrap_or_else(store::log::raft::RaftId::none)
                .index;
            if db_index != self.last_log.index {
                println!(
                    "Raft index mismatch!!! {} != {}\n",
                    db_index, self.last_log.index
                );
            }
        }

        self.uncommitted_index = self.last_log.index;

        let (event_tx, event_rx) = watch::channel(crate::cluster::leader::Event::new(
            self.last_log.index,
            self.uncommitted_index,
        ));
        let init_rx = self.spawn_raft_leader_init(event_rx.clone());
        self.peers
            .iter()
            .filter(|p| p.is_in_shard(self.shard_id))
            .for_each(|p| self.spawn_raft_leader(p, event_rx.clone(), init_rx.clone().into()));
        self.state = State::Leader {
            tx: event_tx,
            rx: event_rx,
        };
        self.reset_votes();
        Ok(())
    }

    pub fn add_follower(&self, peer_id: PeerId) {
        if let State::Leader { rx, .. } = &self.state {
            self.spawn_raft_leader(self.get_peer(peer_id).unwrap(), rx.clone(), None)
        }
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn set_leader(&self, term: TermId) {
        self.cluster
            .as_ref()
            .unwrap()
            .state
            .store(RAFT_LOG_LEADER, Ordering::Relaxed);
        self.store.raft_term.store(term, Ordering::Relaxed);
        self.store
            .tombstone_deletions
            .store(true, Ordering::Relaxed);
        self.store.id_assigner.invalidate_all();
        #[cfg(not(test))]
        {
            self.store.acl_tokens.invalidate_all();
        }
        self.store.recipients.invalidate_all();
        self.store.shared_documents.invalidate_all();
        self.state_change
            .clone()
            .send(state_change::Event::Start)
            .await
            .ok();
    }

    pub fn is_leader(&self) -> bool {
        self.cluster
            .as_ref()
            .map(|cluster| cluster.state.load(Ordering::Relaxed) == RAFT_LOG_LEADER)
            .unwrap_or(true)
    }
}
