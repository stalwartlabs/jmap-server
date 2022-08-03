use super::Cluster;
use super::State;
use crate::JMAPServer;
use std::sync::atomic::Ordering;
use store::log::raft::{LogIndex, RaftId, TermId};
use store::tracing::error;
use store::Store;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn log_is_behind_or_eq(&self, last_log_term: TermId, last_log_index: LogIndex) -> bool {
        last_log_term > self.last_log.term
            || (last_log_term == self.last_log.term
                && last_log_index.wrapping_add(1) >= self.last_log.index.wrapping_add(1))
    }

    pub fn log_is_behind(&self, last_log_term: TermId, last_log_index: LogIndex) -> bool {
        last_log_term > self.last_log.term
            || (last_log_term == self.last_log.term
                && last_log_index.wrapping_add(1) > self.last_log.index.wrapping_add(1))
    }

    pub fn send_append_entries(&self) {
        if let State::Leader { tx, .. } = &self.state {
            if let Err(err) = tx.send(crate::cluster::leader::Event::new(
                self.last_log.index,
                self.uncommitted_index,
            )) {
                error!("Failed to broadcast append entries: {}", err);
            }
        }
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn update_raft_index(&self, index: LogIndex) {
        self.store.raft_index.store(index, Ordering::Relaxed);
    }

    pub async fn get_last_log(&self) -> store::Result<Option<RaftId>> {
        let store = self.store.clone();
        self.spawn_worker(move || store.get_prev_raft_id(RaftId::new(TermId::MAX, LogIndex::MAX)))
            .await
    }

    pub async fn get_prev_raft_id(&self, key: RaftId) -> store::Result<Option<RaftId>> {
        let store = self.store.clone();
        self.spawn_worker(move || store.get_prev_raft_id(key)).await
    }

    pub async fn get_next_raft_id(&self, key: RaftId) -> store::Result<Option<RaftId>> {
        let store = self.store.clone();
        self.spawn_worker(move || store.get_next_raft_id(key)).await
    }

    pub async fn update_last_log(&self, last_log: RaftId) {
        if let Some(cluster) = &self.cluster {
            if cluster
                .tx
                .send(crate::cluster::Event::UpdateLastLog { last_log })
                .await
                .is_err()
            {
                error!("Failed to send store changed event.");
            }
        }
    }
}
