use super::COMMIT_TIMEOUT_MS;
use super::{Cluster, PeerId};
use crate::JMAPServer;
use std::time::{Duration, Instant};
use store::log::raft::LogIndex;
use store::tracing::{debug, error};
use store::Store;
use tokio::time;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn advance_commit_index(
        &mut self,
        peer_id: PeerId,
        commit_index: LogIndex,
    ) -> store::Result<bool> {
        let mut indexes = Vec::with_capacity(self.peers.len() + 1);
        for peer in self.peers.iter_mut() {
            if peer.is_in_shard(self.shard_id) {
                if peer.peer_id == peer_id {
                    peer.commit_index = commit_index;
                }
                indexes.push(peer.commit_index.wrapping_add(1));
            }
        }
        indexes.push(self.uncommitted_index.wrapping_add(1));
        indexes.sort_unstable();

        // Use div_floor when stabilized.
        let commit_index = indexes[((indexes.len() as f64) / 2.0).floor() as usize];
        if commit_index > self.last_log.index.wrapping_add(1) {
            self.last_log.index = commit_index.wrapping_sub(1);
            self.last_log.term = self.term;

            let last_log_index = self.last_log.index;
            let core = self.core.clone();

            // Commit pending updates
            tokio::spawn(async move {
                if let Err(err) = core.commit_leader(last_log_index, false).await {
                    error!("Failed to commit leader: {:?}", err);
                }
            });

            // Notify peers
            self.send_append_entries();

            // Notify clients
            if let Err(err) = self.commit_index_tx.send(last_log_index) {
                error!("Failed to send commit index: {:?}", err);
            }

            debug!(
                "Advancing commit index to {} [cluster: {:?}].",
                self.last_log.index, indexes
            );
        }
        Ok(true)
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn commit_index(&self, index: LogIndex) -> bool {
        if let Some(cluster) = &self.cluster {
            if self.is_leader() {
                if cluster
                    .tx
                    .send(crate::cluster::Event::AdvanceUncommittedIndex {
                        uncommitted_index: index,
                    })
                    .await
                    .is_ok()
                {
                    let mut commit_index_rx = cluster.commit_index_rx.clone();
                    let wait_start = Instant::now();
                    let mut wait_timeout = Duration::from_millis(COMMIT_TIMEOUT_MS);

                    loop {
                        match time::timeout(wait_timeout, commit_index_rx.changed()).await {
                            Ok(Ok(())) => {
                                let commit_index = *commit_index_rx.borrow();
                                if commit_index >= index {
                                    debug!(
                                        "Successfully committed index {} in {}ms (latest index: {}).",
                                        index, wait_start.elapsed().as_millis(), commit_index
                                    );
                                    return true;
                                }

                                let wait_elapsed = wait_start.elapsed().as_millis() as u64;
                                if wait_elapsed >= COMMIT_TIMEOUT_MS {
                                    break;
                                }
                                wait_timeout =
                                    Duration::from_millis(COMMIT_TIMEOUT_MS - wait_elapsed);
                            }
                            Ok(Err(err)) => {
                                error!(
                                    "Failed to commit index {}, channel failure: {}",
                                    index, err
                                );
                                break;
                            }
                            Err(_) => {
                                error!(
                                    "Failed to commit index {}, timeout after {} ms.",
                                    index, COMMIT_TIMEOUT_MS
                                );
                                break;
                            }
                        }
                    }
                } else {
                    error!(
                        "Failed to commit index {}, unable to send store changed event.",
                        index
                    );
                }
            } else {
                error!(
                    "Failed to commit index {}, this node is no longer the leader.",
                    index
                );
            }
        }
        false
    }

    /*#[cfg(test)]
    pub async fn commit_last_index(&self) -> LogIndex {
        let uncommitted_index = self.get_last_log().await.unwrap().unwrap().index;
        if !self.commit_index(uncommitted_index).await {
            panic!("Failed to commit index {}", uncommitted_index);
        }
        uncommitted_index
    }*/
}
