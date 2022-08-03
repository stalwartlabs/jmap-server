use super::request::Request;
use super::{rpc, Cluster};
use std::net::SocketAddr;
use store::Store;

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_join(&mut self, id: usize, mut dest: SocketAddr, port: u16) {
        dest.set_port(port);
        self.send_gossip(dest, Request::JoinReply { id }).await;
    }

    pub async fn handle_join_reply(&mut self, id: usize) {
        if let Some(peer) = self.peers.get(id) {
            if peer.is_seed() {
                peer.dispatch_request(rpc::Request::UpdatePeers {
                    peers: self.build_peer_info(),
                })
                .await;
            }
        }
    }
}
