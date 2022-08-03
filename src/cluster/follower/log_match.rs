use store::log::raft::RaftId;
use store::tracing::debug;
use store::Store;

use crate::cluster::log::AppendEntriesResponse;
use crate::JMAPServer;

use super::rpc::Response;

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_match_log(&self, last_log: RaftId) -> Option<Response>
    where
        T: for<'x> Store<'x> + 'static,
    {
        Response::AppendEntries(AppendEntriesResponse::Match {
            match_log: match self.get_prev_raft_id(last_log).await {
                Ok(Some(matched)) => {
                    self.set_up_to_date(matched == last_log);
                    matched
                }
                Ok(None) => {
                    if last_log.is_none() {
                        self.set_up_to_date(true);
                    }
                    RaftId::none()
                }
                Err(err) => {
                    debug!("Failed to get prev raft id: {:?}", err);
                    return None;
                }
            },
        })
        .into()
    }
}
