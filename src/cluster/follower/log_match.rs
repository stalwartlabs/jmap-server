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
