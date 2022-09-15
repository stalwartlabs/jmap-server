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
use store::tracing::{debug, error};
use store::Store;

use crate::cluster::log::AppendEntriesResponse;
use crate::JMAPServer;

use super::rpc::Response;

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn handle_synchronize_log(&self, match_terms: Vec<RaftId>) -> Option<Response> {
        if match_terms.is_empty() {
            error!("Log sync failed: match terms list is empty.");
            return None;
        }

        let local_match_terms = match self.get_raft_match_terms().await {
            Ok(local_match_terms) => {
                debug_assert!(!local_match_terms.is_empty());
                local_match_terms
            }
            Err(err) => {
                error!("Error getting raft match list: {:?}", err);
                return None;
            }
        };

        let mut matched_id = RaftId::none();
        for (local_id, remote_id) in local_match_terms.into_iter().zip(match_terms) {
            if local_id == remote_id {
                matched_id = local_id;
            } else {
                break;
            }
        }

        Response::AppendEntries(AppendEntriesResponse::Synchronize {
            match_indexes: if !matched_id.is_none() {
                match self.get_raft_match_indexes(matched_id.index).await {
                    Ok((_, match_indexes)) => {
                        if !match_indexes.is_empty() {
                            let mut bytes = Vec::with_capacity(match_indexes.serialized_size());
                            if let Err(err) = match_indexes.serialize_into(&mut bytes) {
                                error!("Failed to serialize match indexes: {}", err);
                                return None;
                            }
                            bytes
                        } else {
                            debug_assert!(false);
                            debug!("No match indexes found for match indexes {:?}", matched_id);
                            return None;
                        }
                    }
                    Err(err) => {
                        error!("Error getting raft match indexes: {:?}", err);
                        return None;
                    }
                }
            } else {
                vec![]
            },
        })
        .into()
    }
}
