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

use jmap_sharing::principal::account::JMAPAccountStore;
use serde::{Deserialize, Serialize};
use store::{tracing::error, RecipientType, Store};
use tokio::sync::oneshot;

use crate::{
    cluster::{self, Cluster},
    lmtp::session::RcptType,
    JMAPServer,
};

use super::{Request, Response};

#[derive(Debug, Serialize, Deserialize)]
pub enum Command {
    ExpandRcpt {
        mailbox: String,
    },
    IngestMessage {
        mail_from: String,
        rcpt_to: Vec<RcptType>,
        raw_message: Vec<u8>,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum CommandResponse {
    ExpandRcpt {
        rt: RecipientType,
    },
    IngestMessage {
        result: Result<Vec<RcptType>, String>,
    },
    Error {
        message: String,
    },
}

impl<T> Cluster<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn send_command(
        &mut self,
        command: Command,
        response_tx: oneshot::Sender<CommandResponse>,
    ) {
        if let Some(peer) = self.leader_peer() {
            let peer_tx = peer.tx.clone();
            tokio::spawn(async move {
                let response = match (Request::Command { command }).send(&peer_tx).await {
                    Some(Response::Command { response }) => response,
                    err => {
                        error!("Received invalid command response: {:?}.", err);
                        CommandResponse::Error {
                            message: "RPC failure".to_string(),
                        }
                    }
                };

                if response_tx.send(response).is_err() {
                    error!("Failed to send response to command sender.");
                }
            });
        } else if response_tx
            .send(CommandResponse::Error {
                message: "Leader not elected.".to_string(),
            })
            .is_err()
        {
            error!("Failed to send response to command sender.");
        }
    }

    pub async fn handle_command(
        &mut self,
        command: Command,
        response_tx: oneshot::Sender<super::Response>,
    ) {
        if self.is_leading() {
            let core = self.core.clone();
            tokio::spawn(async move {
                let response = match command {
                    Command::ExpandRcpt { mailbox } => {
                        let store = core.store.clone();
                        match core.spawn_worker(move || store.expand_rcpt(mailbox)).await {
                            Ok(rt) => CommandResponse::ExpandRcpt {
                                rt: rt.as_ref().clone(),
                            },
                            Err(err) => {
                                error!("Failed to expand rcpt: {}", err);
                                CommandResponse::Error {
                                    message: "Temporary database failure".to_string(),
                                }
                            }
                        }
                    }
                    Command::IngestMessage {
                        mail_from,
                        rcpt_to,
                        raw_message,
                    } => CommandResponse::IngestMessage {
                        result: core.mail_ingest(mail_from, rcpt_to, raw_message).await,
                    },
                };

                response_tx
                    .send(super::Response::Command { response })
                    .unwrap_or_else(|_| error!("Oneshot response channel closed."));
            });
        } else {
            response_tx
                .send(super::Response::Command {
                    response: CommandResponse::Error {
                        message: "Not leading cluster.".to_string(),
                    },
                })
                .unwrap_or_else(|_| error!("Oneshot response channel closed."));
        }
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn rpc_command(&self, command: Command) -> Option<CommandResponse> {
        let cluster = self.cluster.as_ref()?;
        let (tx, rx) = oneshot::channel();
        if cluster
            .tx
            .send(cluster::Event::RpcCommand {
                command,
                response_tx: tx,
            })
            .await
            .is_ok()
        {
            rx.await.ok()
        } else {
            error!("Failed to send RPC command to cluster.");
            None
        }
    }
}
