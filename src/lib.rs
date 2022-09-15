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

use std::sync::Arc;

use authorization::{auth::RemoteAddress, rate_limit::Limiter};
use cluster::ClusterIpc;
use store::{moka::future::Cache, JMAPStore};
use tokio::sync::{mpsc, watch};

pub mod api;
pub mod authorization;
pub mod cluster;
pub mod lmtp;
pub mod server;
pub mod services;

#[cfg(test)]
pub mod tests;

pub const DEFAULT_HTTP_PORT: u16 = 8080;
pub const DEFAULT_RPC_PORT: u16 = 7911;

pub struct JMAPServer<T> {
    pub store: Arc<JMAPStore<T>>,
    pub worker_pool: rayon::ThreadPool,
    pub base_session: api::session::Session,
    pub cluster: Option<ClusterIpc>,

    pub state_change: mpsc::Sender<services::state_change::Event>,
    pub email_delivery: mpsc::Sender<services::email_delivery::Event>,
    pub housekeeper: mpsc::Sender<services::housekeeper::Event>,
    pub lmtp: watch::Sender<bool>,

    pub oauth: Box<authorization::oauth::OAuth>,
    pub oauth_codes: Cache<String, Arc<authorization::oauth::OAuthCode>>,

    pub sessions: Cache<String, authorization::Session>,
    pub rate_limiters: Cache<RemoteAddress, Arc<Limiter>>,

    #[cfg(test)]
    pub is_offline: std::sync::atomic::AtomicBool,
}
