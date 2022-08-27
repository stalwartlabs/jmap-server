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
