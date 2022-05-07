use std::sync::Arc;

use api::session::Session;
use cluster::ClusterIpc;
use store::JMAPStore;
use tokio::sync::mpsc;

pub mod api;
pub mod blob;
pub mod cluster;
pub mod server;
pub mod state;

pub use actix_web;
pub use futures;
pub use rayon;
pub use tokio;

pub const DEFAULT_HTTP_PORT: u16 = 8080;
pub const DEFAULT_RPC_PORT: u16 = 7911;

pub struct JMAPServer<T> {
    pub store: Arc<JMAPStore<T>>,
    pub worker_pool: rayon::ThreadPool,
    pub base_session: Session,
    pub cluster: Option<ClusterIpc>,
    pub state_change: mpsc::Sender<state::Event>,

    #[cfg(feature = "debug")]
    pub is_offline: std::sync::atomic::AtomicBool,
}
