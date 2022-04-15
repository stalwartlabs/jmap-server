use actix_web::http::header::ContentType;
use actix_web::http::StatusCode;
use actix_web::{post, web, HttpRequest, HttpResponse, ResponseError};

use jmap::json::JSONValue;
use jmap::request::{Invocation, Method, Object, Request, Response};
use jmap::{JMAPError, RequestError, RequestLimitError};
use store::tracing::{debug, error};
use store::ColumnFamily;
use store::{
    serialize::{StoreDeserialize, StoreSerialize},
    Store, StoreError,
};
use tokio::sync::oneshot;

use crate::cluster::Event;
use crate::JMAPServer;

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn get_key<U>(&self, key: &'static str) -> store::Result<Option<U>>
    where
        U: StoreDeserialize + Send + Sync + 'static,
    {
        let store = self.store.clone();
        self.spawn_worker(move || store.db.get(ColumnFamily::Values, key.as_bytes()))
            .await
    }

    pub async fn set_key<U>(&self, key: &'static str, value: U) -> store::Result<()>
    where
        U: StoreSerialize + Send + Sync + 'static,
    {
        let store = self.store.clone();
        self.spawn_worker(move || {
            store.db.set(
                ColumnFamily::Values,
                key.as_bytes(),
                &value.serialize().ok_or_else(|| {
                    StoreError::SerializeError(format!("Failed to serialize value for key {}", key))
                })?,
            )
        })
        .await
    }

    pub fn queue_set_key<U>(&self, key: &'static str, value: U)
    where
        U: StoreSerialize + Send + Sync + 'static,
    {
        let store = self.store.clone();

        self.worker_pool.spawn(move || {
            let bytes = match value.serialize() {
                Some(bytes) => bytes,
                None => {
                    error!("Failed to serialize value for key {}", key);
                    return;
                }
            };

            if let Err(err) = store.db.set(ColumnFamily::Values, key.as_bytes(), &bytes) {
                error!("Failed to set key: {:?}", err);
            }
        });
    }

    pub async fn spawn_worker<U, V>(&self, f: U) -> store::Result<V>
    where
        U: FnOnce() -> store::Result<V> + Send + 'static,
        V: Sync + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        self.worker_pool.spawn(move || {
            tx.send(f()).ok();
        });

        rx.await
            .map_err(|e| StoreError::InternalError(format!("Await error: {}", e)))?
    }

    pub async fn shutdown(&self) {
        if self.store.config.is_in_cluster && self.cluster_tx.send(Event::Shutdown).await.is_err() {
            error!("Failed to send shutdown event to cluster.");
        }
    }

    #[cfg(test)]
    pub async fn set_offline(&self, is_offline: bool, notify_peers: bool) {
        self.is_offline
            .store(is_offline, std::sync::atomic::Ordering::Relaxed);
        self.set_follower();
        if self
            .cluster_tx
            .send(Event::SetOffline {
                is_offline,
                notify_peers,
            })
            .await
            .is_err()
        {
            error!("Failed to send offline event to cluster.");
        }
    }

    #[cfg(test)]
    pub fn is_offline(&self) -> bool {
        self.is_offline.load(std::sync::atomic::Ordering::Relaxed)
    }
}

pub async fn handle_jmap_request<T>(
    request: web::Bytes,
    core: web::Data<JMAPServer<T>>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    let (status_code, body) = if request.len() < core.store.config.max_size_request {
        match serde_json::from_slice::<Request>(&request) {
            Ok(request) => {
                if request.method_calls.len() < core.store.config.max_calls_in_request {
                    (
                        StatusCode::OK,
                        handle_method_calls(request, core).await.to_string(),
                    )
                } else {
                    (
                        StatusCode::BAD_REQUEST,
                        RequestError::limit(RequestLimitError::CallsIn).to_string(),
                    )
                }
            }
            Err(err) => {
                debug!("Failed to parse request: {}", err);

                (
                    StatusCode::BAD_REQUEST,
                    RequestError::not_request().to_string(),
                )
            }
        }
    } else {
        (
            StatusCode::BAD_REQUEST,
            RequestError::limit(RequestLimitError::Size).to_string(),
        )
    };

    HttpResponse::build(status_code)
        .insert_header(ContentType::json())
        .body(body)
}

pub async fn handle_method_calls<T>(request: Request, core: web::Data<JMAPServer<T>>) -> Response
where
    T: for<'x> Store<'x> + 'static,
{
    let mut responses = Response::new("abc".to_string(), request.method_calls.len());

    for (name, arguments, call_id) in request.method_calls {
        match Invocation::parse(&name, arguments, &responses) {
            Ok(invocation) => match handle_method_call(invocation, &core).await {
                Ok(response) => responses.push_response(name, call_id, response),
                Err(err) => responses.push_error(call_id, err),
            },
            Err(err) => responses.push_error(call_id, err),
        }
    }

    responses
}

pub async fn handle_method_call<T>(
    invocation: Invocation,
    core: &web::Data<JMAPServer<T>>,
) -> jmap::Result<JSONValue>
where
    T: for<'x> Store<'x> + 'static,
{
    match (invocation.obj, invocation.call) {
        (Object::Core, Method::Echo(arguments)) => Ok(arguments),
        _ => Err(JMAPError::AccountNotFound),
    }
}
