use super::{Protocol, Request, Response, RpcEvent};
use crate::cluster::Peer;
use store::tracing::error;
use tokio::sync::{mpsc, oneshot};

impl Peer {
    // Sends a request and "waits" asynchronically until the response is available.
    pub async fn send_request(&self, request: Request) -> Response {
        let (response_tx, rx) = oneshot::channel();
        if let Err(err) = self
            .tx
            .send(RpcEvent::NeedResponse {
                request,
                response_tx,
            })
            .await
        {
            error!("Channel failed: {}", err);
            return Response::None;
        }
        rx.await.unwrap_or(Response::None)
    }

    // Submits a request, the result is returned at a later time via the main channel.
    pub async fn dispatch_request(&self, request: Request) {
        //debug!("OUT: {:?}", request);
        if let Err(err) = self.tx.send(RpcEvent::FireAndForget { request }).await {
            error!("Channel failed: {}", err);
        }
    }
}

impl Protocol {
    pub fn unwrap_request(self) -> Request {
        match self {
            Protocol::Request(req) => req,
            _ => Request::None,
        }
    }

    pub fn unwrap_response(self) -> Response {
        match self {
            Protocol::Response(res) => res,
            _ => Response::None,
        }
    }
}

impl Request {
    pub async fn send(self, peer_tx: &mpsc::Sender<RpcEvent>) -> Option<Response> {
        let (response_tx, rx) = oneshot::channel();
        peer_tx
            .send(RpcEvent::NeedResponse {
                request: self,
                response_tx,
            })
            .await
            .ok()?;
        rx.await.unwrap_or(Response::None).into()
    }
}
