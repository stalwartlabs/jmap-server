use crate::api::invocation::handle_method_calls;
use crate::api::request::Request;
use crate::api::response::{serialize_hex, Response};
use crate::api::{method, RequestError, RequestErrorType, RequestLimitError};
use crate::authorization::Session;
use crate::services::LONG_SLUMBER_MS;
use crate::JMAPServer;
use actix::{Actor, ActorContext, AsyncContext, Handler, Message, StreamHandler};
use actix_web::{web, HttpRequest, HttpResponse};
use actix_web_actors::ws::{self, WsResponseBuilder};
use jmap::types::jmap::JMAPId;
use jmap::types::state::JMAPState;
use jmap::types::type_state::TypeState;
use std::borrow::Cow;
use std::time::{Duration, Instant};
use store::ahash::AHashMap;
use store::core::ahash_is_empty;
use store::core::bitmap::Bitmap;
use store::core::vec_map::VecMap;
use store::tracing::log::debug;
use store::Store;

#[derive(Debug, serde::Deserialize)]
struct WebSocketRequest {
    #[serde(rename = "@type")]
    pub _type: WebSocketRequestType,

    pub id: Option<String>,

    pub using: Vec<String>,

    #[serde(rename = "methodCalls")]
    pub method_calls: Vec<method::Call<method::Request>>,

    #[serde(rename = "createdIds")]
    pub created_ids: Option<AHashMap<String, JMAPId>>,
}

#[derive(Message, Debug, serde::Serialize)]
#[rtype(result = "()")]
pub struct WebSocketResponse {
    #[serde(rename = "@type")]
    _type: WebSocketResponseType,

    #[serde(rename = "methodResponses")]
    method_responses: Vec<method::Call<method::Response>>,

    #[serde(rename = "sessionState")]
    #[serde(serialize_with = "serialize_hex")]
    session_state: u32,

    #[serde(rename(deserialize = "createdIds"))]
    #[serde(skip_serializing_if = "ahash_is_empty")]
    created_ids: AHashMap<String, JMAPId>,

    #[serde(rename = "requestId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    request_id: Option<String>,
}

#[derive(Debug, PartialEq, Eq, serde::Serialize)]
enum WebSocketResponseType {
    Response,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
struct WebSocketPushEnable {
    #[serde(rename = "@type")]
    _type: WebSocketPushEnableType,
    #[serde(rename = "dataTypes")]
    data_types: Option<Vec<TypeState>>,
    #[serde(rename = "pushState")]
    push_state: Option<String>,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
struct WebSocketPushDisable {
    #[serde(rename = "@type")]
    _type: WebSocketPushDisableType,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
enum WebSocketRequestType {
    Request,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
enum WebSocketPushEnableType {
    WebSocketPushEnable,
}

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
enum WebSocketPushDisableType {
    WebSocketPushDisable,
}

#[derive(Debug, serde::Deserialize)]
#[serde(untagged)]
enum WebSocketMessage {
    Request(WebSocketRequest),
    PushEnable(WebSocketPushEnable),
    PushDisable(WebSocketPushDisable),
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum WebSocketStateChangeType {
    StateChange,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct WebSocketStateChange {
    #[serde(rename = "@type")]
    pub type_: WebSocketStateChangeType,
    pub changed: VecMap<JMAPId, VecMap<TypeState, JMAPState>>,
    #[serde(rename = "pushState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    push_state: Option<String>,
}

#[derive(Debug, Message, serde::Serialize)]
#[rtype(result = "()")]
pub struct WebSocketRequestError {
    #[serde(rename = "@type")]
    pub type_: WebSocketRequestErrorType,

    #[serde(rename(serialize = "type"))]
    p_type: RequestErrorType,

    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<RequestLimitError>,
    status: u16,
    detail: Cow<'static, str>,

    #[serde(rename = "requestId")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum WebSocketRequestErrorType {
    RequestError,
}

pub struct WebSocket<T>
where
    T: for<'x> Store<'x> + 'static,
{
    session: Session,
    core: web::Data<JMAPServer<T>>,
    state_handle: Option<actix::SpawnHandle>,
    hb: Instant,
}

impl<T> WebSocket<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn new(core: web::Data<JMAPServer<T>>, session: Session) -> Self {
        Self {
            hb: Instant::now(),
            core,
            session,
            state_handle: None,
        }
    }

    fn hb(&self, ctx: &mut <Self as Actor>::Context) {
        let heartbeat_interval =
            Duration::from_millis(self.core.store.config.ws_heartbeat_interval);
        let client_timeout = Duration::from_millis(self.core.store.config.ws_client_timeout);

        ctx.run_interval(heartbeat_interval, move |act, ctx| {
            if Instant::now().duration_since(act.hb) > client_timeout {
                debug!("Websocket Client heartbeat failed, disconnecting!");
                ctx.stop();
                return;
            }
            ctx.ping(b"");
        });
    }
}

impl<T> Actor for WebSocket<T>
where
    T: for<'x> Store<'x> + 'static,
{
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.hb(ctx);
    }
}

impl<T> Handler<WebSocketResponse> for WebSocket<T>
where
    T: for<'x> Store<'x> + 'static,
{
    type Result = ();

    fn handle(&mut self, msg: WebSocketResponse, ctx: &mut Self::Context) -> Self::Result {
        ctx.text(serde_json::to_string(&msg).unwrap_or_default());
    }
}

impl<T> Handler<WebSocketRequestError> for WebSocket<T>
where
    T: for<'x> Store<'x> + 'static,
{
    type Result = ();

    fn handle(&mut self, error: WebSocketRequestError, ctx: &mut Self::Context) -> Self::Result {
        ctx.text(serde_json::to_string(&error).unwrap_or_default());
    }
}

impl<T> StreamHandler<WebSocketStateChange> for WebSocket<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn handle(&mut self, item: WebSocketStateChange, ctx: &mut Self::Context) {
        ctx.text(serde_json::to_string(&item).unwrap_or_default());
    }
}

impl<T> StreamHandler<Result<ws::Message, ws::ProtocolError>> for WebSocket<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(msg)) => {
                self.hb = Instant::now();
                ctx.pong(&msg);
            }
            Ok(ws::Message::Pong(_)) => {
                self.hb = Instant::now();
            }
            Ok(ws::Message::Text(request)) => {
                let error = if request.len() < self.core.store.config.max_size_request {
                    match serde_json::from_slice::<WebSocketMessage>(request.as_bytes()) {
                        Ok(message) => match message {
                            WebSocketMessage::Request(request) => {
                                if request.method_calls.len()
                                    < self.core.store.config.max_calls_in_request
                                {
                                    let addr = ctx.address();
                                    let core = self.core.clone();
                                    let session = self.session.clone();

                                    tokio::spawn(async move {
                                        if let Ok(_in_flight_request) =
                                            core.is_account_allowed(session.account_id()).await
                                        {
                                            addr.do_send(WebSocketResponse::from_response(
                                                handle_method_calls(
                                                    Request {
                                                        using: request.using,
                                                        method_calls: request.method_calls,
                                                        created_ids: request.created_ids,
                                                    },
                                                    core,
                                                    session,
                                                )
                                                .await,
                                                request.id,
                                            ));
                                        } else {
                                            addr.do_send(WebSocketRequestError::from_error(
                                                RequestError::limit(RequestLimitError::Concurrent),
                                                request.id,
                                            ));
                                        }
                                    });
                                    return;
                                } else {
                                    WebSocketRequestError::from_error(
                                        RequestError::limit(RequestLimitError::CallsIn),
                                        request.id,
                                    )
                                }
                            }
                            WebSocketMessage::PushEnable(request) => {
                                let core = self.core.clone();
                                let account_id = self.session.account_id();
                                let throttle_ms = core.store.config.ws_throttle;
                                let types = if let Some(data_types) = request.data_types {
                                    if !data_types.is_empty() {
                                        data_types.into()
                                    } else {
                                        Bitmap::all()
                                    }
                                } else {
                                    Bitmap::all()
                                };

                                self.state_handle = Some(ctx.add_stream(async_stream::stream! {
                                    let mut change_rx = if let Some(change_rx) = core
                                        .subscribe_state_manager(account_id, account_id, types)
                                        .await
                                    {
                                        change_rx
                                    } else {
                                        return;
                                    };

                                    let mut last_message =
                                        Instant::now() - Duration::from_millis(throttle_ms);
                                    let mut timeout = Duration::from_millis(LONG_SLUMBER_MS);
                                    let mut response = WebSocketStateChange::new(None);

                                    loop {
                                        match tokio::time::timeout(timeout, change_rx.recv()).await
                                        {
                                            Ok(Some(state_change)) => {
                                                for (type_state, change_id) in state_change.types {
                                                    response
                                                        .changed
                                                        .get_mut_or_insert(state_change.account_id.into())
                                                        .set(type_state, change_id.into());
                                                }
                                            }
                                            Ok(None) => {
                                                debug!("Broadcast channel was closed.");
                                                break;
                                            }
                                            Err(_) => (),
                                        }

                                        timeout = if !response.changed.is_empty() {
                                            let elapsed = last_message.elapsed().as_millis() as u64;
                                            if elapsed >= throttle_ms {
                                                last_message = Instant::now();
                                                yield response;

                                                response = WebSocketStateChange::new(None);
                                                Duration::from_millis(LONG_SLUMBER_MS)
                                            } else {
                                                Duration::from_millis(throttle_ms - elapsed)
                                            }
                                        } else {
                                            Duration::from_millis(LONG_SLUMBER_MS)
                                        };
                                    }
                                }));

                                return;
                            }
                            WebSocketMessage::PushDisable(_) => {
                                if let Some(state_handle) = self.state_handle.take() {
                                    ctx.cancel_future(state_handle);
                                }
                                return;
                            }
                        },
                        Err(err) => {
                            debug!("Failed to parse request: {}", err);

                            WebSocketRequestError::from_error(RequestError::not_request(), None)
                        }
                    }
                } else {
                    WebSocketRequestError::from_error(
                        RequestError::limit(RequestLimitError::Size),
                        None,
                    )
                };

                ctx.text(serde_json::to_string(&error).unwrap_or_default());
            }
            Ok(ws::Message::Binary(_)) => (),
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            _ => ctx.stop(),
        }
    }
}

pub async fn handle_ws<T>(
    req: HttpRequest,
    stream: web::Payload,
    core: web::Data<JMAPServer<T>>,
    session: Session,
) -> actix_web::Result<HttpResponse>
where
    T: for<'x> Store<'x> + 'static,
{
    WsResponseBuilder::new(WebSocket::new(core, session), &req, stream)
        .protocols(&["jmap"])
        .start()
}

impl WebSocketRequestError {
    pub fn from_error(error: RequestError, request_id: Option<String>) -> Self {
        Self {
            type_: WebSocketRequestErrorType::RequestError,
            p_type: error.p_type,
            limit: error.limit,
            status: error.status,
            detail: error.detail,
            request_id,
        }
    }
}

impl WebSocketResponse {
    pub fn from_response(response: Response, request_id: Option<String>) -> Self {
        Self {
            _type: WebSocketResponseType::Response,
            method_responses: response.method_responses,
            session_state: response.session_state,
            created_ids: response.created_ids,
            request_id,
        }
    }
}

impl WebSocketStateChange {
    pub fn new(push_state: Option<String>) -> Self {
        WebSocketStateChange {
            type_: WebSocketStateChangeType::StateChange,
            changed: VecMap::new(),
            push_state,
        }
    }
}
