use actix::{Actor, ActorContext, AsyncContext, Handler, Message, StreamHandler};
use actix_web::{web, HttpRequest, HttpResponse};
use actix_web_actors::ws::{self, WsResponseBuilder};
use jmap::error::request::{RequestError, RequestErrorType, RequestLimitError};
use jmap::id::state::JMAPState;
use jmap::id::JMAPIdSerialize;
use jmap::protocol::request::Request;
use jmap::protocol::response::{serialize_hex, Response};
use jmap::protocol::{invocation::Object, json::JSONValue};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use store::core::collection::Collections;
use store::tracing::log::debug;
use store::{JMAPId, Store};

use crate::api::invocation::handle_method_calls;
use crate::state::{LONG_SLUMBER_MS, THROTTLE_MS};
use crate::JMAPServer;

const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const CLIENT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
struct WebSocketRequest {
    #[serde(rename = "@type")]
    pub _type: WebSocketRequestType,
    pub id: Option<String>,
    pub using: Vec<String>,
    #[serde(rename = "methodCalls")]
    pub method_calls: Vec<(String, JSONValue, String)>,
    #[serde(rename = "createdIds")]
    pub created_ids: Option<HashMap<String, String>>,
}

#[derive(Message, Debug, PartialEq, Eq, serde::Serialize)]
#[rtype(result = "()")]
pub struct WebSocketResponse {
    #[serde(rename = "@type")]
    _type: WebSocketResponseType,

    #[serde(rename = "methodResponses")]
    method_responses: Vec<(String, JSONValue, String)>,

    #[serde(rename = "sessionState")]
    #[serde(serialize_with = "serialize_hex")]
    session_state: u64,

    #[serde(rename(deserialize = "createdIds"))]
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    created_ids: HashMap<String, String>,

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
    data_types: Option<Vec<Object>>,
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

#[derive(Debug, PartialEq, Eq, serde::Deserialize)]
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
    pub changed: HashMap<String, HashMap<Object, String>>,
    #[serde(rename = "pushState")]
    #[serde(skip_serializing_if = "Option::is_none")]
    push_state: Option<String>,
}

#[derive(Debug, serde::Serialize)]
pub struct WebSocketRequestError {
    #[serde(rename = "@type")]
    pub type_: WebSocketRequestErrorType,

    #[serde(rename(serialize = "type"))]
    error: RequestErrorType,

    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<RequestLimitError>,
    status: u32,
    detail: String,

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
    core: web::Data<JMAPServer<T>>,
    state_handle: Option<actix::SpawnHandle>,
    hb: Instant,
}

impl<T> WebSocket<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn new(core: web::Data<JMAPServer<T>>) -> Self {
        Self {
            hb: Instant::now(),
            core,
            state_handle: None,
        }
    }

    fn hb(&self, ctx: &mut <Self as Actor>::Context) {
        ctx.run_interval(HEARTBEAT_INTERVAL, |act, ctx| {
            if Instant::now().duration_since(act.hb) > CLIENT_TIMEOUT {
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
        /*println!(
            "Send response: {}",
            serde_json::to_string_pretty(&msg).unwrap_or_default()
        );*/
        ctx.text(serde_json::to_string(&msg).unwrap_or_default());
    }
}

impl<T> StreamHandler<WebSocketStateChange> for WebSocket<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn handle(&mut self, item: WebSocketStateChange, ctx: &mut Self::Context) {
        /*println!(
            "Send state: {}",
            serde_json::to_string_pretty(&item).unwrap_or_default()
        );*/
        ctx.text(serde_json::to_string(&item).unwrap_or_default());
    }
}

impl<T> StreamHandler<Result<ws::Message, ws::ProtocolError>> for WebSocket<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        //println!("WS: {:?}", msg);
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
                                    //TODO limit concurrent calls
                                    tokio::spawn(async move {
                                        addr.do_send(WebSocketResponse::from_response(
                                            handle_method_calls(
                                                Request {
                                                    using: request.using,
                                                    method_calls: request.method_calls,
                                                    created_ids: request.created_ids,
                                                },
                                                core,
                                            )
                                            .await,
                                            request.id,
                                        ));
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
                                let _account_id = 1; //TODO obtain from session, plus shared accounts + device ids limit
                                let collections = if let Some(data_types) = request.data_types {
                                    if !data_types.is_empty() {
                                        let mut collections = Collections::default();
                                        data_types.into_iter().for_each(|data_type| {
                                            collections.insert(data_type.into());
                                        });
                                        collections
                                    } else {
                                        Collections::all()
                                    }
                                } else {
                                    Collections::all()
                                };

                                self.state_handle = Some(ctx.add_stream(async_stream::stream! {
                                    let mut change_rx = if let Some(change_rx) = core
                                        .subscribe_state_manager(
                                            _account_id,
                                            _account_id,
                                            collections,
                                        )
                                        .await
                                    {
                                        change_rx
                                    } else {
                                        return;
                                    };

                                    let mut last_message =
                                        Instant::now() - Duration::from_millis(THROTTLE_MS);
                                    let mut timeout = Duration::from_millis(LONG_SLUMBER_MS);
                                    let mut response = WebSocketStateChange::new(None);

                                    loop {
                                        match tokio::time::timeout(timeout, change_rx.recv()).await
                                        {
                                            Ok(Some(state_change)) => {
                                                response
                                                    .changed
                                                    .entry(
                                                        (state_change.account_id as JMAPId)
                                                            .to_jmap_string(),
                                                    )
                                                    .or_insert_with(HashMap::new)
                                                    .insert(
                                                        state_change.collection.into(),
                                                        JMAPState::from(state_change.id)
                                                            .to_jmap_string(),
                                                    );
                                            }
                                            Ok(None) => {
                                                debug!("Broadcast channel was closed.");
                                                break;
                                            }
                                            Err(_) => (),
                                        }

                                        timeout = if !response.changed.is_empty() {
                                            let elapsed = last_message.elapsed().as_millis() as u64;
                                            if elapsed >= THROTTLE_MS {
                                                last_message = Instant::now();
                                                yield response;

                                                response = WebSocketStateChange::new(None);
                                                Duration::from_millis(LONG_SLUMBER_MS)
                                            } else {
                                                Duration::from_millis(THROTTLE_MS - elapsed)
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
) -> actix_web::Result<HttpResponse>
where
    T: for<'x> Store<'x> + 'static,
{
    WsResponseBuilder::new(WebSocket::new(core), &req, stream)
        .protocols(&["jmap"])
        .start()
}

impl WebSocketRequestError {
    pub fn from_error(error: RequestError, request_id: Option<String>) -> Self {
        Self {
            type_: WebSocketRequestErrorType::RequestError,
            error: error.error,
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
            changed: HashMap::new(),
            push_state,
        }
    }
}
