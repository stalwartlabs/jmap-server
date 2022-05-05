use actix_web::{http::StatusCode, web, HttpResponse};
use async_stream::stream;
use jmap::{
    error::problem_details::ProblemDetails,
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::invocation::Object,
};
use std::{collections::HashMap, time::Duration};
use store::{
    core::collection::{Collection, Collections},
    tracing::debug,
    JMAPId, Store,
};
use tokio::time::{self};

use super::{
    server::JMAPServer,
    state_change::{subscribe_state_manager, StateChangeResponse},
};

#[derive(Debug, Copy, Clone, serde::Deserialize)]
pub enum CloseAfter {
    #[serde(rename(deserialize = "state"))]
    State,
    #[serde(rename(deserialize = "no"))]
    No,
}

#[derive(Debug, serde::Deserialize)]
#[allow(dead_code)]
pub struct Params {
    types: String,
    closeafter: CloseAfter,
    ping: u32,
}

struct Ping {
    interval: u64,
    payload: web::Bytes,
}

pub async fn handle_jmap_event_source<T>(
    params: web::Query<Params>,
    core: web::Data<JMAPServer<T>>,
) -> HttpResponse
where
    T: for<'x> Store<'x> + 'static,
{
    // Parse parameters
    let mut collections = Collections::default();
    for object_type in params.types.split(',') {
        if object_type == "*" {
            collections = Collections::all();
            break;
        } else if let Some(object) = Object::parse(object_type).and_then(|o| {
            let c: Collection = o.into();
            if c != Collection::None {
                Some(c)
            } else {
                None
            }
        }) {
            collections.insert(object);
        } else {
            return HttpResponse::build(StatusCode::BAD_REQUEST)
                .insert_header(("Content-Type", "application/problem+json"))
                .body(ProblemDetails::invalid_parameters().to_json());
        }
    }
    let mut ping = if params.ping > 0 {
        let interval = std::cmp::max(params.ping, 30);
        Ping {
            interval: interval as u64,
            payload: web::Bytes::from(format!(
                "event: ping\ndata: {{\"interval\": {}}}\n\n",
                interval
            )),
        }
        .into()
    } else {
        None
    };
    let _account_id = 1;
    let account_ids = vec![_account_id]; //TODO obtain from session, plus shared accounts
    let mut response = StateChangeResponse::new();
    let close_after_state = matches!(params.closeafter, CloseAfter::State);

    // Register with state manager
    let mut change_rx = if let Some(change_rx) =
        subscribe_state_manager(core, _account_id, account_ids, collections).await
    {
        change_rx
    } else {
        return HttpResponse::build(StatusCode::BAD_REQUEST)
            .insert_header(("Content-Type", "application/problem+json"))
            .body(ProblemDetails::internal_server_error().to_json());
    };

    HttpResponse::Ok()
        .insert_header(("Content-Type", "text/event-stream"))
        .insert_header(("Cache-Control", "no-store"))
        .streaming::<_, std::io::Error>(stream! {
            loop {
                if let Some(ping) = &mut ping {
                    match time::timeout( Duration::from_secs(ping.interval), change_rx.recv()).await {
                        Ok(Some(state_change)) => {
                            if state_change.is_some()
                            {
                                response
                                    .changed
                                    .entry((state_change.account_id as JMAPId).to_jmap_string())
                                    .or_insert_with(HashMap::new)
                                    .insert(
                                        state_change.collection.into(),
                                        JMAPState::from(state_change.id).to_jmap_string(),
                                    );
                            } else {
                                break;
                            }
                        }
                        Ok(None) => {
                            debug!("Broadcast channel was closed.");
                            break;
                        }
                        Err(_) => (),
                    }

                    if response.changed.is_empty() {
                        yield Ok(ping.payload.clone());
                        continue;
                    }
                } else if let Some(state_change) = change_rx.recv().await {
                    if state_change.is_some()
                    {
                        response
                            .changed
                            .entry((state_change.account_id as JMAPId).to_jmap_string())
                            .or_insert_with(HashMap::new)
                            .insert(
                                state_change.collection.into(),
                                JMAPState::from(state_change.id).to_jmap_string(),
                            );
                    } else {
                        break;
                    }
                } else {
                    debug!("Broadcast channel was closed.");
                    break;
                }

                yield Ok(web::Bytes::from(format!(
                    "event: state\ndata: {}\n\n",
                    serde_json::to_string(&response).unwrap()
                )));

                if close_after_state {
                    break;
                }
                response.changed.clear();
            }
        })
}
