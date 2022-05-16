use actix_web::{http::StatusCode, web, HttpResponse};
use async_stream::stream;
use jmap::{
    error::problem_details::ProblemDetails,
    id::{state::JMAPState, JMAPIdSerialize},
    protocol::invocation::Object,
};
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use store::{
    core::collection::{Collection, Collections},
    tracing::debug,
    JMAPId, Store,
};
use tokio::time::{self};

use crate::JMAPServer;

use super::StateChangeResponse;

#[cfg(test)]
const THROTTLE_MS: u64 = 500;

#[cfg(not(test))]
const THROTTLE_MS: u64 = 1000;
const LONG_SLUMBER_MS: u64 = 60 * 60 * 24 * 1000;

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
    last_ping: Instant,
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
        #[cfg(not(test))]
        let interval = std::cmp::max(params.ping, 30);
        #[cfg(test)]
        let interval = params.ping * 1000;

        Ping {
            interval: interval as u64,
            last_ping: Instant::now() - Duration::from_millis(interval as u64),
            payload: web::Bytes::from(format!(
                "event: ping\ndata: {{\"interval\": {}}}\n\n",
                interval
            )),
        }
        .into()
    } else {
        None
    };
    let _account_id = 1; //TODO obtain from session, plus shared accounts + device ids limit
    let mut response = StateChangeResponse::new();
    let close_after_state = matches!(params.closeafter, CloseAfter::State);

    // Register with state manager
    let mut change_rx = if let Some(change_rx) = core
        .subscribe_state_manager(_account_id, _account_id, collections)
        .await
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
            let mut last_message = Instant::now() - Duration::from_millis(THROTTLE_MS);
            let mut timeout = Duration::from_millis(LONG_SLUMBER_MS);

            loop {
                match time::timeout(timeout, change_rx.recv()).await {
                    Ok(Some(state_change)) => {
                        response
                            .changed
                            .entry((state_change.account_id as JMAPId).to_jmap_string())
                            .or_insert_with(HashMap::new)
                            .insert(
                                state_change.collection.into(),
                                JMAPState::from(state_change.id).to_jmap_string(),
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
                        yield Ok(web::Bytes::from(format!(
                            "event: state\ndata: {}\n\n",
                            serde_json::to_string(&response).unwrap()
                        )));

                        if close_after_state {
                            break;
                        }

                        response.changed.clear();
                        Duration::from_millis(
                            ping.as_ref().map(|p| p.interval).unwrap_or(LONG_SLUMBER_MS),
                        )
                    } else {
                        Duration::from_millis(THROTTLE_MS - elapsed)
                    }
                } else if let Some(ping) = &mut ping {
                    let elapsed = ping.last_ping.elapsed().as_millis() as u64;
                    if elapsed >= ping.interval {
                        ping.last_ping = Instant::now();
                        yield Ok(ping.payload.clone());
                        Duration::from_millis(ping.interval)
                    } else {
                        Duration::from_millis(ping.interval - elapsed)
                    }
                } else {
                    Duration::from_millis(
                        ping.as_ref().map(|p| p.interval).unwrap_or(LONG_SLUMBER_MS),
                    )
                };
            }
        })
}
