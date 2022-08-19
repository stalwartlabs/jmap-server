use actix_web::{web, HttpResponse};
use async_stream::stream;
use jmap::types::type_state::TypeState;
use std::time::{Duration, Instant};
use store::{core::bitmap::Bitmap, tracing::debug, Store};
use tokio::time::{self};

use crate::{
    api::{RequestError, StateChangeResponse},
    authorization::Session,
    services::LONG_SLUMBER_MS,
    JMAPServer,
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
    last_ping: Instant,
    payload: web::Bytes,
}

pub async fn handle_jmap_event_source<T>(
    params: web::Query<Params>,
    core: web::Data<JMAPServer<T>>,
    session: Session,
) -> Result<HttpResponse, RequestError>
where
    T: for<'x> Store<'x> + 'static,
{
    // Parse parameters
    let mut types = Bitmap::default();
    for type_state in params.types.split(',') {
        if type_state == "*" {
            types = Bitmap::all();
            break;
        } else {
            let t = TypeState::parse(type_state);
            if !matches!(t, TypeState::None) {
                types.insert(t);
            } else {
                return Err(RequestError::invalid_parameters());
            }
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
    let mut response = StateChangeResponse::new();
    let close_after_state = matches!(params.closeafter, CloseAfter::State);
    let throttle_ms = core.store.config.event_source_throttle;

    // Register with state manager
    let mut change_rx = if let Some(change_rx) = core
        .subscribe_state_manager(session.account_id(), session.account_id(), types)
        .await
    {
        change_rx
    } else {
        return Err(RequestError::internal_server_error());
    };

    Ok(HttpResponse::Ok()
        .insert_header(("Content-Type", "text/event-stream"))
        .insert_header(("Cache-Control", "no-store"))
        .streaming::<_, std::io::Error>(stream! {
            let mut last_message = Instant::now() - Duration::from_millis(throttle_ms);
            let mut timeout =
                Duration::from_millis(ping.as_ref().map(|p| p.interval).unwrap_or(LONG_SLUMBER_MS));

            loop {
                match time::timeout(timeout, change_rx.recv()).await {
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
                        Duration::from_millis(throttle_ms - elapsed)
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
                    Duration::from_millis(LONG_SLUMBER_MS)
                };
            }
        }))
}
