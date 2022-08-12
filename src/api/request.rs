use actix_web::{
    http::{header::ContentType, StatusCode},
    web, HttpResponse, ResponseError,
};
use jmap::types::jmap::JMAPId;
use store::{ahash::AHashMap, tracing::debug, Store};

use crate::{
    api::{invocation::handle_method_calls, Redirect, RequestError, RequestLimitError},
    authorization::Session,
    JMAPServer,
};

use super::method;

#[derive(Debug, serde::Deserialize)]
pub struct Request {
    pub using: Vec<String>,

    #[serde(rename = "methodCalls")]
    pub method_calls: Vec<method::Call<method::Request>>,

    #[serde(rename = "createdIds")]
    pub created_ids: Option<AHashMap<String, JMAPId>>,
}

pub async fn handle_jmap_request<T>(
    request: web::Bytes,
    core: web::Data<JMAPServer<T>>,
    session: Session,
) -> Result<HttpResponse, RequestError>
where
    T: for<'x> Store<'x> + 'static,
{
    if request.len() < core.store.config.max_size_request {
        println!(
            "{}",
            serde_json::to_string_pretty(
                &serde_json::from_slice::<serde_json::Value>(&request).unwrap()
            )
            .unwrap()
        );

        match serde_json::from_slice::<Request>(&request) {
            Ok(request) => {
                if request.method_calls.len() < core.store.config.max_calls_in_request {
                    // Make sure this node is still the leader
                    if !core.is_leader() {
                        // Redirect requests if at least one method requires write access
                        // or if this node is behind on the log.
                        let do_redirect = !core.is_up_to_date()
                            || request
                                .method_calls
                                .iter()
                                .any(|r| !r.method.is_read_only());

                        if do_redirect {
                            if let Some(leader_hostname) = core
                                .cluster
                                .as_ref()
                                .unwrap()
                                .leader_hostname
                                .lock()
                                .as_ref()
                            {
                                let redirect_uri = format!("{}/jmap", leader_hostname);
                                debug!("Redirecting JMAP request to '{}'", redirect_uri);

                                return Ok(Redirect::temporary(redirect_uri).error_response());
                            } else {
                                debug!("Rejecting request, no leader has been elected.");

                                return Err(RequestError::unavailable());
                            }
                        }
                    }

                    let result = handle_method_calls(request, core, session).await;
                    println!("{}", serde_json::to_string_pretty(&result).unwrap());

                    Ok(HttpResponse::build(StatusCode::OK)
                        .insert_header(ContentType::json())
                        .json(result))
                } else {
                    Err(RequestError::limit(RequestLimitError::CallsIn))
                }
            }
            Err(err) => {
                println!("Failed to parse request: {}", err);
                debug!("Failed to parse request: {}", err);

                Err(RequestError::not_request())
            }
        }
    } else {
        Err(RequestError::limit(RequestLimitError::Size))
    }
}
