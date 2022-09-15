/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

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

                    Ok(HttpResponse::build(StatusCode::OK)
                        .insert_header(ContentType::json())
                        .json(result))
                } else {
                    Err(RequestError::limit(RequestLimitError::CallsIn))
                }
            }
            Err(err) => {
                debug!("Failed to parse request: {}", err);

                Err(RequestError::not_request())
            }
        }
    } else {
        Err(RequestError::limit(RequestLimitError::Size))
    }
}
