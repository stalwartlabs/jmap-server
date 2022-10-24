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

use std::{
    fmt::Display,
    future::{ready, Ready},
    net::{IpAddr, Ipv4Addr},
    sync::Arc,
};

use actix_web::{
    dev::{forward_ready, Payload, Service, ServiceRequest, ServiceResponse, Transform},
    http::header::{self},
    web, Error, FromRequest, HttpMessage, HttpRequest,
};
use futures::FutureExt;
use futures_util::future::LocalBoxFuture;
use jmap::types::jmap::JMAPId;
use jmap_mail::mail_parser::decoders::base64::decode_base64;
use jmap_sharing::principal::account::JMAPAccountStore;
use store::{
    core::error::StoreError,
    tracing::{debug, error, warn},
    AccountId, Store,
};

use crate::{
    api::{Redirect, RequestError},
    JMAPServer,
};

use super::{rate_limit::InFlightRequest, Session};

pub struct SessionMiddleware<S, T>
where
    T: for<'x> Store<'x> + 'static,
{
    core: web::Data<JMAPServer<T>>,
    service: Arc<S>,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub enum RemoteAddress {
    IpAddress(IpAddr),
    IpAddressFwd(String),
    AccountId(AccountId),
}

impl<S, B, T> Service<ServiceRequest> for SessionMiddleware<S, T>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    T: for<'x> Store<'x> + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Future = LocalBoxFuture<'static, Result<Self::Response, Self::Error>>;

    forward_ready!(service);

    fn call(&self, req: ServiceRequest) -> Self::Future {
        let core = self.core.clone();
        let service = self.service.clone();

        async move {
            // Redirect request if this node is not the leader.
            if !core.is_leader() {
                // Obtain path
                let request_path = req
                    .uri()
                    .path_and_query()
                    .map(|pq| pq.as_str())
                    .unwrap_or("");

                // Check whether a redirect is needed
                let do_redirect = !core.is_up_to_date()
                    || request_path.starts_with("/jmap/upload")
                    || request_path.starts_with("/jmap/ws")
                    || request_path.starts_with("/jmap/eventsource")
                    || request_path.starts_with("/auth")
                    || request_path.starts_with("/.well-known/oauth-authorization-server");

                // Redirect requests to /jmap are evaluated after parsing
                if do_redirect {
                    let cluster = core.cluster.as_ref().unwrap();
                    if let Some(leader_hostname) = cluster.leader_hostname.lock().as_ref() {
                        let redirect_uri = format!("{}{}", leader_hostname, request_path);
                        debug!(
                            "Redirecting '{}{}' to '{}'",
                            core.base_session.api_url().split_once("/jmap").unwrap().0,
                            request_path,
                            redirect_uri
                        );

                        return Err(Redirect::temporary(redirect_uri).into());
                    } else {
                        debug!("Rejecting request, no leader has been elected.");

                        return Err(RequestError::unavailable().into());
                    }
                }
            }

            let mut authorized = None;

            if let Some((mechanism, token)) = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|h| h.to_str().ok())
                .and_then(|h| h.split_once(' ').map(|(l, t)| (l, t.trim())))
            {
                if let Some(session) = core.sessions.get(&token.to_string()) {
                    authorized = session.into();
                } else {
                    let session = if mechanism.eq_ignore_ascii_case("basic") {
                        // Enforce rate limit for authentication requests
                        core.is_auth_allowed(
                            req.remote_address(core.store.config.use_forwarded_header),
                        )
                        .await?;

                        // Decode the base64 encoded credentials
                        if let Some((login, secret)) = decode_base64(token.as_bytes())
                            .and_then(|token| String::from_utf8(token).ok())
                            .and_then(|token| {
                                token.split_once(':').map(|(login, secret)| {
                                    (login.trim().to_lowercase(), secret.to_string())
                                })
                            })
                        {
                            let store = core.store.clone();
                            core.spawn_worker(move || {
                                // Validate password
                                Ok(
                                    if let Some(account_id) = store.authenticate(&login, &secret)? {
                                        Session::new(
                                            account_id,
                                            store.get_acl_token(account_id)?.as_ref(),
                                        )
                                        .into()
                                    } else {
                                        None
                                    },
                                )
                            })
                            .await
                        } else {
                            debug!("Failed to decode Basic auth request.",);
                            Ok(None)
                        }
                    } else if mechanism.eq_ignore_ascii_case("bearer") {
                        // Enforce anonymous rate limit for bearer auth requests
                        core.is_anonymous_allowed(
                            req.remote_address(core.store.config.use_forwarded_header),
                        )
                        .await?;

                        // Validate OAuth bearer token
                        match core.validate_access_token("access_token", token).await {
                            Ok((account_id, _, _)) => {
                                let store = core.store.clone();
                                core.spawn_worker(move || {
                                    Ok(Session::new(
                                        account_id,
                                        store.get_acl_token(account_id)?.as_ref(),
                                    )
                                    .into())
                                })
                                .await
                            }
                            Err(StoreError::DeserializeError(e)) => {
                                debug!("Failed to deserialize access token: {}", e);
                                Ok(None)
                            }
                            Err(err) => Err(err),
                        }
                    } else {
                        // Enforce anonymous rate limit
                        core.is_anonymous_allowed(
                            req.remote_address(core.store.config.use_forwarded_header),
                        )
                        .await?;

                        Ok(None)
                    };

                    match session {
                        Ok(Some(session)) => {
                            // Authentication successful, add token to session store
                            core.sessions
                                .insert(token.to_string(), session.clone())
                                .await;
                            authorized = session.into();
                        }
                        Ok(None) => {
                            return service.call(req).await;
                        }
                        Err(err) => {
                            error!("Store error during authentication: {}", err);
                            return Err(RequestError::internal_server_error().into());
                        }
                    }
                }
            }

            if let Some(session) = authorized {
                let in_flight_request = core.is_account_allowed(session.account_id).await?;

                // Add session to request
                req.extensions_mut()
                    .insert::<(Session, InFlightRequest)>((session, in_flight_request));
            } else {
                let request_path = req
                    .uri()
                    .path_and_query()
                    .map(|pq| pq.as_str())
                    .unwrap_or("");
                if request_path == "/auth" || request_path == "/auth/code" {
                    // OAuth authentication endpoints
                    core.is_auth_allowed(req.remote_address(core.store.config.use_forwarded_header))
                        .await?
                } else {
                    core.is_anonymous_allowed(
                        req.remote_address(core.store.config.use_forwarded_header),
                    )
                    .await?
                }
            }

            service.call(req).await
        }
        .boxed_local()
    }
}

trait ServiceRequestAddr {
    fn remote_address(&self, use_forwarded: bool) -> RemoteAddress;
}

impl ServiceRequestAddr for ServiceRequest {
    fn remote_address(&self, use_forwarded: bool) -> RemoteAddress {
        let peer_addr = self
            .peer_addr()
            .map(|addr| addr.ip())
            .unwrap_or_else(|| Ipv4Addr::new(127, 0, 0, 1).into());

        if use_forwarded || peer_addr.is_loopback() {
            self.connection_info()
                .realip_remote_addr()
                .map(|ip| RemoteAddress::IpAddressFwd(ip.to_string()))
                .unwrap_or_else(|| {
                    warn!("Warning: No remote address found in request, using loopback.");
                    RemoteAddress::IpAddress(peer_addr)
                })
        } else {
            RemoteAddress::IpAddress(peer_addr)
        }
    }
}

pub struct SessionFactory<T>
where
    T: for<'x> Store<'x> + 'static,
{
    core: web::Data<JMAPServer<T>>,
}

impl<T> SessionFactory<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn new(core: web::Data<JMAPServer<T>>) -> Self {
        SessionFactory { core }
    }
}

impl<S, B, T> Transform<S, ServiceRequest> for SessionFactory<T>
where
    S: Service<ServiceRequest, Response = ServiceResponse<B>, Error = Error> + 'static,
    T: for<'x> Store<'x> + 'static,
{
    type Response = ServiceResponse<B>;
    type Error = Error;
    type Transform = SessionMiddleware<S, T>;
    type InitError = ();
    type Future = Ready<Result<Self::Transform, Self::InitError>>;

    fn new_transform(&self, service: S) -> Self::Future {
        ready(Ok(SessionMiddleware {
            core: self.core.clone(),
            service: service.into(),
        }))
    }
}

impl FromRequest for Session {
    type Error = RequestError;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        ready(match req.extensions().get::<(Session, InFlightRequest)>() {
            Some((session, _)) => Ok(session.clone()),
            None => Err(RequestError::unauthorized()),
        })
    }
}

impl Display for RemoteAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RemoteAddress::IpAddressFwd(addr) => write!(f, "from IP Address [{}]", addr),
            RemoteAddress::IpAddress(addr) => write!(f, "from IP Address [{}]", addr),
            RemoteAddress::AccountId(id) => write!(f, "for Account {}", JMAPId::from(*id)),
        }
    }
}
