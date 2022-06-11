use std::{
    fmt::Display,
    future::{ready, Ready},
    net::SocketAddr,
    sync::Arc,
};

use actix_web::{
    dev::{forward_ready, Payload, Service, ServiceRequest, ServiceResponse, Transform},
    http::header::{self},
    web, Error, FromRequest, HttpMessage, HttpRequest,
};
use futures::FutureExt;
use futures_util::future::LocalBoxFuture;
use jmap::{base64, principal::account::JMAPAccountStore, types::jmap::JMAPId};
use store::{
    tracing::{debug, error},
    AccountId, Store,
};

use crate::{api::RequestError, JMAPServer};

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
    SocketAddr(SocketAddr),
    IpAddress(String),
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
            let mut authorized = None;

            if let Some((mechanism, token)) = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|h| h.to_str().ok())
                .and_then(|h| h.split_once(' ').map(|(l, t)| (l, t.trim())))
            {
                if let Some(session) = core.sessions.get(&token.to_string()) {
                    authorized = session.into();
                } else if mechanism.eq_ignore_ascii_case("basic") {
                    // Before authenticating enforce rate limit for anonymous requests
                    core.is_anonymous_allowed(
                        req.remote_address(core.store.config.use_forwarded_header),
                    )
                    .await?;

                    // Decode the base64 encoded credentials
                    if let Some((login, secret)) = base64::decode(token)
                        .ok()
                        .and_then(|token| String::from_utf8(token).ok())
                        .and_then(|token| {
                            token.split_once(':').map(|(login, secret)| {
                                (login.trim().to_lowercase(), secret.to_string())
                            })
                        })
                    {
                        let store = core.store.clone();
                        match core
                            .spawn_worker(move || {
                                // Validate password
                                Ok(
                                    if let Some(account_id) = store.authenticate(&login, &secret)? {
                                        let member_of = store.get_member_accounts(account_id)?;
                                        Session::new(
                                            account_id,
                                            &member_of,
                                            &store.get_shared_accounts(&member_of)?,
                                        )
                                        .into()
                                    } else {
                                        None
                                    },
                                )
                            })
                            .await
                        {
                            Ok(Some(session)) => {
                                // Basic authentication successful, add token to session store
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
                    } else {
                        debug!("Failed to decode Basic auth request.",);
                    }
                }
            }

            if let Some(session) = authorized {
                let in_flight_request = core.is_account_allowed(session.account_id).await?;

                // Add session to request
                req.extensions_mut()
                    .insert::<(Session, InFlightRequest)>((session, in_flight_request));
            } else {
                core.is_anonymous_allowed(
                    req.remote_address(core.store.config.use_forwarded_header),
                )
                .await?
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
        if use_forwarded {
            self.connection_info()
                .realip_remote_addr()
                .map(|ip| RemoteAddress::IpAddress(ip.to_string()))
        } else {
            self.peer_addr().map(RemoteAddress::SocketAddr)
        }
        .unwrap_or_else(|| {
            debug!("Warning: No remote address found in request, using localhost.");
            RemoteAddress::IpAddress("127.0.0.1".to_string())
        })
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
            RemoteAddress::SocketAddr(addr) => write!(f, "from IP Address [{}]", addr),
            RemoteAddress::IpAddress(addr) => write!(f, "from IP Address [{}]", addr),
            RemoteAddress::AccountId(id) => write!(f, "for Account {}", JMAPId::from(*id)),
        }
    }
}
