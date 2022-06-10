use std::{
    fmt::Display,
    future::{ready, Ready},
    net::SocketAddr,
    sync::Arc,
};

use actix_web::{
    dev::{forward_ready, Payload, Service, ServiceRequest, ServiceResponse, Transform},
    error,
    http::{
        header::{self, ContentType},
        StatusCode,
    },
    web, Error, FromRequest, HttpMessage, HttpRequest, HttpResponse,
};
use futures::FutureExt;
use futures_util::future::LocalBoxFuture;
use jmap::{base64, types::jmap::JMAPId};
use store::{
    tracing::{debug, error, warn},
    AccountId, Store,
};

use crate::{api::ProblemDetails, JMAPServer};

use super::{base::JMAPSessionStore, InFlightRequest, Session};

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
            let mut authenticated_id = None;

            if let Some((mechanism, token)) = req
                .headers()
                .get(header::AUTHORIZATION)
                .and_then(|h| h.to_str().ok())
                .and_then(|h| h.split_once(' ').map(|(l, t)| (l, t.trim())))
            {
                if let Some(account_id) = core.session_tokens.get(&token.to_string()) {
                    // Enforce rate limit for an authenticated user
                    if core.is_allowed(RemoteAddress::AccountId(account_id)).await {
                        authenticated_id = Some(account_id);
                    } else {
                        warn!(
                            "Rate limited request {}.",
                            RemoteAddress::AccountId(account_id)
                        );
                        return Err(ProblemDetails::too_many_requests().into());
                    }
                } else if mechanism.eq_ignore_ascii_case("basic") {
                    // Before authenticating enforce rate limit for an anonymous request
                    if core
                        .is_allowed(req.remote_address(core.store.config.use_forwarded_header))
                        .await
                    {
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
                                    // Map login to account_id
                                    if let Some(account_id) = store.find_account(login.clone())? {
                                        // Validate password
                                        if store.auth(account_id, &login, &secret)? {
                                            return Ok(Some(account_id));
                                        }
                                    } else {
                                        debug!("Authentication failed: Login {} not found.", login);
                                    }

                                    Ok(None)
                                })
                                .await
                            {
                                Ok(Some(account_id)) => {
                                    // Basic authentication successful, add token to session store
                                    core.session_tokens.insert(token.to_string(), account_id);
                                    authenticated_id = Some(account_id);
                                }
                                Ok(None) => {
                                    return service.call(req).await;
                                }
                                Err(err) => {
                                    error!("Store error during authentication: {}", err);
                                    return Err(ProblemDetails::internal_server_error().into());
                                }
                            }
                        } else {
                            debug!(concat!(
                                "Authentication failed: ",
                                "Failed to parse Basic Authentication header."
                            ));
                            return service.call(req).await;
                        }
                    } else {
                        warn!(
                            "Rate limited request {}.",
                            req.remote_address(core.store.config.use_forwarded_header)
                        );
                        return Err(ProblemDetails::too_many_requests().into());
                    }
                }
            }

            if let Some(authenticated_id) = authenticated_id {
                let session = if let Some(session) = core.sessions.get(&authenticated_id) {
                    session
                } else {
                    let store = core.store.clone();
                    match core
                        .spawn_worker(move || store.build_session(authenticated_id))
                        .await
                    {
                        Ok(Some(session)) => {
                            let session = Arc::new(session);
                            core.sessions.insert(authenticated_id, session.clone());
                            session
                        }
                        Ok(None) => {
                            error!(
                                "Failed to build session for account {}",
                                JMAPId::from(authenticated_id)
                            );
                            return service.call(req).await;
                        }
                        Err(err) => {
                            error!("Store error while building session: {}", err);
                            return Err(ProblemDetails::internal_server_error().into());
                        }
                    }
                };

                // Add session to request
                req.extensions_mut().insert::<Arc<Session>>(session);
            } else if !core
                .is_allowed(req.remote_address(core.store.config.use_forwarded_header))
                .await
            {
                warn!(
                    "Rate limited request {}.",
                    req.remote_address(core.store.config.use_forwarded_header)
                );
                return Err(ProblemDetails::too_many_requests().into());
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

pub struct Authorized(Arc<Session>);

impl FromRequest for Authorized {
    type Error = ProblemDetails;
    type Future = Ready<Result<Self, Self::Error>>;

    fn from_request(req: &HttpRequest, _payload: &mut Payload) -> Self::Future {
        ready(match req.extensions().get::<Arc<Session>>() {
            Some(session) => Ok(Authorized(session.clone())),
            None => Err(ProblemDetails::unauthorized()),
        })
    }
}

impl std::ops::Deref for Authorized {
    type Target = Arc<Session>;

    fn deref(&self) -> &Self::Target {
        &self.0
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
