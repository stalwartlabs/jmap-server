use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use jmap::SUPERUSER_ID;
use store::{parking_lot::Mutex, AccountId, Store};

use crate::{
    api::{RequestError, RequestLimitError},
    JMAPServer,
};

use super::auth::RemoteAddress;

#[derive(Debug)]
pub struct RateLimiter {
    max_requests: f64,
    max_interval: f64,
    limiter: Arc<Mutex<(Instant, f64)>>,
}

#[derive(Debug)]
pub struct ConcurrencyLimiter {
    concurrent: Arc<AtomicUsize>,
}

pub struct Limiter {
    request_limiter: RateLimiter,
    ltype: LimiterType,
}

pub enum LimiterType {
    Anonymous {
        auth_limiter: RateLimiter,
    },
    Authenticated {
        concurrent_request: ConcurrencyLimiter,
        concurrent_uploads: ConcurrencyLimiter,
    },
}

pub struct InFlightRequest {
    concurrent_requests: Arc<AtomicUsize>,
}

impl Drop for InFlightRequest {
    fn drop(&mut self) {
        self.concurrent_requests.fetch_sub(1, Ordering::Relaxed);
    }
}

impl RateLimiter {
    pub fn new(max_requests: u64, max_interval: u64) -> Self {
        RateLimiter {
            max_requests: max_requests as f64,
            max_interval: max_interval as f64,
            limiter: Arc::new(Mutex::new((Instant::now(), max_requests as f64))),
        }
    }

    // Token bucket rate limiter
    pub fn is_allowed(&self) -> bool {
        let mut limiter = self.limiter.lock();
        let elapsed = limiter.0.elapsed().as_secs_f64();
        limiter.0 = Instant::now();
        limiter.1 += elapsed * (self.max_requests / self.max_interval);
        if limiter.1 > self.max_requests {
            limiter.1 = self.max_requests;
        }
        if limiter.1 >= 1.0 {
            limiter.1 -= 1.0;
            true
        } else {
            false
        }
    }

    pub fn reset(&self) {
        *self.limiter.lock() = (Instant::now(), self.max_requests);
    }
}

impl ConcurrencyLimiter {
    pub fn new(concurrent: usize) -> Self {
        ConcurrencyLimiter {
            concurrent: Arc::new(AtomicUsize::new(concurrent)),
        }
    }

    pub fn is_allowed(&self, limit: usize) -> Option<InFlightRequest> {
        if self.concurrent.load(Ordering::Relaxed) < limit {
            self.concurrent.fetch_add(1, Ordering::Relaxed);
            Some(InFlightRequest {
                concurrent_requests: self.concurrent.clone(),
            })
        } else {
            None
        }
    }
}

impl Limiter {
    pub fn new_anonymous(
        max_requests: u64,
        max_interval: u64,
        max_auth_requests: u64,
        max_auth_interval: u64,
    ) -> Self {
        Limiter {
            request_limiter: RateLimiter::new(max_requests, max_interval),
            ltype: LimiterType::Anonymous {
                auth_limiter: RateLimiter::new(max_auth_requests, max_auth_interval),
            },
        }
    }

    pub fn new_authenticated(max_requests: u64, max_interval: u64) -> Self {
        Limiter {
            request_limiter: RateLimiter::new(max_requests, max_interval),
            ltype: LimiterType::Authenticated {
                concurrent_request: ConcurrencyLimiter::new(0),
                concurrent_uploads: ConcurrencyLimiter::new(0),
            },
        }
    }

    pub fn is_rate_allowed(&self) -> bool {
        self.request_limiter.is_allowed()
    }

    pub fn is_auth_allowed(&self) -> bool {
        match &self.ltype {
            LimiterType::Anonymous { auth_limiter } => auth_limiter.is_allowed(),
            LimiterType::Authenticated { .. } => true,
        }
    }

    pub fn is_request_allowed(&self, max_requests: usize) -> Option<InFlightRequest> {
        match &self.ltype {
            LimiterType::Authenticated {
                concurrent_request, ..
            } => concurrent_request.is_allowed(max_requests),
            _ => None,
        }
    }

    pub fn is_upload_allowed(&self, max_requests: usize) -> Option<InFlightRequest> {
        match &self.ltype {
            LimiterType::Authenticated {
                concurrent_uploads, ..
            } => concurrent_uploads.is_allowed(max_requests),
            _ => None,
        }
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub async fn is_account_allowed(
        &self,
        account_id: AccountId,
    ) -> Result<InFlightRequest, RequestError> {
        if account_id != SUPERUSER_ID {
            let limiter = self
                .rate_limiters
                .get_with(RemoteAddress::AccountId(account_id), async {
                    Arc::new(Limiter::new_authenticated(
                        self.store.config.rate_limit_authenticated.0,
                        self.store.config.rate_limit_authenticated.1,
                    ))
                })
                .await;

            if limiter.is_rate_allowed() {
                if let Some(in_flight_request) =
                    limiter.is_request_allowed(self.store.config.max_concurrent_requests)
                {
                    Ok(in_flight_request)
                } else {
                    Err(RequestError::limit(RequestLimitError::Concurrent))
                }
            } else {
                Err(RequestError::too_many_requests())
            }
        } else {
            Ok(InFlightRequest {
                concurrent_requests: Arc::new(0.into()),
            })
        }
    }

    pub async fn is_anonymous_allowed(&self, addr: RemoteAddress) -> Result<(), RequestError> {
        if self
            .rate_limiters
            .get_with(addr, async {
                Arc::new(Limiter::new_anonymous(
                    self.store.config.rate_limit_anonymous.0,
                    self.store.config.rate_limit_anonymous.1,
                    self.store.config.rate_limit_auth.0,
                    self.store.config.rate_limit_auth.1,
                ))
            })
            .await
            .is_rate_allowed()
        {
            Ok(())
        } else {
            Err(RequestError::too_many_requests())
        }
    }

    pub async fn is_auth_allowed(&self, addr: RemoteAddress) -> Result<(), RequestError> {
        if self
            .rate_limiters
            .get_with(addr, async {
                Arc::new(Limiter::new_anonymous(
                    self.store.config.rate_limit_anonymous.0,
                    self.store.config.rate_limit_anonymous.1,
                    self.store.config.rate_limit_auth.0,
                    self.store.config.rate_limit_auth.1,
                ))
            })
            .await
            .is_auth_allowed()
        {
            Ok(())
        } else {
            Err(RequestError::too_many_auth_attempts())
        }
    }
}
