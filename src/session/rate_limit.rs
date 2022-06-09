use std::{sync::Arc, time::Instant};

use store::{parking_lot::Mutex, Store};

use crate::JMAPServer;

use super::auth::RemoteAddress;

pub struct RateLimiter {
    max_requests: f64,
    max_interval: f64,
    remaining_requests: f64,
    last_request: Instant,
}

// Token bucket rate limiter
impl RateLimiter {
    pub fn new(max_requests: u64, max_interval: u64) -> Self {
        RateLimiter {
            max_requests: max_requests as f64,
            max_interval: max_interval as f64,
            remaining_requests: max_requests as f64,
            last_request: Instant::now(),
        }
    }

    pub fn is_allowed(&mut self) -> bool {
        let elapsed = self.last_request.elapsed().as_secs_f64();
        self.last_request = Instant::now();
        self.remaining_requests += elapsed * (self.max_requests / self.max_interval);
        if self.remaining_requests > self.max_requests {
            self.remaining_requests = self.max_requests;
        }
        if self.remaining_requests >= 1.0 {
            self.remaining_requests -= 1.0;
            true
        } else {
            false
        }
    }
}

impl<T> JMAPServer<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn is_allowed(&self, addr: RemoteAddress) -> bool {
        let is_authenticated = matches!(&addr, RemoteAddress::AccountId(_));
        self.rate_limiters
            .get_or_insert_with(addr, || {
                Arc::new(Mutex::new(if is_authenticated {
                    RateLimiter::new(
                        self.store.config.rate_limit_authenticated.0,
                        self.store.config.rate_limit_authenticated.1,
                    )
                } else {
                    RateLimiter::new(
                        self.store.config.rate_limit_anonymous.0,
                        self.store.config.rate_limit_anonymous.1,
                    )
                }))
            })
            .lock()
            .is_allowed()
    }
}
