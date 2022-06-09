pub mod auth;
pub mod base;
pub mod rate_limit;

use std::sync::atomic::AtomicUsize;

use store::{parking_lot::Mutex, AccountId};

use self::rate_limit::RateLimiter;

pub struct Session {
    pub email: String,
    pub secret: String,

    pub primary_id: AccountId,
    pub member_ids: Vec<AccountId>,
    pub shared_ids: Vec<AccountId>,

    pub in_flight_requests: AtomicUsize,
}
