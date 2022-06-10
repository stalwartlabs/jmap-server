pub mod auth;
pub mod base;
pub mod rate_limit;

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use jmap::SUPERUSER_ID;
use store::AccountId;

use crate::api::ProblemDetails;

pub struct Session {
    email: String,

    primary_id: AccountId,
    member_of: Arc<Vec<AccountId>>,
    access_to: Vec<AccountId>,

    state: u32,
    in_flight_requests: AtomicUsize,
}

pub struct InFlightRequest<'x> {
    session: &'x Session,
}

impl Drop for InFlightRequest<'_> {
    fn drop(&mut self) {
        self.session
            .in_flight_requests
            .fetch_sub(1, Ordering::Relaxed);
    }
}

impl Session {
    pub fn new(
        email: String,
        primary_id: AccountId,
        member_of: Vec<AccountId>,
        access_to: Vec<AccountId>,
    ) -> Self {
        // Hash state
        let mut s = DefaultHasher::new();
        email.hash(&mut s);
        member_of.hash(&mut s);
        access_to.hash(&mut s);

        Self {
            email,
            primary_id,
            member_of: member_of.into(),
            access_to,
            state: s.finish() as u32,
            in_flight_requests: 0.into(),
        }
    }

    pub fn primary_id(&self) -> AccountId {
        self.primary_id
    }

    pub fn email(&self) -> &str {
        &self.email
    }

    pub fn member_of(&self) -> &[AccountId] {
        &self.member_of
    }

    pub fn access_to(&self) -> &[AccountId] {
        &self.access_to
    }

    pub fn clone_member_of(&self) -> Arc<Vec<AccountId>> {
        self.member_of.clone()
    }

    pub fn is_owner(&self, account_id: AccountId) -> bool {
        self.primary_id == account_id
            || self.member_of.contains(&account_id)
            || self.primary_id == SUPERUSER_ID
    }

    pub fn is_shared(&self, account_id: AccountId) -> bool {
        self.access_to.contains(&account_id)
    }

    pub fn assert_is_owner(&self, account_id: AccountId) -> Result<(), ProblemDetails> {
        if self.is_owner(account_id) {
            Ok(())
        } else {
            Err(ProblemDetails::forbidden())
        }
    }

    pub fn assert_concurrent_requests(
        &self,
        max_requests: usize,
    ) -> Result<InFlightRequest, ProblemDetails> {
        if self.in_flight_requests.load(Ordering::Relaxed) < max_requests {
            self.in_flight_requests.fetch_add(1, Ordering::Relaxed);
            Ok(InFlightRequest { session: self })
        } else {
            Err(ProblemDetails::too_many_requests())
        }
    }

    pub fn state(&self) -> u32 {
        self.state
    }
}
