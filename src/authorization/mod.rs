pub mod auth;
pub mod rate_limit;

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use store::{core::acl::ACLToken, AccountId};

#[derive(Debug, Clone)]
pub struct Session {
    account_id: AccountId,
    state: u32,
}

impl Session {
    pub fn new(account_id: AccountId, acl_token: &ACLToken) -> Self {
        // Hash state
        let mut s = DefaultHasher::new();
        acl_token.member_of.hash(&mut s);
        acl_token.access_to.hash(&mut s);

        Self {
            account_id,
            state: s.finish() as u32,
        }
    }

    pub fn account_id(&self) -> AccountId {
        self.account_id
    }

    pub fn state(&self) -> u32 {
        self.state
    }
}
