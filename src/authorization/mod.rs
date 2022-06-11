pub mod auth;
pub mod rate_limit;

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use store::AccountId;

#[derive(Debug, Clone)]
pub struct Session {
    account_id: AccountId,
    state: u32,
}

impl Session {
    pub fn new(account_id: AccountId, member_of: &[AccountId], access_to: &[AccountId]) -> Self {
        // Hash state
        let mut s = DefaultHasher::new();
        member_of.hash(&mut s);
        access_to.hash(&mut s);

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
