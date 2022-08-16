pub mod auth;
pub mod oauth;
pub mod rate_limit;

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use aes_gcm_siv::{
    aead::{generic_array::GenericArray, Aead},
    AeadInPlace, Aes256GcmSiv, KeyInit, Nonce,
};
use store::{blake3, core::acl::ACLToken, AccountId};

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

pub struct SymmetricEncrypt {
    aes: Aes256GcmSiv,
}

impl SymmetricEncrypt {
    pub const ENCRYPT_TAG_LEN: usize = 16;
    pub const NONCE_LEN: usize = 12;

    pub fn new(key: &[u8], context: &str) -> Self {
        SymmetricEncrypt {
            aes: Aes256GcmSiv::new(&GenericArray::clone_from_slice(
                &blake3::derive_key(context, key)[..],
            )),
        }
    }

    #[allow(clippy::ptr_arg)]
    pub fn encrypt_in_place(&self, bytes: &mut Vec<u8>, nonce: &[u8]) -> Result<(), String> {
        self.aes
            .encrypt_in_place(Nonce::from_slice(nonce), b"", bytes)
            .map_err(|e| e.to_string())
    }

    pub fn encrypt(&self, bytes: &[u8], nonce: &[u8]) -> Result<Vec<u8>, String> {
        self.aes
            .encrypt(Nonce::from_slice(nonce), bytes)
            .map_err(|e| e.to_string())
    }

    pub fn decrypt(&self, bytes: &[u8], nonce: &[u8]) -> Result<Vec<u8>, String> {
        self.aes
            .decrypt(Nonce::from_slice(nonce), bytes)
            .map_err(|e| e.to_string())
    }
}
