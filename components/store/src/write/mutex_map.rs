use core::hash::Hash;
use std::{hash::Hasher, time::Duration};

use ahash::AHasher;
use parking_lot::{Mutex, MutexGuard};

pub struct MutexMap<T: Default> {
    map: Box<[Mutex<T>]>,
    mask: u64,
    hasher: AHasher,
}

pub struct MutexMapLockError;
pub type Result<T> = std::result::Result<T, MutexMapLockError>;

#[allow(clippy::mutex_atomic)]
impl<T: Default> MutexMap<T> {
    pub fn with_capacity(size: usize) -> MutexMap<T> {
        let size = size.next_power_of_two();
        MutexMap {
            map: (0..size)
                .map(|_| T::default().into())
                .collect::<Vec<Mutex<T>>>()
                .into_boxed_slice(),
            mask: (size - 1) as u64,
            hasher: AHasher::default(),
        }
    }

    pub fn lock<U>(&self, key: U) -> MutexGuard<'_, T>
    where
        U: Into<u64> + Copy,
    {
        let hash = key.into() & self.mask;
        self.map[hash as usize].lock()
    }

    pub fn try_lock<U>(&self, key: U, timeout: Duration) -> Option<MutexGuard<'_, T>>
    where
        U: Into<u64> + Copy,
    {
        let hash = key.into() & self.mask;
        self.map[hash as usize].try_lock_for(timeout)
    }

    pub fn lock_hash<U>(&self, key: U) -> MutexGuard<'_, T>
    where
        U: Hash,
    {
        let mut hasher = self.hasher.clone();
        key.hash(&mut hasher);
        let hash = hasher.finish() & self.mask;
        self.map[hash as usize].lock()
    }

    pub fn try_lock_hash<U>(&self, key: U, timeout: Duration) -> Option<MutexGuard<'_, T>>
    where
        U: Hash,
    {
        let mut hasher = self.hasher.clone();
        key.hash(&mut hasher);
        let hash = hasher.finish() & self.mask;
        self.map[hash as usize].try_lock_for(timeout)
    }
}
