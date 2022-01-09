use core::hash::Hash;
use std::{
    collections::hash_map::DefaultHasher,
    hash::Hasher,
    sync::{Mutex, MutexGuard},
};

pub struct MutexMap<T: Default> {
    map: Box<[Mutex<T>]>,
    mask: u64,
    hasher: DefaultHasher,
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
            hasher: DefaultHasher::new(),
        }
    }

    pub fn lock<U>(&self, key: U) -> Result<MutexGuard<'_, T>>
    where
        U: Into<u64> + Copy,
    {
        let hash = key.into() & self.mask;
        self.map[hash as usize]
            .lock()
            .map_err(|_| MutexMapLockError)
    }

    pub fn lock_hash<U>(&self, key: U) -> Result<MutexGuard<'_, T>>
    where
        U: Hash,
    {
        let mut hasher = self.hasher.clone();
        key.hash(&mut hasher);
        let hash = hasher.finish() & self.mask;
        self.map[hash as usize]
            .lock()
            .map_err(|_| MutexMapLockError)
    }
}
