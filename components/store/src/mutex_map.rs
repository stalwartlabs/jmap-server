use core::hash::Hash;
use std::{
    collections::{
        hash_map::{DefaultHasher, Entry},
        HashMap,
    },
    hash::Hasher,
    sync::{Mutex, MutexGuard},
};

pub struct MutexMap {
    map: Box<[Mutex<usize>]>,
    mask: u64,
    hasher: DefaultHasher,
}

pub struct MutexMapLock<'x, T> {
    values: Vec<T>,
    locks: HashMap<u64, MutexGuard<'x, usize>>,
}

impl<'x, T> MutexMapLock<'x, T> {
    pub fn get_values(self) -> Vec<T> {
        self.values
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

pub struct MutexMapLockError;
pub type Result<T> = std::result::Result<T, MutexMapLockError>;

#[allow(clippy::mutex_atomic)]
impl MutexMap {
    pub fn with_capacity(size: usize) -> MutexMap {
        let size = size.next_power_of_two();
        MutexMap {
            map: (0..size)
                .map(|_| 0.into())
                .collect::<Vec<Mutex<usize>>>()
                .into_boxed_slice(),
            mask: (size - 1) as u64,
            hasher: DefaultHasher::new(),
        }
    }

    pub fn lock_many_hash<T, I>(&self, it: T) -> Result<MutexMapLock<I>>
    where
        T: Iterator<Item = I>,
        I: Eq + Hash,
    {
        let len = it.size_hint().1.unwrap_or_else(|| it.size_hint().0);
        let mut obtained_locks = MutexMapLock {
            values: Vec::with_capacity(len),
            locks: HashMap::with_capacity(len),
        };

        for key in it {
            let mut hasher = self.hasher.clone();
            key.hash(&mut hasher);
            obtained_locks.values.push(key);
            let hash = hasher.finish() & self.mask;
            if let Entry::Vacant(entry) = obtained_locks.locks.entry(hash) {
                entry.insert(
                    self.map[hash as usize]
                        .lock()
                        .map_err(|_| MutexMapLockError)?,
                );
            }
        }

        Ok(obtained_locks)
    }

    pub fn lock_many<T, I>(&self, it: T) -> Result<MutexMapLock<I>>
    where
        T: Iterator<Item = I>,
        I: Into<u64> + Copy,
    {
        let len = it.size_hint().1.unwrap_or_else(|| it.size_hint().0);
        let mut obtained_locks = MutexMapLock {
            values: Vec::with_capacity(len),
            locks: HashMap::with_capacity(len),
        };

        for key in it {
            let hash = key.into() & self.mask;
            obtained_locks.values.push(key);
            if let Entry::Vacant(entry) = obtained_locks.locks.entry(hash) {
                entry.insert(
                    self.map[hash as usize]
                        .lock()
                        .map_err(|_| MutexMapLockError)?,
                );
            }
        }

        Ok(obtained_locks)
    }

    pub fn lock<T>(&self, key: T) -> Result<MutexGuard<'_, usize>>
    where
        T: Into<u64> + Copy,
    {
        let hash = key.into() & self.mask;
        self.map[hash as usize]
            .lock()
            .map_err(|_| MutexMapLockError)
    }
}
