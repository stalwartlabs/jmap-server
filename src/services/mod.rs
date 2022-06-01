pub mod email_delivery;
pub mod push_subscription;
pub mod state_change;

#[cfg(test)]
pub const THROTTLE_MS: u64 = 500;

#[cfg(not(test))]
pub const THROTTLE_MS: u64 = 1000;

pub const LONG_SLUMBER_MS: u64 = 60 * 60 * 24 * 1000;
