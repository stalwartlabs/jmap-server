use serde::{Deserialize, Serialize};

pub mod json;
pub mod json_pointer;
pub mod request;
pub mod response;

#[derive(Debug, Serialize, Deserialize, Eq, PartialEq, Hash, Clone)]
pub enum TypeState {
    Mailbox,
    Thread,
    Email,
    EmailDelivery,
    Identity,
    EmailSubmission,
}
