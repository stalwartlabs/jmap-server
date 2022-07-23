use std::fmt::Display;

#[derive(Debug, Clone)]
pub enum StoreError {
    InternalError(String),
    SerializeError(String),
    DeserializeError(String),
    InvalidArguments(String),
    AnchorNotFound,
    DataCorruption(String),
}

impl StoreError {
    pub fn into_owned(&self) -> StoreError {
        self.clone()
    }
}

impl Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreError::InternalError(s) => write!(f, "Internal error: {}", s),
            StoreError::SerializeError(s) => write!(f, "Serialization error: {}", s),
            StoreError::DeserializeError(s) => write!(f, "Deserialization error: {}", s),
            StoreError::InvalidArguments(s) => write!(f, "Invalid arguments: {}", s),
            StoreError::AnchorNotFound => write!(f, "Anchor not found."),
            StoreError::DataCorruption(s) => write!(f, "Data corruption: {}", s),
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(err: std::io::Error) -> Self {
        StoreError::InternalError(format!("I/O failure: {}", err))
    }
}
