#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub mechanism: String,
    pub initial_response: Option<Vec<u8>>,
}
