use super::capability::Capability;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub capabilities: Vec<Capability>,
}
