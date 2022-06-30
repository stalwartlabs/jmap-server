use super::Sequence;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Arguments {
    pub sequence_set: Vec<Sequence>,
    pub attributes: Vec<Attribute>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Attribute {
    Envelope,
    Flags,
    InternalDate,
    Rfc822,
    Rfc822Size,
    Rfc822Header,
    Rfc822Text,
    Body,
    BodyStructure,
    BodySection {
        peek: bool,
        sections: Vec<Section>,
        partial: Option<(u64, u64)>,
    },
    Uid,
    Binary {
        peek: bool,
        sections: Vec<u64>,
        partial: Option<(u64, u64)>,
    },
    BinarySize {
        sections: Vec<u64>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Section {
    Part { num: u64 },
    Header,
    HeaderFields { not: bool, fields: Vec<Vec<u8>> },
    Text,
    Mime,
}
