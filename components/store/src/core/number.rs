use crate::{serialize::StoreSerialize, Float, Integer, LongInteger};

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum Number {
    Integer(Integer),
    LongInteger(LongInteger),
    Float(Float),
}

impl Number {
    pub fn to_be_bytes(&self) -> Vec<u8> {
        match self {
            Number::Integer(i) => i.to_be_bytes().to_vec(),
            Number::LongInteger(i) => i.to_be_bytes().to_vec(),
            Number::Float(f) => f.to_be_bytes().to_vec(),
        }
    }
}

impl From<LongInteger> for Number {
    fn from(value: LongInteger) -> Self {
        Number::LongInteger(value)
    }
}

impl From<Integer> for Number {
    fn from(value: Integer) -> Self {
        Number::Integer(value)
    }
}

impl From<Float> for Number {
    fn from(value: Float) -> Self {
        Number::Float(value)
    }
}

impl StoreSerialize for Number {
    fn serialize(&self) -> Option<Vec<u8>> {
        match self {
            Number::Integer(i) => i.serialize(),
            Number::LongInteger(i) => i.serialize(),
            Number::Float(f) => f.serialize(),
        }
    }
}
