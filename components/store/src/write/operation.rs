use crate::ColumnFamily;

#[derive(Debug, Clone)]
pub enum WriteOperation {
    Set {
        cf: ColumnFamily,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Merge {
        cf: ColumnFamily,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: ColumnFamily,
        key: Vec<u8>,
    },
}

impl WriteOperation {
    pub fn set(cf: ColumnFamily, key: Vec<u8>, value: Vec<u8>) -> Self {
        WriteOperation::Set { cf, key, value }
    }

    pub fn merge(cf: ColumnFamily, key: Vec<u8>, value: Vec<u8>) -> Self {
        WriteOperation::Merge { cf, key, value }
    }

    pub fn delete(cf: ColumnFamily, key: Vec<u8>) -> Self {
        WriteOperation::Delete { cf, key }
    }
}
