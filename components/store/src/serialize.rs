use std::convert::TryInto;

use naive_cityhash::cityhash64;
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    blob::BlobId,
    leb128::Leb128,
    log::ChangeId,
    log::{LogIndex, RaftId},
    AccountId, Collection, DocumentId, FieldId, Float, Integer, LongInteger, Tag,
};

pub const COLLECTION_PREFIX_LEN: usize =
    std::mem::size_of::<AccountId>() + std::mem::size_of::<Collection>();
pub const FIELD_PREFIX_LEN: usize = COLLECTION_PREFIX_LEN + std::mem::size_of::<FieldId>();
pub const ACCOUNT_KEY_LEN: usize = std::mem::size_of::<AccountId>()
    + std::mem::size_of::<Collection>()
    + std::mem::size_of::<DocumentId>();

pub const BM_DOCUMENT_IDS: u8 = 0;
pub const BM_TERM: u8 = 0x01;
pub const BM_TAG: u8 = 0x02;

pub const TERM_EXACT: u8 = 0x00;
pub const TERM_STEMMED: u8 = 0x10;
pub const TERM_STRING: u8 = 0x20;
pub const TERM_HASH: u8 = 0x40;

pub const TAG_ID: u8 = 0x00;
pub const TAG_TEXT: u8 = 0x10;
pub const TAG_STATIC: u8 = 0x20;
pub const TAG_BYTES: u8 = 0x40;

pub const INTERNAL_KEY_PREFIX: u8 = 0;
pub const BLOB_KEY_PREFIX: &[u8; 2] = &[INTERNAL_KEY_PREFIX, 0];
pub const TEMP_BLOB_KEY_PREFIX: &[u8; 2] = &[INTERNAL_KEY_PREFIX, 1];

pub const FOLLOWER_COMMIT_INDEX_KEY: &[u8; 2] = &[INTERNAL_KEY_PREFIX, 2];
pub const LEADER_COMMIT_INDEX_KEY: &[u8; 2] = &[INTERNAL_KEY_PREFIX, 3];

pub struct ValueKey {}
pub struct BitmapKey {}
pub struct IndexKey {}
pub struct LogKey {}

impl ValueKey {
    pub const VALUE: u8 = 0;
    pub const TAGS: u8 = 1;
    pub const KEYWORDS: u8 = 2;
    pub const BLOBS: u8 = 3;

    pub fn serialize_account(account: AccountId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<AccountId>());
        account.to_leb128_bytes(&mut bytes);
        bytes
    }

    pub fn serialize_collection(account: AccountId, collection: Collection) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            std::mem::size_of::<AccountId>() + std::mem::size_of::<Collection>(),
        );
        account.to_leb128_bytes(&mut bytes);
        bytes.push(collection.into());
        bytes
    }

    pub fn serialize_value(
        account: AccountId,
        collection: Collection,
        document: DocumentId,
        field: FieldId,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + std::mem::size_of::<FieldId>() + 1);
        account.to_leb128_bytes(&mut bytes);
        bytes.push(collection.into());
        document.to_leb128_bytes(&mut bytes);
        bytes.push(field);
        bytes.push(ValueKey::VALUE);
        bytes
    }

    pub fn serialize_document_tag_list(
        account: AccountId,
        collection: Collection,
        document: DocumentId,
        field: FieldId,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + std::mem::size_of::<FieldId>() + 1);
        account.to_leb128_bytes(&mut bytes);
        bytes.push(collection.into());
        document.to_leb128_bytes(&mut bytes);
        bytes.push(field);
        bytes.push(ValueKey::TAGS);
        bytes
    }

    pub fn serialize_document_blob(
        account: AccountId,
        collection: Collection,
        document: DocumentId,
        index: u32,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + 1);
        account.to_leb128_bytes(&mut bytes);
        bytes.push(collection.into());
        document.to_leb128_bytes(&mut bytes);
        index.to_leb128_bytes(&mut bytes);
        bytes.push(ValueKey::BLOBS);
        bytes
    }

    pub fn serialize_temporary_blob(account: AccountId, hash: u64, timestamp: u64) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + TEMP_BLOB_KEY_PREFIX.len());
        bytes.extend_from_slice(TEMP_BLOB_KEY_PREFIX);
        timestamp.to_leb128_bytes(&mut bytes);
        hash.to_leb128_bytes(&mut bytes);
        account.to_leb128_bytes(&mut bytes);
        bytes
    }

    pub fn serialize_blob(id: &BlobId) -> Vec<u8> {
        let mut key =
            Vec::with_capacity(id.hash.len() + std::mem::size_of::<u32>() + BLOB_KEY_PREFIX.len());
        key.extend_from_slice(BLOB_KEY_PREFIX);
        key.extend_from_slice(&id.hash);
        id.size.to_leb128_bytes(&mut key);
        key
    }

    pub fn serialize_term_index(
        account: AccountId,
        collection: Collection,
        document: DocumentId,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            std::mem::size_of::<AccountId>()
                + std::mem::size_of::<Collection>()
                + std::mem::size_of::<DocumentId>(),
        );
        account.to_leb128_bytes(&mut bytes);
        bytes.push(collection.into());
        document.to_leb128_bytes(&mut bytes);
        bytes
    }
}

impl BitmapKey {
    pub fn serialize_account(account: AccountId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<AccountId>());
        account.to_leb128_bytes(&mut bytes);
        bytes
    }

    pub fn serialize_term(
        account: AccountId,
        collection: Collection,
        field: FieldId,
        term: &str,
        is_exact: bool,
    ) -> Vec<u8> {
        let (mut bytes, bm_type) = match term.len() as u32 {
            1..=9 => {
                let mut bytes =
                    Vec::with_capacity(ACCOUNT_KEY_LEN + std::mem::size_of::<u64>() + 3);
                bytes.extend_from_slice(term.as_bytes());
                (bytes, TERM_STRING)
            }
            10..=20 => {
                let mut bytes =
                    Vec::with_capacity(ACCOUNT_KEY_LEN + std::mem::size_of::<u64>() + 3);
                bytes.extend_from_slice(&xxh3_64(term.as_bytes()).to_be_bytes());
                term.len().to_leb128_bytes(&mut bytes);
                (bytes, TERM_HASH)
            }
            21..=u32::MAX => {
                let mut bytes =
                    Vec::with_capacity(ACCOUNT_KEY_LEN + (std::mem::size_of::<u64>() * 2) + 3);
                bytes.extend_from_slice(&xxh3_64(term.as_bytes()).to_be_bytes());
                bytes.extend_from_slice(&cityhash64(term.as_bytes()).to_be_bytes());
                term.len().to_leb128_bytes(&mut bytes);
                (bytes, TERM_HASH)
            }
            0 => {
                panic!("Term cannot be empty");
            }
        };

        account.to_leb128_bytes(&mut bytes);
        bytes.push(collection.into());
        bytes.push(field);
        bytes.push(BM_TERM | bm_type | if is_exact { TERM_EXACT } else { TERM_HASH });
        bytes
    }

    pub fn serialize_tag(
        account: AccountId,
        collection: Collection,
        field: FieldId,
        tag: &Tag,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + tag.len() + 1);
        let bm_type = match tag {
            Tag::Static(id) => {
                bytes.push(*id);
                TAG_STATIC
            }
            Tag::Id(id) => {
                (*id).to_leb128_bytes(&mut bytes);
                TAG_ID
            }
            Tag::Text(text) => {
                bytes.extend_from_slice(text.as_bytes());
                TAG_TEXT
            }
            Tag::Bytes(value) => {
                bytes.extend_from_slice(value);
                TAG_BYTES
            }
            Tag::Default => {
                bytes.push(0);
                TAG_STATIC
            }
        };
        account.to_leb128_bytes(&mut bytes);
        bytes.push(collection.into());
        bytes.push(field);
        bytes.push(BM_TAG | bm_type);
        bytes
    }

    pub fn serialize_document_ids(account: AccountId, collection: Collection) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + 1);
        account.to_leb128_bytes(&mut bytes);
        bytes.push(collection.into());
        bytes.push(BM_DOCUMENT_IDS);
        bytes
    }
}

impl IndexKey {
    pub fn serialize(
        account: AccountId,
        collection: Collection,
        document: DocumentId,
        field: FieldId,
        key: &[u8],
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + key.len());
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes.push(collection.into());
        bytes.extend_from_slice(&field.to_be_bytes());
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&document.to_be_bytes());
        bytes
    }

    pub fn serialize_account(account: AccountId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<AccountId>());
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes
    }

    pub fn serialize_collection(account: AccountId, collection: Collection) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            std::mem::size_of::<AccountId>() + std::mem::size_of::<Collection>(),
        );
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes.push(collection.into());
        bytes
    }

    pub fn serialize_field(account: AccountId, collection: u8, field: FieldId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN);
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes.push(collection);
        bytes.extend_from_slice(&field.to_be_bytes());
        bytes
    }

    pub fn serialize_key(
        account: AccountId,
        collection: Collection,
        field: FieldId,
        key: &[u8],
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + key.len());
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes.push(collection.into());
        bytes.extend_from_slice(&field.to_be_bytes());
        bytes.extend_from_slice(key);
        bytes
    }

    #[inline(always)]
    pub fn deserialize_document_id(bytes: &[u8]) -> Option<DocumentId> {
        DocumentId::from_be_bytes(
            bytes
                .get(bytes.len() - std::mem::size_of::<DocumentId>()..)?
                .try_into()
                .ok()?,
        )
        .into()
    }
}

impl LogKey {
    pub const CHANGE_KEY_PREFIX: u8 = 0;
    pub const RAFT_KEY_PREFIX: u8 = 1;
    pub const ROLLBACK_KEY_PREFIX: u8 = 2;
    pub const PENDING_UPDATES_KEY_PREFIX: u8 = 3;
    pub const TOMBSTONE_KEY_PREFIX: u8 = 3;

    pub const CHANGE_KEY_LEN: usize = std::mem::size_of::<AccountId>()
        + std::mem::size_of::<Collection>()
        + std::mem::size_of::<ChangeId>()
        + 1;
    pub const RAFT_KEY_LEN: usize = std::mem::size_of::<RaftId>() + 1;
    pub const ROLLBACK_KEY_LEN: usize =
        std::mem::size_of::<AccountId>() + std::mem::size_of::<Collection>() + 1;
    pub const TOMBSTONE_KEY_LEN: usize = std::mem::size_of::<LogIndex>()
        + std::mem::size_of::<AccountId>()
        + std::mem::size_of::<Collection>()
        + 1;

    pub const RAFT_TERM_POS: usize = std::mem::size_of::<LogIndex>() + 1;
    pub const CHANGE_ID_POS: usize =
        std::mem::size_of::<AccountId>() + std::mem::size_of::<Collection>() + 1;
    pub const ACCOUNT_POS: usize = 1;
    pub const COLLECTION_POS: usize = std::mem::size_of::<AccountId>() + 1;

    pub const TOMBSTONE_INDEX_POS: usize = 1;
    pub const TOMBSTONE_ACCOUNT_POS: usize = std::mem::size_of::<LogIndex>() + 1;

    pub fn deserialize_raft(bytes: &[u8]) -> Option<RaftId> {
        RaftId {
            index: bytes.deserialize_be_u64(1)?,
            term: bytes.deserialize_be_u64(1 + std::mem::size_of::<LogIndex>())?,
        }
        .into()
    }

    pub fn serialize_raft(id: &RaftId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(LogKey::RAFT_KEY_LEN);
        bytes.push(LogKey::RAFT_KEY_PREFIX);
        bytes.extend_from_slice(&id.index.to_be_bytes());
        bytes.extend_from_slice(&id.term.to_be_bytes());
        bytes
    }

    pub fn serialize_change(
        account: AccountId,
        collection: Collection,
        change_id: ChangeId,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(LogKey::CHANGE_KEY_LEN);
        bytes.push(LogKey::CHANGE_KEY_PREFIX);
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes.push(collection.into());
        bytes.extend_from_slice(&change_id.to_be_bytes());
        bytes
    }

    pub fn serialize_rollback(account: AccountId, collection: Collection) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(LogKey::ROLLBACK_KEY_LEN);
        bytes.push(LogKey::ROLLBACK_KEY_PREFIX);
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes.push(collection.into());
        bytes
    }

    pub fn serialize_pending_update(index: LogIndex, seq_id: LogIndex) -> Vec<u8> {
        let mut bytes = Vec::with_capacity((std::mem::size_of::<LogIndex>() * 2) + 1);
        bytes.push(LogKey::PENDING_UPDATES_KEY_PREFIX);
        bytes.extend_from_slice(&index.to_be_bytes());
        bytes.extend_from_slice(&seq_id.to_be_bytes());
        bytes
    }

    pub fn serialize_tombstone(index: LogIndex, account: AccountId) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(LogKey::TOMBSTONE_KEY_LEN + 1);
        bytes.push(LogKey::PENDING_UPDATES_KEY_PREFIX);
        bytes.extend_from_slice(&index.to_be_bytes());
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes
    }

    pub fn deserialize_change_id(bytes: &[u8]) -> Option<ChangeId> {
        bytes.deserialize_be_u64(LogKey::CHANGE_ID_POS)
    }
}

pub trait DeserializeBigEndian {
    fn deserialize_be_u32(&self, index: usize) -> Option<Integer>;
    fn deserialize_be_u64(&self, index: usize) -> Option<LongInteger>;
}

impl DeserializeBigEndian for &[u8] {
    fn deserialize_be_u32(&self, index: usize) -> Option<Integer> {
        Integer::from_be_bytes(
            self.get(index..index + std::mem::size_of::<Integer>())?
                .try_into()
                .ok()?,
        )
        .into()
    }

    fn deserialize_be_u64(&self, index: usize) -> Option<LongInteger> {
        LongInteger::from_be_bytes(
            self.get(index..index + std::mem::size_of::<LongInteger>())?
                .try_into()
                .ok()?,
        )
        .into()
    }
}

pub trait StoreDeserialize: Sized + Sync + Send {
    fn deserialize(bytes: &[u8]) -> Option<Self>;
}

pub trait StoreSerialize: Sized {
    fn serialize(&self) -> Option<Vec<u8>>;
}

impl StoreDeserialize for Vec<u8> {
    fn deserialize(bytes: &[u8]) -> Option<Vec<u8>> {
        bytes.to_vec().into()
    }
}

impl StoreDeserialize for String {
    fn deserialize(bytes: &[u8]) -> Option<Self> {
        String::from_utf8(bytes.to_vec()).ok()
    }
}

impl StoreDeserialize for Float {
    fn deserialize(bytes: &[u8]) -> Option<Float> {
        Float::from_le_bytes(bytes.try_into().ok()?).into()
    }
}

impl StoreDeserialize for LongInteger {
    fn deserialize(bytes: &[u8]) -> Option<LongInteger> {
        LongInteger::from_le_bytes(bytes.try_into().ok()?).into()
    }
}

impl StoreDeserialize for Integer {
    fn deserialize(bytes: &[u8]) -> Option<Integer> {
        Integer::from_le_bytes(bytes.try_into().ok()?).into()
    }
}

impl StoreDeserialize for i64 {
    fn deserialize(bytes: &[u8]) -> Option<i64> {
        i64::from_le_bytes(bytes.try_into().ok()?).into()
    }
}

impl StoreSerialize for LongInteger {
    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_le_bytes().to_vec())
    }
}

impl StoreSerialize for Integer {
    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_le_bytes().to_vec())
    }
}

impl StoreSerialize for i64 {
    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_le_bytes().to_vec())
    }
}

impl StoreSerialize for f64 {
    fn serialize(&self) -> Option<Vec<u8>> {
        Some(self.to_le_bytes().to_vec())
    }
}
