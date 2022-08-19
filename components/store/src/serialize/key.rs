use std::convert::TryInto;

use crate::{
    blob::{BlobId, BLOB_EXTERNAL, BLOB_HASH_LEN, BLOB_LOCAL},
    core::{collection::Collection, tag::Tag},
    log::{
        changes::ChangeId,
        raft::{LogIndex, RaftId},
    },
    AccountId, DocumentId, FieldId,
};

use super::{
    leb128::{Leb128Iterator, Leb128Reader, Leb128Vec},
    DeserializeBigEndian,
};

pub const COLLECTION_PREFIX_LEN: usize =
    std::mem::size_of::<AccountId>() + std::mem::size_of::<Collection>();
pub const FIELD_PREFIX_LEN: usize = COLLECTION_PREFIX_LEN + std::mem::size_of::<FieldId>();
pub const ACCOUNT_KEY_LEN: usize = std::mem::size_of::<AccountId>()
    + std::mem::size_of::<Collection>()
    + std::mem::size_of::<DocumentId>();

pub const BM_DOCUMENT_IDS: u8 = 0;
pub const BM_TERM: u8 = 0x10;
pub const BM_TAG: u8 = 0x20;

pub const TERM_EXACT: u8 = 0x00;
pub const TERM_STEMMED: u8 = 0x01;
pub const TERM_STRING: u8 = 0x02;
pub const TERM_HASH: u8 = 0x04;

pub const TAG_ID: u8 = 0x00;
pub const TAG_TEXT: u8 = 0x01;
pub const TAG_STATIC: u8 = 0x02;

pub const INTERNAL_KEY_PREFIX: u8 = 0;

pub const FOLLOWER_COMMIT_INDEX_KEY: &[u8; 2] = &[INTERNAL_KEY_PREFIX, 1];
pub const LEADER_COMMIT_INDEX_KEY: &[u8; 2] = &[INTERNAL_KEY_PREFIX, 2];

pub struct ValueKey {}
pub struct BitmapKey {}
pub struct IndexKey {}
pub struct LogKey {}
pub struct BlobKey {}

impl ValueKey {
    pub fn serialize_collection(account: AccountId, collection: Collection) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            std::mem::size_of::<AccountId>() + std::mem::size_of::<Collection>(),
        );
        bytes.push_leb128(account);
        bytes.push(collection.into());
        bytes
    }

    pub fn serialize_value(
        account: AccountId,
        collection: Collection,
        document: DocumentId,
        field: FieldId,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + std::mem::size_of::<FieldId>());
        bytes.push_leb128(account);
        bytes.push(collection.into());
        bytes.push_leb128(document);
        bytes.push(field);
        bytes
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
        bytes.push_leb128(account);
        bytes.push(collection.into());
        bytes.push_leb128(document);
        bytes
    }

    pub fn serialize_acl(
        grant_account: AccountId,
        to_account: AccountId,
        to_collection: Collection,
        to_document: DocumentId,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + std::mem::size_of::<AccountId>() + 1);
        bytes.push_leb128(grant_account);
        bytes.push(u8::MAX);
        bytes.push_leb128(to_account);
        bytes.push(to_collection.into());
        bytes.push_leb128(to_document);
        bytes
    }

    pub fn serialize_acl_prefix(
        grant_account: AccountId,
        to_account: AccountId,
        to_collection: Collection,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<AccountId>() + 1);
        bytes.push_leb128(grant_account);
        bytes.push(u8::MAX);
        if to_account != AccountId::MAX {
            bytes.push_leb128(to_account);
        }
        if to_collection != Collection::None {
            bytes.push(to_collection.into());
        }
        bytes
    }

    pub fn deserialize_acl_target(bytes: &[u8]) -> Option<(AccountId, Collection, DocumentId)> {
        let mut bytes = bytes.iter();
        Some((
            bytes.next_leb128()?,
            (*bytes.next()?).into(),
            bytes.next_leb128()?,
        ))
    }
}

impl BlobKey {
    pub fn serialize_link(
        id: &BlobId,
        account: AccountId,
        collection: Collection,
        document: DocumentId,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(BLOB_HASH_LEN + ACCOUNT_KEY_LEN + 1);
        key.push(if id.is_local() {
            BLOB_LOCAL
        } else {
            BLOB_EXTERNAL
        });
        key.extend_from_slice(id.hash());
        key.push_leb128(account);
        key.push(collection.into());
        key.push_leb128(document);
        key
    }

    pub fn serialize_prefix(id: &BlobId, account: AccountId) -> Vec<u8> {
        let mut key = Vec::with_capacity(BLOB_HASH_LEN + std::mem::size_of::<AccountId>() + 1);
        key.push(if id.is_local() {
            BLOB_LOCAL
        } else {
            BLOB_EXTERNAL
        });
        key.extend_from_slice(id.hash());
        if account != AccountId::MAX {
            key.push_leb128(account);
        }
        key
    }

    pub fn serialize_collection(
        id: &BlobId,
        account: AccountId,
        collection: Collection,
    ) -> Vec<u8> {
        let mut key = Vec::with_capacity(BLOB_HASH_LEN + std::mem::size_of::<AccountId>() + 1);
        key.push(if id.is_local() {
            BLOB_LOCAL
        } else {
            BLOB_EXTERNAL
        });
        key.extend_from_slice(id.hash());
        key.push_leb128(account);
        key.push(collection.into());
        key
    }

    pub fn serialize(id: &BlobId) -> Vec<u8> {
        let mut key = Vec::with_capacity(BLOB_HASH_LEN + 1);
        key.push(if id.is_local() {
            BLOB_LOCAL
        } else {
            BLOB_EXTERNAL
        });
        key.extend_from_slice(id.hash());
        key
    }
}

impl BitmapKey {
    pub fn serialize_term(
        account: AccountId,
        collection: Collection,
        field: FieldId,
        term: &str,
        is_exact: bool,
    ) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + term.len() + 3);
        bytes.extend_from_slice(term.as_bytes());
        bytes.push(field);
        bytes.push(collection.into());
        bytes.push(BM_TERM | if is_exact { TERM_EXACT } else { TERM_STEMMED });
        bytes.push_leb128(account);
        bytes
    }

    #[cfg(feature = "term_hash")]
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
                bytes.extend_from_slice(&xxhash_rust::xxh3::xxh3_64(term.as_bytes()).to_be_bytes());
                bytes.push_leb128(term.len());
                (bytes, TERM_HASH)
            }
            21..=u32::MAX => {
                let mut bytes =
                    Vec::with_capacity(ACCOUNT_KEY_LEN + (std::mem::size_of::<u64>() * 2) + 3);
                bytes.extend_from_slice(&xxhash_rust::xxh3::xxh3_64(term.as_bytes()).to_be_bytes());
                bytes.extend_from_slice(&naive_cityhash::cityhash64(term.as_bytes()).to_be_bytes());
                bytes.push_leb128(term.len());
                (bytes, TERM_HASH)
            }
            0 => {
                panic!("Term cannot be empty");
            }
        };

        bytes.push(field);
        bytes.push(collection.into());
        bytes.push(BM_TERM | bm_type | if is_exact { TERM_EXACT } else { TERM_STEMMED });
        bytes.push_leb128(account);
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
                bytes.push_leb128(*id);
                TAG_ID
            }
            Tag::Text(text) => {
                bytes.extend_from_slice(text.as_bytes());
                TAG_TEXT
            }
            Tag::Default => {
                bytes.push(0);
                TAG_STATIC
            }
        };
        bytes.push(field);
        bytes.push(collection.into());
        bytes.push(BM_TAG | bm_type);
        bytes.push_leb128(account);
        bytes
    }

    pub fn serialize_document_ids(account: AccountId, collection: Collection) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(ACCOUNT_KEY_LEN + 1);
        bytes.push(collection.into());
        bytes.push(BM_DOCUMENT_IDS);
        bytes.push_leb128(account);
        bytes
    }

    pub fn deserialize_account_id(bytes: &[u8]) -> Option<AccountId> {
        bytes.get(..bytes.len() - 1).and_then(|range| {
            range
                .iter()
                .rposition(|&byte| byte & 0x80 == 0)
                .and_then(|start_pos| {
                    bytes
                        .get(start_pos + 1..)
                        .and_then(|range| range.read_leb128().map(|r| r.0))
                })
        })
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
        bytes.push(field);
        bytes.extend_from_slice(key);
        bytes.extend_from_slice(&document.to_be_bytes());
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
        bytes.push(field);
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
        bytes.push(field);
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
        bytes.push(LogKey::TOMBSTONE_KEY_PREFIX);
        bytes.extend_from_slice(&index.to_be_bytes());
        bytes.extend_from_slice(&account.to_be_bytes());
        bytes
    }

    pub fn deserialize_change_id(bytes: &[u8]) -> Option<ChangeId> {
        bytes.deserialize_be_u64(LogKey::CHANGE_ID_POS)
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        core::{collection::Collection, tag::Tag},
        AccountId,
    };

    use super::BitmapKey;

    #[test]
    fn bitmap_account_id() {
        for (bytes, account_id) in [
            (
                BitmapKey::serialize_term(1, Collection::Mail, 10, "hello world", true),
                1,
            ),
            (
                BitmapKey::serialize_term(AccountId::MAX / 2, Collection::Mail, 20, "a", true),
                AccountId::MAX / 2,
            ),
            (
                BitmapKey::serialize_tag(
                    AccountId::MAX,
                    Collection::Mailbox,
                    2,
                    &Tag::Text("hello there".to_string()),
                ),
                AccountId::MAX,
            ),
        ] {
            assert_eq!(
                BitmapKey::deserialize_account_id(&bytes).unwrap(),
                account_id
            );
        }
    }
}
