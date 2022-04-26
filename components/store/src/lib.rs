pub mod batch;
pub mod bitmap;
pub mod blob;
pub mod config;
pub mod delete;
pub mod field;
pub mod get;
pub mod id;
pub mod leb128;
pub mod log;
pub mod mutex_map;
pub mod query;
pub mod search_snippet;
pub mod serialize;
pub mod term_index;
pub mod update;

use std::{
    fmt::Display,
    ops::Deref,
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use blob::BlobStoreWrapper;
use config::EnvSettings;
use id::{IdAssigner, IdCacheKey};
use log::{LogIndex, RaftId};
use moka::sync::Cache;
use mutex_map::MutexMap;
use nlp::Language;
use parking_lot::{Mutex, MutexGuard};
use roaring::RoaringBitmap;
use serialize::StoreDeserialize;

pub use bincode;
pub use chrono;
pub use lz4_flex;
pub use parking_lot;
pub use roaring;
pub use sha2;
pub use tracing;

#[derive(Debug, Clone)]
pub enum StoreError {
    InternalError(String),
    SerializeError(String),
    DeserializeError(String),
    InvalidArguments(String),
    AnchorNotFound,
    DataCorruption,
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
            StoreError::DataCorruption => write!(f, "Data corruption."),
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(err: std::io::Error) -> Self {
        StoreError::InternalError(format!("I/O failure: {}", err))
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub type AccountId = u32;
pub type DocumentId = u32;
pub type ThreadId = u32;
pub type FieldId = u8;
pub type TagId = u8;
pub type Integer = u32;
pub type LongInteger = u64;
pub type Float = f64;
pub type JMAPId = u64;

pub trait JMAPIdPrefix {
    fn from_parts(prefix_id: DocumentId, doc_id: DocumentId) -> JMAPId;
    fn get_document_id(&self) -> DocumentId;
    fn get_prefix_id(&self) -> DocumentId;
}

impl JMAPIdPrefix for JMAPId {
    fn from_parts(prefix_id: DocumentId, doc_id: DocumentId) -> JMAPId {
        (prefix_id as JMAPId) << 32 | doc_id as JMAPId
    }

    fn get_document_id(&self) -> DocumentId {
        (self & 0xFFFFFFFF) as DocumentId
    }

    fn get_prefix_id(&self) -> DocumentId {
        (self >> 32) as DocumentId
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum Collection {
    Blob = 0,
    Account = 1,
    PushSubscription = 2,
    Mail = 3,
    Mailbox = 4,
    Thread = 5,
    Identity = 6,
    EmailSubmission = 7,
    VacationResponse = 8,
    None = 9,
}

impl From<u8> for Collection {
    fn from(value: u8) -> Self {
        match value {
            0 => Collection::Account,
            1 => Collection::PushSubscription,
            2 => Collection::Mail,
            3 => Collection::Mailbox,
            4 => Collection::Thread,
            5 => Collection::Identity,
            6 => Collection::EmailSubmission,
            7 => Collection::VacationResponse,
            _ => {
                debug_assert!(false, "Invalid collection value: {}", value);
                Collection::None
            }
        }
    }
}

impl From<Collection> for u8 {
    fn from(collection: Collection) -> u8 {
        collection as u8
    }
}

impl From<Collection> for u64 {
    fn from(collection: Collection) -> u64 {
        collection as u64
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct Collections {
    pub collections: u64,
}

impl Collections {
    pub fn all() -> Self {
        Self {
            collections: u64::MAX >> (64 - (Collection::None as u64)),
        }
    }

    pub fn union(&mut self, items: &Collections) {
        self.collections |= items.collections;
    }

    pub fn insert(&mut self, item: Collection) {
        debug_assert_ne!(item, Collection::None);
        self.collections |= 1 << item as u64;
    }

    pub fn pop(&mut self) -> Option<Collection> {
        if self.collections != 0 {
            let collection_id = 63 - self.collections.leading_zeros();
            self.collections ^= 1 << collection_id;
            Some(Collection::from(collection_id as u8))
        } else {
            None
        }
    }

    pub fn contains(&self, item: Collection) -> bool {
        self.collections & (1 << item as u64) != 0
    }

    pub fn is_empty(&self) -> bool {
        self.collections == 0
    }

    pub fn clear(&mut self) -> Self {
        let collections = self.collections;
        self.collections = 0;
        Collections { collections }
    }
}

impl From<u64> for Collections {
    fn from(value: u64) -> Self {
        Self { collections: value }
    }
}

impl AsRef<u64> for Collections {
    fn as_ref(&self) -> &u64 {
        &self.collections
    }
}

impl Deref for Collections {
    type Target = u64;

    fn deref(&self) -> &Self::Target {
        &self.collections
    }
}

impl Iterator for Collections {
    type Item = Collection;

    fn next(&mut self) -> Option<Self::Item> {
        if self.collections != 0 {
            let collection_id = 63 - self.collections.leading_zeros();
            self.collections ^= 1 << collection_id;
            Some(Collection::from(collection_id as u8))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum FieldValue {
    Keyword(String),
    Text(String),
    FullText(TextQuery),
    Integer(Integer),
    LongInteger(LongInteger),
    Float(Float),
    Tag(Tag),
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq, Hash)]
pub enum Tag {
    Static(TagId),
    Id(Integer),
    Text(String),
    Bytes(Vec<u8>),
    Default,
}

#[derive(Debug)]
pub struct TextQuery {
    pub text: String,
    pub language: Language,
    pub match_phrase: bool,
}

impl TextQuery {
    pub fn query(text: String, language: Language) -> Self {
        TextQuery {
            language,
            match_phrase: (text.starts_with('"') && text.ends_with('"'))
                || (text.starts_with('\'') && text.ends_with('\'')),
            text,
        }
    }

    pub fn query_english(text: String) -> Self {
        TextQuery::query(text, Language::English)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ComparisonOperator {
    LowerThan,
    LowerEqualThan,
    GreaterThan,
    GreaterEqualThan,
    Equal,
}

#[derive(Debug)]
pub struct FilterCondition {
    pub field: FieldId,
    pub op: ComparisonOperator,
    pub value: FieldValue,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum LogicalOperator {
    And,
    Or,
    Not,
}

#[derive(Debug)]
pub enum Filter {
    Condition(FilterCondition),
    Operator(FilterOperator),
    DocumentSet(RoaringBitmap),
    None,
}

impl Default for Filter {
    fn default() -> Self {
        Filter::None
    }
}

impl Filter {
    pub fn new_condition(field: FieldId, op: ComparisonOperator, value: FieldValue) -> Self {
        Filter::Condition(FilterCondition { field, op, value })
    }

    pub fn eq(field: FieldId, value: FieldValue) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::Equal,
            value,
        })
    }

    pub fn lt(field: FieldId, value: FieldValue) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::LowerThan,
            value,
        })
    }

    pub fn le(field: FieldId, value: FieldValue) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::LowerEqualThan,
            value,
        })
    }

    pub fn gt(field: FieldId, value: FieldValue) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::GreaterThan,
            value,
        })
    }

    pub fn ge(field: FieldId, value: FieldValue) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::GreaterEqualThan,
            value,
        })
    }

    pub fn and(conditions: Vec<Filter>) -> Self {
        Filter::Operator(FilterOperator {
            operator: LogicalOperator::And,
            conditions,
        })
    }

    pub fn or(conditions: Vec<Filter>) -> Self {
        Filter::Operator(FilterOperator {
            operator: LogicalOperator::Or,
            conditions,
        })
    }

    pub fn not(conditions: Vec<Filter>) -> Self {
        Filter::Operator(FilterOperator {
            operator: LogicalOperator::Not,
            conditions,
        })
    }
}

#[derive(Debug)]
pub struct FilterOperator {
    pub operator: LogicalOperator,
    pub conditions: Vec<Filter>,
}

#[derive(Debug)]
pub struct FieldComparator {
    pub field: FieldId,
    pub ascending: bool,
}

#[derive(Debug)]
pub struct DocumentSetComparator {
    pub set: RoaringBitmap,
    pub ascending: bool,
}

#[derive(Debug)]
pub enum Comparator {
    List(Vec<Comparator>),
    Field(FieldComparator),
    DocumentSet(DocumentSetComparator),
    None,
}

impl Default for Comparator {
    fn default() -> Self {
        Comparator::None
    }
}

impl Comparator {
    pub fn ascending(field: FieldId) -> Self {
        Comparator::Field(FieldComparator {
            field,
            ascending: true,
        })
    }

    pub fn descending(field: FieldId) -> Self {
        Comparator::Field(FieldComparator {
            field,
            ascending: false,
        })
    }
}

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub enum ColumnFamily {
    Bitmaps,
    Values,
    Indexes,
    Terms,
    Logs,
}

pub enum Direction {
    Forward,
    Backward,
}

#[derive(Debug)]
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

pub trait Store<'x>
where
    Self: Sized + Send + Sync,
{
    type Iterator: Iterator<Item = (Box<[u8]>, Box<[u8]>)> + 'x;

    fn open(settings: &EnvSettings) -> Result<Self>;
    fn delete(&self, cf: ColumnFamily, key: &[u8]) -> Result<()>;
    fn set(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<()>;
    fn get<U>(&self, cf: ColumnFamily, key: &[u8]) -> Result<Option<U>>
    where
        U: StoreDeserialize;
    fn exists(&self, cf: ColumnFamily, key: &[u8]) -> Result<bool>;

    fn merge(&self, cf: ColumnFamily, key: &[u8], value: &[u8]) -> Result<()>;
    fn write(&self, batch: Vec<WriteOperation>) -> Result<()>;
    fn multi_get<T, U>(&self, cf: ColumnFamily, keys: Vec<U>) -> Result<Vec<Option<T>>>
    where
        T: StoreDeserialize,
        U: AsRef<[u8]>;
    fn iterator<'y: 'x>(
        &'y self,
        cf: ColumnFamily,
        start: &[u8],
        direction: Direction,
    ) -> Result<Self::Iterator>;
    fn compact(&self, cf: ColumnFamily) -> Result<()>;
}

pub struct JMAPStore<T> {
    pub db: T,
    pub blob: BlobStoreWrapper,
    pub config: JMAPConfig,

    pub account_lock: MutexMap<()>,

    pub doc_id_cache: Cache<IdCacheKey, Arc<Mutex<IdAssigner>>>,

    pub raft_term: AtomicU64,
    pub raft_index: AtomicU64,
}

pub struct JMAPConfig {
    pub is_in_cluster: bool,

    pub blob_temp_ttl: u64,
    pub default_language: Language,

    pub max_size_upload: usize,
    pub max_concurrent_upload: usize,
    pub max_size_request: usize,
    pub max_concurrent_requests: usize,
    pub max_calls_in_request: usize,
    pub max_objects_in_get: usize,
    pub max_objects_in_set: usize,

    pub query_max_results: usize,
    pub changes_max_results: usize,
    pub mailbox_name_max_len: usize,
    pub mailbox_max_total: usize,
    pub mailbox_max_depth: usize,
    pub mail_attachments_max_size: usize,
    pub mail_import_max_items: usize,
    pub mail_parse_max_items: usize,
}

impl From<&EnvSettings> for JMAPConfig {
    fn from(settings: &EnvSettings) -> Self {
        JMAPConfig {
            max_size_upload: 50000000,
            max_concurrent_upload: 8,
            max_size_request: 10000000,
            max_concurrent_requests: 8,
            max_calls_in_request: 32,
            max_objects_in_get: 500,
            max_objects_in_set: 500,
            blob_temp_ttl: 3600, //TODO configure all params
            changes_max_results: 1000,
            query_max_results: 1000,
            mailbox_name_max_len: 255, //TODO implement
            mailbox_max_total: 1000,
            mailbox_max_depth: 10,
            mail_attachments_max_size: 50000000, //TODO implement
            mail_import_max_items: 2,
            mail_parse_max_items: 5,
            default_language: Language::English,
            is_in_cluster: settings.get("cluster").is_some(),
        }
    }
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn new(db: T, config: JMAPConfig, settings: &EnvSettings) -> Self {
        let mut store = Self {
            config,
            blob: BlobStoreWrapper::new(settings).unwrap(),
            doc_id_cache: Cache::builder()
                .initial_capacity(128)
                .max_capacity(settings.parse("id-cache-size").unwrap_or(32 * 1024 * 1024))
                .time_to_idle(Duration::from_secs(60 * 60))
                .build(),
            account_lock: MutexMap::with_capacity(1024),
            raft_index: 0.into(),
            raft_term: 0.into(),
            db,
        };

        // Obtain last Raft ID
        let raft_id = store
            .get_prev_raft_id(RaftId::new(LogIndex::MAX, LogIndex::MAX))
            .unwrap()
            .map(|mut id| {
                id.index += 1;
                id
            })
            .unwrap_or(RaftId {
                term: 0,
                index: LogIndex::MAX,
            });
        store.raft_term = raft_id.term.into();
        store.raft_index = raft_id.index.into();
        store
    }

    pub fn lock_account(&self, account: AccountId, collection: Collection) -> MutexGuard<'_, ()> {
        self.account_lock.lock_hash((account, collection))
    }
}
