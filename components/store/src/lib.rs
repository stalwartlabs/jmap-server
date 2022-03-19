pub mod batch;
pub mod bitmap;
pub mod blob;
pub mod changes;
pub mod config;
pub mod delete;
pub mod field;
pub mod get;
pub mod id;
pub mod leb128;
pub mod mutex_map;
pub mod query;
pub mod raft;
pub mod search_snippet;
pub mod serialize;
pub mod term;
pub mod term_index;
pub mod update;

use std::{
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use config::EnvSettings;
use id::{IdAssigner, IdCacheKey};
use moka::sync::Cache;
use mutex_map::MutexMap;
use nlp::Language;
use parking_lot::{Mutex, MutexGuard};
use raft::{LogIndex, RaftId};
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use serialize::{StoreDeserialize, LAST_TERM_ID_KEY};

pub use bincode;
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
    ParseError,
    DataCorruption,
    NotFound,
    InvalidArgument,
}

impl StoreError {
    pub fn into_owned(&self) -> StoreError {
        self.clone()
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
pub type TermId = u64;
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

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Collection {
    Mail = 0,
    Mailbox = 1,
    Thread = 2,
    None = 255,
}

impl From<u8> for Collection {
    fn from(value: u8) -> Self {
        match value {
            0 => Collection::Mail,
            1 => Collection::Mailbox,
            2 => Collection::Thread,
            _ => Collection::None,
        }
    }
}

impl From<Collection> for u8 {
    fn from(collection: Collection) -> u8 {
        collection as u8
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
    pub config: JMAPStoreConfig,

    pub account_lock: MutexMap<()>,
    pub blob_lock: MutexMap<()>,

    pub doc_id_cache: Cache<IdCacheKey, Arc<Mutex<IdAssigner>>>,

    pub term_id_cache: Cache<String, TermId>,
    pub term_id_lock: MutexMap<()>,
    pub term_id_last: AtomicU64,

    pub raft_term: AtomicU64,
    pub raft_log_index: AtomicU64,
}

pub struct JMAPStoreConfig {
    pub blob_base_path: PathBuf,
    pub blob_hash_levels: Vec<usize>,
    pub blob_temp_ttl: u64,

    pub default_language: Language,
    pub query_max_results: usize,
    pub get_max_results: usize,
    pub set_max_changes: usize,
    pub mailbox_set_max_changes: usize,
    pub mailbox_max_total: usize,
    pub mailbox_max_depth: usize,
    pub mail_thread_max_results: usize,
    pub mail_import_max_items: usize,
    pub mail_parse_max_items: usize,
}

impl From<&EnvSettings> for JMAPStoreConfig {
    fn from(settings: &EnvSettings) -> Self {
        JMAPStoreConfig {
            blob_base_path: PathBuf::from(
                settings
                    .get("db-path")
                    .unwrap_or_else(|| "stalwart-jmap".to_string()),
            ),
            blob_hash_levels: vec![1],
            blob_temp_ttl: 3600, //TODO configure all params
            get_max_results: 100,
            set_max_changes: 100,
            query_max_results: 1000,
            mailbox_set_max_changes: 100,
            mailbox_max_total: 1000,
            mailbox_max_depth: 10,
            mail_thread_max_results: 100,
            mail_import_max_items: 2,
            mail_parse_max_items: 5,
            default_language: Language::English,
        }
    }
}

impl<T> JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    pub fn new(db: T, settings: &EnvSettings) -> Self {
        let mut store = Self {
            config: settings.into(),
            term_id_last: db
                .get::<TermId>(ColumnFamily::Values, LAST_TERM_ID_KEY)
                .unwrap()
                .unwrap_or(0)
                .into(),
            term_id_cache: Cache::builder()
                .initial_capacity(1024)
                .max_capacity(
                    settings
                        .parse("term-cache-size")
                        .unwrap_or(32 * 1024 * 1024),
                )
                .time_to_idle(Duration::from_millis(
                    settings.parse("term-cache-ttl").unwrap_or(10 * 60 * 1000),
                ))
                .build(),
            term_id_lock: MutexMap::with_capacity(1024),
            doc_id_cache: Cache::builder()
                .initial_capacity(128)
                .max_capacity(settings.parse("id-cache-size").unwrap_or(32 * 1024 * 1024))
                .time_to_idle(Duration::from_secs(60 * 60))
                .build(),
            blob_lock: MutexMap::with_capacity(1024),
            account_lock: MutexMap::with_capacity(1024),
            raft_log_index: 0.into(),
            raft_term: 0.into(),
            db,
        };

        // Obtain last Raft ID
        let raft_id = store
            .get_prev_raft_id(RaftId::new(TermId::MAX, LogIndex::MAX))
            .unwrap()
            .map(|mut id| {
                id.index += 1;
                id
            })
            .unwrap_or(RaftId {
                term: 0,
                index: LogIndex::MAX,
            });
        store.raft_log_index = raft_id.index.into();
        store.raft_term = raft_id.term.into();
        store
    }

    pub fn lock_account(&self, account: AccountId, collection: Collection) -> MutexGuard<'_, ()> {
        self.account_lock.lock_hash((account, collection))
    }
}
