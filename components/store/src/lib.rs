pub mod batch;
pub mod bitmap;
pub mod blob;
pub mod changelog;
pub mod config;
pub mod delete;
pub mod field;
pub mod get;
pub mod id;
pub mod leb128;
pub mod mutex_map;
pub mod query;
pub mod search_snippet;
pub mod serialize;
pub mod term;
pub mod term_index;
pub mod update;

use std::{
    iter::FromIterator,
    ops::Range,
    path::PathBuf,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

use batch::WriteBatch;
use changelog::{ChangeLog, ChangeLogId, ChangeLogQuery, RaftId};
use config::EnvSettings;
use id::{IdAssigner, IdCacheKey};
use moka::future::Cache;
use mutex_map::MutexMap;
use nlp::Language;
use parking_lot::Mutex;
use roaring::RoaringBitmap;
use serialize::{StoreDeserialize, StoreSerialize, LAST_TERM_ID_KEY};

pub use bincode;
pub use parking_lot;
pub use roaring;
pub use tokio;
use tokio::sync::oneshot;

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
pub type CollectionId = u8;
pub type DocumentId = u32;
pub type ThreadId = u32;
pub type FieldId = u8;
pub type TagId = u8;
pub type Integer = u32;
pub type LongInteger = u64;
pub type Float = f64;
pub type TermId = u64;
pub type JMAPId = u64;

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

pub struct FieldComparator {
    pub field: FieldId,
    pub ascending: bool,
}

pub struct DocumentSetComparator {
    pub set: RoaringBitmap,
    pub ascending: bool,
}

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

#[derive(Debug)]
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

    fn delete(&self, cf: ColumnFamily, key: Vec<u8>) -> Result<()>;
    fn set(&self, cf: ColumnFamily, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn get<U>(&self, cf: ColumnFamily, key: Vec<u8>) -> Result<Option<U>>
    where
        U: StoreDeserialize;
    fn exists(&self, cf: ColumnFamily, key: Vec<u8>) -> Result<bool>;

    fn merge(&self, cf: ColumnFamily, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn write(&self, batch: Vec<WriteOperation>) -> Result<()>;
    fn multi_get<U>(&self, cf: ColumnFamily, keys: Vec<Vec<u8>>) -> Result<Vec<Option<U>>>
    where
        U: StoreDeserialize;
    fn iterator<'y: 'x>(
        &'y self,
        cf: ColumnFamily,
        start: Vec<u8>,
        direction: Direction,
    ) -> Result<Self::Iterator>;
    fn compact(&self, cf: ColumnFamily) -> Result<()>;
}

pub struct JMAPStore<T> {
    pub db: Arc<T>,
    pub worker_pool: rayon::ThreadPool,
    pub config: JMAPStoreConfig,
    pub blob_lock: MutexMap<()>,
    pub doc_id_cache: Cache<IdCacheKey, Arc<Mutex<IdAssigner>>>,
    pub term_id_cache: Cache<String, TermId>,
    pub term_id_lock: MutexMap<()>,
    pub term_id_last: AtomicU64,
}

pub struct JMAPStoreConfig {
    pub blob_base_path: PathBuf,
    pub blob_hash_levels: Vec<usize>,
    pub blob_temp_ttl: u64,
}

impl JMAPStoreConfig {
    pub fn default_config(path: &str) -> JMAPStoreConfig {
        JMAPStoreConfig {
            blob_base_path: PathBuf::from(path),
            blob_hash_levels: vec![1],
            blob_temp_ttl: 3600,
        }
    }
}

impl<'x, T> JMAPStore<T>
where
    T: Store<'x> + 'static,
{
    pub fn new(db: T, settings: &EnvSettings) -> Self {
        Self {
            worker_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(
                    settings
                        .parse("worker-pool-size")
                        .filter(|v| *v > 0)
                        .unwrap_or_else(num_cpus::get),
                )
                .build()
                .unwrap(),
            config: JMAPStoreConfig::default_config("/tmp/jmap"),
            term_id_last: db
                .get::<TermId>(ColumnFamily::Terms, LAST_TERM_ID_KEY.to_vec())
                .unwrap()
                .unwrap()
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
            doc_id_cache: Cache::builder()
                .initial_capacity(128)
                .max_capacity(settings.parse("id-cache-size").unwrap_or(32 * 1024 * 1024))
                .time_to_idle(Duration::from_secs(60 * 60))
                .build(),
            term_id_lock: MutexMap::with_capacity(1024),
            blob_lock: MutexMap::with_capacity(1024),
            db: Arc::new(db),
        }
    }

    pub async fn spawn_worker<U, V>(&self, f: U) -> Result<V>
    where
        U: FnOnce() -> Result<V> + Send + 'static,
        V: Sync + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        self.worker_pool.spawn(move || {
            tx.send(f()).ok();
        });

        rx.await.map_err(|e| {
            StoreError::InternalError(format!("Failed to write batch: Await error: {}", e))
        })?
    }

    pub async fn spawn_blocking<U, V>(&self, f: U) -> Result<V>
    where
        U: FnOnce() -> Result<V> + Send + 'static,
        V: Sync + Send + 'static,
    {
        let (tx, rx) = oneshot::channel();

        tokio::task::spawn_blocking(move || {
            tx.send(f()).ok();
        });

        rx.await.map_err(|e| {
            StoreError::InternalError(format!("Failed to write batch: Await error: {}", e))
        })?
    }
}
