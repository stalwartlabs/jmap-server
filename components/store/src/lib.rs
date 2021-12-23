pub mod batch;
pub mod field;
pub mod leb128;
pub mod mutex_map;
pub mod search_snippet;
pub mod serialize;
pub mod term_index;

use batch::WriteOperation;
use nlp::Language;

#[derive(Debug)]
pub enum StoreError {
    InternalError(String),
    SerializeError(String),
    ParseError,
    DataCorruption,
    NotFound,
    InvalidArgument,
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub type AccountId = u32;
pub type CollectionId = u8;
pub type DocumentId = u32;
pub type ThreadId = u64;
pub type FieldId = u8;
pub type FieldNumber = u16;
pub type TagId = u8;
pub type Integer = u32;
pub type LongInteger = u64;
pub type Float = f64;
pub type TermId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BaseId {
    pub account_id: AccountId,
    pub collection_id: CollectionId,
}

impl BaseId {
    pub fn new(account_id: AccountId, collection_id: CollectionId) -> BaseId {
        BaseId {
            account_id,
            collection_id,
        }
    }
}

pub enum FieldValue<'x> {
    Keyword(&'x str),
    Text(&'x str),
    FullText(TextQuery<'x>),
    Integer(Integer),
    LongInteger(LongInteger),
    Float(Float),
    Tag(Tag<'x>),
}

#[derive(Debug, Copy, Clone)]
pub enum Tag<'x> {
    Static(TagId),
    Id(LongInteger),
    Text(&'x str),
}

pub struct TextQuery<'x> {
    pub text: &'x str,
    pub language: Language,
    pub match_phrase: bool,
}

impl<'x> TextQuery<'x> {
    pub fn query(text: &'x str, language: Language) -> Self {
        TextQuery {
            text,
            language,
            match_phrase: (text.starts_with('"') && text.ends_with('"'))
                || (text.starts_with('\'') && text.ends_with('\'')),
        }
    }

    pub fn query_english(text: &'x str) -> Self {
        TextQuery::query(text, Language::English)
    }
}

pub enum ComparisonOperator {
    LowerThan,
    LowerEqualThan,
    GreaterThan,
    GreaterEqualThan,
    Equal,
}

pub struct FilterCondition<'x> {
    pub field: FieldId,
    pub op: ComparisonOperator,
    pub value: FieldValue<'x>,
}

#[derive(Debug, Eq, PartialEq, Clone, Copy)]
pub enum LogicalOperator {
    And,
    Or,
    Not,
}

pub enum Filter<'x> {
    Condition(FilterCondition<'x>),
    Operator(FilterOperator<'x>),
}

impl<'x> Filter<'x> {
    pub fn new_condition(field: FieldId, op: ComparisonOperator, value: FieldValue<'x>) -> Self {
        Filter::Condition(FilterCondition { field, op, value })
    }

    pub fn eq(field: FieldId, value: FieldValue<'x>) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::Equal,
            value,
        })
    }

    pub fn lt(field: FieldId, value: FieldValue<'x>) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::LowerThan,
            value,
        })
    }

    pub fn le(field: FieldId, value: FieldValue<'x>) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::LowerEqualThan,
            value,
        })
    }

    pub fn gt(field: FieldId, value: FieldValue<'x>) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::GreaterThan,
            value,
        })
    }

    pub fn ge(field: FieldId, value: FieldValue<'x>) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::GreaterEqualThan,
            value,
        })
    }

    pub fn and(conditions: Vec<Filter<'x>>) -> Self {
        Filter::Operator(FilterOperator {
            operator: LogicalOperator::And,
            conditions,
        })
    }

    pub fn or(conditions: Vec<Filter<'x>>) -> Self {
        Filter::Operator(FilterOperator {
            operator: LogicalOperator::Or,
            conditions,
        })
    }

    pub fn not(conditions: Vec<Filter<'x>>) -> Self {
        Filter::Operator(FilterOperator {
            operator: LogicalOperator::Not,
            conditions,
        })
    }
}

pub struct FilterOperator<'x> {
    pub operator: LogicalOperator,
    pub conditions: Vec<Filter<'x>>,
}

pub struct Comparator {
    pub field: FieldId,
    pub ascending: bool,
}

impl Comparator {
    pub fn ascending(field: FieldId) -> Self {
        Comparator {
            field,
            ascending: true,
        }
    }

    pub fn descending(field: FieldId) -> Self {
        Comparator {
            field,
            ascending: false,
        }
    }
}

pub trait StoreUpdate {
    fn update(&self, document: WriteOperation) -> crate::Result<DocumentId> {
        self.update_bulk(vec![document])?
            .pop()
            .ok_or_else(|| StoreError::InternalError("No document id returned".to_string()))
    }

    fn update_bulk(&self, documents: Vec<WriteOperation>) -> Result<Vec<DocumentId>>;
}

pub trait StoreQuery<'x> {
    type Iter: Iterator<Item = DocumentId>;
    fn query(
        &'x self,
        account: AccountId,
        collection: CollectionId,
        filter: Option<Filter>,
        sort: Option<Vec<Comparator>>,
    ) -> Result<Self::Iter>;
}

pub trait StoreGet {
    fn get_value<T>(
        &self,
        account: Option<AccountId>,
        collection: Option<CollectionId>,
        field: Option<FieldId>,
    ) -> Result<Option<T>>
    where
        Vec<u8>: serialize::StoreDeserialize<T>;

    fn get_document_value<T>(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
        field_num: FieldNumber,
    ) -> Result<Option<T>>
    where
        Vec<u8>: serialize::StoreDeserialize<T>;

    fn get_multi_document_value<T>(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: &[DocumentId],
        field: FieldId,
    ) -> Result<Vec<Option<T>>>
    where
        Vec<u8>: serialize::StoreDeserialize<T>;
}

pub trait StoreTag {
    type Iter: Iterator<Item = DocumentId>;

    fn get_tag(
        &self,
        account: AccountId,
        collection: CollectionId,
        field: FieldId,
        tag: Tag,
    ) -> Result<Option<Self::Iter>> {
        Ok(self
            .get_tags(account, collection, field, &[tag])?
            .pop()
            .unwrap())
    }

    fn get_tags(
        &self,
        account: AccountId,
        collection: CollectionId,
        field: FieldId,
        tags: &[Tag],
    ) -> Result<Vec<Option<Self::Iter>>>;

    fn set_tag(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
        tag: Tag,
    ) -> Result<()>;

    fn clear_tag(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
        tag: Tag,
    ) -> Result<()>;

    fn has_tag(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
        tag: Tag,
    ) -> Result<bool>;
}

pub trait StoreDelete {
    fn delete_document(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
    ) -> Result<()> {
        self.delete_document_bulk(account, collection, &[document])
    }
    fn delete_document_bulk(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: &[DocumentId],
    ) -> Result<()>;
    fn delete_account(&self, account: AccountId) -> Result<()>;
    fn delete_collection(&self, account: AccountId, collection: CollectionId) -> Result<()>;
}

pub trait Store<'x>:
    StoreUpdate + StoreQuery<'x> + StoreGet + StoreDelete + StoreTag + Send + Sync + Sized
{
}
