pub mod document;
pub mod field;
pub mod search_snippet;
pub mod serialize;
pub mod term_index;

use document::DocumentBuilder;

#[derive(Debug)]
pub enum StoreError {
    InternalError(String),
    DataCorruption,
    NotFound,
    InvalidArgument,
}

pub type Result<T> = std::result::Result<T, StoreError>;

pub type AccountId = u32;
pub type CollectionId = u8;
pub type DocumentId = u32;
pub type FieldId = u8;
pub type TagId = u8;
pub type Integer = u32;
pub type LongInteger = u64;
pub type Float = f64;
pub type ArrayPos = u16;
pub type TermId = u64;

pub enum FieldValue<'x> {
    Keyword(&'x str),
    Text(&'x str),
    FullText(&'x str),
    Integer(Integer),
    LongInteger(LongInteger),
    Float(Float),
    Tag(Tag<'x>),
}

#[derive(Debug)]
pub enum Tag<'x> {
    Static(TagId),
    Id(DocumentId),
    Text(&'x str),
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

#[derive(Debug, Eq, PartialEq)]
pub enum LogicalOperator {
    And,
    Or,
    Not,
}

pub enum Condition<'x> {
    FilterCondition(FilterCondition<'x>),
    FilterOperator(FilterOperator<'x>),
}

impl<'x> Condition<'x> {
    pub fn new_condition(field: FieldId, op: ComparisonOperator, value: FieldValue<'x>) -> Self {
        Condition::FilterCondition(FilterCondition { field, op, value })
    }

    pub fn and(conditions: Vec<Condition<'x>>) -> Self {
        Condition::FilterOperator(FilterOperator {
            operator: LogicalOperator::And,
            conditions,
        })
    }

    pub fn or(conditions: Vec<Condition<'x>>) -> Self {
        Condition::FilterOperator(FilterOperator {
            operator: LogicalOperator::Or,
            conditions,
        })
    }

    pub fn not(conditions: Vec<Condition<'x>>) -> Self {
        Condition::FilterOperator(FilterOperator {
            operator: LogicalOperator::Not,
            conditions,
        })
    }
}

pub struct FilterOperator<'x> {
    pub operator: LogicalOperator,
    pub conditions: Vec<Condition<'x>>,
}

pub struct OrderBy {
    pub field: FieldId,
    pub ascending: bool,
}

pub trait StoreInsert {
    fn insert(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentBuilder,
    ) -> Result<DocumentId>;
    fn insert_bulk(
        &self,
        account: AccountId,
        collection: CollectionId,
        documents: Vec<DocumentBuilder>,
    ) -> Result<Vec<DocumentId>>;
}

pub trait StoreQuery<T: IntoIterator<Item = DocumentId>> {
    fn query(
        &self,
        account: AccountId,
        collection: CollectionId,
        filter: &FilterOperator,
        order_by: &[OrderBy],
    ) -> Result<T>;
}

pub trait StoreGet {
    fn get_value(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
    ) -> Result<Option<Vec<u8>>>;

    fn get_value_by_pos(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
        pos: ArrayPos,
    ) -> Result<Option<Vec<u8>>>;
}

pub trait StoreTag {
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

pub trait Store<T: IntoIterator<Item = DocumentId>>:
    StoreInsert + StoreQuery<T> + StoreGet + StoreTag + Send + Sync + Sized
{
}
