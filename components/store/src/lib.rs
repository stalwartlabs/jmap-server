pub mod document;
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

pub enum FieldValue<'x> {
    Text(&'x str),
    Integer(Integer),
    LongInteger(LongInteger),
    Float(Float),
    Tag(Tag<'x>),
}

pub enum Tag<'x> {
    Static(TagId),
    Id(DocumentId),
    Text(&'x str),
}

pub enum Filter<'x> {
    LowerThan(FieldValue<'x>),
    LowerEqualThan(FieldValue<'x>),
    GreaterThan(FieldValue<'x>),
    GreaterEqualThan(FieldValue<'x>),
    Equal(FieldValue<'x>),
    EqualMany(Vec<FieldValue<'x>>),
}

pub struct FilterCondition<'x> {
    pub field: FieldId,
    pub filter: Filter<'x>,
}

pub enum Operator {
    And,
    Or,
    Not,
}

pub enum Condition<'x> {
    FilterCondition(FilterCondition<'x>),
    FilterOperator(FilterOperator<'x>),
}

pub struct FilterOperator<'x> {
    pub operator: Operator,
    pub conditions: Vec<Condition<'x>>,
}

pub struct OrderBy {
    pub field: FieldId,
    pub ascending: bool,
}

pub trait Store {
    fn insert(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: DocumentBuilder,
    ) -> Result<DocumentId>;

    fn get_value(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
    ) -> Result<Option<Vec<u8>>>;
    fn get_value_by_pos(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
        pos: &ArrayPos,
    ) -> Result<Option<Vec<u8>>>;

    fn set_tag(
        &mut self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
        tag: &Tag,
    ) -> Result<()>;
    fn clear_tag(
        &mut self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
        tag: &Tag,
    ) -> Result<()>;
    fn has_tag(
        &mut self,
        account: &AccountId,
        collection: &CollectionId,
        document: &DocumentId,
        field: &FieldId,
        tag: &Tag,
    ) -> Result<bool>;

    fn search(
        &self,
        account: &AccountId,
        collection: &CollectionId,
        filter: &Filter,
        order_by: &[OrderBy],
    ) -> Result<Vec<DocumentId>>;
}
