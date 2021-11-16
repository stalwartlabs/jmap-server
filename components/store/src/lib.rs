pub mod object_builder;
//pub mod search_snippet;
//pub mod token_map;

use object_builder::JMAPObjectBuilder;

#[derive(Debug)]
pub enum JMAPStoreError {
    DataCorruption,
    NotFound,
    InvalidArgument,
}

pub type Result<T> = std::result::Result<T, JMAPStoreError>;

pub type AccountId = u32;
pub type CollectionId = u8;
pub type ObjectId = u64;
pub type FieldId = u8;
pub type TagId = u8;
pub type Integer = u32;
pub type LongInteger = u64;
pub type Float = f64;

pub enum FieldValue<'x> {
    Text(&'x str),
    Integer(Integer),
    LongInteger(LongInteger),
    Float(Float),
    Tag(Tag<'x>),
}

pub enum Tag<'x> {
    Static(u8),
    Id(ObjectId),
    Text(&'x str),
}

pub enum Filter<'x> {
    LowerThan(FieldValue<'x>),
    LowerEqualThan(FieldValue<'x>),
    GreaterThan(FieldValue<'x>),
    GreaterEqualThan(FieldValue<'x>),
    Equal(FieldValue<'x>),
    MatchAll(Vec<FieldValue<'x>>),
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

pub trait JMAPStore {
    fn set_tag(&mut self, account: AccountId, collection: CollectionId, tag: Tag) -> Result<()>;
    fn clear_tag(&mut self, account: AccountId, collection: CollectionId, tag: Tag) -> Result<()>;

    fn search(
        &self,
        account: AccountId,
        collection: CollectionId,
        filter: Filter,
    ) -> Result<Vec<ObjectId>>;
    fn iterate(
        &self,
        account: AccountId,
        collection: CollectionId,
        field: FieldId,
        seek_to: Option<FilterCondition>,
        ascending: bool,
    ) -> Result<Vec<ObjectId>>;

    fn index(&self, object: JMAPObjectBuilder) -> Result<ObjectId>;
}
