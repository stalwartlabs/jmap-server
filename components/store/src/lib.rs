pub mod document;
pub mod field;
pub mod search_snippet;
pub mod serialize;
pub mod term_index;

use document::DocumentBuilder;
use nlp::Language;

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
    FullText(TextQuery<'x>),
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

pub trait StoreQuery<'x, T: Iterator<Item = DocumentId>> {
    fn query(
        &'x self,
        account: AccountId,
        collection: CollectionId,
        filter: Option<Filter>,
        sort: Option<Vec<Comparator>>,
    ) -> Result<T>;
}

pub trait StoreGet {
    fn get_stored_value(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
    ) -> Result<Option<Vec<u8>>>;

    fn get_stored_value_by_pos(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
        pos: ArrayPos,
    ) -> Result<Option<Vec<u8>>>;

    fn get_integer(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
    ) -> Result<Option<Integer>> {
        if let Some(bytes) = self.get_stored_value(account, collection, document, field)? {
            Ok(Some(serialize::deserialize_integer(bytes).ok_or_else(
                || StoreError::InternalError("Failed to deserialize integer".to_string()),
            )?))
        } else {
            Ok(None)
        }
    }

    fn get_long_integer(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
    ) -> Result<Option<LongInteger>> {
        if let Some(bytes) = self.get_stored_value(account, collection, document, field)? {
            Ok(Some(
                serialize::deserialize_long_integer(bytes).ok_or_else(|| {
                    StoreError::InternalError("Failed to deserialize long integer".to_string())
                })?,
            ))
        } else {
            Ok(None)
        }
    }

    fn get_float(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
    ) -> Result<Option<Float>> {
        if let Some(bytes) = self.get_stored_value(account, collection, document, field)? {
            Ok(Some(serialize::deserialize_float(bytes).ok_or_else(
                || StoreError::InternalError("Failed to deserialize float".to_string()),
            )?))
        } else {
            Ok(None)
        }
    }

    fn get_text(
        &self,
        account: AccountId,
        collection: CollectionId,
        document: DocumentId,
        field: FieldId,
    ) -> Result<Option<String>> {
        if let Some(bytes) = self.get_stored_value(account, collection, document, field)? {
            Ok(Some(serialize::deserialize_text(bytes).ok_or_else(
                || StoreError::InternalError("Failed to decode UTF-8 string".to_string()),
            )?))
        } else {
            Ok(None)
        }
    }
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

pub trait Store<'x, T: Iterator<Item = DocumentId>>:
    StoreInsert + StoreQuery<'x, T> + StoreGet + StoreTag + Send + Sync + Sized
{
}
