use roaring::RoaringBitmap;

use crate::{core::tag::Tag, nlp::Language, FieldId, Float, Integer, LongInteger};

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
pub enum FieldValue {
    Keyword(String),
    Text(String),
    FullText(TextQuery),
    Integer(Integer),
    LongInteger(LongInteger),
    Float(Float),
    Tag(Tag),
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
