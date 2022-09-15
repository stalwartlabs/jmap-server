/*
 * Copyright (c) 2020-2022, Stalwart Labs Ltd.
 *
 * This file is part of the Stalwart JMAP Server.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of
 * the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 * in the LICENSE file at the top-level directory of this distribution.
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can be released from the requirements of the AGPLv3 license by
 * purchasing a commercial license. Please contact licensing@stalw.art
 * for more details.
*/

use roaring::RoaringBitmap;

use crate::{
    core::tag::Tag,
    nlp::{lang::LanguageDetector, Language},
    FieldId, Float, Integer, LongInteger,
};

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
    pub value: Query,
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
    pub fn new_condition(field: FieldId, op: ComparisonOperator, value: Query) -> Self {
        Filter::Condition(FilterCondition { field, op, value })
    }

    pub fn eq(field: FieldId, value: Query) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::Equal,
            value,
        })
    }

    pub fn lt(field: FieldId, value: Query) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::LowerThan,
            value,
        })
    }

    pub fn le(field: FieldId, value: Query) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::LowerEqualThan,
            value,
        })
    }

    pub fn gt(field: FieldId, value: Query) -> Self {
        Filter::Condition(FilterCondition {
            field,
            op: ComparisonOperator::GreaterThan,
            value,
        })
    }

    pub fn ge(field: FieldId, value: Query) -> Self {
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
pub enum Query {
    Keyword(String),
    Tokenize(String),
    Index(String),
    Match(Text),
    Integer(Integer),
    LongInteger(LongInteger),
    Float(Float),
    Tag(Tag),
}

#[derive(Debug)]
pub struct Text {
    pub text: String,
    pub language: Language,
    pub match_phrase: bool,
}

impl Text {
    pub fn new(mut text: String, mut language: Language) -> Self {
        let match_phrase = (text.starts_with('"') && text.ends_with('"'))
            || (text.starts_with('\'') && text.ends_with('\''));

        if !match_phrase && language == Language::Unknown {
            language = if let Some((l, t)) = text
                .split_once(':')
                .and_then(|(l, t)| (Language::from_iso_639(l)?, t.to_string()).into())
            {
                text = t;
                l
            } else {
                LanguageDetector::detect_single(&text)
                    .and_then(|(l, c)| if c > 0.3 { Some(l) } else { None })
                    .unwrap_or(Language::Unknown)
            };
        }

        Text {
            language,
            match_phrase,
            text,
        }
    }
}

impl Query {
    pub fn match_text(text: String, language: Language) -> Self {
        Query::Match(Text::new(text, language))
    }

    pub fn match_english(text: String) -> Self {
        Query::match_text(text, Language::English)
    }
}
