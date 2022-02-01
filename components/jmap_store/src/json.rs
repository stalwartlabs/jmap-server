use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::id::JMAPIdSerialize;
use crate::JMAPId;

#[derive(Debug, PartialEq, Eq, Hash)]
pub enum JSONPointer {
    Root,
    Wildcard,
    String(String),
    Number(u64),
    Path(Vec<JSONPointer>),
}

impl JSONPointer {
    pub fn parse(value: &str) -> Option<JSONPointer> {
        let mut path = Vec::new();
        let mut is_number = false;
        let mut is_wildcard = false;
        let mut is_escaped = false;
        let mut is_string = false;
        let mut last_pos = 0;

        for (mut pos, ch) in value.char_indices() {
            let mut add_token = false;
            match ch {
                '0'..='9' => {
                    is_number = true;
                }
                '~' => {
                    is_escaped = true;
                }
                '*' => {
                    is_wildcard = true;
                }
                '/' => {
                    if pos > 0 {
                        add_token = true;
                    } else {
                        last_pos = pos + 1;
                    }
                }
                _ => {
                    is_string = true;
                }
            }
            if !add_token && pos + ch.len_utf8() == value.len() {
                add_token = true;
                pos = value.len();
            }
            if add_token {
                if is_number && !is_escaped && !is_string && !is_wildcard {
                    path.push(JSONPointer::Number(
                        value.get(last_pos..pos)?.parse().unwrap_or(0),
                    ));
                } else if is_wildcard && (pos - last_pos) == 1 {
                    path.push(JSONPointer::Wildcard);
                } else if is_escaped {
                    path.push(JSONPointer::String(
                        value
                            .get(last_pos..pos)?
                            .replace("~1", "/")
                            .replace("~0", "~"),
                    ));
                } else {
                    path.push(JSONPointer::String(value.get(last_pos..pos)?.to_string()));
                }

                is_number = false;
                is_wildcard = false;
                is_escaped = false;
                is_string = false;

                last_pos = pos + 1;
            }
        }

        match path.len() {
            1 => path.pop(),
            0 => JSONPointer::Root.into(),
            _ => JSONPointer::Path(path).into(),
        }
    }

    pub fn to_string(&self) -> Option<&str> {
        match self {
            JSONPointer::String(s) => s.as_str().into(),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Clone)]
pub enum JSONValue {
    Null,
    Bool(bool),
    String(String),
    Number(i64),
    Array(Vec<JSONValue>),
    Object(HashMap<String, JSONValue>),
}

impl Default for JSONValue {
    fn default() -> Self {
        JSONValue::Null
    }
}

impl From<HashMap<String, JSONValue>> for JSONValue {
    fn from(o: HashMap<String, JSONValue>) -> Self {
        JSONValue::Object(o)
    }
}

impl From<Vec<JSONValue>> for JSONValue {
    fn from(a: Vec<JSONValue>) -> Self {
        JSONValue::Array(a)
    }
}

impl From<String> for JSONValue {
    fn from(s: String) -> Self {
        JSONValue::String(s)
    }
}

impl From<bool> for JSONValue {
    fn from(b: bool) -> Self {
        JSONValue::Bool(b)
    }
}

impl From<i64> for JSONValue {
    fn from(i: i64) -> Self {
        JSONValue::Number(i)
    }
}

impl From<usize> for JSONValue {
    fn from(i: usize) -> Self {
        JSONValue::Number(i as i64)
    }
}

impl JSONValue {
    pub fn is_null(&self) -> bool {
        matches!(self, JSONValue::Null)
    }

    pub fn to_array(&self) -> Option<&Vec<JSONValue>> {
        match self {
            JSONValue::Array(array) => Some(array),
            _ => None,
        }
    }

    pub fn to_object(&self) -> Option<&HashMap<String, JSONValue>> {
        match self {
            JSONValue::Object(object) => Some(object),
            _ => None,
        }
    }

    pub fn to_string(&self) -> Option<&str> {
        match self {
            JSONValue::String(string) => Some(string),
            _ => None,
        }
    }

    pub fn to_number(&self) -> Option<i64> {
        match self {
            JSONValue::Number(number) => Some(*number),
            _ => None,
        }
    }

    pub fn to_bool(&self) -> Option<bool> {
        match self {
            JSONValue::Bool(bool) => Some(*bool),
            _ => None,
        }
    }

    pub fn to_jmap_id(&self) -> Option<JMAPId> {
        match self {
            JSONValue::String(string) => JMAPId::from_jmap_string(string),
            _ => None,
        }
    }

    pub fn to_pointer(&self) -> Option<JSONPointer> {
        match self {
            JSONValue::String(string) => Some(JSONPointer::parse(string.as_str())?),
            _ => None,
        }
    }

    pub fn unwrap_array(self) -> Option<Vec<JSONValue>> {
        match self {
            JSONValue::Array(array) => array.into(),
            _ => None,
        }
    }

    pub fn unwrap_object(self) -> Option<HashMap<String, JSONValue>> {
        match self {
            JSONValue::Object(object) => object.into(),
            _ => None,
        }
    }

    pub fn unwrap_string(self) -> Option<String> {
        match self {
            JSONValue::String(string) => Some(string),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::JSONPointer;

    #[test]
    fn json_pointer() {
        for (input, output) in vec![
            ("hello", JSONPointer::String("hello".to_string())),
            ("9a", JSONPointer::String("9a".to_string())),
            ("a9", JSONPointer::String("a9".to_string())),
            ("*a", JSONPointer::String("*a".to_string())),
            (
                "/hello/world",
                JSONPointer::Path(vec![
                    JSONPointer::String("hello".to_string()),
                    JSONPointer::String("world".to_string()),
                ]),
            ),
            ("*", JSONPointer::Wildcard),
            (
                "/hello/*",
                JSONPointer::Path(vec![
                    JSONPointer::String("hello".to_string()),
                    JSONPointer::Wildcard,
                ]),
            ),
            ("1234", JSONPointer::Number(1234)),
            (
                "/hello/1234",
                JSONPointer::Path(vec![
                    JSONPointer::String("hello".to_string()),
                    JSONPointer::Number(1234),
                ]),
            ),
            ("~0~1", JSONPointer::String("~/".to_string())),
            (
                "/hello/~0~1",
                JSONPointer::Path(vec![
                    JSONPointer::String("hello".to_string()),
                    JSONPointer::String("~/".to_string()),
                ]),
            ),
            (
                "/hello/world/*/99",
                JSONPointer::Path(vec![
                    JSONPointer::String("hello".to_string()),
                    JSONPointer::String("world".to_string()),
                    JSONPointer::Wildcard,
                    JSONPointer::Number(99),
                ]),
            ),
            ("/", JSONPointer::String("".to_string())),
            (
                "///",
                JSONPointer::Path(vec![
                    JSONPointer::String("".to_string()),
                    JSONPointer::String("".to_string()),
                ]),
            ),
            ("", JSONPointer::Root),
        ] {
            assert_eq!(JSONPointer::parse(input), Some(output), "{}", input);
        }
    }
}
