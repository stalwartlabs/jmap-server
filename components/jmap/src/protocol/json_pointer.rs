use std::{collections::HashMap, fmt};

use serde::Deserialize;
use store::JMAPId;

use crate::{
    error::method::MethodError,
    id::{blob::JMAPBlob, state::JMAPState},
};

use super::json::JSONValue;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
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
                    let mut buf = String::with_capacity(pos - last_pos);
                    let mut last_is_escaped = false;

                    for ch in value.get(last_pos..pos)?.chars() {
                        match ch {
                            '~' => {
                                last_is_escaped = true;
                            }
                            '0' if last_is_escaped => {
                                buf.push('~');
                                last_is_escaped = false;
                            }
                            '1' if last_is_escaped => {
                                buf.push('/');
                                last_is_escaped = false;
                            }
                            _ => {
                                buf.push(ch);
                                last_is_escaped = false;
                            }
                        }
                    }

                    path.push(JSONPointer::String(buf));
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

struct JSONPointerVisitor;

impl<'de> serde::de::Visitor<'de> for JSONPointerVisitor {
    type Value = JSONPointer;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a valid JSON Pointer")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        JSONPointer::parse(v).ok_or_else(|| E::custom(format!("Invalid JSON Pointer: {}", v)))
    }
}

impl<'de> Deserialize<'de> for JSONPointer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(JSONPointerVisitor)
    }
}
