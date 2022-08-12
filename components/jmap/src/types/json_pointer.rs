use std::fmt;

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize)]
pub enum JSONPointer {
    Root,
    Wildcard,
    String(String),
    Number(u64),
    Path(Vec<JSONPointer>),
}

pub trait JSONPointerEval {
    fn eval_json_pointer(&self, ptr: &JSONPointer) -> Option<Vec<u64>>;
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

    pub fn unwrap_string(self) -> Option<String> {
        match self {
            JSONPointer::String(s) => s.into(),
            _ => None,
        }
    }

    pub fn is_item_query(&self, name: &str) -> bool {
        match self {
            JSONPointer::String(property) => property == name,
            JSONPointer::Path(path) if path.len() == 2 => {
                if let (Some(JSONPointer::String(property)), Some(JSONPointer::Wildcard)) =
                    (path.get(0), path.get(1))
                {
                    property == name
                } else {
                    false
                }
            }
            _ => false,
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

#[cfg(test)]
mod tests {
    use super::JSONPointer;

    #[test]
    fn json_pointer_parse() {
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
