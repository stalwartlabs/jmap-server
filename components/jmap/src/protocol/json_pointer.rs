use std::collections::HashMap;

use store::JMAPId;

use crate::{
    error::method::MethodError,
    id::{blob::JMAPBlob, state::JMAPState},
};

use super::json::JSONValue;

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

impl JSONValue {
    pub fn to_pointer(&self) -> Option<JSONPointer> {
        match self {
            JSONValue::String(string) => Some(JSONPointer::parse(string.as_str())?),
            _ => None,
        }
    }

    pub fn eval(&self, pointer: &str) -> crate::Result<JSONValue> {
        self.eval_ptr(JSONPointer::parse(pointer).ok_or_else(|| {
            MethodError::InvalidResultReference(format!("Failed to parse: {}", pointer))
        })?)
    }

    pub fn eval_ptr(&self, pointer: JSONPointer) -> crate::Result<JSONValue> {
        let path = match pointer {
            JSONPointer::Path(path) => {
                if path.len() > 5 {
                    return Err(MethodError::InvalidResultReference(format!(
                        "Too many arguments, {} provided, max is 5.",
                        path.len()
                    )));
                }
                path
            }
            path_item @ (JSONPointer::String(_) | JSONPointer::Number(_)) => {
                vec![path_item]
            }
            JSONPointer::Root | JSONPointer::Wildcard => return Ok(self.clone()),
        };

        let mut eval_item = self;
        for (path_pos, path_item) in path.iter().enumerate() {
            let is_last = path_pos == path.len() - 1;

            match (path_item, eval_item) {
                (JSONPointer::String(name), JSONValue::Object(obj)) => {
                    if let Some(value) = obj.get(name) {
                        if !is_last {
                            eval_item = value;
                        } else {
                            return Ok(value.clone());
                        }
                    } else {
                        return Err(MethodError::InvalidResultReference(format!(
                            "Item '{}' not found.",
                            name
                        )));
                    }
                }
                (JSONPointer::Number(name), JSONValue::Object(obj)) => {
                    if let Some(value) = obj.get(&name.to_string()) {
                        if !is_last {
                            eval_item = value;
                        } else {
                            return Ok(value.clone());
                        }
                    } else {
                        return Err(MethodError::InvalidResultReference(format!(
                            "Item '{}' not found.",
                            name
                        )));
                    }
                }
                (JSONPointer::Number(pos), JSONValue::Array(array)) => {
                    if let Some(array_item) = array.get(*pos as usize) {
                        if !is_last {
                            eval_item = array_item;
                        } else {
                            return Ok(array_item.clone());
                        }
                    } else {
                        return Err(MethodError::InvalidResultReference(format!(
                            "Array position {} is out of bounds.",
                            pos
                        )));
                    }
                }
                (JSONPointer::Wildcard, JSONValue::Array(array)) => {
                    if !is_last {
                        let mut results = Vec::new();
                        for array_item in array {
                            eval_item = array_item;

                            for (path_pos, path_item) in path.iter().enumerate().skip(path_pos + 1)
                            {
                                let is_last = path_pos == path.len() - 1;

                                if let JSONValue::Object(obj) = eval_item {
                                    if let Some(value) = match path_item {
                                        JSONPointer::String(str) => obj.get(str),
                                        JSONPointer::Number(num) => obj.get(&num.to_string()),
                                        JSONPointer::Wildcard => obj.get("*"),
                                        _ => unreachable!(),
                                    } {
                                        if !is_last {
                                            eval_item = value;
                                        } else if let JSONValue::Array(array_items) = value {
                                            results.extend(array_items.iter().cloned());
                                        } else {
                                            results.push(value.clone());
                                        }
                                    } else {
                                        return Err(MethodError::InvalidResultReference(format!(
                                            "Item '{:?}' not found.",
                                            path_item
                                        )));
                                    }
                                } else {
                                    return Err(MethodError::InvalidResultReference(format!(
                                        "Could not evaluate path item {:?}.",
                                        path_item
                                    )));
                                }
                            }
                        }
                        return Ok(JSONValue::Array(results));
                    } else {
                        return Ok(eval_item.clone());
                    }
                }
                _ => {
                    return Err(MethodError::InvalidResultReference(format!(
                        "Could not evaluate path item {:?}.",
                        path_item
                    )));
                }
            }
        }

        Err(MethodError::InvalidResultReference(format!(
            "Could not evaluate path {:?}.",
            path
        )))
    }

    pub fn eval_unwrap_array(&self, pointer: &str) -> Vec<JSONValue> {
        self.eval(pointer).unwrap().unwrap_array().unwrap()
    }

    pub fn eval_unwrap_object(&self, pointer: &str) -> HashMap<String, JSONValue> {
        self.eval(pointer).unwrap().unwrap_object().unwrap()
    }

    pub fn eval_unwrap_string(&self, pointer: &str) -> String {
        self.eval(pointer).unwrap().unwrap_string().unwrap()
    }

    pub fn eval_unwrap_bool(&self, pointer: &str) -> bool {
        self.eval(pointer).unwrap().unwrap_bool().unwrap()
    }

    pub fn eval_unwrap_unsigned_int(&self, pointer: &str) -> u64 {
        self.eval(pointer).unwrap().unwrap_unsigned_int().unwrap()
    }

    pub fn eval_unwrap_jmap_id(&self, pointer: &str) -> JMAPId {
        self.eval(pointer).unwrap().to_jmap_id().unwrap()
    }

    pub fn eval_unwrap_jmap_state(&self, pointer: &str) -> JMAPState {
        self.eval(pointer).unwrap().to_jmap_state().unwrap()
    }

    pub fn eval_unwrap_blob(&self, pointer: &str) -> JMAPBlob {
        self.eval(pointer).unwrap().to_blob().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::{JSONPointer, JSONValue};

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

    #[test]
    fn json_pointer_eval() {
        let input = serde_json::from_slice::<JSONValue>(
            br#"{
            "foo": ["bar", "baz"],
            "": 0,
            "a/b": 1,
            "c%d": 2,
            "e^f": 3,
            "g|h": 4,
            "i\\j": 5,
            "k\"l": 6,
            " ": 7,
            "m~n": 8
         }"#,
        )
        .unwrap();

        for (pointer, expected_result) in [
            ("", input.clone()),
            (
                "/foo",
                vec!["bar".to_string().into(), "baz".to_string().into()].into(),
            ),
            ("/foo/0", "bar".to_string().into()),
            ("/", 0u64.into()),
            ("/a~1b", 1u64.into()),
            ("/c%d", 2u64.into()),
            ("/e^f", 3u64.into()),
            ("/g|h", 4u64.into()),
            ("/i\\j", 5u64.into()),
            ("/k\"l", 6u64.into()),
            ("/ ", 7u64.into()),
            ("/m~0n", 8u64.into()),
        ] {
            assert_eq!(input.eval(pointer).unwrap(), expected_result, "{}", pointer);
        }

        assert_eq!(
            serde_json::from_slice::<JSONValue>(
                br#"{
                "accountId": "A1",
                "queryState": "abcdefg",
                "canCalculateChanges": true,
                "position": 0,
                "total": 101,
                "ids": [ "msg1023", "msg223", "msg110", "msg93", "msg91",
                    "msg38", "msg36", "msg33", "msg11", "msg1" ]
            }"#
            )
            .unwrap()
            .eval("/ids")
            .unwrap(),
            serde_json::from_slice::<JSONValue>(
                br#"[ "msg1023", "msg223", "msg110", "msg93", "msg91",
        "msg38", "msg36", "msg33", "msg11", "msg1" ]"#
            )
            .unwrap()
        );

        assert_eq!(
            serde_json::from_slice::<JSONValue>(
                br#"{
            "accountId": "A1",
            "state": "123456",
            "list": [{
                "id": "msg1023",
                "threadId": "trd194"
            }, {
                "id": "msg223",
                "threadId": "trd114"
            }],
            "notFound": []
        }"#
            )
            .unwrap()
            .eval("/list/*/threadId")
            .unwrap(),
            serde_json::from_slice::<JSONValue>(br#"[ "trd194", "trd114" ]"#).unwrap()
        );

        assert_eq!(
            serde_json::from_slice::<JSONValue>(
                br#"{
            "accountId": "A1",
            "state": "123456",
            "list": [{
                "id": "trd194",
                "emailIds": [ "msg1020", "msg1021", "msg1023" ]
            }, {
                "id": "trd114",
                "emailIds": [ "msg201", "msg223" ]
            }],
            "notFound": []
        }"#
            )
            .unwrap()
            .eval("/list/*/emailIds")
            .unwrap(),
            serde_json::from_slice::<JSONValue>(
                br#"[ "msg1020", "msg1021", "msg1023", "msg201", "msg223" ]"#
            )
            .unwrap()
        );
    }
}
