use std::{collections::HashMap, fs, iter::FromIterator, path::PathBuf};

use jmap_mail::{JMAPMailLocalStore, JMAPMailProperties};
use jmap_store::{json::JSONValue, JMAPSet};

use crate::jmap_mail_get::UntaggedJSONValue;

impl<'x> From<UntaggedJSONValue> for JSONValue {
    fn from(value: UntaggedJSONValue) -> Self {
        match value {
            UntaggedJSONValue::Null => JSONValue::Null,
            UntaggedJSONValue::Bool(b) => JSONValue::Bool(b),
            UntaggedJSONValue::String(s) => JSONValue::String(s),
            UntaggedJSONValue::Number(n) => JSONValue::Number(n),
            UntaggedJSONValue::Array(a) => {
                JSONValue::Array(a.into_iter().map(JSONValue::from).collect())
            }
            UntaggedJSONValue::Object(o) => JSONValue::Object(
                o.into_iter()
                    .map(|(k, v)| (k, JSONValue::from(v)))
                    .collect(),
            ),
        }
    }
}

pub fn test_jmap_mail_set<T>(mail_store: T)
where
    T: for<'x> JMAPMailLocalStore<'x>,
{
    let mut test_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_dir.push("resources");
    test_dir.push("jmap_mail_set");

    for file_name in fs::read_dir(&test_dir).unwrap() {
        let mut file_name = file_name.as_ref().unwrap().path();
        if file_name.extension().map_or(true, |e| e != "json") {
            continue;
        }

        let result = mail_store
            .mail_set(JMAPSet {
                account_id: 0,
                if_in_state: None,
                create: Some(HashMap::from_iter(
                    vec![(
                        "1".to_string(),
                        JSONValue::from(
                            serde_json::from_slice::<UntaggedJSONValue>(
                                &fs::read(&file_name).unwrap(),
                            )
                            .unwrap(),
                        )
                        .unwrap_object()
                        .into_iter()
                        .map(|(k, v)| (JMAPMailProperties::parse(&k).unwrap(), v))
                        .collect::<HashMap<JMAPMailProperties, JSONValue>>(),
                    )]
                    .into_iter(),
                )),
                update: None,
                destroy: None,
            })
            .unwrap();
    }
}
