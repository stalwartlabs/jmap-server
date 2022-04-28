use std::collections::HashMap;

use jmap::{
    id::JMAPIdSerialize, jmap_store::get::JMAPGet, protocol::json::JSONValue,
    request::get::GetRequest,
};
use jmap_mail::{mail::import::JMAPMailImport, thread::get::GetThread};
use store::{AccountId, JMAPId, JMAPStore, Store};

pub fn jmap_mail_thread<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
    let mut expected_result = vec![JSONValue::Null; 5];
    let mut thread_id = 0;

    for num in [5, 3, 1, 2, 4] {
        let result = mail_store
            .mail_import(
                account_id,
                0.into(),
                format!("Subject: test\nReferences: <1234>\n\n{}", num).as_bytes(),
                vec![],
                vec![],
                Some(10000i64 + num as i64),
            )
            .unwrap();
        thread_id = result.thread_id;
        expected_result[num - 1] = result.id.to_jmap_string().into();
    }

    assert_eq!(
        JSONValue::from(
            mail_store
                .get::<GetThread<T>>(GetRequest {
                    account_id,
                    ids: Some(vec![thread_id as JMAPId]),
                    properties: JSONValue::Null,
                    arguments: HashMap::new()
                })
                .unwrap()
        )
        .eval_unwrap_array("/list/0/emailIds"),
        expected_result
    );
}
