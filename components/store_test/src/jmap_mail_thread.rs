use std::collections::HashMap;

use jmap::{protocol::json::JSONValue, request::get::GetRequest};
use jmap_mail::{mail::import::JMAPMailImport, thread::get::JMAPMailThreadGet};
use store::{AccountId, JMAPStore, Store};

pub fn jmap_mail_thread<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
    let mut expected_result = vec![JSONValue::Null; 5];
    let mut thread_id = 0;

    for num in [5, 3, 1, 2, 4] {
        let result = mail_store
            .mail_import_blob(
                account_id,
                format!("Subject: test\nReferences: <1234>\n\n{}", num).into_bytes(),
                vec![],
                vec![],
                Some(10000i64 + num as i64),
            )
            .unwrap();
        thread_id = result.eval_unwrap_jmap_id("/threadId");
        expected_result[num - 1] = result.eval("/id").unwrap();
    }

    assert_eq!(
        mail_store
            .thread_get(GetRequest {
                account_id,
                ids: Some(vec![thread_id]),
                properties: JSONValue::Null,
                arguments: HashMap::new()
            })
            .unwrap()
            .eval_unwrap_array("/list/0/emailIds"),
        expected_result
    );
}
