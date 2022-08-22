use reqwest::header;
use store_rocksdb::RocksDB;

use super::{jmap::init_jmap_tests, store::utils::destroy_temp_dir};

pub mod email_changes;
pub mod email_copy;
pub mod email_get;
pub mod email_ingest;
pub mod email_parse;
pub mod email_query;
pub mod email_query_changes;
pub mod email_set;
pub mod email_submission;
pub mod email_thread;
pub mod email_thread_merge;
pub mod mailbox;
pub mod search_snippet;
pub mod vacation_response;

pub async fn ingest_message(raw_message: Vec<u8>, recipients: &[&str]) -> Vec<Dsn> {
    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        header::HeaderValue::from_str("Bearer DO_NOT_ATTEMPT_THIS_AT_HOME").unwrap(),
    );

    serde_json::from_slice(
        &reqwest::Client::builder()
            .default_headers(headers)
            .build()
            .unwrap_or_default()
            .post(&format!(
                "http://127.0.0.1:8001/ingest?to={}",
                recipients.join(",")
            ))
            .body(raw_message)
            .send()
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap(),
    )
    .unwrap()
}

#[actix_web::test]
#[ignore]
async fn jmap_mail_tests() {
    let (server, mut client, temp_dir) = init_jmap_tests::<RocksDB>("jmap_mail_tests").await;

    // Run tests
    email_changes::test(server.clone(), &mut client).await;
    email_query_changes::test(server.clone(), &mut client).await;
    email_thread::test(server.clone(), &mut client).await;
    email_thread_merge::test(server.clone(), &mut client).await;
    email_get::test(server.clone(), &mut client).await;
    email_parse::test(server.clone(), &mut client).await;
    email_set::test(server.clone(), &mut client).await;
    email_query::test(server.clone(), &mut client).await;
    email_copy::test(server.clone(), &mut client).await;
    email_submission::test(server.clone(), &mut client).await;
    email_ingest::test(server.clone(), &mut client).await;
    vacation_response::test(server.clone(), &mut client).await;
    mailbox::test(server.clone(), &mut client).await;
    search_snippet::test(server.clone(), &mut client).await;

    destroy_temp_dir(&temp_dir);
}

pub fn find_values(string: &str, name: &str) -> Vec<String> {
    let mut last_pos = 0;
    let mut values = Vec::new();

    while let Some(pos) = string[last_pos..].find(name) {
        let mut value = string[last_pos + pos + name.len()..]
            .split('"')
            .nth(1)
            .unwrap();
        if value.ends_with('\\') {
            value = &value[..value.len() - 1];
        }
        values.push(value.to_string());
        last_pos += pos + name.len();
    }

    values
}

pub fn replace_values(mut string: String, find: &[String], replace: &[String]) -> String {
    for (find, replace) in find.iter().zip(replace.iter()) {
        string = string.replace(find, replace);
    }
    string
}

pub fn replace_boundaries(string: String) -> String {
    let values = find_values(&string, "boundary=");
    if !values.is_empty() {
        replace_values(
            string,
            &values,
            &(0..values.len())
                .map(|i| format!("boundary_{}", i))
                .collect::<Vec<_>>(),
        )
    } else {
        string
    }
}

pub fn replace_blob_ids(string: String) -> String {
    let values = find_values(&string, "blobId\":");
    if !values.is_empty() {
        replace_values(
            string,
            &values,
            &(0..values.len())
                .map(|i| format!("blob_{}", i))
                .collect::<Vec<_>>(),
        )
    } else {
        string
    }
}
