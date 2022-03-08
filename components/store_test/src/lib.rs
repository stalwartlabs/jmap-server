use std::{collections::HashMap, io::Read, iter::FromIterator, path::PathBuf};

use flate2::read::GzDecoder;
use store::config::EnvSettings;

pub mod db_blobs;
pub mod db_insert_filter_sort;
pub mod db_term_id;
pub mod db_tombstones;
pub mod jmap_changes;
pub mod jmap_mail_get;
pub mod jmap_mail_merge_threads;
pub mod jmap_mail_parse;
pub mod jmap_mail_query;
pub mod jmap_mail_query_changes;
pub mod jmap_mail_set;
pub mod jmap_mail_thread;
pub mod jmap_mailbox;

pub fn deflate_artwork_data() -> Vec<u8> {
    let mut csv_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    csv_path.push("resources");
    csv_path.push("artwork_data.csv.gz");

    let mut decoder = GzDecoder::new(std::io::BufReader::new(
        std::fs::File::open(csv_path).unwrap(),
    ));
    let mut result = Vec::new();
    decoder.read_to_end(&mut result).unwrap();
    result
}

pub fn init_temp_dir(name: &str, delete_if_exists: bool) -> (EnvSettings, PathBuf) {
    let mut temp_dir = std::env::temp_dir();
    temp_dir.push(name);

    if delete_if_exists && temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
    (
        EnvSettings {
            args: HashMap::from_iter(
                vec![
                    (
                        "db-path".to_string(),
                        temp_dir.to_str().unwrap().to_string(),
                    ),
                    ("worker-pool-size".to_string(), "8".to_string()),
                ]
                .into_iter(),
            ),
        },
        temp_dir,
    )
}

pub fn destroy_temp_dir(temp_dir: PathBuf) {
    std::fs::remove_dir_all(&temp_dir).unwrap();
}
