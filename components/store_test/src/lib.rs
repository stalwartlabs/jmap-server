use std::{io::Read, path::PathBuf};

use flate2::read::GzDecoder;

pub mod insert_filter_sort;
pub mod jmap_changes;
pub mod jmap_mail_merge_threads;
pub mod jmap_mail_query;
pub mod jmap_mail_query_changes;
pub mod tombstones;

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
