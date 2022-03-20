use std::collections::BTreeMap;
use std::{collections::HashMap, io::Read, iter::FromIterator, path::PathBuf};

use flate2::read::GzDecoder;
use jmap_mail::{MessageData, MessageOutline, MESSAGE_DATA};
use store::blob::BlobEntries;
use store::leb128::Leb128;
use store::serialize::{
    DeserializeBigEndian, IndexKey, StoreDeserialize, BLOB_KEY, LAST_TERM_ID_KEY, TEMP_BLOB_KEY,
};
use store::{
    config::EnvSettings,
    roaring::RoaringBitmap,
    serialize::{BM_KEYWORD, BM_TAG_ID, BM_TAG_STATIC, BM_TAG_TEXT},
    AccountId, ColumnFamily, JMAPStore, Store,
};
use store::{Collection, DocumentId};

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

pub fn init_settings(
    name: &str,
    peer_num: u32,
    total_peers: u32,
    delete_if_exists: bool,
) -> (EnvSettings, PathBuf) {
    let mut temp_dir = std::env::temp_dir();
    temp_dir.push(format!("{}_{}", name, peer_num));

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
                    ("cluster".to_string(), "secret_key".to_string()),
                    ("http-port".to_string(), (8000 + peer_num).to_string()),
                    ("rpc-port".to_string(), (9000 + peer_num).to_string()),
                    (
                        "seed-nodes".to_string(),
                        (1..=total_peers)
                            .filter_map(|i| {
                                if i == peer_num {
                                    None
                                } else {
                                    Some(format!("127.0.0.1:{}", (9000 + i)))
                                }
                            })
                            .collect::<Vec<_>>()
                            .join(";"),
                    ),
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

pub trait StoreCompareWith<T> {
    fn compare_with(&self, other: &JMAPStore<T>) -> BTreeMap<ColumnFamily, usize>;
}

const ASSERT: bool = true;

impl<T> StoreCompareWith<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn compare_with(&self, other: &JMAPStore<T>) -> BTreeMap<ColumnFamily, usize> {
        let mut last_account_id = AccountId::MAX;
        let mut last_collection = Collection::None;
        let mut last_ids = RoaringBitmap::new();
        let mut total_keys = BTreeMap::from_iter([
            (ColumnFamily::Bitmaps, 0),
            (ColumnFamily::Values, 0),
            (ColumnFamily::Indexes, 0),
            (ColumnFamily::Logs, 0),
        ]);

        for cf in [
            ColumnFamily::Bitmaps,
            ColumnFamily::Values,
            ColumnFamily::Indexes,
            ColumnFamily::Logs,
        ] {
            for (key, value) in self
                .db
                .iterator(cf, &[0u8], store::Direction::Forward)
                .unwrap()
            {
                match cf {
                    ColumnFamily::Bitmaps => {
                        if [BM_KEYWORD, BM_TAG_ID, BM_TAG_TEXT, BM_TAG_STATIC]
                            .contains(key.last().unwrap())
                        {
                            let (account_id, _) = AccountId::from_leb128_bytes(&key).unwrap();
                            let collection = key[key.len() - 3].into();
                            if account_id != last_account_id || last_collection != collection {
                                last_account_id = account_id;
                                last_collection = collection;
                                last_ids = self
                                    .get_document_ids(account_id, collection)
                                    .unwrap()
                                    .unwrap_or_default();
                            }
                            let mut tagged_docs = RoaringBitmap::deserialize(&value).unwrap();
                            tagged_docs &= &last_ids;
                            let mut other_tagged_docs = other
                                .db
                                .get::<RoaringBitmap>(cf, &key)
                                .unwrap()
                                .unwrap_or_default();
                            other_tagged_docs &= &last_ids;

                            if ASSERT {
                                assert_eq!(
                                    tagged_docs,
                                    other_tagged_docs,
                                    "{:?}/{}/{:?}/{} -> used ids {:?}, tombstones: {:?}",
                                    cf,
                                    account_id,
                                    collection,
                                    key.last().unwrap(),
                                    //String::from_utf8_lossy(&key[1..key.len() - 3]),
                                    self.get_document_ids_used(account_id, collection)
                                        .unwrap()
                                        .unwrap_or_default(),
                                    self.get_tombstoned_ids(account_id, collection)
                                        .unwrap()
                                        .unwrap_or_default()
                                );
                            } else if tagged_docs != other_tagged_docs {
                                println!(
                                    "{:?} != {:?} for {:?}/{}/{:?}/{} -> active ids {:?}",
                                    tagged_docs,
                                    other_tagged_docs,
                                    cf,
                                    account_id,
                                    collection,
                                    key.last().unwrap(),
                                    last_ids
                                );
                            }

                            if !tagged_docs.is_empty() {
                                *total_keys.get_mut(&cf).unwrap() += 1;
                            }
                        }
                    }
                    ColumnFamily::Values => {
                        if (0..=9).contains(&key[0])
                            && !key.starts_with(BLOB_KEY)
                            && !key.starts_with(TEMP_BLOB_KEY)
                            && &key[..] != LAST_TERM_ID_KEY
                        {
                            let (account_id, pos) = AccountId::from_leb128_bytes(&key).unwrap();
                            let collection = key[pos].into();
                            let (document_id, _) =
                                DocumentId::from_leb128_bytes(&key[pos + 1..]).unwrap();

                            if account_id != last_account_id || last_collection != collection {
                                last_account_id = account_id;
                                last_collection = collection;
                                last_ids = self
                                    .get_document_ids(account_id, collection)
                                    .unwrap()
                                    .unwrap_or_default();
                            }

                            if last_ids.contains(document_id) {
                                *total_keys.get_mut(&cf).unwrap() += 1;

                                let other_value =
                                    other.db.get::<Vec<u8>>(cf, &key).unwrap().unwrap().into();
                                if value != other_value {
                                    if key.ends_with(BLOB_KEY) {
                                        let value = BlobEntries::deserialize(&value).unwrap();
                                        let other_value =
                                            BlobEntries::deserialize(&other_value).unwrap();

                                        for (blob_index, (entry, other_entry)) in value
                                            .items
                                            .into_iter()
                                            .zip(other_value.items)
                                            .enumerate()
                                        {
                                            assert_eq!(
                                                entry.size, other_entry.size,
                                                "{:?}/{}/{:?}/{}, blob index {}",
                                                cf, account_id, collection, document_id, blob_index
                                            );

                                            if entry.hash != other_entry.hash {
                                                let blob = self
                                                    .get_blob(
                                                        account_id,
                                                        Collection::Mail,
                                                        document_id,
                                                        blob_index,
                                                    )
                                                    .unwrap()
                                                    .unwrap();
                                                let other_blob = self
                                                    .get_blob(
                                                        account_id,
                                                        Collection::Mail,
                                                        document_id,
                                                        blob_index,
                                                    )
                                                    .unwrap()
                                                    .unwrap();

                                                if collection == Collection::Mail
                                                    && blob_index == MESSAGE_DATA
                                                {
                                                    let mut this_message_data = None;
                                                    let mut this_message_outline = None;

                                                    for message_data_bytes in vec![blob, other_blob]
                                                    {
                                                        let (message_data_len, read_bytes) =
                                                            usize::from_leb128_bytes(
                                                                &message_data_bytes[..],
                                                            )
                                                            .unwrap();

                                                        let message_data =
                                                            MessageData::deserialize(
                                                                &message_data_bytes[read_bytes
                                                                    ..read_bytes
                                                                        + message_data_len],
                                                            )
                                                            .unwrap();

                                                        let message_outline =
                                                            MessageOutline::deserialize(
                                                                &message_data_bytes[read_bytes
                                                                    + message_data_len..],
                                                            )
                                                            .unwrap();

                                                        if let Some(this_message_data) =
                                                            std::mem::take(&mut this_message_data)
                                                        {
                                                            assert_eq!(
                                                                this_message_data,
                                                                message_data
                                                            );
                                                            let this_message_outline: MessageOutline =
                                                            std::mem::take(&mut this_message_outline).unwrap();

                                                            assert_eq!(
                                                                this_message_outline.received_at,
                                                                message_outline.received_at
                                                            );
                                                            assert_eq!(
                                                                this_message_outline.body_offset,
                                                                message_outline.body_offset
                                                            );
                                                            assert_eq!(
                                                                this_message_outline.headers,
                                                                message_outline.headers
                                                            );
                                                        } else {
                                                            this_message_data = Some(message_data);
                                                            this_message_outline =
                                                                Some(message_outline);
                                                        }
                                                    }
                                                } else {
                                                    assert_eq!(
                                                        blob,
                                                        other_blob,
                                                        "{:?}/{}/{:?}/{}, blob index {}",
                                                        cf,
                                                        account_id,
                                                        collection,
                                                        document_id,
                                                        blob_index
                                                    );
                                                }
                                            }
                                        }
                                    } else {
                                        panic!(
                                            "{:?}/{}/{:?}/{}, {:?} != {:?}",
                                            cf,
                                            account_id,
                                            collection,
                                            document_id,
                                            value,
                                            other_value,
                                        );
                                    }
                                }
                            }
                        }
                    }
                    ColumnFamily::Indexes => {
                        let account_id = key.as_ref().deserialize_be_u32(0).unwrap();
                        let collection = key[std::mem::size_of::<AccountId>()].into();
                        let document_id = IndexKey::deserialize_document_id(&key).unwrap();

                        if account_id != last_account_id || last_collection != collection {
                            last_account_id = account_id;
                            last_collection = collection;
                            last_ids = self
                                .get_document_ids(account_id, collection)
                                .unwrap()
                                .unwrap_or_default();
                        }

                        if last_ids.contains(document_id) {
                            *total_keys.get_mut(&cf).unwrap() += 1;

                            assert_eq!(
                                value,
                                other.db.get::<Vec<u8>>(cf, &key).unwrap().unwrap().into(),
                                "{:?}/{}/{:?}",
                                cf,
                                account_id,
                                collection
                            );
                        }
                    }
                    ColumnFamily::Logs => {
                        *total_keys.get_mut(&cf).unwrap() += 1;

                        if ASSERT {
                            assert_eq!(
                                value,
                                other.db.get::<Vec<u8>>(cf, &key).unwrap().unwrap().into(),
                                "{:?} {:?}",
                                cf,
                                key
                            );
                        } else if let Some(other_value) = other.db.get::<Vec<u8>>(cf, &key).unwrap()
                        {
                            let other_value = other_value.into_boxed_slice();
                            if value != other_value {
                                println!(
                                    "Value mismatch: {:?} -> {:?} != {:?}",
                                    key, value, other_value
                                );
                            }
                        } else {
                            println!("Missing key: {:?}", key);
                        }
                    }
                    _ => (),
                }
            }
        }
        total_keys
    }
}
