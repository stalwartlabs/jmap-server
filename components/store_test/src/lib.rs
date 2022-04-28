use std::collections::BTreeMap;
use std::{collections::HashMap, io::Read, iter::FromIterator, path::PathBuf};

use flate2::read::GzDecoder;

use jmap::protocol::json::JSONValue;
use jmap_mail::mail::{MessageData, MessageOutline};
use store::leb128::Leb128;
use store::serialize::{
    DeserializeBigEndian, IndexKey, LogKey, StoreDeserialize, ValueKey, FOLLOWER_COMMIT_INDEX_KEY,
    LEADER_COMMIT_INDEX_KEY,
};
use store::term_index::TermIndex;
use store::{
    config::EnvSettings, roaring::RoaringBitmap, AccountId, ColumnFamily, JMAPStore, Store,
};
use store::{log, Collection, DocumentId};

pub mod db_blobs;
pub mod db_insert_filter_sort;
pub mod db_log;
pub mod jmap_changes;
pub mod jmap_mail_get;
pub mod jmap_mail_merge_threads;
pub mod jmap_mail_parse;
pub mod jmap_mail_query;
pub mod jmap_mail_query_changes;
pub mod jmap_mail_set;
pub mod jmap_mail_thread;
pub mod jmap_mailbox;

#[derive(Debug, Clone)]
pub struct JMAPComparator<T> {
    pub property: T,
    pub is_ascending: bool,
    pub collation: Option<String>,
}

impl<T> JMAPComparator<T> {
    pub fn ascending(property: T) -> Self {
        Self {
            property,
            is_ascending: true,
            collation: None,
        }
    }

    pub fn descending(property: T) -> Self {
        Self {
            property,
            is_ascending: false,
            collation: None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum JMAPLogicalOperator {
    And,
    Or,
    Not,
}

#[derive(Debug, Clone)]
pub struct JMAPFilterOperator<T> {
    pub operator: JMAPLogicalOperator,
    pub conditions: Vec<JMAPFilter<T>>,
}

#[derive(Debug, Clone)]
pub enum JMAPFilter<T> {
    Condition(T),
    Operator(JMAPFilterOperator<T>),
    None,
}

impl<T> Default for JMAPFilter<T> {
    fn default() -> Self {
        JMAPFilter::None
    }
}

impl<T> JMAPFilter<T> {
    pub fn condition(cond: T) -> Self {
        JMAPFilter::Condition(cond)
    }

    pub fn and(conditions: Vec<JMAPFilter<T>>) -> Self {
        JMAPFilter::Operator(JMAPFilterOperator {
            operator: JMAPLogicalOperator::And,
            conditions,
        })
    }

    pub fn or(conditions: Vec<JMAPFilter<T>>) -> Self {
        JMAPFilter::Operator(JMAPFilterOperator {
            operator: JMAPLogicalOperator::Or,
            conditions,
        })
    }

    pub fn not(conditions: Vec<JMAPFilter<T>>) -> Self {
        JMAPFilter::Operator(JMAPFilterOperator {
            operator: JMAPLogicalOperator::Not,
            conditions,
        })
    }
}

impl<T> From<JMAPFilterOperator<T>> for JSONValue
where
    JSONValue: From<T>,
{
    fn from(filter: JMAPFilterOperator<T>) -> Self {
        let mut map = HashMap::new();
        map.insert(
            "operator".to_string(),
            match filter.operator {
                JMAPLogicalOperator::And => "AND".to_string().into(),
                JMAPLogicalOperator::Or => "OR".to_string().into(),
                JMAPLogicalOperator::Not => "NOT".to_string().into(),
            },
        );
        map.insert(
            "conditions".to_string(),
            filter
                .conditions
                .into_iter()
                .map(|c| c.into())
                .collect::<Vec<_>>()
                .into(),
        );
        map.into()
    }
}

impl<T> From<JMAPFilter<T>> for JSONValue
where
    JSONValue: From<T>,
{
    fn from(filter: JMAPFilter<T>) -> Self {
        match filter {
            JMAPFilter::Condition(cond) => cond.into(),
            JMAPFilter::Operator(op) => op.into(),
            JMAPFilter::None => JSONValue::Null,
        }
    }
}

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
        if total_peers > 1 {
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
            }
        } else {
            EnvSettings {
                args: HashMap::from_iter(
                    vec![(
                        "db-path".to_string(),
                        temp_dir.to_str().unwrap().to_string(),
                    )]
                    .into_iter(),
                ),
            }
        },
        temp_dir,
    )
}

pub fn destroy_temp_dir(temp_dir: PathBuf) {
    std::fs::remove_dir_all(&temp_dir).unwrap();
}

pub trait StoreCompareWith<T> {
    fn compare_with(&self, other: &JMAPStore<T>) -> BTreeMap<ColumnFamily, usize>;
    fn assert_is_empty(&self);
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
            (ColumnFamily::Blobs, 0),
        ]);

        for cf in [
            ColumnFamily::Bitmaps,
            ColumnFamily::Values,
            ColumnFamily::Indexes,
            ColumnFamily::Logs,
            ColumnFamily::Blobs,
        ] {
            for (key, value) in self
                .db
                .iterator(cf, &[0u8], store::Direction::Forward)
                .unwrap()
            {
                match cf {
                    ColumnFamily::Bitmaps => {
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
                                "{:?}/{}/{:?}/{} -> used ids {:?}",
                                cf,
                                account_id,
                                collection,
                                key.last().unwrap(),
                                //String::from_utf8_lossy(&key[1..key.len() - 3]),
                                self.get_document_ids(account_id, collection)
                                    .unwrap()
                                    .unwrap_or_default(),
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
                    ColumnFamily::Values => {
                        if (0..=9).contains(&key[0])
                            && &key[..] != FOLLOWER_COMMIT_INDEX_KEY
                            && &key[..] != LEADER_COMMIT_INDEX_KEY
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
                                    if key
                                        == ValueKey::serialize_term_index(
                                            account_id,
                                            collection,
                                            document_id,
                                        )
                                        .into_boxed_slice()
                                    {
                                        let value = TermIndex::deserialize(&value).unwrap();
                                        let other_value =
                                            TermIndex::deserialize(&other_value).unwrap();
                                        assert_eq!(
                                            value.items.len(),
                                            other_value.items.len(),
                                            "{:?} != {:?}",
                                            value,
                                            other_value
                                        );
                                        for (item, other_item) in
                                            value.items.iter().zip(other_value.items.iter())
                                        {
                                            assert_eq!(
                                                item.field_id, other_item.field_id,
                                                "{:?} != {:?}",
                                                value, other_value
                                            );
                                            assert_eq!(
                                                item.part_id, other_item.part_id,
                                                "{:?} != {:?}",
                                                value, other_value
                                            );
                                            assert_eq!(
                                                item.terms_len, other_item.terms_len,
                                                "{:?} != {:?}",
                                                value, other_value
                                            );
                                        }
                                    } else if ASSERT {
                                        panic!(
                                            "{:?}/{}/{:?}/{}, key[{:?}] {:?} != {:?}",
                                            cf,
                                            account_id,
                                            collection,
                                            document_id,
                                            key,
                                            value,
                                            other_value,
                                        );
                                    } else {
                                        println!(
                                            "{:?}/{}/{:?}/{}, key[{:?}] {:?} != {:?}",
                                            cf,
                                            account_id,
                                            collection,
                                            document_id,
                                            key,
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
                        if let Some(other_value) = other.db.get::<Vec<u8>>(cf, &key).unwrap() {
                            if key.starts_with(&[LogKey::RAFT_KEY_PREFIX]) {
                                let entry = log::Entry::deserialize(&value).unwrap();
                                let other_entry = log::Entry::deserialize(&other_value).unwrap();
                                let mut do_panic = false;
                                match (&entry, &other_entry) {
                                    (
                                        log::Entry::Snapshot { changed_accounts },
                                        log::Entry::Snapshot {
                                            changed_accounts: other_changed_accounts,
                                        },
                                    ) => {
                                        for changed_account in changed_accounts {
                                            if !other_changed_accounts.contains(changed_account) {
                                                do_panic = true;
                                                break;
                                            }
                                        }
                                        if !do_panic {
                                            for other_changed_account in other_changed_accounts {
                                                if !changed_accounts.contains(other_changed_account)
                                                {
                                                    do_panic = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                    _ => {
                                        do_panic = value[..] != other_value[..];
                                    }
                                }
                                if do_panic {
                                    if ASSERT {
                                        panic!(
                                            "Raft entry mismatch: {:?} -> {:?} != {:?}",
                                            key, entry, other_entry
                                        );
                                    } else {
                                        println!(
                                            "Raft entry mismatch: {:?} -> {:?} != {:?}",
                                            key, entry, other_entry
                                        );
                                    }
                                }
                            } else if ASSERT {
                                assert_eq!(value, other_value.into(), "{:?} {:?}", cf, key);
                            } else {
                                let other_value = other_value.into_boxed_slice();
                                if value != other_value {
                                    println!(
                                        "Value mismatch: {:?} -> {:?} != {:?}",
                                        key, value, other_value
                                    );
                                }
                            }
                        } else if ASSERT {
                            panic!("Missing log key: [{:?}]", key);
                        } else {
                            println!("Missing log key: [{:?}]", key);
                        };
                    }
                    ColumnFamily::Blobs => {
                        *total_keys.get_mut(&cf).unwrap() += 1;
                        if let Some(other_value) = other.db.get::<Vec<u8>>(cf, &key).unwrap() {
                            if ASSERT {
                                assert_eq!(value, other_value.into(), "{:?} {:?}", cf, key);
                            } else {
                                let other_value = other_value.into_boxed_slice();
                                if value != other_value {
                                    println!(
                                        "Blob mismatch: {:?} -> {:?} != {:?}",
                                        key, value, other_value
                                    );
                                }
                            }
                        } else if ASSERT {
                            panic!("Missing Blob key: [{:?}]", key);
                        } else {
                            println!("Missing Blob key: [{:?}]", key);
                        };
                    }
                    _ => (),
                }
            }
        }
        total_keys
    }

    fn assert_is_empty(&self) {
        let mut keys = BTreeMap::new();
        for cf in [
            ColumnFamily::Bitmaps,
            ColumnFamily::Values,
            ColumnFamily::Indexes,
        ] {
            let mut total_keys = 0;
            for (key, value) in self
                .db
                .iterator(cf, &[0u8], store::Direction::Forward)
                .unwrap()
            {
                total_keys += 1;
                match cf {
                    ColumnFamily::Bitmaps => {
                        assert_eq!(
                            RoaringBitmap::deserialize(&value).unwrap(),
                            RoaringBitmap::new(),
                            "{:?}",
                            key
                        );
                    }
                    ColumnFamily::Values if (0..=9).contains(&key[0]) => {
                        panic!("{:?} {:?}={:?}", cf, key, value);
                    }
                    ColumnFamily::Indexes => {
                        panic!("{:?} {:?}={:?}", cf, key, value);
                    }
                    ColumnFamily::Blobs => {
                        panic!("{:?} {:?}={:?}", cf, key, value);
                    }
                    _ => (),
                }
            }
            keys.insert(cf, total_keys);
        }
        //println!("Store is empty: {:?}", keys);
    }
}
