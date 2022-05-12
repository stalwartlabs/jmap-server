use std::{
    collections::{BTreeMap, HashMap},
    io::Read,
    iter::FromIterator,
    path::PathBuf,
};

use flate2::bufread::GzDecoder;
use jmap::{jmap_store::orm::TinyORM, protocol::json::JSONValue};
use jmap_mail::{mail::MessageField, mailbox::MailboxProperty};
use store::{
    blob::BLOB_HASH_LEN,
    serialize::{key::ValueKey, leb128::Leb128},
};
use store::{
    config::env_settings::EnvSettings,
    core::collection::Collection,
    roaring::RoaringBitmap,
    serialize::{
        key::{BM_DOCUMENT_IDS, FOLLOWER_COMMIT_INDEX_KEY, LEADER_COMMIT_INDEX_KEY},
        StoreDeserialize,
    },
    AccountId, ColumnFamily, DocumentId, JMAPStore, Store,
};
use store::{
    log,
    serialize::{
        key::{IndexKey, LogKey},
        DeserializeBigEndian,
    },
};

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
    csv_path.push("src");
    csv_path.push("tests");
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
                        (
                            "jmap-url".to_string(),
                            format!("http://127.0.0.1:{}", 8000 + peer_num),
                        ),
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
                    vec![
                        (
                            "db-path".to_string(),
                            temp_dir.to_str().unwrap().to_string(),
                        ),
                        (
                            "jmap-url".to_string(),
                            format!("http://127.0.0.1:{}", 8000 + peer_num),
                        ),
                        ("http-port".to_string(), (8000 + peer_num).to_string()),
                    ]
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
                        let (account_id, collection) = if key.last().unwrap() == &BM_DOCUMENT_IDS {
                            (key[0] as AccountId, key[1].into())
                        } else {
                            (key[key.len() - 3] as AccountId, key[key.len() - 3].into())
                        };
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

                                let other_value = if let Some(other_value) =
                                    other.db.get::<Vec<u8>>(cf, &key).unwrap()
                                {
                                    other_value.into()
                                } else if ASSERT {
                                    panic!("Missing value key: {:?}", key);
                                } else {
                                    println!("Missing value key: {:?}", key);
                                    continue;
                                };

                                if value != other_value {
                                    if key
                                        == ValueKey::serialize_value(
                                            account_id,
                                            collection,
                                            document_id,
                                            255,
                                        )
                                        .into_boxed_slice()
                                    {
                                        match collection {
                                            Collection::Account => todo!(),
                                            Collection::PushSubscription => todo!(),
                                            Collection::Mail => assert_eq!(
                                                TinyORM::<MessageField>::deserialize(&value)
                                                    .unwrap(),
                                                TinyORM::<MessageField>::deserialize(&other_value)
                                                    .unwrap()
                                            ),
                                            Collection::Mailbox => assert_eq!(
                                                TinyORM::<MailboxProperty>::deserialize(&value)
                                                    .unwrap(),
                                                TinyORM::<MailboxProperty>::deserialize(
                                                    &other_value
                                                )
                                                .unwrap()
                                            ),
                                            Collection::Identity => todo!(),
                                            Collection::EmailSubmission => todo!(),
                                            Collection::VacationResponse => todo!(),
                                            Collection::Thread | Collection::None => unreachable!(),
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

                            let other_value = if let Some(other_value) =
                                other.db.get::<Vec<u8>>(cf, &key).unwrap()
                            {
                                other_value.into()
                            } else if ASSERT {
                                panic!("Missing index key: {:?}", key);
                            } else {
                                println!("Missing index key: {:?}", key);
                                continue;
                            };

                            assert_eq!(
                                value, other_value,
                                "{:?}/{}/{:?}",
                                cf, account_id, collection
                            );
                        }
                    }
                    ColumnFamily::Logs => {
                        *total_keys.get_mut(&cf).unwrap() += 1;
                        if let Some(other_value) = other.db.get::<Vec<u8>>(cf, &key).unwrap() {
                            if key.starts_with(&[LogKey::RAFT_KEY_PREFIX]) {
                                let entry = log::entry::Entry::deserialize(&value).unwrap();
                                let other_entry =
                                    log::entry::Entry::deserialize(&other_value).unwrap();
                                let mut do_panic = false;
                                match (&entry, &other_entry) {
                                    (
                                        log::entry::Entry::Snapshot { changed_accounts },
                                        log::entry::Entry::Snapshot {
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
                        if key.len()
                            > BLOB_HASH_LEN
                                + u32::from_leb128_bytes(&key[BLOB_HASH_LEN..]).unwrap().1
                        {
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
                            }
                        }
                    }
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
                        if key.len()
                            > BLOB_HASH_LEN
                                + u32::from_leb128_bytes(&key[BLOB_HASH_LEN..]).unwrap().1
                        {
                            panic!("{:?} {:?}={:?}", cf, key, value);
                        }
                    }
                    _ => (),
                }
            }
            keys.insert(cf, total_keys);
        }
        //println!("Store is empty: {:?}", keys);
    }
}
