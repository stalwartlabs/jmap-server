use std::{io::Read, iter::FromIterator, path::PathBuf};

use flate2::bufread::GzDecoder;

use jmap::orm::TinyORM;
use jmap::principal::schema::Principal;
use jmap::push_subscription::schema::PushSubscription;
use jmap_mail::email_submission::schema::EmailSubmission;
use jmap_mail::identity::schema::Identity;
use jmap_mail::mail::schema::Email;
use jmap_mail::mailbox::schema::Mailbox;
use jmap_mail::vacation_response::schema::VacationResponse;
use store::serialize::key::ValueKey;
use store::serialize::leb128::Leb128Reader;
use store::{ahash::AHashMap, blob::BLOB_HASH_LEN};
use store::{
    config::env_settings::EnvSettings,
    core::collection::Collection,
    roaring::RoaringBitmap,
    serialize::{
        key::{FOLLOWER_COMMIT_INDEX_KEY, LEADER_COMMIT_INDEX_KEY},
        StoreDeserialize,
    },
    AccountId, ColumnFamily, JMAPStore, Store,
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

pub fn make_temp_dir(name: &str, peer_num: u32) -> PathBuf {
    let mut temp_dir = std::env::temp_dir();
    temp_dir.push(format!("{}_{}", name, peer_num));
    temp_dir
}

pub fn init_settings(
    name: &str,
    peer_num: u32,
    total_peers: u32,
    delete_if_exists: bool,
) -> (EnvSettings, PathBuf) {
    let temp_dir = make_temp_dir(name, peer_num);

    if delete_if_exists && temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir).unwrap();
    }
    let mut args = AHashMap::from_iter(
        vec![
            (
                "db-path".to_string(),
                temp_dir.to_str().unwrap().to_string(),
            ),
            (
                "jmap-hostname".to_string(),
                format!("127.0.0.1:{}", 8000 + peer_num),
            ),
            ("max-objects-in-set".to_string(), "100000".to_string()),
            ("query-max-results".to_string(), "100000".to_string()),
            ("jmap-port".to_string(), (8000 + peer_num).to_string()),
            ("smtp-relay".to_string(), "!127.0.0.1:9999".to_string()),
            ("max-concurrent-uploads".to_string(), "4".to_string()),
            ("max-concurrent-requests".to_string(), "8".to_string()),
            ("push-attempt-interval".to_string(), "500".to_string()),
            ("push-throttle".to_string(), "500".to_string()),
            ("event-source-throttle".to_string(), "500".to_string()),
            ("ws-throttle".to_string(), "500".to_string()),
            ("oauth-user-code-expiry".to_string(), "1".to_string()),
            ("oauth-token-expiry".to_string(), "1".to_string()),
            ("oauth-refresh-token-expiry".to_string(), "3".to_string()),
            ("oauth-refresh-token-renew".to_string(), "2".to_string()),
            ("oauth-max-attempts".to_string(), "1".to_string()),
            ("rate-limit-anonymous".to_string(), "100/60".to_string()),
            ("rate-limit-auth".to_string(), "100/60".to_string()),
            (
                "rate-limit-authenticated".to_string(),
                "1000/60".to_string(),
            ),
        ]
        .into_iter(),
    );
    if total_peers > 1 {
        let mut pem_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        pem_dir.push("src");
        pem_dir.push("tests");
        pem_dir.push("resources");
        pem_dir.push("cert.pem");
        let cert = pem_dir.to_str().unwrap().to_string();
        pem_dir.set_file_name("key.pem");
        let key = pem_dir.to_str().unwrap().to_string();

        args.insert("rpc-key".to_string(), "secret_key".to_string());
        args.insert("rpc-cert-path".to_string(), cert);
        args.insert("rpc-key-path".to_string(), key);
        args.insert("rpc-port".to_string(), (9000 + peer_num).to_string());
        args.insert("rpc-allow-invalid-certs".to_string(), "true".to_string());
        args.insert(
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
        );
    }

    (EnvSettings { args }, temp_dir)
}

pub fn destroy_temp_dir(temp_dir: &PathBuf) {
    if temp_dir.exists() {
        std::fs::remove_dir_all(temp_dir).unwrap();
    }
}

#[allow(clippy::disallowed_types)]
pub trait StoreCompareWith<T> {
    fn compare_with(&self, other: &JMAPStore<T>)
        -> std::collections::BTreeMap<ColumnFamily, usize>;
    fn assert_is_empty(&self);
}

const ASSERT: bool = true;

#[allow(clippy::disallowed_types)]
impl<T> StoreCompareWith<T> for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn compare_with(
        &self,
        other: &JMAPStore<T>,
    ) -> std::collections::BTreeMap<ColumnFamily, usize> {
        let mut last_account_id = AccountId::MAX;
        let mut last_collection = Collection::None;
        let mut last_ids = RoaringBitmap::new();
        let mut total_keys = std::collections::BTreeMap::from_iter([
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
                        let account_id = key[key.len() - 1] as AccountId;
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
                            let (account_id, pos) = key.read_leb128().unwrap();
                            let collection = key[pos].into();
                            let (document_id, _) = (&key[pos + 1..]).read_leb128().unwrap();

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
                                            Collection::Principal => assert_eq!(
                                                TinyORM::<Principal>::deserialize(&value).unwrap(),
                                                TinyORM::<Principal>::deserialize(&other_value)
                                                    .unwrap()
                                            ),
                                            Collection::PushSubscription => assert_eq!(
                                                TinyORM::<PushSubscription>::deserialize(&value)
                                                    .unwrap(),
                                                TinyORM::<PushSubscription>::deserialize(
                                                    &other_value
                                                )
                                                .unwrap()
                                            ),
                                            Collection::Mail => assert_eq!(
                                                TinyORM::<Email>::deserialize(&value).unwrap(),
                                                TinyORM::<Email>::deserialize(&other_value)
                                                    .unwrap(),
                                                "Account {}, Document {}",
                                                account_id,
                                                document_id
                                            ),
                                            Collection::Mailbox => assert_eq!(
                                                TinyORM::<Mailbox>::deserialize(&value).unwrap(),
                                                TinyORM::<Mailbox>::deserialize(&other_value)
                                                    .unwrap(),
                                                "Account {}, Document {}",
                                                account_id,
                                                document_id
                                            ),
                                            Collection::Identity => assert_eq!(
                                                TinyORM::<Identity>::deserialize(&value).unwrap(),
                                                TinyORM::<Identity>::deserialize(&other_value)
                                                    .unwrap()
                                            ),
                                            Collection::EmailSubmission => assert_eq!(
                                                TinyORM::<EmailSubmission>::deserialize(&value)
                                                    .unwrap(),
                                                TinyORM::<EmailSubmission>::deserialize(
                                                    &other_value
                                                )
                                                .unwrap()
                                            ),
                                            Collection::VacationResponse => assert_eq!(
                                                TinyORM::<VacationResponse>::deserialize(&value)
                                                    .unwrap(),
                                                TinyORM::<VacationResponse>::deserialize(
                                                    &other_value
                                                )
                                                .unwrap()
                                            ),
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
                        if key.len() > BLOB_HASH_LEN + 1
                            && key.len()
                                > BLOB_HASH_LEN
                                    + (&key[BLOB_HASH_LEN + 1..]).read_leb128::<u32>().unwrap().1
                                    + 1
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
        let mut keys = std::collections::BTreeMap::new();
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
                        panic!(
                            "{:?} {:?}={:?} (key: {:?})",
                            cf,
                            key,
                            value,
                            String::from_utf8_lossy(&key)
                        );
                    }
                    ColumnFamily::Blobs => {
                        if key.len()
                            > BLOB_HASH_LEN
                                + (&key[BLOB_HASH_LEN..]).read_leb128::<u32>().unwrap().1
                        {
                            panic!("{:?} {:?}={:?}", cf, key, value);
                        }
                    }
                    _ => (),
                }
            }
            keys.insert(cf, total_keys);
        }

        self.id_assigner.invalidate_all();
    }
}
