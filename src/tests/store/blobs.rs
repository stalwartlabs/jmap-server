use std::{sync::Arc, time::SystemTime};

use store::{
    ahash::AHashMap,
    blob::{BlobId, BLOB_HASH_LEN},
    core::{collection::Collection, document::Document},
    serialize::{key::BlobKey, leb128::skip_leb128_value, StoreDeserialize, StoreSerialize},
    write::{
        batch::WriteBatch,
        options::{IndexOptions, Options},
    },
    ColumnFamily, Direction, JMAPStore, Store,
};

pub fn test<T>(db: Arc<JMAPStore<T>>)
where
    T: for<'x> Store<'x> + 'static,
{
    let blob_1 = vec![b'a'; 1024];
    let blob_2 = vec![b'b'; 1024];

    let blob_local = BlobId::new_local(&blob_1);
    let blob_external = BlobId::new_external(&blob_2);

    let expired_timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - (db.config.blob_temp_ttl + 2);

    // Insert the same blobs concurrently
    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap()
        .scope_fifo(|s| {
            for _ in 1..=100 {
                let db_ = db.clone();
                let blob_1 = blob_1.clone();
                let blob_local = blob_local.clone();
                s.spawn_fifo(move |_| {
                    db_.blob_store(&blob_local, blob_1).unwrap();
                });

                let db_ = db.clone();
                let blob_2 = blob_2.clone();
                let blob_external = blob_external.clone();
                s.spawn_fifo(move |_| {
                    db_.blob_store(&blob_external, blob_2).unwrap();
                    db_.blob_link_ephemeral(&blob_external, 1).unwrap();
                });
            }
        });

    // Count number of blobs
    let mut expected_count = AHashMap::from_iter([
        (blob_local.clone(), (0, 1)),
        (blob_external.clone(), (0, 2)),
    ]);
    assert_eq!(expected_count, db.get_all_blobs());

    // Purgimg should not delete any blobs at this point
    db.purge_blobs().unwrap();
    assert_eq!(expected_count, db.get_all_blobs());

    // Link blob to an account
    let mut document = Document::new(Collection::Mail, 2);
    document.blob(blob_local.clone(), IndexOptions::new());
    db.write(WriteBatch::insert(2, document)).unwrap();

    // Check expected count
    expected_count.insert(blob_local.clone(), (1, 1));
    assert_eq!(expected_count, db.get_all_blobs());

    // Expire ephemeral link to blob_local and check counts
    db.db
        .set(
            ColumnFamily::Blobs,
            &BlobKey::serialize_prefix(&blob_local, 0),
            &expired_timestamp.serialize().unwrap(),
        )
        .unwrap();
    db.purge_blobs().unwrap();
    expected_count.insert(blob_local.clone(), (1, 0));
    assert_eq!(expected_count, db.get_all_blobs());

    // Unlink blob, purge and make sure it is removed.
    let mut document = Document::new(Collection::Mail, 2);
    document.blob(blob_local.clone(), IndexOptions::new().clear());
    let mut wb = WriteBatch::new(2);
    wb.update_document(document);
    db.write(wb).unwrap();
    db.purge_blobs().unwrap();
    expected_count.remove(&blob_local);
    assert_eq!(expected_count, db.get_all_blobs());

    // Force expire both ephemeral links to blob_external
    for account_id in [0, 1] {
        db.db
            .set(
                ColumnFamily::Blobs,
                &BlobKey::serialize_prefix(&blob_external, account_id),
                &expired_timestamp.serialize().unwrap(),
            )
            .unwrap();
    }
    db.purge_blobs().unwrap();
    expected_count.remove(&blob_external);
    assert_eq!(expected_count, db.get_all_blobs());
}

trait GetAllBlobs {
    fn get_all_blobs(&self) -> AHashMap<BlobId, (u32, u32)>;
}

impl<T> GetAllBlobs for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_all_blobs(&self) -> AHashMap<BlobId, (u32, u32)> {
        let mut result = AHashMap::new();
        let mut blob_id = vec![0u8; BLOB_HASH_LEN + 1];
        let mut blob_link_count = u32::MAX;
        let mut blob_ephemeral_count = u32::MAX;

        for (key, _) in self
            .db
            .iterator(ColumnFamily::Blobs, &[], Direction::Forward)
            .unwrap()
        {
            if key[..BLOB_HASH_LEN + 1] != blob_id {
                if blob_link_count != u32::MAX {
                    result.insert(
                        BlobId::deserialize(&blob_id).unwrap(),
                        (blob_link_count, blob_ephemeral_count),
                    );
                }
                blob_link_count = 0;
                blob_ephemeral_count = 0;
                blob_id.copy_from_slice(&key[..BLOB_HASH_LEN + 1]);
            }

            if key.len() > BLOB_HASH_LEN + 1 {
                if let Some(bytes_read) = skip_leb128_value(&key[BLOB_HASH_LEN + 1..]) {
                    if key.len() == BLOB_HASH_LEN + 1 + bytes_read {
                        blob_ephemeral_count += 1;
                    } else {
                        blob_link_count += 1;
                    }
                }
            }
        }

        if blob_link_count != u32::MAX {
            result.insert(
                BlobId::deserialize(&blob_id).unwrap(),
                (blob_link_count, blob_ephemeral_count),
            );
        }

        result
    }
}
