use std::sync::Arc;

use store::{
    batch::WriteBatch,
    blob::BlobEntries,
    changelog::RaftId,
    field::FieldOptions,
    futures::future::join_all,
    parking_lot::RwLock,
    serialize::{StoreDeserialize, BLOB_KEY},
    tokio, ColumnFamily, Direction, JMAPStore, Store, StoreError,
};

trait GetAllBlobs {
    fn get_all_blobs(&self) -> store::Result<Vec<(std::path::PathBuf, i64)>>;
}

impl<T> GetAllBlobs for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_all_blobs(&self) -> store::Result<Vec<(std::path::PathBuf, i64)>> {
        let mut result = Vec::new();

        for (key, value) in
            self.db
                .iterator(ColumnFamily::Values, BLOB_KEY.to_vec(), Direction::Forward)?
        {
            if key.starts_with(BLOB_KEY) {
                let value = i64::deserialize(&value).ok_or_else(|| {
                    StoreError::InternalError("Failed to convert blob key to i64".to_string())
                })?;

                result.push((
                    BlobEntries::deserialize(&key[BLOB_KEY.len()..])
                        .ok_or(StoreError::DataCorruption)?
                        .items
                        .get(0)
                        .ok_or(StoreError::DataCorruption)?
                        .as_path(
                            self.config.blob_base_path.clone(),
                            &self.config.blob_hash_levels,
                        )?,
                    value,
                ));
            } else {
                break;
            }
        }

        Ok(result)
    }
}

pub async fn blobs<T>(db: JMAPStore<T>)
where
    T: for<'x> Store<'x> + 'static,
{
    let mut blobs = Vec::new();

    for blob_id in 0..4 {
        let mut parts = Vec::new();
        for id in 0..10 {
            parts.push(format!("{}_part_{}", blob_id, id).into_bytes());
        }
        blobs.push(parts);
    }
    let blobs = Arc::new(RwLock::new(blobs));

    let db = Arc::new(db);
    let _blobs = blobs.clone();
    let mut futures = Vec::new();
    for account in 0..100 {
        let db = db.clone();
        let blobs = blobs.clone();
        futures.push(tokio::spawn(async move {
            let mut document =
                WriteBatch::insert(0, db.assign_document_id(account, 0).await.unwrap(), 0u64);
            for (blob_index, blob) in (blobs.read()[(account & 3) as usize])
                .iter()
                .enumerate()
                .rev()
            {
                document.binary(0, blob.to_vec(), FieldOptions::StoreAsBlob(blob_index));
            }
            db.update_document(account, RaftId::default(), document)
                .await
                .unwrap();
        }));
    }

    join_all(futures).await;

    for account in 0..100 {
        db.get_blobs(
            account,
            0,
            0,
            (0..10).into_iter().map(|v| (v, 0..u32::MAX)).collect(),
        )
        .await
        .unwrap()
        .into_iter()
        .for_each(|entry| {
            assert_eq!(entry.1, blobs.read()[(account & 3) as usize][entry.0]);
        });

        db.get_blobs(
            account,
            0,
            0,
            (0..10).into_iter().map(|idx| (idx, 0..1)).collect(),
        )
        .await
        .unwrap()
        .into_iter()
        .for_each(|entry| {
            assert_eq!(entry.1, blobs.read()[(account & 3) as usize][entry.0][0..1]);
        });
    }

    let blobs = db.get_all_blobs().unwrap();
    assert_eq!(blobs.len(), 40);

    for account in 0..100 {
        db.update_document(account, RaftId::default(), WriteBatch::delete(0, 0, 0u64))
            .await
            .unwrap();
    }

    for (_, ref_count) in db.get_all_blobs().unwrap() {
        assert_eq!(0, ref_count);
    }

    db.purge_blobs().await.unwrap();

    assert_eq!(db.get_all_blobs().unwrap().len(), 0);

    for (blob_path, _) in blobs {
        assert!(
            !blob_path.exists(),
            "Blob file {} should not exist",
            blob_path.display()
        );
    }
}
