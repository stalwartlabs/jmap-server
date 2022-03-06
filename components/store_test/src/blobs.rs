use std::sync::Arc;

use store::{
    batch::WriteBatch, changelog::RaftId, field::FieldOptions, BlobEntry, Store, StoreBlobTest,
};

/*

    #[cfg(test)]
    fn get_all_blobs(&self) -> crate::Result<Vec<(std::path::PathBuf, i64)>> {
        let cf_values = self.get_handle("values")?;
        let mut result = Vec::new();

        for (key, value) in self
            .db
            .iterator_cf(&cf_values, IteratorMode::From(BLOB_KEY, Direction::Forward))
        {
            if key.starts_with(BLOB_KEY) {
                let value = i64::from_le_bytes(value.as_ref().try_into().map_err(|err| {
                    StoreError::InternalError(format!(
                        "Failed to convert blob key to i64: {:}",
                        err
                    ))
                })?);

                result.push((
                    BlobFile::new(
                        self.blob_path.clone(),
                        &key[BLOB_KEY.len()..],
                        &self.config.hash_levels,
                        false,
                    )
                    .map_err(|err| {
                        StoreError::InternalError(format!("Failed to create blob file: {:}", err))
                    })?
                    .path
                    .clone(),
                    value,
                ));
            } else {
                break;
            }
        }

        Ok(result)
    }

*/

pub fn test_blobs<T>(db: T)
where
    T: for<'x> Store<'x> + StoreBlobTest,
{
    let mut blobs = Vec::new();

    for blob_id in 0..4 {
        let mut parts = Vec::new();
        for id in 0..10 {
            parts.push(format!("{}_part_{}", blob_id, id).into_bytes());
        }
        blobs.push(parts);
    }

    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap()
        .scope_fifo(|s| {
            let db = Arc::new(&db);
            let blobs = Arc::new(&blobs);
            for account in 0..100 {
                let db = db.clone();
                let blobs = blobs.clone();
                s.spawn_fifo(move |_| {
                    let mut document =
                        WriteBatch::insert(0, db.assign_document_id(account, 0).unwrap(), 0u64);
                    for (blob_index, blob) in
                        (&blobs[(account & 3) as usize]).iter().enumerate().rev()
                    {
                        document.binary(0, blob.to_vec(), FieldOptions::StoreAsBlob(blob_index));
                    }
                    db.update_document(account, RaftId::default(), document)
                        .unwrap();
                });
            }
        });

    for account in 0..100 {
        db.get_blobs(account, 0, 0, (0..10).into_iter().map(BlobEntry::new))
            .unwrap()
            .into_iter()
            .for_each(|entry| {
                assert_eq!(entry.value, blobs[(account & 3) as usize][entry.index]);
            });

        db.get_blobs(
            account,
            0,
            0,
            (0..10)
                .into_iter()
                .map(|idx| BlobEntry::new_range(idx, 0..1)),
        )
        .unwrap()
        .into_iter()
        .for_each(|entry| {
            assert_eq!(
                entry.value,
                blobs[(account & 3) as usize][entry.index][0..1]
            );
        });
    }

    let blobs = db.get_all_blobs().unwrap();
    assert_eq!(blobs.len(), 40);

    for account in 0..100 {
        db.update_document(account, RaftId::default(), WriteBatch::delete(0, 0, 0u64))
            .unwrap();
    }

    for (_, ref_count) in db.get_all_blobs().unwrap() {
        assert_eq!(0, ref_count);
    }

    db.purge_blobs().unwrap();

    assert_eq!(db.get_all_blobs().unwrap().len(), 0);

    for (blob_path, _) in blobs {
        assert!(
            !blob_path.exists(),
            "Blob file {} should not exist",
            blob_path.display()
        );
    }
}
