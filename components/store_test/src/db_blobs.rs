use std::sync::Arc;

use store::{
    batch::{Document, WriteBatch},
    serialize::StoreDeserialize,
    Collection, ColumnFamily, Direction, JMAPStore, Store, StoreError,
};

trait GetAllBlobs {
    fn get_all_blobs(&self) -> store::Result<Vec<(std::path::PathBuf, i64)>>;
}

/*
impl<T> GetAllBlobs for JMAPStore<T>
where
    T: for<'x> Store<'x> + 'static,
{
    fn get_all_blobs(&self) -> store::Result<Vec<(std::path::PathBuf, i64)>> {
        let mut result = Vec::new();

        for (key, value) in
            self.db
                .iterator(ColumnFamily::Values, BLOB_KEY_PREFIX, Direction::Forward)?
        {
            if key.starts_with(BLOB_KEY_PREFIX) {
                let value = i64::deserialize(&value).ok_or_else(|| {
                    StoreError::InternalError("Failed to convert blob key to i64".to_string())
                })?;

                let entries = BlobEntries::deserialize(&key[BLOB_KEY_PREFIX.len()..])
                    .ok_or(StoreError::DataCorruption)?;
                result.push((
                    entries
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
}*/

pub fn blobs<T>(db: JMAPStore<T>)
where
    T: for<'x> Store<'x> + 'static,
{
    //TODO implement
    /*let mut blobs = Vec::new();

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
            for account in 1..=100 {
                let db = db.clone();
                let blobs = blobs.clone();
                s.spawn_fifo(move |_| {
                    let mut document = Document::new(
                        Collection::Mail,
                        db.assign_document_id(account, Collection::Mail).unwrap(),
                    );
                    for (blob_index, blob) in
                        (&blobs[(account & 3) as usize]).iter().enumerate().rev()
                    {
                        document.binary(
                            0,
                            blob.clone(),
                            IndexOptions::new().store_blob(blob_index as BlobIndex),
                        );
                    }
                    db.write(WriteBatch::insert(account, document)).unwrap();
                });
            }
        });

    for account in 1..=100 {
        db.get_blobs(
            account,
            Collection::Mail,
            0,
            (0..10).into_iter().map(|v| (v, 0..u32::MAX)).collect(),
        )
        .unwrap()
        .into_iter()
        .for_each(|entry| {
            assert_eq!(entry.1, blobs[(account & 3) as usize][entry.0 as usize]);
        });

        db.get_blobs(
            account,
            Collection::Mail,
            0,
            (0..10).into_iter().map(|idx| (idx, 0..1)).collect(),
        )
        .unwrap()
        .into_iter()
        .for_each(|entry| {
            assert_eq!(
                entry.1,
                blobs[(account & 3) as usize][entry.0 as usize][0..1]
            );
        });
    }

    let blobs = db.get_all_blobs().unwrap();
    assert_eq!(blobs.len(), 40);

    for account in 1..=100 {
        db.write(WriteBatch::delete(account, Collection::Mail, 0, false))
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
    }*/
}
