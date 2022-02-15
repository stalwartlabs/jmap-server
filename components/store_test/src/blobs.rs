use std::sync::Arc;

use store::{batch::DocumentWriter, field::FieldOptions, BlobEntry, Store, StoreBlobTest};

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
                        DocumentWriter::insert(0, db.assign_document_id(account, 0).unwrap());
                    for (blob_index, blob) in
                        (&blobs[(account & 3) as usize]).iter().enumerate().rev()
                    {
                        document.binary(0, blob.into(), FieldOptions::StoreAsBlob(blob_index));
                    }
                    db.update_document(account, document).unwrap();
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
        db.update_document(account, DocumentWriter::delete(0, 0))
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
