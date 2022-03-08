use std::iter::FromIterator;

use nlp::Language;
use store::batch::WriteBatch;
use store::changelog::RaftId;
use store::field::{FieldOptions, FullText, Text};
use store::query::JMAPStoreQuery;
use store::{
    Comparator, FieldId, FieldValue, Filter, Float, Integer, JMAPStore, LongInteger, Store, Tag,
    TextQuery,
};

pub async fn tombstones<T>(db: JMAPStore<T>)
where
    T: for<'x> Store<'x> + 'static,
{
    for raw_doc_num in 0u64..10u64 {
        let mut builder =
            WriteBatch::insert(0, db.assign_document_id(0, 0).await.unwrap(), raw_doc_num);
        builder.text(
            0,
            Text::Keyword(format!("keyword_{}", raw_doc_num)),
            FieldOptions::StoreAndSort,
        );
        builder.text(
            1,
            Text::Tokenized(format!("this is the text number {}", raw_doc_num)),
            FieldOptions::StoreAndSort,
        );
        builder.text(
            2,
            Text::Full(FullText::new_lang(
                format!("and here goes the full text number {}", raw_doc_num),
                Language::English,
            )),
            FieldOptions::StoreAndSort,
        );
        builder.float(3, raw_doc_num as Float, FieldOptions::StoreAndSort);
        builder.integer(4, raw_doc_num as Integer, FieldOptions::StoreAndSort);
        builder.long_int(5, raw_doc_num as LongInteger, FieldOptions::StoreAndSort);
        builder.tag(6, Tag::Id(0), FieldOptions::None);
        builder.tag(7, Tag::Static(0), FieldOptions::None);
        builder.tag(8, Tag::Text("my custom tag".into()), FieldOptions::None);

        db.update_document(0, RaftId::default(), builder)
            .await
            .unwrap();
    }

    db.update_document(0, RaftId::default(), WriteBatch::delete(0, 9, 9u64))
        .await
        .unwrap();
    db.update_document(0, RaftId::default(), WriteBatch::delete(0, 0, 0u64))
        .await
        .unwrap();

    for do_purge in [true, false] {
        for field in 0..6 {
            assert_eq!(
                db.query(JMAPStoreQuery::new(
                    0,
                    0,
                    Filter::None,
                    Comparator::ascending(field),
                    0,
                ))
                .await
                .unwrap()
                .results,
                Vec::from_iter(1..9),
                "Field {}",
                field
            );

            for field in 0..6 {
                assert!(db
                    .get_document_value::<Vec<u8>>(0, 0, 0, field)
                    .await
                    .unwrap()
                    .is_none());
                assert!(db
                    .get_document_value::<Vec<u8>>(0, 0, 9, field)
                    .await
                    .unwrap()
                    .is_none());
                for doc_id in 1..9 {
                    assert!(db
                        .get_document_value::<Vec<u8>>(0, 0, doc_id, field)
                        .await
                        .unwrap()
                        .is_some());
                }
            }
        }

        assert_eq!(
            db.query(JMAPStoreQuery::new(
                0,
                0,
                Filter::eq(1, FieldValue::Text("text".into())),
                Comparator::None,
                0,
            ))
            .await
            .unwrap()
            .results,
            Vec::from_iter(1..9),
            "before purge: {}",
            do_purge
        );

        assert_eq!(
            db.query(JMAPStoreQuery::new(
                0,
                0,
                Filter::eq(
                    2,
                    FieldValue::FullText(TextQuery::query_english("text".into()))
                ),
                Comparator::None,
                0,
            ))
            .await
            .unwrap()
            .results,
            Vec::from_iter(1..9)
        );

        for (pos, tag) in vec![
            Tag::Id(0),
            Tag::Static(0),
            Tag::Text("my custom tag".into()),
        ]
        .into_iter()
        .enumerate()
        {
            let tags = db
                .get_tag(0, 0, 6 + pos as FieldId, tag)
                .await
                .unwrap()
                .unwrap();
            assert!(!tags.contains(0));
            assert!(!tags.contains(9));
            for doc_id in 1..9 {
                assert!(tags.contains(doc_id));
            }
        }

        if do_purge {
            assert_eq!(
                db.get_tombstoned_ids(0, 0).await.unwrap().unwrap(),
                [0, 9].iter().copied().collect()
            );
            db.purge_tombstoned(0, 0).await.unwrap();
            assert!(db.get_tombstoned_ids(0, 0).await.unwrap().is_none());
        }
    }
}
