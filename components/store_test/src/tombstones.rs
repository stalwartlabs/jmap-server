use std::iter::FromIterator;

use nlp::Language;
use store::batch::DocumentWriter;
use store::field::Text;
use store::{
    Comparator, DocumentId, DocumentSet, FieldId, FieldValue, Filter, Float, Integer, LongInteger,
    Store, StoreTombstone, Tag, TextQuery,
};

pub fn test_tombstones<T>(db: T)
where
    T: for<'x> Store<'x> + StoreTombstone,
{
    let mut last_assigned_id = None;
    for raw_doc_num in 0..10 {
        last_assigned_id = Some(db.assign_document_id(0, 0, last_assigned_id).unwrap());
        let mut builder = DocumentWriter::insert(0, 0, last_assigned_id.clone().unwrap());
        builder.add_text(
            0,
            0,
            Text::Keyword(format!("keyword_{}", raw_doc_num).into()),
            true,
            true,
        );
        builder.add_text(
            1,
            0,
            Text::Tokenized(format!("this is the text number {}", raw_doc_num).into()),
            true,
            true,
        );
        builder.add_text(
            2,
            0,
            Text::Full((
                format!("and here goes the full text number {}", raw_doc_num).into(),
                Language::English,
            )),
            true,
            true,
        );
        builder.add_float(3, 0, raw_doc_num as Float, true, true);
        builder.add_integer(4, 0, raw_doc_num as Integer, true, true);
        builder.add_long_int(5, 0, raw_doc_num as LongInteger, true, true);
        builder.set_tag(6, Tag::Id(0));
        builder.set_tag(7, Tag::Static(0));
        builder.set_tag(8, Tag::Text("my custom tag".into()));

        db.update_document(builder).unwrap();
    }

    db.delete_document(0, 0, 9).unwrap();
    db.delete_document(0, 0, 0).unwrap();

    for do_purge in [true, false] {
        for field in 0..6 {
            assert_eq!(
                db.query(0, 0, Filter::None, Comparator::ascending(field))
                    .unwrap()
                    .collect::<Vec<DocumentId>>(),
                Vec::from_iter(1..9),
                "Field {}",
                field
            );

            for field in 0..6 {
                assert!(db
                    .get_document_value::<Vec<u8>>(0, 0, 0, field, 0)
                    .unwrap()
                    .is_none());
                assert!(db
                    .get_document_value::<Vec<u8>>(0, 0, 9, field, 0)
                    .unwrap()
                    .is_none());
                for doc_id in 1..9 {
                    assert!(db
                        .get_document_value::<Vec<u8>>(0, 0, doc_id, field, 0)
                        .unwrap()
                        .is_some());
                }
            }
        }

        assert_eq!(
            db.query(
                0,
                0,
                Filter::eq(1, FieldValue::Text("text".into())),
                Comparator::None
            )
            .unwrap()
            .collect::<Vec<DocumentId>>(),
            Vec::from_iter(1..9),
            "before purge: {}",
            do_purge
        );

        assert_eq!(
            db.query(
                0,
                0,
                Filter::eq(
                    2,
                    FieldValue::FullText(TextQuery::query_english("text".into()))
                ),
                Comparator::None
            )
            .unwrap()
            .collect::<Vec<DocumentId>>(),
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
            let tags = db.get_tag(0, 0, 6 + pos as FieldId, tag).unwrap().unwrap();
            assert!(!tags.contains(0));
            assert!(!tags.contains(9));
            for doc_id in 1..9 {
                assert!(tags.contains(doc_id));
            }
        }

        if do_purge {
            assert_eq!(
                db.get_tombstoned_ids(0, 0).unwrap().unwrap(),
                [0, 9].iter().copied().collect()
            );
            db.purge_tombstoned(0, 0).unwrap();
            assert!(db.get_tombstoned_ids(0, 0).unwrap().is_none());
        }
    }
}
