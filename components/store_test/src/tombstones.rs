use std::iter::FromIterator;

use nlp::Language;
use store::batch::DocumentWriter;
use store::field::{FieldOptions, FullText, Text};
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
        let mut builder = DocumentWriter::insert(0, last_assigned_id.clone().unwrap());
        builder.text(
            0,
            Text::Keyword(format!("keyword_{}", raw_doc_num).into()),
            FieldOptions::StoreAndSort,
        );
        builder.text(
            1,
            Text::Tokenized(format!("this is the text number {}", raw_doc_num).into()),
            FieldOptions::StoreAndSort,
        );
        builder.text(
            2,
            Text::Full(FullText::new_lang(
                format!("and here goes the full text number {}", raw_doc_num).into(),
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

        db.update_document(0, builder, 0.into()).unwrap();
    }

    db.update_document(0, DocumentWriter::delete(0, 9), None)
        .unwrap();
    db.update_document(0, DocumentWriter::delete(0, 0), None)
        .unwrap();

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
                    .get_document_value::<Vec<u8>>(0, 0, 0, field)
                    .unwrap()
                    .is_none());
                assert!(db
                    .get_document_value::<Vec<u8>>(0, 0, 9, field)
                    .unwrap()
                    .is_none());
                for doc_id in 1..9 {
                    assert!(db
                        .get_document_value::<Vec<u8>>(0, 0, doc_id, field)
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
