use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

use nlp::Language;
use store::{
    batch::DocumentWriter, field::Text, Comparator, ComparisonOperator, FieldValue, Filter, Store,
    TextQuery,
};

use crate::deflate_artwork_data;

pub const FIELDS: [&str; 20] = [
    "id",
    "accession_number",
    "artist",
    "artistRole",
    "artistId",
    "title",
    "dateText",
    "medium",
    "creditLine",
    "year",
    "acquisitionYear",
    "dimensions",
    "width",
    "height",
    "depth",
    "units",
    "inscription",
    "thumbnailCopyright",
    "thumbnailUrl",
    "url",
];

enum FieldType {
    Keyword,
    Text,
    FullText,
    Integer,
}

const FIELDS_OPTIONS: [FieldType; 20] = [
    FieldType::Integer,  // "id",
    FieldType::Keyword,  // "accession_number",
    FieldType::Text,     // "artist",
    FieldType::Keyword,  // "artistRole",
    FieldType::Integer,  // "artistId",
    FieldType::FullText, // "title",
    FieldType::FullText, // "dateText",
    FieldType::FullText, // "medium",
    FieldType::FullText, // "creditLine",
    FieldType::Integer,  // "year",
    FieldType::Integer,  // "acquisitionYear",
    FieldType::FullText, // "dimensions",
    FieldType::Integer,  // "width",
    FieldType::Integer,  // "height",
    FieldType::Integer,  // "depth",
    FieldType::Text,     // "units",
    FieldType::FullText, // "inscription",
    FieldType::Text,     // "thumbnailCopyright",
    FieldType::Text,     // "thumbnailUrl",
    FieldType::Text,     // "url",
];

#[allow(clippy::mutex_atomic)]
pub fn test_insert_filter_sort<T>(db: T, do_insert: bool)
where
    T: for<'x> Store<'x>,
{
    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap()
        .scope_fifo(|s| {
            if do_insert {
                let db = Arc::new(&db);
                let now = Instant::now();
                let documents = Arc::new(Mutex::new(Vec::new()));
                let mut last_assigned_id = None;

                for record in csv::ReaderBuilder::new()
                    .has_headers(true)
                    .from_reader(&deflate_artwork_data()[..])
                    .records()
                    .into_iter()
                {
                    let record = record.unwrap();
                    let documents = documents.clone();
                    last_assigned_id = Some(db.assign_document_id(0, 0, last_assigned_id).unwrap());
                    let record_id = last_assigned_id.clone().unwrap();

                    s.spawn_fifo(move |_| {
                        let mut builder = DocumentWriter::insert(0, 0, record_id);
                        for (pos, field) in record.iter().enumerate() {
                            match FIELDS_OPTIONS[pos] {
                                FieldType::Text => {
                                    if !field.is_empty() {
                                        builder.add_text(
                                            pos as u8,
                                            0,
                                            Text::Tokenized(field.to_lowercase().into()),
                                            false,
                                            true,
                                        );
                                    }
                                }
                                FieldType::FullText => {
                                    if !field.is_empty() {
                                        builder.add_text(
                                            pos as u8,
                                            0,
                                            Text::Full((
                                                field.to_lowercase().into(),
                                                Language::English,
                                            )),
                                            false,
                                            true,
                                        );
                                    }
                                }
                                FieldType::Integer => {
                                    builder.add_integer(
                                        pos as u8,
                                        0,
                                        field.parse::<u32>().unwrap_or(0),
                                        true,
                                        true,
                                    );
                                }
                                FieldType::Keyword => {
                                    if !field.is_empty() {
                                        builder.add_text(
                                            pos as u8,
                                            0,
                                            Text::Keyword(field.to_lowercase().into()),
                                            true,
                                            true,
                                        );
                                    }
                                }
                            }
                        }
                        documents.lock().unwrap().push(builder);
                    });
                }

                let mut documents = documents.lock().unwrap();
                let documents_len = documents.len();
                let mut document_chunk = Vec::new();

                println!(
                    "Parsed {} entries in {} ms.",
                    documents_len,
                    now.elapsed().as_millis()
                );

                for (pos, document) in documents.drain(..).enumerate() {
                    document_chunk.push(document);
                    if document_chunk.len() == 1000 || pos == documents_len - 1 {
                        let db = db.clone();
                        let chunk = document_chunk;
                        document_chunk = Vec::new();

                        s.spawn_fifo(move |_| {
                            let now = Instant::now();
                            let num_docs = chunk.len();
                            db.update_documents(chunk).unwrap();
                            println!(
                                "Inserted {} entries in {} ms (Thread {}/{}).",
                                num_docs,
                                now.elapsed().as_millis(),
                                rayon::current_thread_index().unwrap(),
                                rayon::current_num_threads()
                            );
                        });
                    }
                }
            }
        });

    println!("Running filter tests...");
    test_filter(&db);

    println!("Running sort tests...");
    test_sort(&db);
}

pub fn test_filter<'x, T: Store<'x>>(db: &'x T) {
    let mut fields = HashMap::new();
    for (field_num, field) in FIELDS.iter().enumerate() {
        fields.insert(field.to_string(), field_num as u8);
    }

    let tests = [
        (
            Filter::and(vec![
                Filter::new_condition(
                    fields["title"],
                    ComparisonOperator::Equal,
                    FieldValue::FullText(TextQuery::query_english("water".into())),
                ),
                Filter::new_condition(
                    fields["year"],
                    ComparisonOperator::Equal,
                    FieldValue::Integer(1979),
                ),
            ]),
            vec!["p11293"],
        ),
        (
            Filter::and(vec![
                Filter::new_condition(
                    fields["medium"],
                    ComparisonOperator::Equal,
                    FieldValue::FullText(TextQuery::query_english("gelatin".into())),
                ),
                Filter::new_condition(
                    fields["year"],
                    ComparisonOperator::GreaterThan,
                    FieldValue::Integer(2000),
                ),
                Filter::new_condition(
                    fields["width"],
                    ComparisonOperator::LowerThan,
                    FieldValue::Integer(180),
                ),
                Filter::new_condition(
                    fields["width"],
                    ComparisonOperator::GreaterThan,
                    FieldValue::Integer(0),
                ),
            ]),
            vec!["p79426", "p79427", "p79428", "p79429", "p79430"],
        ),
        (
            Filter::and(vec![Filter::new_condition(
                fields["title"],
                ComparisonOperator::Equal,
                FieldValue::FullText(TextQuery::query_english("'rustic bridge'".into())),
            )]),
            vec!["d05503"],
        ),
        (
            Filter::and(vec![
                Filter::new_condition(
                    fields["title"],
                    ComparisonOperator::Equal,
                    FieldValue::FullText(TextQuery::query_english("'rustic'".into())),
                ),
                Filter::new_condition(
                    fields["title"],
                    ComparisonOperator::Equal,
                    FieldValue::FullText(TextQuery::query_english("study".into())),
                ),
            ]),
            vec!["d00399", "d05352"],
        ),
        (
            Filter::and(vec![
                Filter::new_condition(
                    fields["artist"],
                    ComparisonOperator::Equal,
                    FieldValue::Text("mauro kunst".into()),
                ),
                Filter::new_condition(
                    fields["artistRole"],
                    ComparisonOperator::Equal,
                    FieldValue::Keyword("artist".into()),
                ),
                Filter::or(vec![
                    Filter::new_condition(
                        fields["year"],
                        ComparisonOperator::Equal,
                        FieldValue::Integer(1969),
                    ),
                    Filter::new_condition(
                        fields["year"],
                        ComparisonOperator::Equal,
                        FieldValue::Integer(1971),
                    ),
                ]),
            ]),
            vec!["p01764", "t05843"],
        ),
        (
            Filter::and(vec![
                Filter::not(vec![Filter::new_condition(
                    fields["medium"],
                    ComparisonOperator::Equal,
                    FieldValue::FullText(TextQuery::query_english("oil".into())),
                )]),
                Filter::new_condition(
                    fields["creditLine"],
                    ComparisonOperator::Equal,
                    FieldValue::FullText(TextQuery::query_english("bequeath".into())),
                ),
                Filter::or(vec![
                    Filter::and(vec![
                        Filter::new_condition(
                            fields["year"],
                            ComparisonOperator::GreaterEqualThan,
                            FieldValue::Integer(1900),
                        ),
                        Filter::new_condition(
                            fields["year"],
                            ComparisonOperator::LowerThan,
                            FieldValue::Integer(1910),
                        ),
                    ]),
                    Filter::and(vec![
                        Filter::new_condition(
                            fields["year"],
                            ComparisonOperator::GreaterEqualThan,
                            FieldValue::Integer(2000),
                        ),
                        Filter::new_condition(
                            fields["year"],
                            ComparisonOperator::LowerThan,
                            FieldValue::Integer(2010),
                        ),
                    ]),
                ]),
            ]),
            vec![
                "n02478", "n02479", "n03568", "n03658", "n04327", "n04328", "n04721", "n04739",
                "n05095", "n05096", "n05145", "n05157", "n05158", "n05159", "n05298", "n05303",
                "n06070", "t01181", "t03571", "t05805", "t05806", "t12147", "t12154", "t12155",
            ],
        ),
        (
            Filter::and(vec![
                Filter::new_condition(
                    fields["artist"],
                    ComparisonOperator::Equal,
                    FieldValue::Text("warhol".into()),
                ),
                Filter::not(vec![Filter::new_condition(
                    fields["title"],
                    ComparisonOperator::Equal,
                    FieldValue::FullText(TextQuery::query_english("'campbell'".into())),
                )]),
                Filter::not(vec![Filter::or(vec![
                    Filter::new_condition(
                        fields["year"],
                        ComparisonOperator::GreaterThan,
                        FieldValue::Integer(1980),
                    ),
                    Filter::and(vec![
                        Filter::new_condition(
                            fields["width"],
                            ComparisonOperator::GreaterThan,
                            FieldValue::Integer(500),
                        ),
                        Filter::new_condition(
                            fields["height"],
                            ComparisonOperator::GreaterThan,
                            FieldValue::Integer(500),
                        ),
                    ]),
                ])]),
                Filter::new_condition(
                    fields["acquisitionYear"],
                    ComparisonOperator::Equal,
                    FieldValue::Integer(2008),
                ),
            ]),
            vec!["ar00039", "t12600"],
        ),
        (
            Filter::and(vec![
                Filter::new_condition(
                    fields["title"],
                    ComparisonOperator::Equal,
                    FieldValue::FullText(TextQuery::query_english("study".into())),
                ),
                Filter::new_condition(
                    fields["medium"],
                    ComparisonOperator::Equal,
                    FieldValue::FullText(TextQuery::query_english("paper".into())),
                ),
                Filter::new_condition(
                    fields["creditLine"],
                    ComparisonOperator::Equal,
                    FieldValue::FullText(TextQuery::query_english("'purchased'".into())),
                ),
                Filter::not(vec![
                    Filter::new_condition(
                        fields["title"],
                        ComparisonOperator::Equal,
                        FieldValue::FullText(TextQuery::query_english("'anatomical'".into())),
                    ),
                    Filter::new_condition(
                        fields["title"],
                        ComparisonOperator::Equal,
                        FieldValue::FullText(TextQuery::query_english("'for'".into())),
                    ),
                ]),
                Filter::new_condition(
                    fields["year"],
                    ComparisonOperator::GreaterThan,
                    FieldValue::Integer(1900),
                ),
                Filter::new_condition(
                    fields["acquisitionYear"],
                    ComparisonOperator::GreaterThan,
                    FieldValue::Integer(2000),
                ),
            ]),
            vec![
                "p80042", "p80043", "p80044", "p80045", "p80203", "t11937", "t12172",
            ],
        ),
    ];

    for (filter, expected_results) in tests {
        let mut results: Vec<String> = Vec::with_capacity(expected_results.len());

        for doc_id in db
            .query(
                0,
                0,
                filter,
                Comparator::ascending(fields["accession_number"]),
            )
            .unwrap()
        {
            results.push(
                db.get_document_value(0, 0, doc_id, fields["accession_number"], 0)
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(results, expected_results);
    }
}

pub fn test_sort<'x, T: Store<'x>>(db: &'x T) {
    let mut fields = HashMap::new();
    for (field_num, field) in FIELDS.iter().enumerate() {
        fields.insert(field.to_string(), field_num as u8);
    }

    let tests = [
        (
            Filter::and(vec![
                Filter::gt(fields["year"], FieldValue::Integer(0)),
                Filter::gt(fields["acquisitionYear"], FieldValue::Integer(0)),
                Filter::gt(fields["width"], FieldValue::Integer(0)),
            ]),
            vec![
                Comparator::descending(fields["year"]),
                Comparator::ascending(fields["acquisitionYear"]),
                Comparator::ascending(fields["width"]),
                Comparator::descending(fields["accession_number"]),
            ],
            vec![
                "t13655", "t13811", "p13352", "p13351", "p13350", "p13349", "p13348", "p13347",
                "p13346", "p13345", "p13344", "p13342", "p13341", "p13340", "p13339", "p13338",
                "p13337", "p13336", "p13335", "p13334", "p13333", "p13332", "p13331", "p13330",
                "p13329", "p13328", "p13327", "p13326", "p13325", "p13324", "p13323", "t13786",
                "p13322", "p13321", "p13320", "p13319", "p13318", "p13317", "p13316", "p13315",
                "p13314", "t13588", "t13587", "t13586", "t13585", "t13584", "t13540", "t13444",
                "ar01154", "ar01153",
            ],
        ),
        (
            Filter::and(vec![
                Filter::gt(fields["width"], FieldValue::Integer(0)),
                Filter::gt(fields["height"], FieldValue::Integer(0)),
            ]),
            vec![
                Comparator::descending(fields["width"]),
                Comparator::ascending(fields["height"]),
            ],
            vec![
                "t03681", "t12601", "ar00166", "t12625", "t12915", "p04182", "t06483", "ar00703",
                "t07671", "ar00021", "t05557", "t07918", "p06298", "p05465", "p06640", "t12855",
                "t01355", "t12800", "t12557", "t02078",
            ],
        ),
        (
            Filter::None,
            vec![
                Comparator::descending(fields["medium"]),
                Comparator::descending(fields["artistRole"]),
                Comparator::ascending(fields["accession_number"]),
            ],
            vec![
                "ar00627", "ar00052", "t00352", "t07275", "t12318", "t04931", "t13683", "t13686",
                "t13687", "t13688", "t13689", "t13690", "t13691", "t07766", "t07918", "t12993",
                "ar00044", "t13326", "t07614", "t12414",
            ],
        ),
    ];

    for (filter, sort, expected_results) in tests {
        let mut results: Vec<String> = Vec::with_capacity(expected_results.len());

        for doc_id in db.query(0, 0, filter, Comparator::List(sort)).unwrap() {
            let val = db
                .get_document_value(0, 0, doc_id, fields["accession_number"], 0)
                .unwrap()
                .unwrap();
            results.push(val);

            if results.len() == expected_results.len() {
                break;
            }
        }
        assert_eq!(results, expected_results);
    }
}
