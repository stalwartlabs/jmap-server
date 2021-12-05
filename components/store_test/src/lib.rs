use std::{
    borrow::Cow,
    sync::{Arc, Mutex}, time::Instant,
};

use nlp::Language;
use store::{
    document::{DocumentBuilder, IndexOptions, OptionValue},
    DocumentId, Store,
};

const FIELDS: [&str; 20] = [
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

pub fn insert_artworks<T, I>(db: T)
where
    T: Store<I>,
    I: IntoIterator<Item = DocumentId>,
{
    rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap()
        .scope(|s| {
            let db = Arc::new(db);
            let documents = Arc::new(Mutex::new(Vec::new()));

            for record in csv::ReaderBuilder::new()
                .has_headers(true)
                .from_path("/terastore/datasets/artwork_data.csv")
                .unwrap()
                .records()
                .into_iter()
            {
                let record = record.unwrap();
                let documents = documents.clone();
                s.spawn(move |_| {
                    let mut builder = DocumentBuilder::new();
                    for (pos, field) in record.iter().enumerate() {
                        if field.is_empty() {
                            continue;
                        }

                        match FIELDS_OPTIONS[pos] {
                            FieldType::Text => {
                                builder.add_text(
                                    pos as u8,
                                    field.to_lowercase().into(),
                                    <OptionValue>::Sortable,
                                );
                            }
                            FieldType::FullText => {
                                builder.add_full_text(
                                    pos as u8,
                                    field.to_lowercase().into(),
                                    Some(Language::English),
                                    <OptionValue>::Sortable,
                                );
                            }
                            FieldType::Integer => {
                                if let Ok(value) = field.parse::<u32>() {
                                    builder.add_integer(
                                        pos as u8,
                                        value,
                                        <OptionValue>::Sortable | <OptionValue>::Stored,
                                    );
                                }
                            }
                            FieldType::Keyword => {
                                builder.add_keyword(
                                    pos as u8,
                                    field.to_lowercase().into(),
                                    <OptionValue>::Sortable | <OptionValue>::Stored,
                                );
                            }
                        }
                    }
                    documents.lock().unwrap().push(builder);
                });
            }

            let mut documents = documents.lock().unwrap();
            let documents_len = documents.len();
            let mut document_chunk = Vec::new();

            println!("Parsed {} entries.", documents_len);

            for (pos, document) in documents.drain(..).enumerate() {
                document_chunk.push(document);
                if document_chunk.len() == 1000 || pos == documents_len - 1 {
                    let db = db.clone();
                    let chunk = document_chunk;
                    document_chunk = Vec::new();

                    s.spawn(move |_| {
                        let now = Instant::now();
                        let doc_ids = db.insert_bulk(0, 0, chunk).unwrap();
                        println!(
                            "Inserted {} entries in {} ms (Thread {}/{}).",
                            doc_ids.len(),
                            now.elapsed().as_millis(),
                            rayon::current_thread_index().unwrap(),
                            rayon::current_num_threads()
                        );
                    });
                }
            }
        });
}
