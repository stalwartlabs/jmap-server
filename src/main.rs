use core::time;
use std::{collections::HashMap, fs, sync::Arc, thread};

use jmap_mail::{parse::parse_message, MailField};
use nlp::Language;
use store::{
    ComparisonOperator, Condition, FieldValue, FilterCondition, FilterOperator, Store, Tag,
};
use store_rocksdb::RocksDBStore;

fn main() {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(8)
        .build()
        .unwrap();
    let db = Arc::new(RocksDBStore::open("/terastore/db/0").unwrap());
    let mut counter = 0;

    for file_name in fs::read_dir("/terastore/mailboxes/dovecot").unwrap() {
        let file_name = file_name.as_ref().unwrap().path();
        if file_name.extension().map_or(false, |e| e != "eml") {
            continue;
        }
        let task_id = counter;
        counter += 1;
        let t_db = Arc::clone(&db);
        pool.spawn(move || {
            //if !file_name.file_name().unwrap().to_str().unwrap().starts_with("m005") {
            //    continue;
            //}
            //str::parse::<u32>(&file_name[1..file_name.len() - 4]).unwrap()
            let input = fs::read(&file_name).unwrap();
            if let Ok(builder) = parse_message(&input) {
                t_db.insert(&0, &0, builder).unwrap();
            }
            //let file_name2 = file_name.file_name().unwrap().to_str().unwrap();
            //thread::sleep(time::Duration::from_millis(str::parse::<u64>(&file_name2[1..file_name2.len() - 4]).unwrap()));
            println!("{} -> {}", task_id, file_name.display());
        });
    }

    //println!("{} {:?} {:?}", pool.current_num_threads(), pool.current_thread_has_pending_tasks(), pool.current_thread_index());

    thread::sleep(time::Duration::from_millis(1000000));

    /*let filter = FilterOperator {
        operator: store::LogicalOperator::And,
        conditions: vec![
            /*Condition::new_condition(
                MailField::HeaderField.into() + 0.into(),
                ComparisonOperator::Equal,
                FieldValue::Text(TextSearchField {
                    value: "authentication mechanism",
                    language: Language::English,
                    match_phrase: false,
                    stem: false,
                }),
            ),*/
            Condition::new_condition(
                MailField::Size.into(),
                ComparisonOperator::LowerEqualThan,
                FieldValue::Integer(1200),
            ),
        ],
    };

    db.search(&0, &0, &filter, &[]).unwrap();*/
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::Deserialize;
    use store::document::IndexOptions;
    use store::document::{DocumentBuilder, OptionValue};
    use store::Store;
    use store_rocksdb::RocksDBStore;

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

    const FIELDS_OPTIONS: [u32; 20] = [
        0,                                                // "id",
        <OptionValue>::Text | <OptionValue>::Stored, // "accession_number",
        <OptionValue>::Text | <OptionValue>::Stored, // "artist",
        <OptionValue>::Text | <OptionValue>::Stored, // "artistRole",
        0,                                                // "artistId",
        <OptionValue>::FullText | <OptionValue>::Stored,  // "title",
        <OptionValue>::FullText | <OptionValue>::Stored,  // "dateText",
        <OptionValue>::FullText | <OptionValue>::Stored,  // "medium",
        <OptionValue>::FullText | <OptionValue>::Stored,  // "creditLine",
        0,                                                // "year",
        0,                                                // "acquisitionYear",
        <OptionValue>::FullText | <OptionValue>::Stored,  // "dimensions",
        0,                                                // "width",
        0,                                                // "height",
        0,                                                // "depth",
        <OptionValue>::Text | <OptionValue>::Stored, // "units",
        <OptionValue>::FullText | <OptionValue>::Stored,  // "inscription",
        <OptionValue>::Text | <OptionValue>::Stored, // "thumbnailCopyright",
        <OptionValue>::Text | <OptionValue>::Stored, // "thumbnailUrl",
        <OptionValue>::Text | <OptionValue>::Stored, // "url",
    ];

    #[test]
    fn create_database() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(8)
            .build()
            .unwrap()
            .scope(|s| {
                let db = Arc::new(RocksDBStore::open("/terastore/db/artwork_termid2").unwrap());
                let mut reader = csv::ReaderBuilder::new()
                    .has_headers(true)
                    .from_path("/terastore/datasets/artwork_data.csv")
                    .unwrap();
                let mut count = 0;

                for record in reader.records() {
                    let record = record.unwrap();
                    let t_db = db.clone();
                    s.spawn(move |_| {
                        let mut builder = DocumentBuilder::new();
                        for (pos, field) in record.iter().enumerate() {
                            if field.is_empty() {
                                continue;
                            }
                            if FIELDS_OPTIONS[pos] == 0 {
                                if let Ok(value) = field.parse::<u32>() {
                                    builder.add_integer(
                                        pos as u8,
                                        value,
                                        <OptionValue>::Sortable | <OptionValue>::Stored,
                                    );
                                }
                            } else {
                                builder.add_text(pos as u8, field.into(), FIELDS_OPTIONS[pos]);
                                builder.add_text(
                                    pos as u8,
                                    field.to_lowercase().into(),
                                    <OptionValue>::Sortable,
                                );
                            }
                        }
                        t_db.insert(&0, &0, builder).unwrap();
                        println!("Inserted {:?}", record.get(0));
                    });
                }
            });
    }

    #[test]
    fn query_database() {
        let db = RocksDBStore::open("/terastore/db/artwork_termid").unwrap();
        println!("Okye");
    }
}

/*

Flags:
KEYWORD/MAILBOX/THREADS + [FIELD_ID|String] = Bitmap

Full text:
WORD + FIELD_ID = Bitmap

Sort:
Field + String/Date/Int = 0


*/

/*

user.keyword.<keyword> = bitmap //  has attachment, deleted, etc.
user.mailbox.<mailbox_id> = bitmap

<word>.from.<user> = bitmap
<word>.to.<user> = bitmap
<word>.cc.<user> = bitmap
<word>.bcc.<user> = bitmap
<word?>.header.<user> = bitmap


<word>.subject.<user> = bitmap
<word>.body.<user> = bitmap
<word>.text.<user> = bitmap

user.threads = bitmap
user.threads.<thread_id> = bitmap

All In Thread have keyword:
user.keyword.<keyword> AND user.threads.<thread_id> = user.threads.<thread_id>

Some In Thread have keyword:
user.keyword.<keyword> AND user.threads.<thread_id> != 0

None In Thread have keyword:
user.keyword.<keyword> AND user.threads.<thread_id> = 0



Blob
----
user.<ID>.header = serialized mail parser
user.<ID>.body.<BID> = blob body
user.<ID>.attachments.<AID> = blob attachment


Sorting
--------
user.subject.<field>.<ID> = 0
user.from.<field>.<ID> = 0
user.to.<field>.<ID> = 0
user.cc.<field>.<ID> = 0
user.size.<size>.<ID> = 0
user.received_at.<date>.<ID> = 0
user.sent_at.<date>.<ID> = 0

- Message-ID and no redID: Set thread id to ID + create thread id
- Message-ID and refID:
   * Look for parent ID in "user.message-ID.refID" INTERSECT with "user.subject.NEW_SUBJECT"
   * If there is a match, use the ThreadID from the parent + add message ID to thread ID
   * If there is no match, use the ID from the current message + create thread id


Filtering:
- Keyword
- Folder
- Date
- Size
- Thread
- HasAttachment
- Fulltext
  - From
  - To
  - Cc
  - Bcc
  - Subject
  - Body
  - Text Attachments
  - Header?

Sorting:
- receivedAt
- sentAt
- Size
- From
- To
- Subject
- Keyword









*/
