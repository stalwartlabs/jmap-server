use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
};

use jmap_mail::{
    query::{JMAPMailComparator, JMAPMailFilterCondition, MailboxId},
    JMAPMailStoreGet, JMAPMailStoreImport, JMAPMailStoreQuery, MessageField,
};
use jmap_store::{local_store::JMAPLocalStore, JMAPComparator, JMAPFilter, JMAPQuery, JMAP_MAIL};
use store::{Store, Tag};

use crate::insert_filter_sort::FIELDS;

pub fn test_jmap_mail_query<T>(db: T, do_insert: bool)
where
    T: for<'x> Store<'x>,
{
    const MAX_THREADS: usize = 100;
    const MAX_MESSAGES: usize = 1000;
    const MAX_MESSAGES_PER_THREAD: usize = 100;

    let mail_store = JMAPLocalStore::new(db);

    if do_insert {
        let mut fields = HashMap::new();
        for (field_num, field) in FIELDS.iter().enumerate() {
            fields.insert(field.to_string(), field_num);
        }

        let mut total_messages = 0;
        let mut total_threads = 0;
        let mut thread_count = HashMap::new();
        let mut artist_count = HashMap::new();

        /*println!(
            "artist,medium,accession_number,year,artistRole,title,creditLine,inscription,accession_number,tag1,tag2"
        );*/

        'outer: for record in csv::ReaderBuilder::new()
            .has_headers(true)
            .from_path("/terastore/datasets/artwork_data.csv")
            .unwrap()
            .records()
            .into_iter()
        {
            let record = record.unwrap();
            let mut values_str = HashMap::new();
            let mut values_int = HashMap::new();

            for field_name in [
                "year",
                "acquisitionYear",
                "accession_number",
                "artist",
                "artistRole",
                "medium",
                "title",
                "creditLine",
                "inscription",
            ] {
                let field = record.get(fields[field_name]).unwrap();
                if field.is_empty()
                    || (field_name == "title" && (field.contains('[') || field.contains(']')))
                {
                    continue 'outer;
                } else if field_name == "year" || field_name == "acquisitionYear" {
                    let field = field.parse::<i32>().unwrap_or(0);
                    if field < 1000 {
                        continue 'outer;
                    }
                    values_int.insert(field_name.to_string(), field);
                } else {
                    values_str.insert(field_name.to_string(), field.to_string());
                }
            }

            let val = artist_count
                .entry(values_str["artist"].clone())
                .or_insert(0);
            if *val == 3 {
                continue;
            }
            *val += 1;

            match thread_count.entry(values_int["year"]) {
                Entry::Occupied(mut e) => {
                    let messages_per_thread = e.get_mut();
                    if *messages_per_thread == MAX_MESSAGES_PER_THREAD {
                        continue;
                    }
                    *messages_per_thread += 1;
                }
                Entry::Vacant(e) => {
                    if total_threads == MAX_THREADS {
                        continue;
                    }
                    total_threads += 1;
                    e.insert(1);
                }
            }

            total_messages += 1;

            /*println!(
                "{:?},{:?},{:?},{:?},{:?},{:?},{:?},{:?},{:?},{:?}",
                values_str["artist"],
                values_str["medium"],
                values_str["accession_number"],
                values_int["year"],
                values_str["artistRole"],
                values_str["title"],
                values_str["creditLine"],
                values_str["inscription"],
                &values_str["accession_number"][0..1],
                format!(
                    "N{}",
                    &values_str["accession_number"][values_str["accession_number"].len() - 1..]
                )
            );*/

            mail_store
                .mail_import_single(
                    0,
                    jmap_mail::import::JMAPMailImportItem {
                        blob: format!(
                            concat!(
                                "From: {}\nCc: {}\nMessage-ID: <{}>\n",
                                "References: <{}>\nComments: {}\nSubject: [{}]",
                                " Year {}\n\n{}\n{}\n"
                            ),
                            values_str["artist"],
                            values_str["medium"],
                            values_str["accession_number"],
                            values_int["year"],
                            values_str["artistRole"],
                            values_str["title"],
                            values_int["year"],
                            values_str["creditLine"],
                            values_str["inscription"]
                        )
                        .into_bytes()
                        .into(),
                        mailbox_ids: vec![
                            values_int["year"] as MailboxId,
                            values_int["acquisitionYear"] as MailboxId,
                        ],
                        keywords: vec![
                            values_str["medium"].clone().into(),
                            values_str["artistRole"].clone().into(),
                            values_str["accession_number"][0..1].into(),
                            format!(
                                "N{}",
                                &values_str["accession_number"]
                                    [values_str["accession_number"].len() - 1..]
                            )
                            .into(),
                        ],
                        received_at: Some(values_int["year"] as i64),
                    },
                )
                .unwrap();

            if total_messages == MAX_MESSAGES {
                break;
            }
        }
    }

    for thread_id in 0..MAX_THREADS {
        assert!(
            mail_store
                .get_store()
                .get_tag(
                    0,
                    JMAP_MAIL,
                    MessageField::ThreadId.into(),
                    Tag::Id(thread_id as u64)
                )
                .unwrap()
                .is_some(),
            "thread {} not found",
            thread_id
        );
    }

    assert!(
        mail_store
            .get_store()
            .get_tag(
                0,
                JMAP_MAIL,
                MessageField::ThreadId.into(),
                Tag::Id(MAX_THREADS as u64)
            )
            .unwrap()
            .is_none(),
        "thread {} found",
        MAX_THREADS
    );

    test_filter(&mail_store);
}

fn test_filter<'x, T>(mail_store: &'x JMAPLocalStore<T>)
where
    T: Store<'x>,
{
    for (filter, expected_results) in [(
        JMAPFilter::and(vec![
            JMAPFilter::condition(JMAPMailFilterCondition::After(1850)),
            JMAPFilter::condition(JMAPMailFilterCondition::From("george".into())),
        ]),
        vec![
            "N01389", "T10115", "N00618", "N03500", "T01587", "T00397", "N01561", "N05250",
            "N03973", "N04973", "N04057", "N01940", "N01539", "N01612", "N04484", "N01954",
            "N05998", "T02053", "AR00171", "AR00172", "AR00176",
        ],
    )] {
        assert_eq!(
            mail_store
                .mail_query(
                    JMAPQuery {
                        account_id: 0,
                        filter,
                        sort: vec![JMAPComparator::ascending(JMAPMailComparator::Subject)],
                        position: 0,
                        anchor: 0,
                        anchor_offset: 0,
                        limit: 0,
                        calculate_total: true,
                    },
                    false,
                )
                .unwrap()
                .ids
                .into_iter()
                .map(|id| {
                    mail_store
                        .get_headers_rfc(0, id.doc_id)
                        .unwrap()
                        .remove(&mail_parser::HeaderName::MessageId)
                        .unwrap()
                        .unwrap_text()
                })
                .collect::<Vec<Cow<str>>>(),
            expected_results
        );
    }
}
