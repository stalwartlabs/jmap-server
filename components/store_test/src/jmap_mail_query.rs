use std::{
    borrow::Cow,
    collections::{hash_map::Entry, HashMap},
};

use jmap_mail::{
    query::{JMAPMailComparator, JMAPMailFilterCondition, MailboxId},
    JMAPMailStoreGet, JMAPMailStoreImport, JMAPMailStoreQuery, MessageField,
};
use jmap_store::{local_store::JMAPLocalStore, JMAPComparator, JMAPFilter, JMAPQuery, JMAP_MAIL};
use mail_parser::HeaderName;
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
                            (values_int["acquisitionYear"] + 1000) as MailboxId,
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
    for (filter, sort, expected_results) in [
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::After(1850)),
                JMAPFilter::condition(JMAPMailFilterCondition::From("george".into())),
            ]),
            vec![JMAPComparator::ascending(JMAPMailComparator::Subject)],
            vec![
                "N01389", "T10115", "N00618", "N03500", "T01587", "T00397", "N01561", "N05250",
                "N03973", "N04973", "N04057", "N01940", "N01539", "N01612", "N04484", "N01954",
                "N05998", "T02053", "AR00171", "AR00172", "AR00176",
            ],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::InMailbox(1768)),
                JMAPFilter::condition(JMAPMailFilterCondition::Cc("canvas".into())),
            ]),
            vec![JMAPComparator::ascending(JMAPMailComparator::From)],
            vec!["T01882", "N04689", "T00925", "N00121"],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::Subject("study".into())),
                JMAPFilter::condition(JMAPMailFilterCondition::InMailboxOtherThan(vec![
                    1991, 1870, 2011, 1951, 1902, 1808, 1963,
                ])),
            ]),
            vec![JMAPComparator::ascending(JMAPMailComparator::Subject)],
            vec![
                "T10330", "N01744", "N01743", "N04885", "N02688", "N02122", "A00059", "A00058",
                "N02123", "T00651", "T09439", "N05001", "T05848", "T05508",
            ],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::HasKeyword("N0".into())),
                JMAPFilter::not(vec![JMAPFilter::condition(JMAPMailFilterCondition::From(
                    "collins".into(),
                ))]),
                JMAPFilter::condition(JMAPMailFilterCondition::Body("bequeathed".into())),
            ]),
            vec![JMAPComparator::ascending(JMAPMailComparator::Subject)],
            vec![
                "N02640", "A01020", "N01250", "T03430", "N01800", "N00620", "N05250", "N04630",
                "A01040",
            ],
        ),
        (
            JMAPFilter::and(vec![JMAPFilter::condition(
                JMAPMailFilterCondition::NotKeyword("artist".into()),
            )]),
            vec![JMAPComparator::ascending(JMAPMailComparator::Subject)],
            vec!["T08626", "T09334", "T09455", "N01737", "T10965"],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::After(1970)),
                JMAPFilter::condition(JMAPMailFilterCondition::Before(1972)),
                JMAPFilter::condition(JMAPMailFilterCondition::Text("colour".into())),
            ]),
            vec![JMAPComparator::ascending(JMAPMailComparator::From)],
            vec!["T01745", "P01436", "P01437"],
        ),
        (
            JMAPFilter::and(vec![JMAPFilter::condition(JMAPMailFilterCondition::Text(
                "'cats and dogs'".into(),
            ))]),
            vec![JMAPComparator::ascending(JMAPMailComparator::From)],
            vec!["P77623"],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::Header((
                    HeaderName::Comments,
                    Some("attributed".into()),
                ))),
                JMAPFilter::condition(JMAPMailFilterCondition::From("john".into())),
                JMAPFilter::condition(JMAPMailFilterCondition::Cc("oil".into())),
            ]),
            vec![JMAPComparator::ascending(JMAPMailComparator::From)],
            vec!["T10965"],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::AllInThreadHaveKeyword("N".into())),
                JMAPFilter::condition(JMAPMailFilterCondition::Before(1800)),
            ]),
            vec![JMAPComparator::ascending(JMAPMailComparator::From)],
            vec![
                "N01496", "N05916", "N01046", "N00675", "N01320", "N01321", "N00273", "N01453",
                "N02984",
            ],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::NoneInThreadHaveKeyword("N".into())),
                JMAPFilter::condition(JMAPMailFilterCondition::After(1995)),
            ]),
            vec![JMAPComparator::ascending(JMAPMailComparator::From)],
            vec![
                "AR00163", "AR00164", "AR00472", "P11481", "AR00066", "AR00178", "P77895",
                "P77896", "P77897",
            ],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::SomeInThreadHaveKeyword(
                    "Bronze".into(),
                )),
                JMAPFilter::condition(JMAPMailFilterCondition::Before(1878)),
            ]),
            vec![JMAPComparator::ascending(JMAPMailComparator::From)],
            vec![
                "N04326", "N01610", "N02920", "N01587", "T00167", "T00168", "N01554", "N01535",
                "N01536", "N01622", "N01754", "N01594",
            ],
        ),
        // Sorting tests
        (
            JMAPFilter::and(vec![JMAPFilter::condition(
                JMAPMailFilterCondition::Before(1800),
            )]),
            vec![
                JMAPComparator::ascending(JMAPMailComparator::AllInThreadHaveKeyword("N".into())),
                JMAPComparator::ascending(JMAPMailComparator::From),
            ],
            vec![
                "N01496", "N05916", "N01046", "N00675", "N01320", "N01321", "N00273", "N01453",
                "N02984", "T09417", "T01882", "T08820", "N04689", "T08891", "T00986", "N00316",
                "N03544", "N04296", "N04297", "T08234", "N00112", "T00211", "N01497", "N02639",
                "N02640", "T00925", "T11683", "T08269", "D00001", "D00002", "D00046", "N00121",
                "N00126", "T08626",
            ],
        ),
        (
            JMAPFilter::and(vec![JMAPFilter::condition(
                JMAPMailFilterCondition::Before(1800),
            )]),
            vec![
                JMAPComparator::descending(JMAPMailComparator::AllInThreadHaveKeyword("N".into())),
                JMAPComparator::ascending(JMAPMailComparator::From),
            ],
            vec![
                "T09417", "T01882", "T08820", "N04689", "T08891", "T00986", "N00316", "N03544",
                "N04296", "N04297", "T08234", "N00112", "T00211", "N01497", "N02639", "N02640",
                "T00925", "T11683", "T08269", "D00001", "D00002", "D00046", "N00121", "N00126",
                "T08626", "N01496", "N05916", "N01046", "N00675", "N01320", "N01321", "N00273",
                "N01453", "N02984",
            ],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::After(1875)),
                JMAPFilter::condition(JMAPMailFilterCondition::Before(1878)),
            ]),
            vec![
                JMAPComparator::ascending(JMAPMailComparator::SomeInThreadHaveKeyword(
                    "Bronze".into(),
                )),
                JMAPComparator::ascending(JMAPMailComparator::From),
            ],
            vec![
                "N04326", "N01610", "N02920", "N01587", "T00167", "T00168", "N01554", "N01535",
                "N01536", "N01622", "N01754", "N01594", "N01559", "N02123", "N01940", "N03594",
                "N01494", "N04271",
            ],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::After(1875)),
                JMAPFilter::condition(JMAPMailFilterCondition::Before(1878)),
            ]),
            vec![
                JMAPComparator::descending(JMAPMailComparator::SomeInThreadHaveKeyword(
                    "Bronze".into(),
                )),
                JMAPComparator::ascending(JMAPMailComparator::From),
            ],
            vec![
                "N01559", "N02123", "N01940", "N03594", "N01494", "N04271", "N04326", "N01610",
                "N02920", "N01587", "T00167", "T00168", "N01554", "N01535", "N01536", "N01622",
                "N01754", "N01594",
            ],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::After(1786)),
                JMAPFilter::condition(JMAPMailFilterCondition::Before(1840)),
                JMAPFilter::condition(JMAPMailFilterCondition::HasKeyword("T".into())),
            ]),
            vec![
                JMAPComparator::ascending(JMAPMailComparator::HasKeyword("attributed to".into())),
                JMAPComparator::ascending(JMAPMailComparator::From),
            ],
            vec![
                "T09455", "T09334", "T10965", "T08626", "T09417", "T08951", "T01851", "T01852",
                "T08761", "T08123", "T08756", "T10561", "T10562", "T10563", "T00986", "T03424",
                "T03427", "T08234", "T08133", "T06866", "T08897", "T00996", "T00997", "T01095",
                "T03393", "T09456", "T00188", "T02362", "T09065", "T09547", "T10330", "T09187",
                "T03433", "T08635", "T02366", "T03436", "T09150", "T01861", "T09759", "T11683",
                "T02368", "T02369", "T08269", "T01018", "T10066", "T01710", "T01711", "T05764",
            ],
        ),
        (
            JMAPFilter::and(vec![
                JMAPFilter::condition(JMAPMailFilterCondition::After(1786)),
                JMAPFilter::condition(JMAPMailFilterCondition::Before(1840)),
                JMAPFilter::condition(JMAPMailFilterCondition::HasKeyword("T".into())),
            ]),
            vec![
                JMAPComparator::descending(JMAPMailComparator::HasKeyword("attributed to".into())),
                JMAPComparator::ascending(JMAPMailComparator::From),
            ],
            vec![
                "T09417", "T08951", "T01851", "T01852", "T08761", "T08123", "T08756", "T10561",
                "T10562", "T10563", "T00986", "T03424", "T03427", "T08234", "T08133", "T06866",
                "T08897", "T00996", "T00997", "T01095", "T03393", "T09456", "T00188", "T02362",
                "T09065", "T09547", "T10330", "T09187", "T03433", "T08635", "T02366", "T03436",
                "T09150", "T01861", "T09759", "T11683", "T02368", "T02369", "T08269", "T01018",
                "T10066", "T01710", "T01711", "T05764", "T09455", "T09334", "T10965", "T08626",
            ],
        ),
    ] {
        assert_eq!(
            mail_store
                .mail_query(
                    JMAPQuery {
                        account_id: 0,
                        filter,
                        sort,
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
