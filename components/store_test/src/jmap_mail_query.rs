use jmap::{
    id::JMAPIdSerialize,
    json::JSONValue,
    request::{GetRequest, QueryRequest},
};
use jmap_mail::mail::{
    get::JMAPMailGet, import::JMAPMailImport, query::JMAPMailQuery, MailProperties, MessageField,
};
use mail_parser::RfcHeader;
use std::{
    collections::{hash_map::Entry, HashMap},
    iter::FromIterator,
    time::Instant,
};
use store::{
    chrono::{SecondsFormat, TimeZone, Utc},
    query::JMAPIdMapFnc,
    AccountId, Collection, Comparator, DocumentId, FieldValue, Filter, Integer, JMAPId, JMAPStore,
    Store, Tag,
};
use store::{query::JMAPStoreQuery, JMAPIdPrefix};

use crate::{
    db_insert_filter_sort::FIELDS, deflate_artwork_data, jmap_mail_get::build_mail_get_arguments,
    jmap_mail_set::delete_email, JMAPComparator, JMAPFilter, StoreCompareWith,
};

const MAX_THREADS: usize = 100;
const MAX_MESSAGES: usize = 1000;
const MAX_MESSAGES_PER_THREAD: usize = 100;

#[derive(Debug, Clone)]
pub enum JMAPMailFilterCondition {
    InMailbox(DocumentId),
    InMailboxOtherThan(Vec<DocumentId>),
    Before(u64),
    After(u64),
    MinSize(usize),
    MaxSize(usize),
    AllInThreadHaveKeyword(String),
    SomeInThreadHaveKeyword(String),
    NoneInThreadHaveKeyword(String),
    HasKeyword(String),
    NotKeyword(String),
    HasAttachment(bool),
    Text(String),
    From(String),
    To(String),
    Cc(String),
    Bcc(String),
    Subject(String),
    Body(String),
    Header((RfcHeader, Option<String>)),
}

#[derive(Debug, Clone)]
pub enum JMAPMailComparator {
    ReceivedAt,
    Size,
    From,
    To,
    Subject,
    SentAt,
    HasKeyword(String),
    AllInThreadHaveKeyword(String),
    SomeInThreadHaveKeyword(String),
}

#[derive(Debug, Clone)]
pub struct MailQueryRequest {
    pub account_id: AccountId,
    pub filter: JMAPFilter<JMAPMailFilterCondition>,
    pub sort: Vec<JMAPComparator<JMAPMailComparator>>,
    pub position: i64,
    pub anchor: Option<JMAPId>,
    pub anchor_offset: i64,
    pub limit: usize,
    pub calculate_total: bool,
    pub collapse_threads: bool,
}

impl From<JMAPComparator<JMAPMailComparator>> for jmap::query::Comparator {
    fn from(comp: JMAPComparator<JMAPMailComparator>) -> Self {
        let mut arguments = HashMap::new();
        let property = match comp.property {
            JMAPMailComparator::ReceivedAt => "receivedAt".to_string(),
            JMAPMailComparator::Size => "size".to_string(),
            JMAPMailComparator::From => "from".to_string(),
            JMAPMailComparator::To => "to".to_string(),
            JMAPMailComparator::Subject => "subject".to_string(),
            JMAPMailComparator::SentAt => "sentAt".to_string(),
            JMAPMailComparator::HasKeyword(keyword) => {
                arguments.insert("keyword".to_string(), keyword.into());
                "hasKeyword".to_string()
            }
            JMAPMailComparator::AllInThreadHaveKeyword(keyword) => {
                arguments.insert("keyword".to_string(), keyword.into());
                "allInThreadHaveKeyword".to_string()
            }
            JMAPMailComparator::SomeInThreadHaveKeyword(keyword) => {
                arguments.insert("keyword".to_string(), keyword.into());
                "someInThreadHaveKeyword".to_string()
            }
        };

        jmap::query::Comparator {
            property,
            is_ascending: comp.is_ascending,
            collation: None,
            arguments,
        }
    }
}

impl From<JMAPMailFilterCondition> for JSONValue {
    fn from(condition: JMAPMailFilterCondition) -> Self {
        let mut json = HashMap::new();
        match condition {
            JMAPMailFilterCondition::InMailbox(mailbox_id) => {
                json.insert(
                    "inMailbox".to_string(),
                    (mailbox_id as JMAPId).to_jmap_string().into(),
                );
            }
            JMAPMailFilterCondition::InMailboxOtherThan(mailbox_ids) => {
                json.insert(
                    "inMailboxOtherThan".to_string(),
                    JSONValue::Array(
                        mailbox_ids
                            .into_iter()
                            .map(|mailbox_id| (mailbox_id as JMAPId).to_jmap_string().into())
                            .collect(),
                    ),
                );
            }
            JMAPMailFilterCondition::Before(timestamp) => {
                json.insert(
                    "before".to_string(),
                    Utc.timestamp_opt(timestamp as i64, 0)
                        .unwrap()
                        .to_rfc3339_opts(SecondsFormat::Secs, true)
                        .into(),
                );
            }
            JMAPMailFilterCondition::After(timestamp) => {
                json.insert(
                    "after".to_string(),
                    Utc.timestamp_opt(timestamp as i64, 0)
                        .unwrap()
                        .to_rfc3339_opts(SecondsFormat::Secs, true)
                        .into(),
                );
            }
            JMAPMailFilterCondition::MinSize(size) => {
                json.insert("minSize".to_string(), size.into());
            }
            JMAPMailFilterCondition::MaxSize(size) => {
                json.insert("maxSize".to_string(), size.into());
            }
            JMAPMailFilterCondition::AllInThreadHaveKeyword(keyword) => {
                json.insert("allInThreadHaveKeyword".to_string(), keyword.into());
            }
            JMAPMailFilterCondition::SomeInThreadHaveKeyword(keyword) => {
                json.insert("someInThreadHaveKeyword".to_string(), keyword.into());
            }
            JMAPMailFilterCondition::NoneInThreadHaveKeyword(keyword) => {
                json.insert("noneInThreadHaveKeyword".to_string(), keyword.into());
            }
            JMAPMailFilterCondition::HasKeyword(keyword) => {
                json.insert("hasKeyword".to_string(), keyword.into());
            }
            JMAPMailFilterCondition::NotKeyword(keyword) => {
                json.insert("notKeyword".to_string(), keyword.into());
            }
            JMAPMailFilterCondition::HasAttachment(has_attachment) => {
                json.insert("hasAttachment".to_string(), has_attachment.into());
            }
            JMAPMailFilterCondition::Text(text) => {
                json.insert("text".to_string(), text.into());
            }
            JMAPMailFilterCondition::From(from) => {
                json.insert("from".to_string(), from.into());
            }
            JMAPMailFilterCondition::To(to) => {
                json.insert("to".to_string(), to.into());
            }
            JMAPMailFilterCondition::Cc(cc) => {
                json.insert("cc".to_string(), cc.into());
            }
            JMAPMailFilterCondition::Bcc(bcc) => {
                json.insert("bcc".to_string(), bcc.into());
            }
            JMAPMailFilterCondition::Subject(subject) => {
                json.insert("subject".to_string(), subject.into());
            }
            JMAPMailFilterCondition::Body(body) => {
                json.insert("body".to_string(), body.into());
            }
            JMAPMailFilterCondition::Header((header, value)) => {
                let mut h = vec![header.to_string().into()];
                if let Some(value) = value {
                    h.push(value.into());
                }
                json.insert("header".to_string(), h.into());
            }
        }
        json.into()
    }
}

impl From<MailQueryRequest> for QueryRequest {
    fn from(request: MailQueryRequest) -> Self {
        QueryRequest {
            account_id: request.account_id,
            filter: request.filter.into(),
            sort: request
                .sort
                .into_iter()
                .map(|c| c.into())
                .collect::<Vec<_>>()
                .into(),
            position: request.position,
            anchor: request.anchor,
            anchor_offset: request.anchor_offset,
            limit: request.limit,
            calculate_total: request.calculate_total,
            arguments: HashMap::from_iter([(
                "collapseThreads".to_string(),
                request.collapse_threads.into(),
            )]),
        }
    }
}

pub fn jmap_mail_query_prepare<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
    let now = Instant::now();
    let mut fields = HashMap::new();
    for (field_num, field) in FIELDS.iter().enumerate() {
        fields.insert(field.to_string(), field_num);
    }

    let mut total_messages = 0;
    let mut total_threads = 0;
    let mut thread_count = HashMap::new();
    let mut artist_count = HashMap::new();

    'outer: for record in csv::ReaderBuilder::new()
        .has_headers(true)
        .from_reader(&deflate_artwork_data()[..])
        .records()
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

        mail_store
            .mail_import_blob(
                account_id,
                format!(
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
                .into_bytes(),
                vec![
                    values_int["year"] as DocumentId,
                    (values_int["acquisitionYear"] + 1000) as DocumentId,
                ],
                vec![
                    Tag::Text(values_str["medium"].clone()),
                    Tag::Text(values_str["artistRole"].clone()),
                    Tag::Text(values_str["accession_number"][0..1].to_string()),
                    Tag::Text(format!(
                        "N{}",
                        &values_str["accession_number"][values_str["accession_number"].len() - 1..]
                    )),
                ],
                Some(values_int["year"] as i64),
            )
            .unwrap();

        if total_messages == MAX_MESSAGES {
            break;
        }
    }
    println!(
        "Imported {} messages in {} ms (single thread).",
        total_messages,
        now.elapsed().as_millis()
    );
}

pub fn jmap_mail_query<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
    for thread_id in 0..MAX_THREADS {
        assert!(
            mail_store
                .get_tag(
                    account_id,
                    Collection::Mail,
                    MessageField::ThreadId.into(),
                    Tag::Id(thread_id as Integer)
                )
                .unwrap()
                .is_some(),
            "thread {} not found",
            thread_id
        );
    }

    assert!(
        mail_store
            .get_tag(
                account_id,
                Collection::Mail,
                MessageField::ThreadId.into(),
                Tag::Id(MAX_THREADS as Integer)
            )
            .unwrap()
            .is_none(),
        "thread {} found",
        MAX_THREADS
    );

    println!("Running JMAP Mail query tests...");
    test_query(mail_store, account_id);

    println!("Running JMAP Mail query options tests...");
    test_query_options(mail_store, account_id);

    println!("Deleting all messages...");
    for message_id in mail_store
        .query::<JMAPIdMapFnc>(JMAPStoreQuery::new(
            account_id,
            Collection::Mail,
            Filter::None,
            Comparator::None,
        ))
        .unwrap()
    {
        delete_email(mail_store, account_id, message_id);
    }
    mail_store.assert_is_empty();
}

fn test_query<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
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
                    RfcHeader::Comments,
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
                    MailQueryRequest {
                        account_id,
                        filter,
                        sort,
                        position: 0,
                        anchor: None,
                        anchor_offset: 0,
                        limit: 0,
                        calculate_total: true,
                        collapse_threads: false
                    }
                    .into()
                )
                .unwrap()
                .eval_unwrap_array("/ids")
                .into_iter()
                .map(|id| get_message_id(mail_store, id.to_jmap_id().unwrap(), account_id))
                .collect::<Vec<String>>(),
            expected_results
        );
    }
}

fn test_query_options<T>(mail_store: &JMAPStore<T>, account_id: AccountId)
where
    T: for<'x> Store<'x> + 'static,
{
    for (mut query, expected_results, expected_results_collapsed) in [
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: 0,
                anchor: None,
                anchor_offset: 0,
                limit: 10,
                calculate_total: true,

                collapse_threads: false,
            },
            vec![
                "N01496", "N01320", "N01321", "N05916", "N00273", "N01453", "N02984", "T08820",
                "N00112", "T00211",
            ],
            vec![
                "N01496", "N01320", "N05916", "N01453", "T08820", "N01046", "N00675", "T08891",
                "T01882", "N04296",
            ],
        ),
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: 10,
                anchor: None,
                anchor_offset: 0,
                limit: 10,
                calculate_total: true,

                collapse_threads: false,
            },
            vec![
                "N01046", "N00675", "T08891", "N00126", "T01882", "N04689", "T00925", "N00121",
                "N04296", "N04297",
            ],
            vec![
                "T08234", "T09417", "N01110", "T08123", "N01039", "T09456", "T08951", "N01273",
                "N00373", "T09547",
            ],
        ),
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: -10,
                anchor: None,
                anchor_offset: 0,
                limit: 0,
                calculate_total: true,

                collapse_threads: false,
            },
            vec![
                "T07236", "P11481", "AR00066", "P77895", "P77896", "P77897", "AR00163", "AR00164",
                "AR00472", "AR00178",
            ],
            vec![
                "P07639", "P07522", "AR00089", "P02949", "T05820", "P11441", "T06971", "P11481",
                "AR00163", "AR00164",
            ],
        ),
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: -20,
                anchor: None,
                anchor_offset: 0,
                limit: 10,
                calculate_total: true,

                collapse_threads: false,
            },
            vec![
                "P20079", "AR00024", "AR00182", "P20048", "P20044", "P20045", "P20046", "T06971",
                "AR00177", "P77935",
            ],
            vec![
                "T00300", "P06033", "T02310", "T02135", "P04006", "P03166", "P01358", "P07133",
                "P03138", "T03562",
            ],
        ),
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: -100000,
                anchor: None,
                anchor_offset: 0,
                limit: 1,
                calculate_total: true,

                collapse_threads: false,
            },
            vec!["N01496"],
            vec!["N01496"],
        ),
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: -1,
                anchor: None,
                anchor_offset: 0,
                limit: 100000,
                calculate_total: true,

                collapse_threads: false,
            },
            vec!["AR00178"],
            vec!["AR00164"],
        ),
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: 0,
                anchor: get_anchor(mail_store, "N01205", account_id),
                anchor_offset: 0,
                limit: 10,
                calculate_total: true,

                collapse_threads: false,
            },
            vec![
                "N01205", "N01976", "T01139", "N01525", "T00176", "N01405", "N02396", "N04885",
                "N01526", "N02134",
            ],
            vec![
                "N01205", "N01526", "T01455", "N01969", "N05250", "N01781", "N00759", "A00057",
                "N03527", "N01558",
            ],
        ),
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: 0,
                anchor: get_anchor(mail_store, "N01205", account_id),
                anchor_offset: 10,
                limit: 10,
                calculate_total: true,

                collapse_threads: false,
            },
            vec![
                "N01933", "N03618", "T03904", "N02398", "N02399", "N02688", "T01455", "N03051",
                "N01500", "N03411",
            ],
            vec![
                "N01559", "N04326", "N06017", "N01553", "N01617", "N01528", "N01539", "T09439",
                "N01593", "N03988",
            ],
        ),
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: 0,
                anchor: get_anchor(mail_store, "N01205", account_id),
                anchor_offset: -10,
                limit: 10,
                calculate_total: true,

                collapse_threads: false,
            },
            vec![
                "N05779", "N04652", "N01534", "A00845", "N03409", "N03410", "N02061", "N02426",
                "N00662", "N01205",
            ],
            vec![
                "N00443", "N02237", "T03025", "N01722", "N01356", "N01800", "T05475", "T01587",
                "N05779", "N01205",
            ],
        ),
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: 0,
                anchor: get_anchor(mail_store, "N01496", account_id),
                anchor_offset: -10,
                limit: 10,
                calculate_total: true,

                collapse_threads: false,
            },
            vec!["N01496"],
            vec!["N01496"],
        ),
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: 0,
                anchor: get_anchor(mail_store, "AR00164", account_id),
                anchor_offset: 10,
                limit: 10,
                calculate_total: true,

                collapse_threads: false,
            },
            vec![],
            vec![],
        ),
        (
            MailQueryRequest {
                account_id,
                filter: JMAPFilter::None,
                sort: vec![
                    JMAPComparator::ascending(JMAPMailComparator::Subject),
                    JMAPComparator::ascending(JMAPMailComparator::From),
                ],
                position: 0,
                anchor: get_anchor(mail_store, "AR00164", account_id),
                anchor_offset: 0,
                limit: 0,
                calculate_total: true,

                collapse_threads: false,
            },
            vec!["AR00164", "AR00472", "AR00178"],
            vec!["AR00164"],
        ),
    ] {
        assert_eq!(
            mail_store
                .mail_query(query.clone().into())
                .unwrap()
                .eval_unwrap_array("/ids")
                .into_iter()
                .map(|id| get_message_id(mail_store, id.to_jmap_id().unwrap(), account_id))
                .collect::<Vec<String>>(),
            expected_results
        );
        query.collapse_threads = true;
        assert_eq!(
            mail_store
                .mail_query(query.into())
                .unwrap()
                .eval_unwrap_array("/ids")
                .into_iter()
                .map(|id| get_message_id(mail_store, id.to_jmap_id().unwrap(), account_id))
                .collect::<Vec<String>>(),
            expected_results_collapsed
        );
    }
}

fn get_anchor<T>(mail_store: &JMAPStore<T>, anchor: &str, account_id: AccountId) -> Option<JMAPId>
where
    T: for<'x> Store<'x> + 'static,
{
    let doc_id = mail_store
        .query::<JMAPIdMapFnc>(JMAPStoreQuery::new(
            account_id,
            Collection::Mail,
            Filter::eq(
                MessageField::MessageIdRef.into(),
                FieldValue::Keyword(anchor.into()),
            ),
            Comparator::None,
        ))
        .unwrap()
        .next()
        .unwrap()
        .get_document_id();

    let thread_id = mail_store
        .get_document_tag_id(
            account_id,
            Collection::Mail,
            doc_id,
            MessageField::ThreadId.into(),
        )
        .unwrap()
        .unwrap();

    JMAPId::from_parts(thread_id, doc_id).into()
}

fn get_message_id<T>(mail_store: &JMAPStore<T>, jmap_id: JMAPId, account_id: AccountId) -> String
where
    T: for<'x> Store<'x> + 'static,
{
    mail_store
        .mail_get(GetRequest {
            account_id,
            ids: vec![jmap_id].into(),
            properties: vec![MailProperties::MessageId.to_string().into()].into(),
            arguments: build_mail_get_arguments(vec![], false, false, false, 100),
        })
        .unwrap()
        .eval_unwrap_string("/list/0/messageId/0")
}
