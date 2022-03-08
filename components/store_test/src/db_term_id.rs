use std::{collections::HashSet, sync::Arc};

use nlp::Language;
use store::{
    field::TokenIterator,
    serialize::LAST_TERM_ID_KEY,
    term_index::{MatchTerm, Term},
    tokio, ColumnFamily, JMAPStore, Store, TermId,
};

const NUM_TOKENS: u64 = 10;
const NUM_THREADS: usize = 20;

pub async fn term_id<T>(db: JMAPStore<T>)
where
    T: for<'x> Store<'x> + 'static,
{
    // Test concurrent duplicates
    let db = Arc::new(db);
    let mut futures = Vec::new();
    for _ in 0..NUM_THREADS {
        let db = db.clone();
        futures.push(tokio::spawn(async move {
            let text = (0..NUM_TOKENS)
                .map(|x| x.to_string())
                .collect::<Vec<_>>()
                .join(" ");
            let mut term_ids = HashSet::new();
            for term in db
                .get_terms(TokenIterator::new(&text, Language::English, false))
                .await
                .unwrap()
            {
                assert!(
                    (0..NUM_TOKENS).contains(&term.id),
                    "{:?} {:?}",
                    term,
                    term_ids
                );
                assert!(!term_ids.contains(&term.id), "{:?} {:?}", term, term_ids);
                term_ids.insert(term.id);
            }
            assert_eq!(term_ids.len(), NUM_TOKENS as usize);
            assert_eq!(
                db.get::<TermId>(ColumnFamily::Values, LAST_TERM_ID_KEY.to_vec())
                    .await
                    .unwrap()
                    .unwrap(),
                NUM_TOKENS
            );
        }));
    }

    for future in futures {
        future.await.unwrap();
    }

    const TEXT: &str = "love loving lovingly loved lovely";

    assert_eq!(
        db.get_terms(TokenIterator::new(TEXT, Language::English, true))
            .await
            .unwrap(),
        vec![
            Term {
                id: 10,
                id_stemmed: 10,
                offset: 0,
                len: 4
            },
            Term {
                id: 11,
                id_stemmed: 10,
                offset: 5,
                len: 6
            },
            Term {
                id: 12,
                id_stemmed: 10,
                offset: 12,
                len: 8
            },
            Term {
                id: 13,
                id_stemmed: 10,
                offset: 21,
                len: 5
            },
            Term {
                id: 14,
                id_stemmed: 10,
                offset: 27,
                len: 6
            }
        ]
    );

    assert_eq!(
        db.get_match_terms(TokenIterator::new(TEXT, Language::English, true))
            .await
            .unwrap()
            .unwrap(),
        vec![
            MatchTerm {
                id: 10,
                id_stemmed: 10
            },
            MatchTerm {
                id: 11,
                id_stemmed: 10
            },
            MatchTerm {
                id: 12,
                id_stemmed: 10
            },
            MatchTerm {
                id: 13,
                id_stemmed: 10
            },
            MatchTerm {
                id: 14,
                id_stemmed: 10
            }
        ]
    );
}
