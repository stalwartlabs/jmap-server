use std::{collections::HashSet, sync::Arc};

use nlp::Language;
use store::{
    field::TokenIterator,
    serialize::LAST_TERM_ID_KEY,
    term_index::{MatchTerm, Term},
    ColumnFamily, JMAPStore, Store, TermId,
};

const NUM_TOKENS: u64 = 10;
const NUM_THREADS: usize = 20;

pub fn term_id<T>(db: JMAPStore<T>)
where
    T: for<'x> Store<'x> + 'static,
{
    // Test concurrent duplicates
    let db = Arc::new(db);

    rayon::ThreadPoolBuilder::new()
        .num_threads(NUM_THREADS)
        .build()
        .unwrap()
        .scope(|s| {
            for _ in 0..NUM_THREADS {
                let t_db = db.clone();
                s.spawn(move |_| {
                    let text = (0..NUM_TOKENS)
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(" ");
                    let mut term_ids = HashSet::new();
                    for term in t_db
                        .get_terms(TokenIterator::new(&text, Language::English, false))
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
                        t_db.db
                            .get::<TermId>(ColumnFamily::Values, LAST_TERM_ID_KEY)
                            .unwrap()
                            .unwrap(),
                        NUM_TOKENS
                    );
                });
            }
        });

    const TEXT: &str = "love loving lovingly loved lovely";

    assert_eq!(
        db.get_terms(TokenIterator::new(TEXT, Language::English, true))
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
